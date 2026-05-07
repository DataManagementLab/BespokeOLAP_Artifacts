#include "query15.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
#include "db_loader.hpp"
static ThreadPool& pool = get_query_pool();
#include <atomic>
#include <cstring>
#include <immintrin.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// SQL:
/** with revenue (supplier_no, total_revenue) as (
    select
        l_suppkey,
        sum(l_extendedprice * (1 - l_discount))
    from
        lineitem
    where
        l_shipdate >= date '[DATE]'
        and l_shipdate < date '[DATE]' + interval '3' month
    group by
        l_suppkey
)
select
    s_suppkey,
    s_name,
    s_address,
    s_phone,
    total_revenue
from
    supplier,
    revenue
where
    s_suppkey = supplier_no
    and total_revenue = (
        select
            max(total_revenue)
        from
            revenue
    )
order by
    s_suppkey; */

std::vector<std::vector<std::string>> run_q15(Database* db, const Q15Args& args) {
    if (!db) {
        throw std::runtime_error("run_q15: db is null");
    }

    // ── 1. Parse the date argument ────────────────────────────────────────────
    //       DATE is a "YYYY-MM-DD" string.
    auto date_to_days = [](const std::string& s) -> int32_t {
        // Fixed-format parse: YYYY-MM-DD, no heap allocation (tip #13)
        const char* p = s.c_str();
        int y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
        int m = (p[5]-'0')*10 + (p[6]-'0');
        int d = (p[8]-'0')*10 + (p[9]-'0');
        if (m <= 2) { y -= 1; m += 12; }
        int era = (y >= 0 ? y : y - 399) / 400;
        int yoe = y - era * 400;
        int doy = (153 * (m - 3) + 2) / 5 + d - 1;
        int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return static_cast<int32_t>(era * 146097 + doe - 719468);
    };

    // Add N calendar months to an epoch-days value.
    auto add_months = [](int32_t days, int months) -> int32_t {
        // Decode days → (y, m, d)
        int32_t z   = days + 719468;
        int32_t era = (z >= 0 ? z : z - 146096) / 146097;
        int32_t doe = z - era * 146097;
        int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        int32_t y   = yoe + era * 400;
        int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        int32_t mp  = (5 * doy + 2) / 153;
        int32_t dom = doy - (153 * mp + 2) / 5 + 1;
        int32_t mon = mp < 10 ? mp + 3 : mp - 9;
        if (mon <= 2) y += 1;

        // Advance month
        int nm = static_cast<int>(mon) + months;
        int ny = static_cast<int>(y);
        while (nm > 12) { nm -= 12; ny += 1; }
        while (nm <  1) { nm += 12; ny -= 1; }

        // Clamp day to last day of new month
        static const int dim_table[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
        auto is_leap = [](int yr) -> bool {
            return (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
        };
        int dim = dim_table[nm];
        if (nm == 2 && is_leap(ny)) dim = 29;
        int nd = static_cast<int>(dom) > dim ? dim : static_cast<int>(dom);

        // Re-encode
        if (nm <= 2) { ny -= 1; nm += 12; }
        int era2 = (ny >= 0 ? ny : ny - 399) / 400;
        int yoe2 = ny - era2 * 400;
        int doy2 = (153 * (nm - 3) + 2) / 5 + nd - 1;
        int doe2 = yoe2 * 365 + yoe2 / 4 - yoe2 / 100 + doy2;
        return static_cast<int32_t>(era2 * 146097 + doe2 - 719468);
    };

    int32_t date_lo = date_to_days(args.DATE);    // l_shipdate >= DATE
    int32_t date_hi = add_months(date_lo, 3);     // l_shipdate <  DATE + 3 months

    PROFILE_SCOPE("q15_total");

    // ── 2. Build revenue CTE: group by l_suppkey, sum(l_extendedprice*(1-l_discount)) ──
    //
    //   DuckDB plan insight: ~200,000 groups at SF20 (suppkeys in [1, SF*10000]).
    //   Instead of an unordered_map (hash overhead on 120M rows), use a
    //   direct-indexed flat array: revenue[suppkey] = accumulated sum.
    //   At SF20 max suppkey = 200,000 → array is ~1.6 MB, fits well in L3 cache.
    //   Each hot-loop iteration becomes a single array store — zero hash cost.
    //
    const LineitemTable& li = db->lineitem;
    const SupplierTable& supp           = db->supplier;
    const std::vector<int32_t>& supp_by_sk = db->supp_row_by_suppkey;
    const size_t supp_sk_sz = supp_by_sk.size();

    // ── Revenue accumulation in integer (×10000 cents) to avoid FP overhead ─
    // l_extendedprice_int32[i] is extendedprice×100 (int32_t).
    // l_discount_int8[i]       is discount×100       (int8_t, range [0,10]).
    // revenue_cents[sk] += ep_cents * (100 - disc_int)
    //                    = extendedprice * (1 - discount) * 10000   (integer units)
    // Final conversion back to double: revenue_cents[sk] / 10000.0
    //
    // Flat array indexed by suppkey (max 200K at SF=20 → ~1.6 MB in int64).
    const size_t rev_sz = supp_sk_sz;
    std::vector<int64_t> revenue_cents(rev_sz, 0LL);

    const int64_t nrows = li.num_rows;

    TRACE_COUNT("q15_total_lineitem_rows", nrows);

    // ── Binary search on sorted l_shipdate to find the date range ────────────
    // Lineitem is globally sorted by l_shipdate ASC (done in db_loader Phase 2).
    // std::lower_bound / upper_bound reduce the scan from ~120M rows to ~4.5M rows.
    const int32_t* shipdate_data = li.l_shipdate.data();

    int64_t lo_idx, hi_idx;
    {
        // lower_bound for date_lo (first row with l_shipdate >= date_lo)
        const int32_t* lo_ptr = std::lower_bound(shipdate_data, shipdate_data + nrows, date_lo);
        // lower_bound for date_hi (first row with l_shipdate >= date_hi, i.e. past the range)
        const int32_t* hi_ptr = std::lower_bound(lo_ptr, shipdate_data + nrows, date_hi);
        lo_idx = static_cast<int64_t>(lo_ptr - shipdate_data);
        hi_idx = static_cast<int64_t>(hi_ptr - shipdate_data);
    }
    int64_t li_date_pass = hi_idx - lo_idx;
    TRACE_COUNT("q15_li_date_pass", li_date_pass);

    // Raw pointer aliases for tightest inner loop.
    const int32_t* __restrict__ ep_col   = li.l_extendedprice_int32.data();
    const int8_t*  __restrict__ disc_col = li.l_discount_int8.data();
    const int32_t* __restrict__ sk_col   = li.l_suppkey_i32.data();

    const int n_threads   = pool.num_threads;
    const int64_t range   = hi_idx - lo_idx;

    // ── Per-thread local revenue arrays (parallel scatter-add aggregation) ─
    // Strategy: allocate a single flat buffer of agg_threads × rev_sz int64.
    // Use a static thread-safe pre-allocated buffer to avoid per-call allocation.
    // Cap the number of aggregation threads to limit memory: at SF50
    // rev_sz ~500K, so each thread needs ~4MB. Use 8 threads → ~32MB total,
    // which fits comfortably in memory and reduces scatter contention.
    const int agg_threads = std::min(n_threads, 8);

    // Static buffer reused across calls; each thread zeros its own slice
    // in parallel during the parallel_for to avoid a sequential fill cost.
    // Buffer is protected by the single-threaded query dispatch model here.
    static std::vector<int64_t> s_local_revenues;
    const size_t needed = (size_t)agg_threads * rev_sz;
    if (s_local_revenues.size() < needed)
        s_local_revenues.resize(needed); // may leave garbage, cleared in-thread below

    {
    PROFILE_SCOPE("q15_revenue_build_parallel");
    pool.parallel_for([&](int tid, int n_total) {
        if (tid >= agg_threads) return;
        int64_t* __restrict__ local_arr = s_local_revenues.data() + (size_t)tid * rev_sz;

        // Zero this thread's private slice (parallel zeroing avoids sequential fill).
        std::memset(local_arr, 0, rev_sz * sizeof(int64_t));
        TRACE_COUNT("q15_thread_zero_bytes", (long long)(rev_sz * sizeof(int64_t)));

        // This thread's morsel: a contiguous slice of the row range [lo_idx, hi_idx).
        int64_t chunk = (range + agg_threads - 1) / agg_threads;
        int64_t t_lo  = lo_idx + (int64_t)tid * chunk;
        int64_t t_hi  = t_lo + chunk;
        if (t_lo > hi_idx) t_lo = hi_idx;
        if (t_hi > hi_idx) t_hi = hi_idx;
        const int64_t t_range = t_hi - t_lo;
        TRACE_COUNT("q15_thread_rows", t_range);

        // Scatter-add with software prefetch.
        constexpr int64_t PF = 32;

        if (t_range >= PF) {
            for (int64_t i = t_lo; i < t_lo + PF; ++i)
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i])], 1, 1);

            const int64_t pf_end  = t_hi - PF;
            const int64_t pf_end4 = pf_end - 3;
            int64_t i = t_lo;
            for (; i < pf_end4; i += 4) {
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i + PF    ])], 1, 1);
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i + PF + 1])], 1, 1);
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i + PF + 2])], 1, 1);
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i + PF + 3])], 1, 1);
                int32_t sk0 = sk_col[i    ], sk1 = sk_col[i + 1],
                        sk2 = sk_col[i + 2], sk3 = sk_col[i + 3];
                int64_t r0 = static_cast<int64_t>(ep_col[i    ]) * (100 - static_cast<int32_t>(disc_col[i    ]));
                int64_t r1 = static_cast<int64_t>(ep_col[i + 1]) * (100 - static_cast<int32_t>(disc_col[i + 1]));
                int64_t r2 = static_cast<int64_t>(ep_col[i + 2]) * (100 - static_cast<int32_t>(disc_col[i + 2]));
                int64_t r3 = static_cast<int64_t>(ep_col[i + 3]) * (100 - static_cast<int32_t>(disc_col[i + 3]));
                local_arr[static_cast<size_t>(sk0)] += r0;
                local_arr[static_cast<size_t>(sk1)] += r1;
                local_arr[static_cast<size_t>(sk2)] += r2;
                local_arr[static_cast<size_t>(sk3)] += r3;
            }
            for (; i < pf_end; ++i) {
                __builtin_prefetch(&local_arr[static_cast<size_t>(sk_col[i + PF])], 1, 1);
                local_arr[static_cast<size_t>(sk_col[i])] +=
                    static_cast<int64_t>(ep_col[i]) * (100 - static_cast<int32_t>(disc_col[i]));
            }
            for (; i < t_hi; ++i) {
                local_arr[static_cast<size_t>(sk_col[i])] +=
                    static_cast<int64_t>(ep_col[i]) * (100 - static_cast<int32_t>(disc_col[i]));
            }
        } else {
            for (int64_t i = t_lo; i < t_hi; ++i) {
                local_arr[static_cast<size_t>(sk_col[i])] +=
                    static_cast<int64_t>(ep_col[i]) * (100 - static_cast<int32_t>(disc_col[i]));
            }
        }
    });
    } // end q15_revenue_build_parallel

    // ── Parallel merge: each thread reduces its stripe of suppkeys ────────
    // Also compute per-thread local max; global max reduced sequentially at end.
    int64_t* __restrict__ rev_arr = revenue_cents.data();
    // Per-thread max results — static to avoid per-call heap allocation.
    static std::vector<int64_t> s_thread_max;
    if ((int)s_thread_max.size() < n_threads) s_thread_max.assign((size_t)n_threads, 0LL);
    std::fill(s_thread_max.begin(), s_thread_max.begin() + n_threads, 0LL);
    {
    PROFILE_SCOPE("q15_merge_local_revenues");
    pool.parallel_for([&](int tid, int n_total) {
        size_t sk_chunk = (rev_sz + (size_t)n_total - 1) / (size_t)n_total;
        size_t sk_lo    = (size_t)tid * sk_chunk;
        size_t sk_hi    = sk_lo + sk_chunk;
        if (sk_lo > rev_sz) sk_lo = rev_sz;
        if (sk_hi > rev_sz) sk_hi = rev_sz;

        int64_t local_max = 0LL;
        for (size_t sk = sk_lo; sk < sk_hi; ++sk) {
            int64_t sum = 0;
            for (int t = 0; t < agg_threads; ++t)
                sum += s_local_revenues[(size_t)t * rev_sz + sk];
            rev_arr[sk] = sum;
            if (sum > local_max) local_max = sum;
        }
        s_thread_max[(size_t)tid] = local_max;
    });
    } // end q15_merge_local_revenues

    // ── 3. Find max(total_revenue) ───────────────────────────────────────────
    int64_t max_revenue_cents = 0LL;
    int64_t group_count       = 0;
    {
        PROFILE_SCOPE("q15_find_max");
        // Reduce thread-local maxima (tiny loop, sequential is fine).
        for (int t = 0; t < n_threads; ++t)
            if (s_thread_max[(size_t)t] > max_revenue_cents)
                max_revenue_cents = s_thread_max[(size_t)t];
        // Count groups (still need a pass; do it now that rev_arr is ready).
        const int64_t* __restrict__ rv = rev_arr;
        for (size_t sk = 1; sk < rev_sz; ++sk)
            group_count += (rv[sk] > 0);
    }
    TRACE_COUNT("q15_groups_created", group_count);
    TRACE_COUNT("q15_max_revenue_cents",
                static_cast<long long>(max_revenue_cents >= 0LL
                    ? (max_revenue_cents + 50LL) / 100LL
                    : 0LL));

    // ── 4. Collect suppliers whose total_revenue equals the maximum ──────────
    //       Join supplier table via supp_row_by_suppkey (direct-indexed array).
    struct ResultRow {
        int64_t     s_suppkey;
        std::string s_name;
        std::string s_address;
        std::string s_phone;
        double      total_revenue;
    };
    std::vector<ResultRow> result;

    {
    PROFILE_SCOPE("q15_collect_winners");
    for (size_t sk = 1; sk < rev_sz; ++sk) {
        int64_t v = rev_arr[sk];
        if (v != max_revenue_cents) continue;   // exact equality — same accumulation path

        // Look up supplier row via direct index.
        if (sk >= supp_sk_sz) continue;  // should not happen
        int32_t srow = supp_by_sk[sk];
        if (srow < 0) continue;
        size_t sr = static_cast<size_t>(srow);

        result.push_back({
            static_cast<int64_t>(sk),
            supp.s_name[sr],
            supp.s_address[sr],   
            supp.s_phone[sr],
            static_cast<double>(v) / 10000.0  // convert from 1e-4 dollars back to dollars
        });
    }
    } // end q15_collect_winners

    TRACE_COUNT("q15_suppliers_at_max", static_cast<long long>(result.size()));

    // ── 5. Sort by s_suppkey ASC ──────────────────────────────────────────────
    {
        PROFILE_SCOPE("q15_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.s_suppkey < b.s_suppkey;
        });
    }
    TRACE_COUNT("q15_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q15_sort_rows_out", static_cast<long long>(result.size()));

    // ── 6. Format output ──────────────────────────────────────────────────────
    //       total_revenue is formatted to 2 decimal places per TPC-H spec.
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"s_suppkey", "s_name", "s_address", "s_phone", "total_revenue"});

    {
    PROFILE_SCOPE("q15_format_output");
    for (const auto& r : result) {
        // Direct snprintf avoids ostringstream allocation/formatting overhead.
        char buf[32];
        std::snprintf(buf, sizeof(buf), "%.2f", r.total_revenue);
        rows.push_back({
            std::to_string(r.s_suppkey),
            r.s_name,
            r.s_address,
            r.s_phone,
            std::string(buf)
        });
    }
    } // end q15_format_output

    TRACE_COUNT("q15_query_output_rows", static_cast<long long>(rows.size()) - 1); // exclude header

    return rows;
}