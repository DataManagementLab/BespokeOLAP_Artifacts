#include "query22.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

// SQL:
/** select
    cntrycode,
    count(*) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        substring(c_phone from 1 for 2) as cntrycode,
        c_acctbal
    from
        customer
    where
        substring(c_phone from 1 for 2) in ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
        and c_acctbal > (
            select
                avg(c_acctbal)
            from
                customer
            where
                c_acctbal > 0.00
                and substring (c_phone from 1 for 2) in ('[I1]','[I2]','[I3]','[I4]','[I5]','[I6]','[I7]')
        )
        and not exists (
            select *
            from
                orders
            where
                o_custkey = c_custkey
        )
    ) as custsale
group by
    cntrycode
order by
    cntrycode; */

std::vector<std::vector<std::string>> run_q22(Database* db, const Q22Args& args) {
    if (!db) {
        throw std::runtime_error("run_q22: db is null");
    }

    PROFILE_SCOPE("q22_total");

    // ── 1. Build the set of country codes as packed uint16 values ────────────
    const std::string* raw_codes[7] = {
        &args.I1, &args.I2, &args.I3, &args.I4, &args.I5, &args.I6, &args.I7
    };

    int num_codes = 0;
    std::array<uint16_t, 7> code_u16{};
    std::array<std::string, 7> code_str;
    for (int k = 0; k < 7; ++k) {
        const auto& c = *raw_codes[k];
        if (c == "<<NULL>>") continue;
        if (c.size() >= 2) {
            code_u16[num_codes] = static_cast<uint16_t>(
                static_cast<uint8_t>(c[0]) | (static_cast<uint16_t>(static_cast<uint8_t>(c[1])) << 8));
            code_str[num_codes] = c;
            ++num_codes;
        } else if (!c.empty()) {
            code_u16[num_codes] = static_cast<uint8_t>(c[0]);
            code_str[num_codes] = c;
            ++num_codes;
        }
    }

    TRACE_COUNT("q22_total_customer_rows", db->customer.num_rows);
    TRACE_COUNT("q22_total_order_rows",    db->orders.num_rows);
    TRACE_COUNT("q22_num_country_codes",   static_cast<long long>(num_codes));

    // ── 2. Use cust_by_phone_cc CSR to access only matching rows ─────────────
    //       Companion arrays cust_by_pcc_bal[] and cust_by_pcc_ho[] store
    //       c_acctbal and c_has_order in CSR order for cache-friendly sequential
    //       access during both passes.
    //
    //       Pass A (avg): only 840K × 8B = 6.7 MB (7 sequential code blocks).
    //       Pass B (agg): only 840K × 9B = 7.5 MB (same blocks from L3 cache).
    //       Total: ~14 MB (vs. original 54+ MB).  ~4× bandwidth reduction.
    //
    //       The 7 code blocks together fit in L3 cache, so Pass B hits L3.

    const CsrIndex& csr        = db->cust_by_phone_cc;
    const size_t    offsets_sz = csr.offsets.size();
    const double*   __restrict__ pcc_bal = db->cust_by_pcc_bal.data();
    const uint8_t*  __restrict__ pcc_ho  = db->cust_by_pcc_ho.data();

    // Resolve CSR ranges for each code upfront.
    struct CodeRange { int64_t lo, hi; };
    CodeRange ranges[7];
    int valid_n = 0;

    {
    PROFILE_SCOPE("q22_resolve_ranges");
    for (int k = 0; k < num_codes; ++k) {
        const size_t key = static_cast<size_t>(code_u16[k]);
        if (key + 1 >= offsets_sz) continue;
        const int64_t lo = static_cast<int64_t>(csr.offsets[key]);
        const int64_t hi = static_cast<int64_t>(csr.offsets[key + 1]);
        if (lo < hi) {
            ranges[valid_n++] = { lo, hi };
        }
    }
    }
    TRACE_COUNT("q22_valid_codes", valid_n);

    // ── Pass A: compute avg_acctbal ───────────────────────────────────────────
    //            Parallel: each thread processes disjoint sub-ranges.
    //            Thread-local accumulators; sequential merge after.
    const int n_threads = pool.num_threads;

    // Per-thread partial sums/counts for Pass A
    std::vector<double>  th_avg_sum(n_threads, 0.0);
    std::vector<int64_t> th_avg_cnt(n_threads, 0);

    // Build a flat list of (lo, hi) for all valid ranges so we can distribute evenly.
    // Total work units: sum of all range lengths. We partition the flat range set
    // across threads using morsel-driven splitting within each code's range.

    {
    PROFILE_SCOPE("q22_avg_pass");
    // Compute total rows across all valid ranges for even distribution
    int64_t total_avg_rows = 0;
    for (int s = 0; s < valid_n; ++s)
        total_avg_rows += ranges[s].hi - ranges[s].lo;
    TRACE_COUNT("q22_avg_total_rows", total_avg_rows);

    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q22_avg_pass_worker");
        double  local_sum = 0.0;
        int64_t local_cnt = 0;

        // Distribute work by assigning each thread a contiguous chunk of the
        // total rows (across all valid ranges, treated as one flat array).
        // For each range, compute the thread's sub-range within it.
        int64_t rows_per_thread = (total_avg_rows + nt - 1) / nt;
        int64_t my_start = static_cast<int64_t>(tid) * rows_per_thread;
        int64_t my_end   = my_start + rows_per_thread;
        if (my_end > total_avg_rows) my_end = total_avg_rows;

        // Walk through ranges, skipping those that don't overlap [my_start, my_end)
        int64_t offset = 0;
        for (int s = 0; s < valid_n && offset < my_end; ++s) {
            const int64_t rlen = ranges[s].hi - ranges[s].lo;
            const int64_t r_start = offset;
            const int64_t r_end   = offset + rlen;

            // Intersection of [my_start, my_end) with [r_start, r_end)
            const int64_t lo = (my_start > r_start) ? my_start : r_start;
            const int64_t hi = (my_end   < r_end  ) ? my_end   : r_end;

            if (lo < hi) {
                const int64_t base = ranges[s].lo + (lo - r_start);
                const double* __restrict__ bp = pcc_bal + base;
                const int64_t cnt = hi - lo;
                for (int64_t j = 0; j < cnt; ++j) {
                    const double bal = bp[j];
                    local_sum += (bal > 0.0) ? bal : 0.0;
                    local_cnt += (bal > 0.0) ? 1   : 0;
                }
            }
            offset = r_end;
        }

        th_avg_sum[tid] = local_sum;
        th_avg_cnt[tid] = local_cnt;
    });
    }

    // Merge Pass A results
    double  avg_sum = 0.0;
    int64_t avg_cnt = 0;
    for (int t = 0; t < n_threads; ++t) {
        avg_sum += th_avg_sum[t];
        avg_cnt += th_avg_cnt[t];
    }
    TRACE_COUNT("q22_avg_cnt", avg_cnt);
    const double avg_acctbal = (avg_cnt > 0) ? (avg_sum / static_cast<double>(avg_cnt)) : 0.0;
    TRACE_COUNT("q22_avg_acctbal_cents", static_cast<long long>(avg_acctbal * 100.0 + 0.5));

    // ── Pass B: filter + aggregate ────────────────────────────────────────────
    //            Parallel: per-thread local accumulators, then merge.
    //            Each thread writes to its own agg_cnt/agg_sum slice (no locks).
    //
    // Thread-local accumulators: th_agg_cnt[t][s], th_agg_sum[t][s]
    // Use flat arrays: th_agg_cnt[t*7 + s], padded to 64B cache lines to avoid
    // false sharing. Each thread owns 7 int64 + 7 double = 112 bytes → 2 cache lines.
    // Pad to 128 bytes (2×64B) per thread.
    struct alignas(64) ThreadAgg {
        int64_t cnt[7];          // 56 bytes
        int64_t bal_pass;        // 8 bytes  → 64 bytes (1 cache line)
        double  sum[7];          // 56 bytes
        int64_t noord_pass;      // 8 bytes  → 64 bytes (2 cache lines)
        int64_t match_rows;      // 8 bytes
        int64_t _pad[7];         // 56 bytes → 128 bytes total (2 cache lines)
    };
    std::vector<ThreadAgg> th_agg(n_threads);
    for (auto& ta : th_agg) {
        for (int s = 0; s < 7; ++s) { ta.cnt[s] = 0; ta.sum[s] = 0.0; }
        ta.bal_pass = 0; ta.noord_pass = 0; ta.match_rows = 0;
    }

    {
    PROFILE_SCOPE("q22_main_pass");
    // Compute total rows for Pass B (same as Pass A)
    int64_t total_b_rows = 0;
    for (int s = 0; s < valid_n; ++s)
        total_b_rows += ranges[s].hi - ranges[s].lo;

    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q22_main_pass_worker");
        ThreadAgg& ta = th_agg[tid];

        int64_t rows_per_thread = (total_b_rows + nt - 1) / nt;
        int64_t my_start = static_cast<int64_t>(tid) * rows_per_thread;
        int64_t my_end   = my_start + rows_per_thread;
        if (my_end > total_b_rows) my_end = total_b_rows;

        int64_t offset = 0;
        for (int s = 0; s < valid_n && offset < my_end; ++s) {
            const int64_t rlen = ranges[s].hi - ranges[s].lo;
            const int64_t r_start = offset;
            const int64_t r_end   = offset + rlen;

            const int64_t lo = (my_start > r_start) ? my_start : r_start;
            const int64_t hi = (my_end   < r_end  ) ? my_end   : r_end;

            if (lo < hi) {
                const int64_t base = ranges[s].lo + (lo - r_start);
                const double*  __restrict__ bp = pcc_bal + base;
                const uint8_t* __restrict__ hp = pcc_ho  + base;
                const int64_t cnt = hi - lo;
                ta.match_rows += cnt;

                for (int64_t j = 0; j < cnt; ++j) {
                    const double bal = bp[j];
                    if (bal <= avg_acctbal) continue;
                    ++ta.bal_pass;

                    if (__builtin_expect(hp[j] != 0, 1)) continue;
                    ++ta.noord_pass;

                    ta.cnt[s] += 1;
                    ta.sum[s] += bal;
                }
            }
            offset = r_end;
        }
    });
    }

    // Merge Pass B results
    std::array<int64_t, 7> agg_cnt{};
    std::array<double,  7> agg_sum{};
    agg_cnt.fill(0);
    agg_sum.fill(0.0);
    int64_t total_match_rows = 0;
    int64_t main_bal_pass    = 0;
    int64_t main_noord_pass  = 0;
    for (int t = 0; t < n_threads; ++t) {
        for (int s = 0; s < valid_n; ++s) {
            agg_cnt[s] += th_agg[t].cnt[s];
            agg_sum[s] += th_agg[t].sum[s];
        }
        total_match_rows += th_agg[t].match_rows;
        main_bal_pass    += th_agg[t].bal_pass;
        main_noord_pass  += th_agg[t].noord_pass;
    }

    TRACE_COUNT("q22_total_match_rows", total_match_rows);
    TRACE_COUNT("q22_main_bal_pass",    main_bal_pass);
    TRACE_COUNT("q22_main_noord_pass",  main_noord_pass);

    // ── 4. Collect results, sort by cntrycode ASC ────────────────────────────
    //    NOTE: valid_n maps to codes in the order they were resolved.
    //    code_str[k] is the string for code_u16[k].
    //    ranges[s] corresponds to code index s (original order from code_u16[]).
    //    We need to emit (code_str[k], agg_cnt[s], agg_sum[s]) for s=0..valid_n-1
    //    where s corresponds to the k-th code that had valid rows.
    //
    //    Recompute the mapping: ranges are built in the same k order.
    struct ResultRow { std::string cc; int64_t cnt; double sum; };
    std::vector<ResultRow> result;
    result.reserve(num_codes);
    int valid_idx = 0;
    for (int k = 0; k < num_codes; ++k) {
        const size_t key = static_cast<size_t>(code_u16[k]);
        if (key + 1 >= offsets_sz) continue;
        const int64_t lo = static_cast<int64_t>(csr.offsets[key]);
        const int64_t hi = static_cast<int64_t>(csr.offsets[key + 1]);
        if (lo >= hi) continue;
        if (agg_cnt[valid_idx] > 0) {
            result.push_back({code_str[k], agg_cnt[valid_idx], agg_sum[valid_idx]});
        }
        ++valid_idx;
    }

    TRACE_COUNT("q22_distinct_codes_in_result", static_cast<long long>(result.size()));

    {
    PROFILE_SCOPE("q22_sort");
    std::sort(result.begin(), result.end(),
        [](const ResultRow& a, const ResultRow& b) { return a.cc < b.cc; });
    }

    // ── 5. Format output ──────────────────────────────────────────────────────
    auto fmt_money = [](double v) -> std::string {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << v;
        return oss.str();
    };

    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"cntrycode", "numcust", "totacctbal"});
    for (const auto& r : result) {
        rows.push_back({r.cc, std::to_string(r.cnt), fmt_money(r.sum)});
    }
    TRACE_COUNT("q22_query_output_rows", static_cast<long long>(rows.size()) - 1);
    return rows;
}