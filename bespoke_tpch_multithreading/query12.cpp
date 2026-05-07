#include "query12.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& thread_pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// SQL:
/** select 
    l_shipmode,  
    sum(case  
        when o_orderpriority ='1-URGENT' 
            or o_orderpriority ='2-HIGH' 
        then 1 
        else 0 
    end) as high_line_count, 
    sum(case  
        when o_orderpriority <> '1-URGENT' 
            and o_orderpriority <> '2-HIGH' 
        then 1 
        else 0 
    end) as low_line_count 
from  
    orders,  
    lineitem 
where  
    o_orderkey = l_orderkey 
    and l_shipmode in ('[SHIPMODE1]', '[SHIPMODE2]') 
    and l_commitdate < l_receiptdate 
    and l_shipdate < l_commitdate 
    and l_receiptdate >= date '[DATE]' 
    and l_receiptdate < date '[DATE]' + interval '1' year 
group by  
    l_shipmode 
order by  
    l_shipmode; */

// ── local date helpers (same algorithm as query4.cpp) ────────────────────────

static int32_t q12_date_to_days(const std::string& s) {
    int y = std::stoi(s.substr(0, 4));
    int m = std::stoi(s.substr(5, 2));
    int d = std::stoi(s.substr(8, 2));
    if (m <= 2) { y -= 1; m += 12; }
    int era = (y >= 0 ? y : y - 399) / 400;
    int yoe = y - era * 400;
    int doy = (153 * (m - 3) + 2) / 5 + d - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return static_cast<int32_t>(era * 146097 + doe - 719468);
}

static int32_t q12_add_years(int32_t days, int years) {
    // Decode days -> (y, m, d)
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

    int ny = y + years;
    int nm = static_cast<int>(mon);

    // Clamp day to last day of month (handles Feb 29 -> Feb 28 in non-leap)
    static const int dim_table[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    auto is_leap = [](int yr) {
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
}

std::vector<std::vector<std::string>> run_q12(Database* db, const Q12Args& args) {
    if (!db) {
        throw std::runtime_error("run_q12: db is null");
    }

    PROFILE_SCOPE("q12_total");

    // ── 1. Parse arguments ────────────────────────────────────────────────────
    int32_t date_lo = q12_date_to_days(args.DATE);
    int32_t date_hi = q12_add_years(date_lo, 1);

    // ── 2. Resolve shipmode enum codes ────────────────────────────────────────
    const LineitemTable& li  = db->lineitem;
    // Note: ord (db->orders) is no longer needed — prio_is_high is pre-joined into
    // li_rds_packed.flags bit 2 at load time, eliminating the join in the hot loop.

    auto find_shipmode_code = [&](const std::string& name) -> int {
        if (name == "<<NULL>>") return -1;
        for (size_t i = 0; i < li.shipmode_names.size(); ++i) {
            if (li.shipmode_names[i] == name) return static_cast<int>(i);
        }
        return -1;
    };

    int sm1_code = find_shipmode_code(args.SHIPMODE1);
    int sm2_code = find_shipmode_code(args.SHIPMODE2);

    if (sm1_code < 0 && sm2_code < 0) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"l_shipmode", "high_line_count", "low_line_count"});
        return rows;
    }

    // ── 3 & 4. Setup compact lookup tables ────────────────────────────────────
    // Priority codes are no longer resolved at query time — prio_is_high is
    // pre-joined into li_rds_packed.flags bit 2 at load time.
    size_t num_sm = li.shipmode_names.size();

    uint8_t sm_in_list[256] = {};
    if (sm1_code >= 0 && static_cast<size_t>(sm1_code) < num_sm)
        sm_in_list[static_cast<uint8_t>(sm1_code)] = 1;
    if (sm2_code >= 0 && static_cast<size_t>(sm2_code) < num_sm)
        sm_in_list[static_cast<uint8_t>(sm2_code)] = 1;

    // ── 5. Binary-search rdate range in the compact rdate-sorted arrays ───────
    //
    // KEY OPTIMIZATION: Instead of scattering reads through rdate_sorted_idx into
    // the main (l_shipdate-sorted) column arrays, we use the precomputed compact
    // arrays (li_rds_*) which store data in receiptdate-sorted order.
    // This turns all hot-loop accesses into sequential reads, eliminating the
    // cache-miss penalty from the scatter pattern.
    //
    // Binary search uses li_rds_receiptdate (sequential, rdate-sorted) to find
    // the range of rows satisfying l_receiptdate in [date_lo, date_hi).

    const int32_t* rds_rd  = li.li_rds_receiptdate.data();

    const size_t rdate_n = li.li_rds_receiptdate.size();

    // Binary search for rdate range [date_lo, date_hi).
    size_t range_lo, range_hi;
    {
        size_t lo = 0, hi = rdate_n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (rds_rd[mid] < date_lo) lo = mid + 1;
            else hi = mid;
        }
        range_lo = lo;
    }
    {
        size_t lo = range_lo, hi = rdate_n;
        while (lo < hi) {
            size_t mid = lo + (hi - lo) / 2;
            if (rds_rd[mid] < date_hi) lo = mid + 1;
            else hi = mid;
        }
        range_hi = lo;
    }

    int64_t rdate_range_rows = static_cast<int64_t>(range_hi - range_lo);
    TRACE_COUNT("q12_total_lineitem_rows", li.num_rows);
    TRACE_COUNT("q12_rdate_range_rows",    rdate_range_rows);

    // ── 6. Parallel aggregation loop with per-thread local accumulators ───────
    //
    // Uses li_rds_sm_flags: compact 2-byte array = (shipmode<<8)|flags in rdate order.
    // flags: bit0=clt_rd, bit1=slt_cd, bit2=prio_is_high (pre-joined from orders).
    // At 2 bytes/row, a 64-byte cache line covers 32 rows.
    // No random memory accesses: all data is from sequential/L1 reads.
    //
    // Each thread processes a disjoint range of rows, accumulates into its own
    // local high_counts/low_counts arrays (no locks), then results are merged.

    const int n_threads = thread_pool.num_threads;
    const size_t total_rows = range_hi - range_lo;
    const uint16_t* __restrict__ sm_flags_base = li.li_rds_sm_flags.data();

    // Per-thread accumulators: aligned to avoid false sharing between threads.
    // Each ThreadAccum holds 256 high + 256 low int64 counters = 4KB.
    struct alignas(64) ThreadAccum {
        int64_t high[256];
        int64_t low[256];
        int64_t rows_emitted;
        ThreadAccum() {
            std::memset(high, 0, sizeof(high));
            std::memset(low, 0, sizeof(low));
            rows_emitted = 0;
        }
    };
    std::vector<ThreadAccum> accums(n_threads);

    {
        PROFILE_SCOPE("q12_agg_loop_parallel");

        thread_pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q12_agg_worker");

            // Partition [range_lo, range_hi) evenly across threads.
            size_t chunk = (total_rows + (size_t)n_thr - 1) / (size_t)n_thr;
            size_t t_lo  = range_lo + (size_t)tid * chunk;
            size_t t_hi  = t_lo + chunk;
            if (t_lo > range_hi) t_lo = range_hi;
            if (t_hi > range_hi) t_hi = range_hi;

            ThreadAccum& acc = accums[tid];
            int64_t* __restrict__ high_local = acc.high;
            int64_t* __restrict__ low_local  = acc.low;
            int64_t local_emitted = 0;

            const uint16_t* __restrict__ sm_flags = sm_flags_base;

            // Process 8 rows at a time for maximum ILP.
            // 8 x 2 bytes = 16 bytes, one quarter of a cache line.
            size_t k = t_lo;
            const size_t pack_end8 = t_lo + ((t_hi - t_lo) & ~size_t(7));

            for (; k < pack_end8; k += 8) {
                uint16_t v0 = sm_flags[k+0]; uint8_t sm0 = static_cast<uint8_t>(v0 >> 8); uint8_t f0 = static_cast<uint8_t>(v0);
                uint16_t v1 = sm_flags[k+1]; uint8_t sm1 = static_cast<uint8_t>(v1 >> 8); uint8_t f1 = static_cast<uint8_t>(v1);
                uint16_t v2 = sm_flags[k+2]; uint8_t sm2 = static_cast<uint8_t>(v2 >> 8); uint8_t f2 = static_cast<uint8_t>(v2);
                uint16_t v3 = sm_flags[k+3]; uint8_t sm3 = static_cast<uint8_t>(v3 >> 8); uint8_t f3 = static_cast<uint8_t>(v3);
                uint16_t v4 = sm_flags[k+4]; uint8_t sm4 = static_cast<uint8_t>(v4 >> 8); uint8_t f4 = static_cast<uint8_t>(v4);
                uint16_t v5 = sm_flags[k+5]; uint8_t sm5 = static_cast<uint8_t>(v5 >> 8); uint8_t f5 = static_cast<uint8_t>(v5);
                uint16_t v6 = sm_flags[k+6]; uint8_t sm6 = static_cast<uint8_t>(v6 >> 8); uint8_t f6 = static_cast<uint8_t>(v6);
                uint16_t v7 = sm_flags[k+7]; uint8_t sm7 = static_cast<uint8_t>(v7 >> 8); uint8_t f7 = static_cast<uint8_t>(v7);

                // Filter: shipmode in list AND clt_rd AND slt_cd (bits 0,1).
                int pass0 = sm_in_list[sm0] & ((f0 & 3u) == 3u);
                int pass1 = sm_in_list[sm1] & ((f1 & 3u) == 3u);
                int pass2 = sm_in_list[sm2] & ((f2 & 3u) == 3u);
                int pass3 = sm_in_list[sm3] & ((f3 & 3u) == 3u);
                int pass4 = sm_in_list[sm4] & ((f4 & 3u) == 3u);
                int pass5 = sm_in_list[sm5] & ((f5 & 3u) == 3u);
                int pass6 = sm_in_list[sm6] & ((f6 & 3u) == 3u);
                int pass7 = sm_in_list[sm7] & ((f7 & 3u) == 3u);

                local_emitted += pass0+pass1+pass2+pass3+pass4+pass5+pass6+pass7;

                // Branchless accumulation: bit 2 = prio_is_high (pre-joined).
                int h0 = (f0>>2)&1; int h1 = (f1>>2)&1;
                int h2 = (f2>>2)&1; int h3 = (f3>>2)&1;
                int h4 = (f4>>2)&1; int h5 = (f5>>2)&1;
                int h6 = (f6>>2)&1; int h7 = (f7>>2)&1;

                high_local[sm0] += pass0 & h0;  low_local[sm0] += pass0 & (h0^1);
                high_local[sm1] += pass1 & h1;  low_local[sm1] += pass1 & (h1^1);
                high_local[sm2] += pass2 & h2;  low_local[sm2] += pass2 & (h2^1);
                high_local[sm3] += pass3 & h3;  low_local[sm3] += pass3 & (h3^1);
                high_local[sm4] += pass4 & h4;  low_local[sm4] += pass4 & (h4^1);
                high_local[sm5] += pass5 & h5;  low_local[sm5] += pass5 & (h5^1);
                high_local[sm6] += pass6 & h6;  low_local[sm6] += pass6 & (h6^1);
                high_local[sm7] += pass7 & h7;  low_local[sm7] += pass7 & (h7^1);
            }
            // Tail: process remaining rows (0-7).
            for (; k < t_hi; ++k) {
                uint16_t v    = sm_flags[k];
                uint8_t sm    = static_cast<uint8_t>(v >> 8);
                uint8_t flags = static_cast<uint8_t>(v);
                if (!sm_in_list[sm] | ((flags & 3u) != 3u)) continue;
                ++local_emitted;
                int is_high = (flags >> 2) & 1;
                high_local[sm] += is_high;
                low_local[sm]  += (is_high ^ 1);
            }
            acc.rows_emitted = local_emitted;
            TRACE_COUNT("q12_worker_rows_emitted", local_emitted);
        });

    } // end q12_agg_loop_parallel

    // ── Merge per-thread accumulators into final arrays ───────────────────────
    int64_t high_counts_raw[256] = {};
    int64_t low_counts_raw[256]  = {};
    int64_t li_rows_emitted = 0;

    {
        PROFILE_SCOPE("q12_merge");
        for (int t = 0; t < n_threads; ++t) {
            li_rows_emitted += accums[t].rows_emitted;
            for (int i = 0; i < 256; ++i) {
                high_counts_raw[i] += accums[t].high[i];
                low_counts_raw[i]  += accums[t].low[i];
            }
        }
    }

    TRACE_COUNT("q12_li_rows_emitted", li_rows_emitted);

    // ── 7. Collect and sort result rows ───────────────────────────────────────
    struct ResultRow {
        std::string shipmode_name;
        int64_t     high_line_count;
        int64_t     low_line_count;
    };
    std::vector<ResultRow> result;
    result.reserve(2);

    for (size_t i = 0; i < num_sm; ++i) {
        if (!sm_in_list[static_cast<uint8_t>(i)]) continue;
        result.push_back({
            li.shipmode_names[i],
            high_counts_raw[i],
            low_counts_raw[i]
        });
    }

    {
        PROFILE_SCOPE("q12_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.shipmode_name < b.shipmode_name;
        });
    }
    TRACE_COUNT("q12_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q12_sort_rows_out", static_cast<long long>(result.size()));

    // ── 8. Format output ──────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"l_shipmode", "high_line_count", "low_line_count"});

    {
        PROFILE_SCOPE("q12_format_output");
        for (const auto& r : result) {
            rows.push_back({
                r.shipmode_name,
                std::to_string(r.high_line_count),
                std::to_string(r.low_line_count)
            });
        }
    }
    TRACE_COUNT("q12_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}