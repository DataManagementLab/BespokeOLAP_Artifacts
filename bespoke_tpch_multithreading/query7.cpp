#include "query7.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// SQL:
/** select
    supp_nation,
    cust_nation,
    l_year, sum(volume) as revenue
from (
    select
        n1.n_name as supp_nation,
        n2.n_name as cust_nation,
        extract(year from l_shipdate) as l_year,
        l_extendedprice * (1 - l_discount) as volume
    from
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2
    where
        s_suppkey = l_suppkey
        and o_orderkey = l_orderkey
        and c_custkey = o_custkey
        and s_nationkey = n1.n_nationkey
        and c_nationkey = n2.n_nationkey
        and (
            (n1.n_name = '[NATION1]' and n2.n_name = '[NATION2]')
            or (n1.n_name = '[NATION2]' and n2.n_name = '[NATION1]')
        )
        and l_shipdate between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year;  */

std::vector<std::vector<std::string>> run_q7(Database* db, const Q7Args& args) {
    using std::vector;

    if (!db) {
        throw std::runtime_error("run_q7: db is null");
    }

    PROFILE_SCOPE("q7_total");

    // ── 1. Date helpers ──────────────────────────────────────────────────────
    auto date_to_days = [](const std::string& s) -> int32_t {
        int y = std::stoi(s.substr(0, 4));
        int m = std::stoi(s.substr(5, 2));
        int d = std::stoi(s.substr(8, 2));
        if (m <= 2) { y -= 1; m += 12; }
        int era = (y >= 0 ? y : y - 399) / 400;
        int yoe = y - era * 400;
        int doy = (153 * (m - 3) + 2) / 5 + d - 1;
        int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        return static_cast<int32_t>(era * 146097 + doe - 719468);
    };

    // ── 2. Parse / resolve arguments ────────────────────────────────────────
    const std::string& nation1_name = args.NATION1;
    const std::string& nation2_name = args.NATION2;

    int32_t date_lo = date_to_days("1995-01-01");
    int32_t date_hi = date_to_days("1996-12-31");

    const NationTable& nat = db->nation;

    auto it1 = nat.name_to_nationkey.find(nation1_name);
    auto it2 = nat.name_to_nationkey.find(nation2_name);

    if (it1 == nat.name_to_nationkey.end() || it2 == nat.name_to_nationkey.end()) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"supp_nation", "cust_nation", "l_year", "revenue"});
        return rows;
    }

    int32_t nk1 = it1->second;
    int32_t nk2 = it2->second;

    // ── 3. Table references ──────────────────────────────────────────────────
    const LineitemTable& li   = db->lineitem;

    // ── 4. Scan lineitems in the shipdate range ──────────────────────────────
    int64_t li_lo = 0, li_hi = 0;
    {
        PROFILE_SCOPE("q7_scan_bisect");
        auto lo_it = std::lower_bound(li.l_shipdate.begin(), li.l_shipdate.end(), date_lo);
        auto hi_it = std::upper_bound(li.l_shipdate.begin(), li.l_shipdate.end(), date_hi);
        li_lo = static_cast<int64_t>(lo_it - li.l_shipdate.begin());
        li_hi = static_cast<int64_t>(hi_it - li.l_shipdate.begin());
    }

    int64_t total_li_rows    = static_cast<int64_t>(li.l_shipdate.size());
    int64_t li_rows_in_range = li_hi - li_lo;
    TRACE_COUNT("q7_total_lineitem_rows",  total_li_rows);
    TRACE_COUNT("q7_li_rows_in_range",     li_rows_in_range);
    TRACE_COUNT("q7_li_rows_filtered_out", total_li_rows - li_rows_in_range);

    // Cast nation keys to uint8 for fast comparison
    const uint8_t nk1_u8 = static_cast<uint8_t>(nk1);
    const uint8_t nk2_u8 = static_cast<uint8_t>(nk2);

    // Precomputed boundary for fast year extraction (1996-01-01 in days)
    const int32_t date_1996_start = date_to_days("1996-01-01");

    // ── 5. Aggregation buckets ───────────────────────────────────────────────
    // [pair_idx][year_idx]:
    //   pair 0: supp=nk1, cust=nk2;  pair 1: supp=nk2, cust=nk1
    //   year 0: 1995,                  year 1: 1996
    double revenue[2][2] = {{0.0, 0.0}, {0.0, 0.0}};

    int64_t li_pass         = 0;
    int64_t li_rows_emitted = 0;

    {
        PROFILE_SCOPE("q7_agg_loop");

        // ── Single-pass approach using pre-joined columns ─────────────────────
        //
        // All optimizations applied:
        //
        // 1. li_suppkey_nationkey_u8[i] = s_nationkey for lineitem row i.
        //
        // 2. li_cust_nationkey_u8[i] = c_nationkey for lineitem row i.
        //
        // 3. Single-pass: no candidates vector, no 35MB write+read.
        //
        // 4. li_suppnk_custnk_u16[i] = (suppnk << 8) | custnk packed into uint16_t.
        //    One 64B cache line covers 32 rows of combined filter data
        //    vs. 2 separate cache lines (one per array) for 64 rows each.
        //    Halves the cache-line pressure for the filter step.
        //
        // 5. combined_filter[65536] table: fuses suppnk+custnk → pair_idx in one lookup.
        //    65536-entry uint8 table (64KB) fits in L2. After warmup, single L2-hit
        //    replaces 2 separate comparisons + a conditional expression.

        // Build combined filter table: combined_filter[(suppnk << 8) | custnk] = pair_idx (0,1,0xFF)
        // 64KB on stack; fits in L2 cache. Stack allocation avoids static-storage issues.
        // Only 2 entries are non-0xFF: the two valid shipping-lane combinations.
        // Heap-allocated so all threads can share it without stack-size concerns.
        alignas(64) static uint8_t combined_filter[65536];
        {
            PROFILE_SCOPE("q7_build_combined_filter");
            __builtin_memset(combined_filter, 0xFF, sizeof(combined_filter));
            // pair 0: supp_nation=nk1, cust_nation=nk2
            combined_filter[(static_cast<unsigned>(nk1_u8) << 8) |
                             static_cast<unsigned>(nk2_u8)] = 0;
            // pair 1: supp_nation=nk2, cust_nation=nk1
            combined_filter[(static_cast<unsigned>(nk2_u8) << 8) |
                             static_cast<unsigned>(nk1_u8)] = 1;
        }

        // Use packed uint16_t column: (suppnk << 8) | custnk.
        // One cache-line covers 32 rows; reduces memory traffic for the filter step.
        const uint16_t* li_packed_nk = li.li_suppnk_custnk_u16.data();

        const int32_t* li_shipdate_data = li.l_shipdate.data();
        const double*  li_ep_data       = li.l_extendedprice.data();
        const double*  li_disc_data     = li.l_discount.data();

        // ── Parallel scan: partition [li_lo, li_hi) evenly across threads ──
        // Each thread accumulates into its own revenue array (no locks needed).
        // Merge step is O(n_threads) after all threads finish.

        const int n_threads = pool.num_threads;

        // Per-thread accumulator padded to 64B to avoid false sharing.
        struct alignas(64) ThreadRev {
            double  rev[2][2];
            int64_t li_pass;
            int64_t li_rows_emitted;
        };
        std::vector<ThreadRev> thread_rev(static_cast<size_t>(n_threads));
        for (auto& tr : thread_rev) {
            tr.rev[0][0] = tr.rev[0][1] = tr.rev[1][0] = tr.rev[1][1] = 0.0;
            tr.li_pass = 0;
            tr.li_rows_emitted = 0;
        }

        {
            PROFILE_SCOPE("q7_parallel_scan");

            pool.parallel_for([&](int tid, int n) {
                // Each thread gets a disjoint slice of [li_lo, li_hi).
                const int64_t range = li_hi - li_lo;
                const int64_t chunk = (range + n - 1) / n;
                const int64_t t_lo  = li_lo + static_cast<int64_t>(tid) * chunk;
                const int64_t t_hi  = std::min(li_lo + static_cast<int64_t>(tid + 1) * chunk, li_hi);

                double  loc_rev[2][2] = {{0.0, 0.0}, {0.0, 0.0}};
                int64_t loc_pass    = 0;
                int64_t loc_emitted = 0;

                constexpr int64_t PF = 64;
                const int64_t t_hi_pf = t_hi - PF;

                for (int64_t i = t_lo; i < t_hi; ++i) {
                    if (__builtin_expect(i < t_hi_pf, 1)) {
                        __builtin_prefetch(
                            reinterpret_cast<const char*>(
                                li_packed_nk + static_cast<size_t>(i + PF)),
                            0, 1);
                    }

                    uint16_t packed   = li_packed_nk[static_cast<size_t>(i)];
                    uint8_t  pair_idx = combined_filter[packed];
                    if (__builtin_expect(pair_idx == 0xFF, 1)) continue;
                    ++loc_pass;

                    int32_t ship_days = li_shipdate_data[static_cast<size_t>(i)];
                    int     year_idx  = (ship_days >= date_1996_start) ? 1 : 0;

                    double ep   = li_ep_data[static_cast<size_t>(i)];
                    double disc = li_disc_data[static_cast<size_t>(i)];
                    loc_rev[pair_idx][year_idx] += ep * (1.0 - disc);
                    ++loc_emitted;
                }

                // Write results into per-thread slot (no contention between threads).
                thread_rev[static_cast<size_t>(tid)].rev[0][0]      = loc_rev[0][0];
                thread_rev[static_cast<size_t>(tid)].rev[0][1]      = loc_rev[0][1];
                thread_rev[static_cast<size_t>(tid)].rev[1][0]      = loc_rev[1][0];
                thread_rev[static_cast<size_t>(tid)].rev[1][1]      = loc_rev[1][1];
                thread_rev[static_cast<size_t>(tid)].li_pass         = loc_pass;
                thread_rev[static_cast<size_t>(tid)].li_rows_emitted = loc_emitted;
            });
        }

        // ── Sequential merge of per-thread accumulators ───────────────────
        {
            PROFILE_SCOPE("q7_merge");
            for (int t = 0; t < n_threads; ++t) {
                const auto& tr = thread_rev[static_cast<size_t>(t)];
                revenue[0][0]   += tr.rev[0][0];
                revenue[0][1]   += tr.rev[0][1];
                revenue[1][0]   += tr.rev[1][0];
                revenue[1][1]   += tr.rev[1][1];
                li_pass         += tr.li_pass;
                li_rows_emitted += tr.li_rows_emitted;
            }
        }
    }

    TRACE_COUNT("q7_li_pass",         li_pass);
    TRACE_COUNT("q7_li_rows_emitted",     li_rows_emitted);

    int64_t non_zero_buckets = 0;
    for (int p = 0; p < 2; ++p)
        for (int y = 0; y < 2; ++y)
            if (revenue[p][y] != 0.0) ++non_zero_buckets;
    TRACE_COUNT("q7_agg_rows_in",      li_rows_emitted);
    TRACE_COUNT("q7_groups_created",   non_zero_buckets);
    TRACE_COUNT("q7_agg_rows_emitted", non_zero_buckets);

    // ── 6. Format revenue ───────────────────────────────────────────────────
    auto fmt_rev = [](double v) -> std::string {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << v;
        return oss.str();
    };

    // ── 7. Collect and sort result rows ─────────────────────────────────────
    struct ResultRow {
        std::string supp_nation;
        std::string cust_nation;
        int32_t     l_year;
        double      rev;
    };

    std::vector<ResultRow> result;
    result.reserve(4);

    const std::string* pair_supp[2] = {&nation1_name, &nation2_name};
    const std::string* pair_cust[2] = {&nation2_name, &nation1_name};
    const int32_t years[2] = {1995, 1996};

    for (int p = 0; p < 2; ++p)
        for (int y = 0; y < 2; ++y)
            if (revenue[p][y] != 0.0)
                result.push_back({*pair_supp[p], *pair_cust[p], years[y], revenue[p][y]});

    std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
    });

    TRACE_COUNT("q7_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q7_sort_rows_out", static_cast<long long>(result.size()));

    // ── 8. Build output ─────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"supp_nation", "cust_nation", "l_year", "revenue"});

    {
        PROFILE_SCOPE("q7_format_output");
        for (const auto& r : result) {
            rows.push_back({
                r.supp_nation,
                r.cust_nation,
                std::to_string(r.l_year),
                fmt_rev(r.rev)
            });
        }
    }
    TRACE_COUNT("q7_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}