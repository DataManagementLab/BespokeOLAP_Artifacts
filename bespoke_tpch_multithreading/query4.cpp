#include "query4.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
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
    o_orderpriority,
    count(*) as order_count
from
    orders
where
    o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            l_orderkey = o_orderkey
            and l_commitdate < l_receiptdate
    )
group by
    o_orderpriority
order by
    o_orderpriority;

 */

// ── local date helpers ────────────────────────────────────────────────────────

// "YYYY-MM-DD" → days since Unix epoch (1970-01-01)
static int32_t q4_date_to_days(const std::string& s) {
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

// Add N months to a days-since-epoch value, calendar-correct.
// Clamps day-of-month to the last valid day of the resulting month.
static int32_t q4_add_months(int32_t days, int months) {
    // Decode
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int32_t mp  = (5 * doy + 2) / 153;
    int32_t dom = doy - (153 * mp + 2) / 5 + 1;  // day of month (1-based)
    int32_t mon = mp < 10 ? mp + 3 : mp - 9;      // month (1-based)
    if (mon <= 2) y += 1;

    // Advance months
    int total_months = (y * 12 + (mon - 1)) + months;
    int ny = total_months / 12;
    int nm = total_months % 12 + 1;  // 1-based month

    // Days in the resulting month (for clamping)
    static const int days_in_month[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    auto is_leap = [](int yr) {
        return (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
    };
    int dim = days_in_month[nm];
    if (nm == 2 && is_leap(ny)) dim = 29;

    int nd = dom > dim ? dim : dom;  // clamp day-of-month

    // Re-encode: civil_to_days
    int nm2 = nm, ny2 = ny;
    if (nm2 <= 2) { ny2 -= 1; nm2 += 12; }
    int era2 = (ny2 >= 0 ? ny2 : ny2 - 399) / 400;
    int yoe2 = ny2 - era2 * 400;
    int doy2 = (153 * (nm2 - 3) + 2) / 5 + nd - 1;
    int doe2 = yoe2 * 365 + yoe2 / 4 - yoe2 / 100 + doy2;
    return static_cast<int32_t>(era2 * 146097 + doe2 - 719468);
}

std::vector<std::vector<std::string>> run_q4(Database* db, const Q4Args& args) {
    if (!db) {
        throw std::runtime_error("run_q4: db is null");
    }

    PROFILE_SCOPE("q4_total");

    // ── 1. Parse arguments ────────────────────────────────────────────────────
    int32_t date_lo = q4_date_to_days(args.DATE);   // o_orderdate >= date_lo
    int32_t date_hi = q4_add_months(date_lo, 3);    // o_orderdate <  date_hi

    // ── 2. References to tables and indexes ──────────────────────────────────
    const OrdersTable&   ord    = db->orders;

    // ── 3. Binary-search the orders date range ────────────────────────────────
    int64_t row_lo = 0, row_hi = 0;
    {
        PROFILE_SCOPE("q4_scan_bisect");
        auto lo_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_lo);
        auto hi_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_hi);
        row_lo = static_cast<int64_t>(lo_it - ord.o_orderdate.begin());
        row_hi = static_cast<int64_t>(hi_it - ord.o_orderdate.begin());
    }

    int64_t total_ord_rows      = static_cast<int64_t>(ord.o_orderdate.size());
    int64_t ord_rows_in_range   = row_hi - row_lo;
    TRACE_COUNT("q4_total_orders",          total_ord_rows);
    TRACE_COUNT("q4_ord_rows_in_range",     ord_rows_in_range);
    TRACE_COUNT("q4_ord_rows_filtered_out", total_ord_rows - ord_rows_in_range);

    // ── 4. Aggregate: count qualifying orders per o_orderpriority ─────────────
    // Strategy: use the pre-computed o_has_clt_rd byte array to avoid ALL inner
    // CSR scanning. For each in-range order, we need:
    //   - o_has_clt_rd[i]: 1 if EXISTS(commitdate < receiptdate)  (1 byte load)
    //   - o_orderpriority[i]: priority enum (1 byte load)
    // Both arrays are sequential and dense over order rows → pure streaming access.
    size_t num_priorities = ord.orderpriority_names.size();
    std::vector<int64_t> counts(num_priorities, 0);

    // Instrumentation counters
    int64_t ord_exists_true = 0;

    if (ord.o_has_clt_rd.empty()) {
        throw std::runtime_error("run_q4: o_has_clt_rd not built — please reload the database");
    }
    const uint8_t* __restrict__ o_has  = ord.o_has_clt_rd.data();
    const uint8_t* __restrict__ o_prio = ord.o_orderpriority.data();

    const int n_threads = pool.num_threads;

    {
        PROFILE_SCOPE("q4_agg_loop");

        const uint8_t* has_p  = o_has  + row_lo;
        const uint8_t* prio_p = o_prio + row_lo;
        const int64_t  range  = row_hi - row_lo;

        // Max 5 priorities; use a fixed array size per thread.
        // PRIO_STRIDE: padded to 16 int64_t (128 bytes = 2 cache lines) per thread
        // to avoid false sharing between adjacent threads' count arrays.
        constexpr size_t MAX_PRIO    = 16;   // safe upper bound
        constexpr size_t PRIO_STRIDE = 16;   // >= MAX_PRIO, 128-byte aligned per thread

        // Per-thread local accumulator arrays, cache-line isolated.
        // thread_lcounts[tid * PRIO_STRIDE + prio] = count for that thread+priority.
        std::vector<int64_t> thread_lcounts((size_t)n_threads * PRIO_STRIDE, 0);
        std::vector<int64_t> thread_exists_true((size_t)n_threads, 0);

        // Parallel aggregation via morsel-driven range partitioning.
        // Each thread processes a disjoint contiguous chunk of the in-range orders.
        // No synchronization needed during aggregation — purely thread-local writes.
        pool.parallel_for([&](int tid, int n) {
            PROFILE_SCOPE("q4_agg_parallel_chunk");

            // Compute this thread's sub-range [t_lo, t_hi) within [0, range)
            int64_t chunk = (range + n - 1) / n;
            int64_t t_lo  = (int64_t)tid * chunk;
            int64_t t_hi  = t_lo + chunk;
            if (t_hi > range) t_hi = range;
            if (t_lo >= t_hi) return;  // empty slice for this thread

            int64_t* lcounts = thread_lcounts.data() + (size_t)tid * PRIO_STRIDE;
            int64_t local_exists = 0;

            int64_t i = t_lo;
            // 8-way unrolled loop: 8 independent byte loads + branchless adds per iter.
            // Each iteration: 8 has-flags + 8 priorities → 16 bytes total → ILP-friendly.
            for (; i + 8 <= t_hi; i += 8) {
                uint8_t h0 = has_p[i+0], h1 = has_p[i+1], h2 = has_p[i+2], h3 = has_p[i+3];
                uint8_t h4 = has_p[i+4], h5 = has_p[i+5], h6 = has_p[i+6], h7 = has_p[i+7];
                uint8_t p0 = prio_p[i+0], p1 = prio_p[i+1], p2 = prio_p[i+2], p3 = prio_p[i+3];
                uint8_t p4 = prio_p[i+4], p5 = prio_p[i+5], p6 = prio_p[i+6], p7 = prio_p[i+7];
                // Branchless accumulation: h is 0 or 1
                lcounts[p0] += h0;
                lcounts[p1] += h1;
                lcounts[p2] += h2;
                lcounts[p3] += h3;
                lcounts[p4] += h4;
                lcounts[p5] += h5;
                lcounts[p6] += h6;
                lcounts[p7] += h7;
                local_exists += h0 + h1 + h2 + h3 + h4 + h5 + h6 + h7;
            }
            // Scalar tail for remaining rows
            for (; i < t_hi; ++i) {
                uint8_t h = has_p[i];
                uint8_t p = prio_p[i];
                lcounts[p] += h;
                local_exists += h;
            }
            thread_exists_true[tid] = local_exists;
            TRACE_COUNT("q4_agg_parallel_rows", t_hi - t_lo);
        });

        // Sequential merge: accumulate all per-thread results into counts[].
        // Only ~5 priorities × n_threads iterations — negligible cost.
        {
            PROFILE_SCOPE("q4_agg_merge");
            for (int t = 0; t < n_threads; ++t) {
                const int64_t* lcounts = thread_lcounts.data() + (size_t)t * PRIO_STRIDE;
                for (size_t pi = 0; pi < num_priorities; ++pi)
                    counts[pi] += lcounts[pi];
                ord_exists_true += thread_exists_true[t];
            }
        }
    }

    TRACE_COUNT("q4_ord_exists_true", ord_exists_true);

    // ── 5. Collect non-zero groups and sort by o_orderpriority name ASC ──────
    struct GroupRow {
        std::string priority_name;
        int64_t     count;
    };
    std::vector<GroupRow> groups;
    groups.reserve(num_priorities);
    for (size_t i = 0; i < num_priorities; ++i) {
        if (counts[i] > 0) {
            groups.push_back({ord.orderpriority_names[i], counts[i]});
        }
    }

    TRACE_COUNT("q4_agg_rows_in",      ord_exists_true);
    TRACE_COUNT("q4_groups_created",   static_cast<long long>(num_priorities));
    TRACE_COUNT("q4_agg_rows_emitted", static_cast<long long>(groups.size()));

    {
        PROFILE_SCOPE("q4_sort");
        std::sort(groups.begin(), groups.end(), [](const GroupRow& a, const GroupRow& b) {
            return a.priority_name < b.priority_name;
        });
    }
    TRACE_COUNT("q4_sort_rows_in",  static_cast<long long>(groups.size()));
    TRACE_COUNT("q4_sort_rows_out", static_cast<long long>(groups.size()));

    // ── 6. Format output ──────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(groups.size() + 1);
    rows.push_back({"o_orderpriority", "order_count"});

    {
        PROFILE_SCOPE("q4_format_output");
        for (const auto& g : groups) {
            rows.push_back({g.priority_name, std::to_string(g.count)});
        }
    }
    TRACE_COUNT("q4_query_output_rows", static_cast<long long>(rows.size()) - 1); // exclude header

    return rows;
}