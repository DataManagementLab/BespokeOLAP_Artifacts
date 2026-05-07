#include "query5.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <unordered_map>

// SQL:
/** select n_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and l_suppkey = s_suppkey
    and c_nationkey = s_nationkey
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '1' year
GROUP BY
    n_name
ORDER BY
    revenue desc; */

std::vector<std::vector<std::string>> run_q5(Database* db, const Q5Args& args) {
    if (!db) {
        throw std::runtime_error("run_q5: db is null");
    }

    PROFILE_SCOPE("q5_total");

    // ── 1. Local date helpers ─────────────────────────────────────────────────
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

    auto add_years = [&date_to_days](int32_t days, int years) -> int32_t {
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
        int ny = y + years, nm = mon, nd = dom;
        if (nm == 2 && nd == 29) {
            auto is_leap = [](int yr) {
                return (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
            };
            if (!is_leap(ny)) nd = 28;
        }
        int nm2 = nm, ny2 = ny;
        if (nm2 <= 2) { ny2 -= 1; nm2 += 12; }
        int era2 = (ny2 >= 0 ? ny2 : ny2 - 399) / 400;
        int yoe2 = ny2 - era2 * 400;
        int doy2 = (153 * (nm2 - 3) + 2) / 5 + nd - 1;
        int doe2 = yoe2 * 365 + yoe2 / 4 - yoe2 / 100 + doy2;
        return static_cast<int32_t>(era2 * 146097 + doe2 - 719468);
        (void)date_to_days;
    };

    // ── 2. Parse arguments ────────────────────────────────────────────────────
    const std::string& region_name = args.REGION;
    int32_t date_lo = date_to_days(args.DATE);
    int32_t date_hi = add_years(date_lo, 1);

    // ── 3. Resolve region → set of nation keys ────────────────────────────────
    const RegionTable& reg = db->region;
    const NationTable& nat = db->nation;

    auto reg_it = reg.name_to_regionkey.find(region_name);
    if (reg_it == reg.name_to_regionkey.end()) {
        return {{"n_name", "revenue"}};
    }
    int32_t target_regionkey = reg_it->second;

    std::array<bool, 25> is_region_nation{};
    is_region_nation.fill(false);
    for (int32_t i = 0; i < nat.num_rows; ++i) {
        int32_t nk = nat.n_nationkey[static_cast<size_t>(i)];
        if (nk >= 0 && nk < 25) {
            if (nat.nationkey_to_regionkey[static_cast<size_t>(nk)] == target_regionkey)
                is_region_nation[static_cast<size_t>(nk)] = true;
        }
    }

    std::array<int64_t, 25> nation_revenue{};
    nation_revenue.fill(0);

    // ── 4. Setup scan arrays ──────────────────────────────────────────────────
    const OrdersTable&   ord  = db->orders;
    const CustomerTable& cust = db->customer;
    const LineitemTable& li   = db->lineitem;

    TRACE_COUNT("q5_build_cust_map_scan", 0LL);
    {
        PROFILE_SCOPE("q5_build_cust_map");
        (void)cust; (void)li;
    }
    TRACE_COUNT("q5_cust_map_size", 0LL);

    const uint8_t* suppnk_arr   = db->li_by_orderkey_suppnk.data();
    const int32_t* ep_csr_arr   = db->li_by_orderkey_ep_i32.data();
    const int8_t*  disc_csr_arr = db->li_by_orderkey_disc_i8.data();
    TRACE_COUNT("q5_supp_map_size", static_cast<long long>(db->li_by_orderkey_suppnk.size()));

    // ── 4c. Binary search for date range ─────────────────────────────────────
    int64_t row_lo = 0, row_hi = 0;
    {
        PROFILE_SCOPE("q5_scan_bisect");
        auto lo_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_lo);
        auto hi_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_hi);
        row_lo = static_cast<int64_t>(lo_it - ord.o_orderdate.begin());
        row_hi = static_cast<int64_t>(hi_it - ord.o_orderdate.begin());
    }

    int64_t total_ord_rows    = static_cast<int64_t>(ord.o_orderdate.size());
    int64_t ord_rows_in_range = row_hi - row_lo;
    TRACE_COUNT("q5_total_orders",          total_ord_rows);
    TRACE_COUNT("q5_ord_rows_in_range",     ord_rows_in_range);
    TRACE_COUNT("q5_ord_rows_filtered_out", total_ord_rows - ord_rows_in_range);

    int64_t ord_nation_pass = 0;
    int64_t li_rows_scanned = 0;
    int64_t li_rows_emitted = 0;

    int32_t n_threads  = pool.num_threads;
    int64_t range_size = row_hi - row_lo;

    // ── Per-thread revenue accumulators (cache-line aligned, no false sharing) ─
    struct alignas(64) ThreadRev {
        int64_t rev[25];
        int64_t scanned;
        int64_t emitted;
        int64_t ord_pass;
    };
    std::vector<ThreadRev> tl_rev(static_cast<size_t>(n_threads));
    for (int t = 0; t < n_threads; ++t) {
        auto& tr = tl_rev[static_cast<size_t>(t)];
        for (int i = 0; i < 25; ++i) tr.rev[i] = 0;
        tr.scanned  = 0;
        tr.emitted  = 0;
        tr.ord_pass = 0;
    }

    // ── QualOrd: compact struct for qualifying order metadata ─────────────────
    // csr_lo(4B) + csr_hi(4B) + nk(1B) + pad(3B) = 12B
    struct QualOrd {
        int32_t csr_lo;
        int32_t csr_hi;
        uint8_t nk;
        uint8_t _pad[3];
    };
    static_assert(sizeof(QualOrd) == 12, "QualOrd must be 12 bytes");

    {
        PROFILE_SCOPE("q5_agg_loop");

        // ── Phase 1: parallel collect qualifying orders ───────────────────────
        // Each thread builds a thread-local vector of qualifying orders from its
        // own contiguous partition of the date-filtered range.
        const uint8_t* ord_nk_arr = ord.o_cust_nationkey_u8.data();
        const int32_t* csr_lo_arr = ord.o_li_csr_lo.data();
        const int32_t* csr_hi_arr = ord.o_li_csr_hi.data();

        std::vector<std::vector<QualOrd>> tl_qual(static_cast<size_t>(n_threads));

        {
            PROFILE_SCOPE("q5_collect_orders");
            pool.parallel_for([&](int tid, int nt) {
                int64_t chunk   = (range_size + nt - 1) / nt;
                int64_t t_start = row_lo + static_cast<int64_t>(tid)     * chunk;
                int64_t t_end   = row_lo + static_cast<int64_t>(tid + 1) * chunk;
                if (t_start > row_hi) t_start = row_hi;
                if (t_end   > row_hi) t_end   = row_hi;

                auto& local = tl_qual[static_cast<size_t>(tid)];
                local.reserve(static_cast<size_t>((t_end - t_start) / 4));

                for (int64_t oi = t_start; oi < t_end; ++oi) {
                    uint8_t raw_nk = ord_nk_arr[static_cast<size_t>(oi)];
                    if (raw_nk == 0xFF || !is_region_nation[static_cast<size_t>(raw_nk)]) continue;
                    QualOrd qo;
                    qo.csr_lo  = csr_lo_arr[static_cast<size_t>(oi)];
                    qo.csr_hi  = csr_hi_arr[static_cast<size_t>(oi)];
                    qo.nk      = raw_nk;
                    qo._pad[0] = qo._pad[1] = qo._pad[2] = 0;
                    local.push_back(qo);
                }
            });
        }

        // Compute total and record ord_nation_pass.
        size_t total_qual = 0;
        for (int t = 0; t < n_threads; ++t) {
            size_t sz = tl_qual[static_cast<size_t>(t)].size();
            ord_nation_pass += static_cast<int64_t>(sz);
            total_qual      += sz;
        }
        TRACE_COUNT("q5_qual_orders_collected", static_cast<long long>(total_qual));

        // ── Phase 2: parallel scan CSR lineitems ─────────────────────────────
        // Partition the flat qual_orders across threads without materializing a
        // combined array: each thread works directly on its own tl_qual slice.
        {
            PROFILE_SCOPE("q5_scan_lineitems");

            pool.parallel_for([&](int tid, int nt) {
                auto& tr = tl_rev[static_cast<size_t>(tid)];
                constexpr int PREFETCH_DIST = 32;

                // Compute this thread's slice of qual_orders via prefix sums.
                // Distribute total_qual evenly across threads.
                size_t qi_start = (total_qual * static_cast<size_t>(tid))     / static_cast<size_t>(nt);
                size_t qi_end   = (total_qual * static_cast<size_t>(tid + 1)) / static_cast<size_t>(nt);

                // Translate flat indices back into (tl_qual[t], local_idx) pairs
                // by skipping through the per-thread vectors.
                // We iterate the qual entries in order using a two-level cursor.
                size_t t_cursor   = 0;  // which tl_qual[t_cursor] we're in
                size_t loc_cursor = 0;  // index within tl_qual[t_cursor]

                // Advance to qi_start.
                size_t flat_pos = 0;
                while (t_cursor < static_cast<size_t>(nt)) {
                    size_t remaining = tl_qual[t_cursor].size() - loc_cursor;
                    if (flat_pos + remaining > qi_start) {
                        loc_cursor += qi_start - flat_pos;
                        flat_pos    = qi_start;
                        break;
                    }
                    flat_pos   += remaining;
                    loc_cursor  = 0;
                    ++t_cursor;
                }

                for (size_t qi = qi_start; qi < qi_end; ++qi) {
                    // Advance two-level cursor.
                    while (t_cursor < static_cast<size_t>(nt) &&
                           loc_cursor >= tl_qual[t_cursor].size()) {
                        ++t_cursor;
                        loc_cursor = 0;
                    }
                    if (t_cursor >= static_cast<size_t>(nt)) break;

                    // Prefetch PREFETCH_DIST ahead (flat, two-level).
                    if (qi + PREFETCH_DIST < qi_end) {
                        // Simple approximation: peek ahead by PREFETCH_DIST in the
                        // current tl_qual vector (may not cross boundaries perfectly,
                        // but avoids the two-level walk overhead for every prefetch).
                        size_t pf_idx = loc_cursor + PREFETCH_DIST;
                        if (pf_idx < tl_qual[t_cursor].size()) {
                            int32_t pf_lo = tl_qual[t_cursor][pf_idx].csr_lo;
                            __builtin_prefetch(&suppnk_arr[static_cast<size_t>(pf_lo)],   0, 1);
                            __builtin_prefetch(&ep_csr_arr[static_cast<size_t>(pf_lo)],   0, 1);
                            __builtin_prefetch(&disc_csr_arr[static_cast<size_t>(pf_lo)], 0, 1);
                        }
                    }

                    const QualOrd& qo = tl_qual[t_cursor][loc_cursor];
                    ++loc_cursor;

                    const int32_t l_start_v   = qo.csr_lo;
                    const int32_t l_end_v     = qo.csr_hi;
                    const int32_t c_nationkey = static_cast<int32_t>(qo.nk);

                    int64_t local_rev = 0;
                    for (int32_t li_i = l_start_v; li_i < l_end_v; ++li_i) {
                        ++tr.scanned;
                        uint8_t s_nk = suppnk_arr[static_cast<size_t>(li_i)];
                        if (static_cast<int32_t>(s_nk) != c_nationkey) continue;
                        ++tr.emitted;
                        int64_t ep_int   = static_cast<int64_t>(ep_csr_arr[static_cast<size_t>(li_i)]);
                        int64_t disc_int = static_cast<int64_t>(disc_csr_arr[static_cast<size_t>(li_i)]);
                        local_rev += ep_int * (100LL - disc_int);
                    }
                    tr.rev[static_cast<size_t>(c_nationkey)] += local_rev;
                }
            });

            // Sequential merge.
            for (int t = 0; t < n_threads; ++t) {
                const auto& tr = tl_rev[static_cast<size_t>(t)];
                for (int i = 0; i < 25; ++i)
                    nation_revenue[static_cast<size_t>(i)] += tr.rev[i];
                li_rows_scanned += tr.scanned;
                li_rows_emitted += tr.emitted;
            }
        }
    }
    TRACE_COUNT("q5_ord_nation_pass", ord_nation_pass);
    TRACE_COUNT("q5_li_rows_scanned", li_rows_scanned);
    TRACE_COUNT("q5_li_rows_emitted", li_rows_emitted);

    // ── 5. Collect results ────────────────────────────────────────────────────
    struct ResultRow {
        std::string n_name;
        int64_t     rev_int4;
    };
    std::vector<ResultRow> result;
    result.reserve(5);

    for (int32_t i = 0; i < nat.num_rows; ++i) {
        int32_t nk = nat.n_nationkey[static_cast<size_t>(i)];
        if (nk < 0 || nk >= 25) continue;
        if (!is_region_nation[static_cast<size_t>(nk)]) continue;
        result.push_back({nat.n_name[static_cast<size_t>(i)],
                          nation_revenue[static_cast<size_t>(nk)]});
    }

    TRACE_COUNT("q5_agg_rows_in",      li_rows_emitted);
    TRACE_COUNT("q5_groups_created",   static_cast<long long>(result.size()));
    TRACE_COUNT("q5_agg_rows_emitted", static_cast<long long>(result.size()));

    // ── 6. Sort ───────────────────────────────────────────────────────────────
    {
        PROFILE_SCOPE("q5_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.rev_int4 > b.rev_int4;
        });
    }
    TRACE_COUNT("q5_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q5_sort_rows_out", static_cast<long long>(result.size()));

    // ── 7. Format revenue ─────────────────────────────────────────────────────
    auto fmt_rev = [](int64_t rev_int4) -> std::string {
        bool neg    = rev_int4 < 0;
        int64_t abs_val = neg ? -rev_int4 : rev_int4;
        int64_t cents   = (abs_val + 50) / 100;
        long long whole = static_cast<long long>(cents / 100);
        long long frac  = static_cast<long long>(cents % 100);
        char buf[32];
        if (neg) std::snprintf(buf, sizeof(buf), "-%lld.%02lld", whole, frac);
        else     std::snprintf(buf, sizeof(buf), "%lld.%02lld",  whole, frac);
        return std::string(buf);
    };

    // ── 8. Build output ───────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"n_name", "revenue"});
    {
        PROFILE_SCOPE("q5_format_output");
        for (const auto& r : result)
            rows.push_back({r.n_name, fmt_rev(r.rev_int4)});
    }
    TRACE_COUNT("q5_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}