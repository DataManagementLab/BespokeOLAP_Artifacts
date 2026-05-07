#include "query10.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <cstdio>
#include <string>
#include <utility>
#include <vector>

// SQL:
/** select
    c_custkey,
    c_name,
    sum(l_extendedprice * (1 - l_discount)) as revenue,
    c_acctbal,
    n_name,
    c_address,
    c_phone,
    c_comment
from
    customer,
    orders,
    lineitem,
    nation
where
    c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate >= date '[DATE]'
    and o_orderdate < date '[DATE]' + interval '3' month
    and l_returnflag = 'R'
    and c_nationkey = n_nationkey
group by
    c_custkey,
    c_name,
    c_acctbal,
    c_phone,
    n_name,
    c_address,
    c_comment
order by
    revenue desc;  */

// File-scope date helpers
static int32_t q10_date_to_days(const std::string& s) {
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

static int32_t q10_add_months(int32_t days, int months) {
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

    int total_months = (y * 12 + (mon - 1)) + months;
    int ny = total_months / 12;
    int nm = total_months % 12 + 1;

    static const int dim_tab[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (ny % 4 == 0 && ny % 100 != 0) || (ny % 400 == 0);
    int dim = (nm == 2 && leap) ? 29 : dim_tab[nm];
    int nd = (dom > dim) ? dim : dom;

    int nm2 = nm, ny2 = ny;
    if (nm2 <= 2) { ny2 -= 1; nm2 += 12; }
    int era2 = (ny2 >= 0 ? ny2 : ny2 - 399) / 400;
    int yoe2 = ny2 - era2 * 400;
    int doy2 = (153 * (nm2 - 3) + 2) / 5 + nd - 1;
    int doe2 = yoe2 * 365 + yoe2 / 4 - yoe2 / 100 + doy2;
    return static_cast<int32_t>(era2 * 146097 + doe2 - 719468);
}

std::vector<std::vector<std::string>> run_q10(Database* db, const Q10Args& args) {
    if (!db) throw std::runtime_error("run_q10: db is null");

    PROFILE_SCOPE("q10_total");

    // 1. Parse date range
    int32_t date_lo = q10_date_to_days(args.DATE);
    int32_t date_hi = q10_add_months(date_lo, 3);

    // 2. Table references
    const OrdersTable&   ord    = db->orders;
    const LineitemTable& li     = db->lineitem;
    const CustomerTable& cust   = db->customer;

    // 3. Resolve l_returnflag enum code for 'R'
    uint8_t RETURNFLAG_R = 255;
    for (size_t i = 0; i < li.returnflag_names.size(); ++i) {
        if (li.returnflag_names[i] == "R") { RETURNFLAG_R = static_cast<uint8_t>(i); break; }
    }
    if (RETURNFLAG_R == 255) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"c_custkey", "c_name", "revenue", "c_acctbal",
                        "n_name", "c_address", "c_phone", "c_comment"});
        return rows;
    }

    const size_t num_cust_rows = static_cast<size_t>(cust.num_rows);

    // 5. Binary search on sorted o_orderdate for date range
    int64_t row_lo = 0, row_hi = 0;
    {
        PROFILE_SCOPE("q10_scan_bisect");
        auto lo_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_lo);
        auto hi_it = std::lower_bound(ord.o_orderdate.begin(), ord.o_orderdate.end(), date_hi);
        row_lo = static_cast<int64_t>(lo_it - ord.o_orderdate.begin());
        row_hi = static_cast<int64_t>(hi_it - ord.o_orderdate.begin());
    }

    int64_t total_ord_rows    = static_cast<int64_t>(ord.o_orderdate.size());
    int64_t ord_rows_in_range = row_hi - row_lo;
    TRACE_COUNT("q10_total_orders",          total_ord_rows);
    TRACE_COUNT("q10_ord_rows_in_range",     ord_rows_in_range);
    TRACE_COUNT("q10_ord_rows_filtered_out", total_ord_rows - ord_rows_in_range);
    TRACE_COUNT("q10_total_lineitem_rows",   li.num_rows);

    // Cache raw pointers for hot loop.
    // Use int32 custkey (o_custkey_i32) to halve bandwidth vs int64 o_custkey.
    // Use companion arrays (li_by_orderkey_ep_i32, li_by_orderkey_disc_i8) for
    // sequential lineitem column access instead of scattered reads via csr_row_ids.
    const int32_t* o_custkey    = ord.o_custkey_i32.data();
    const int32_t* o_li_lo      = ord.o_li_csr_lo.data();
    const int32_t* o_li_hi      = ord.o_li_csr_hi.data();
    // Precomputed revenue companion: csr_rev_i32[j] = ep_i32 * (100 - disc_i8)
    const int32_t* csr_rev_i32    = db->li_by_orderkey_rev_i32.data();
    // Returnflag companion: sequential read, eliminates scattered l_returnflag[row_ids[j]]
    const uint8_t* csr_returnflag = db->li_by_orderkey_returnflag.data();
    const int32_t* cust_row_map = db->cust_row_by_custkey.data();
    const uint32_t cust_row_sz  = static_cast<uint32_t>(db->cust_row_by_custkey.size());

    // 4. Per-customer revenue accumulator: flat array indexed by cust_row.
    // Atomic adds used for parallel writes (very low contention: each customer
    // has few orders per quarter, and orders are partitioned disjointly across threads).
    std::vector<int64_t> cust_revenue(num_cust_rows, 0LL);
    int64_t* cust_rev_ptr = cust_revenue.data();

    int64_t total_ord_cust_pass   = 0;
    int64_t total_li_rows_scanned = 0;
    int64_t total_li_flag_pass    = 0;
    const int n_threads = pool.num_threads;

    // 6. Parallel aggregation: partition orders range across threads.
    // Each thread accumulates per-order revenue and applies it atomically to cust_revenue.
    {
        PROFILE_SCOPE("q10_agg_loop");
        pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q10_agg_loop_worker");

            // Partition orders range evenly across threads
            int64_t range     = row_hi - row_lo;
            int64_t chunk     = (range + n_thr - 1) / n_thr;
            int64_t t_lo      = row_lo + static_cast<int64_t>(tid) * chunk;
            int64_t t_hi      = t_lo + chunk;
            if (t_hi > row_hi) t_hi = row_hi;
            if (t_lo >= row_hi) return;

            int64_t local_ord_cust_pass   = 0;
            int64_t local_li_rows_scanned = 0;
            int64_t local_li_flag_pass    = 0;

            for (int64_t oi = t_lo; oi < t_hi; ++oi) {
                const size_t oidx = static_cast<size_t>(oi);

                uint32_t custkey = static_cast<uint32_t>(o_custkey[oidx]);
                if (custkey >= cust_row_sz) continue;

                int32_t cust_row = cust_row_map[custkey];
                if (cust_row < 0) continue;
                ++local_ord_cust_pass;

                int32_t li_start = o_li_lo[oidx];
                int32_t li_end   = o_li_hi[oidx];

                int64_t local_rev = 0;
                for (int32_t li_pos = li_start; li_pos < li_end; ++li_pos) {
                    const size_t lpos = static_cast<size_t>(li_pos);
                    ++local_li_rows_scanned;
                    if (csr_returnflag[lpos] != RETURNFLAG_R) continue;
                    ++local_li_flag_pass;
                    local_rev += static_cast<int64_t>(csr_rev_i32[lpos]);
                }
                if (local_rev != 0) {
                    __atomic_fetch_add(&cust_rev_ptr[static_cast<size_t>(cust_row)],
                                       local_rev, __ATOMIC_RELAXED);
                }
            }
            __atomic_fetch_add(&total_ord_cust_pass,   local_ord_cust_pass,   __ATOMIC_RELAXED);
            __atomic_fetch_add(&total_li_rows_scanned, local_li_rows_scanned, __ATOMIC_RELAXED);
            __atomic_fetch_add(&total_li_flag_pass,    local_li_flag_pass,    __ATOMIC_RELAXED);
        });
    } // end q10_agg_loop

    TRACE_COUNT("q10_ord_cust_pass",   total_ord_cust_pass);
    TRACE_COUNT("q10_li_rows_scanned", total_li_rows_scanned);
    TRACE_COUNT("q10_li_flag_pass",    total_li_flag_pass);

    // 7. Collect non-zero revenue entries (scan cust_revenue)
    struct ResultRow {
        int32_t cust_row;
        int64_t revenue;
    };
    std::vector<ResultRow> result;
    result.reserve(num_cust_rows / 20);  // typical ~5% of customers have orders in window
    {
        PROFILE_SCOPE("q10_collect_results");
        for (size_t cr = 0; cr < num_cust_rows; ++cr) {
            if (cust_revenue[cr] != 0LL) {
                result.push_back({static_cast<int32_t>(cr), cust_revenue[cr]});
            }
        }
    }
    TRACE_COUNT("q10_groups_created", static_cast<long long>(result.size()));

    // 8. Sort by revenue DESC
    {
        PROFILE_SCOPE("q10_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            return a.revenue > b.revenue;
        });
    }
    TRACE_COUNT("q10_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q10_sort_rows_out", static_cast<long long>(result.size()));

    // 9. Format output
    // Revenue formatter: int64 in units of 0.0001 -> "NNNN.NNNN"
    auto fmt_rev = [](int64_t v, char* buf) -> int {
        bool neg = v < 0;
        int64_t abs_v = neg ? -v : v;
        long long whole = static_cast<long long>(abs_v / 10000);
        int frac        = static_cast<int>(abs_v % 10000);
        return neg
            ? std::snprintf(buf, 32, "-%lld.%04d", whole, frac)
            : std::snprintf(buf, 32,  "%lld.%04d", whole, frac);
    };

    // Raw pointers for customer columns — avoid per-iteration operator[]
    const std::string* cust_custkey_str = cust.c_custkey_str.data();
    const std::string* cust_acctbal_str = cust.c_acctbal_str.data();
    const std::string* cust_name        = cust.c_name.data();
    const std::string* cust_address     = cust.c_address.data();
    const std::string* cust_phone       = cust.c_phone.data();
    const std::string* cust_comment     = cust.c_comment.data();
    // c_nation_name is always pre-built at load time
    const std::string* cust_nation_name = cust.c_nation_name.data();

    const size_t nresult = result.size();
    std::vector<std::vector<std::string>> rows;
    rows.resize(nresult + 1);
    rows[0] = {"c_custkey", "c_name", "revenue", "c_acctbal",
               "n_name", "c_address", "c_phone", "c_comment"};

    {
        PROFILE_SCOPE("q10_format_output");
        // Parallelize output formatting: each result row is independent.
        // Result rows are sorted by revenue, so cust_row indices are in random order.
        // This is the dominant source of cache misses (random access into 1.68GB of customer strings).
        // Parallel execution across 96 threads dramatically reduces latency.
        pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q10_format_worker");
            char rev_buf[32];
            constexpr size_t PREFETCH_DIST = 16;

            // Each thread handles a contiguous slice of result rows
            size_t chunk = (nresult + static_cast<size_t>(n_thr) - 1) / static_cast<size_t>(n_thr);
            size_t r_lo  = static_cast<size_t>(tid) * chunk;
            size_t r_hi  = r_lo + chunk < nresult ? r_lo + chunk : nresult;
            if (r_lo >= nresult) return;

            for (size_t ri = r_lo; ri < r_hi; ++ri) {
                // Prefetch ahead: hide latency of random customer array accesses
                if (ri + PREFETCH_DIST < r_hi) {
                    const size_t fcidx = static_cast<size_t>(result[ri + PREFETCH_DIST].cust_row);
                    __builtin_prefetch(&cust_custkey_str[fcidx], 0, 1);
                    __builtin_prefetch(&cust_name[fcidx],        0, 1);
                    __builtin_prefetch(&cust_acctbal_str[fcidx], 0, 1);
                    __builtin_prefetch(&cust_nation_name[fcidx], 0, 1);
                    __builtin_prefetch(&cust_address[fcidx],     0, 1);
                    __builtin_prefetch(&cust_phone[fcidx],       0, 1);
                    __builtin_prefetch(&cust_comment[fcidx],     0, 1);
                }

                const size_t cidx = static_cast<size_t>(result[ri].cust_row);
                int rlen = fmt_rev(result[ri].revenue, rev_buf);
                auto& row = rows[ri + 1];
                row.resize(8);
                row[0] = cust_custkey_str[cidx];
                row[1] = cust_name[cidx];
                row[2].assign(rev_buf, static_cast<size_t>(rlen > 0 ? rlen : 0));
                row[3] = cust_acctbal_str[cidx];
                row[4] = cust_nation_name[cidx];
                row[5] = cust_address[cidx];
                row[6] = cust_phone[cidx];
                row[7] = cust_comment[cidx];
            }
        });
    }
    TRACE_COUNT("q10_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}