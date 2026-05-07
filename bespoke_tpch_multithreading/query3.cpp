#include "query3.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();
#include <immintrin.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cmath>
#include <cstdio>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <numeric>
#include <utility>
#include <vector>
#include <cstring>

// SQL:
/** select l_orderkey,
    sum(l_extendedprice*(1-l_discount)) as revenue,
    o_orderdate,
    o_shippriority
FROM
    customer,
    orders,
    lineitem
WHERE
    c_mktsegment = '[SEGMENT]'
    and c_custkey = o_custkey
    and l_orderkey = o_orderkey
    and o_orderdate < date '[DATE]'
    and l_shipdate > date '[DATE]'
GROUP BY
    l_orderkey,
    o_orderdate,
    o_shippriority
ORDER BY
    revenue desc,
    o_orderdate; */

// ── local date helpers ────────────────────────────────────────────────────────

// "YYYY-MM-DD" → days since Unix epoch (1970-01-01)
static int32_t q3_date_to_days(const std::string& s) {
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

// days since Unix epoch → "YYYY-MM-DD"
static std::string q3_days_to_date(int32_t days) {
    int32_t z   = days + 719468;
    int32_t era = (z >= 0 ? z : z - 146096) / 146097;
    int32_t doe = z - era * 146097;
    int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    int32_t mp  = (5 * doy + 2) / 153;
    int32_t d   = doy - (153 * mp + 2) / 5 + 1;
    int32_t m   = mp < 10 ? mp + 3 : mp - 9;
    if (m <= 2) y += 1;
    char buf[12];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
                  static_cast<int>(y), static_cast<int>(m), static_cast<int>(d));
    return std::string(buf);
}

// Fast revenue formatter: writes into buf (no heap alloc), returns length.
// Formats rev_int4 (units 0.0001) as "NNN.NN" with 2dp, rounding at 4dp→2dp.
static int q3_fmt_rev_buf(int64_t rev_int4, char* buf) {
    bool neg = rev_int4 < 0;
    int64_t abs_val = neg ? -rev_int4 : rev_int4;
    int64_t cents = (abs_val + 50) / 100;  // round half-up from 4dp to 2dp
    long long whole = static_cast<long long>(cents / 100);
    long long frac  = static_cast<long long>(cents % 100);
    // Fast manual formatting: avoid snprintf overhead.
    char tmp[32];
    int pos = 31;
    tmp[pos] = '\0';
    // Write 2-digit fraction part
    tmp[--pos] = static_cast<char>('0' + frac % 10);
    tmp[--pos] = static_cast<char>('0' + frac / 10);
    tmp[--pos] = '.';
    // Write whole part (at least 1 digit)
    long long w = whole;
    if (w == 0) { tmp[--pos] = '0'; }
    else {
        while (w > 0) { tmp[--pos] = static_cast<char>('0' + w % 10); w /= 10; }
    }
    if (neg) tmp[--pos] = '-';
    int len = 31 - pos;
    std::memcpy(buf, tmp + pos, static_cast<size_t>(len) + 1);
    return len;
}

static std::string q3_fmt_rev(int64_t rev_int4) {
    char buf[32];
    q3_fmt_rev_buf(rev_int4, buf);
    return std::string(buf);
}

// Fast int64 to string: writes into buf, returns pointer to written start.
// buf must be >= 21 bytes.
static const char* q3_i64_to_str(int64_t v, char* buf) {
    if (v == 0) { buf[0] = '0'; buf[1] = '\0'; return buf; }
    char tmp[22]; int pos = 21; tmp[pos] = '\0';
    bool neg = (v < 0);
    uint64_t u = neg ? static_cast<uint64_t>(-v) : static_cast<uint64_t>(v);
    while (u) { tmp[--pos] = static_cast<char>('0' + u % 10); u /= 10; }
    if (neg) tmp[--pos] = '-';
    int len = 21 - pos;
    std::memcpy(buf, tmp + pos, static_cast<size_t>(len) + 1);
    return buf;
}

// ─────────────────────────────────────────────────────────────────────────────


std::vector<std::vector<std::string>> run_q3(Database* db, const Q3Args& args) {
    if (!db) {
        throw std::runtime_error("run_q3: db is null");
    }

    PROFILE_SCOPE("q3_total");

    // ── 1. Parse arguments ────────────────────────────────────────────────────
    const std::string& segment_str = args.SEGMENT;
    int32_t date_days = q3_date_to_days(args.DATE);
    // Predicates:  o_orderdate < date_days
    //              l_shipdate  > date_days

    // ── 2. Resolve mktsegment enum code ──────────────────────────────────────
    const CustomerTable& cust = db->customer;
    uint8_t seg_code = 255;  // sentinel: not found
    {
        PROFILE_SCOPE("q3_resolve_segment");
        for (size_t i = 0; i < cust.mktsegment_names.size(); ++i) {
            if (cust.mktsegment_names[i] == segment_str) {
                seg_code = static_cast<uint8_t>(i);
                break;
            }
        }
    }
    if (seg_code == 255) {
        return {{"l_orderkey", "revenue", "o_orderdate", "o_shippriority"}};
    }

    // ── 3. Build custkey bitset from qualifying customers ─────────────────────
    const OrdersTable&   ord     = db->orders;
    (void)db->lineitem;  // lineitem accessed via companion arrays
    const CsrIndex&      li_csr  = db->li_by_orderkey;

    int64_t cust_rows_scanned = 0;
    int64_t cust_rows_emitted = 0;
    int64_t ord_rows_scanned  = 0;
    int64_t ord_rows_emitted  = 0;
    int64_t li_rows_scanned   = 0;
    int64_t li_rows_emitted   = 0;

    // ── Customer bitset: byte array for O(1) mktsegment lookup by custkey ─────
    const size_t max_custkey = db->cust_row_by_custkey.size();
    std::vector<uint8_t> cust_qualifies(max_custkey, 0);

    {
        PROFILE_SCOPE("q3_scan_customer");

        const int n_threads = pool.num_threads;
        // Per-thread scanned/emitted counters
        std::vector<int64_t> t_scanned(n_threads, 0);
        std::vector<int64_t> t_emitted(n_threads, 0);

        pool.parallel_for([&](int tid, int n_t) {
            const uint8_t* seg_data  = cust.c_mktsegment.data();
            const int64_t* ckey_data = cust.c_custkey.data();
            int64_t n = cust.num_rows;
            int64_t chunk = (n + n_t - 1) / n_t;
            int64_t lo    = tid * chunk;
            int64_t hi    = std::min(lo + chunk, n);
            int64_t scanned = 0, emitted = 0;
            for (int64_t ci = lo; ci < hi; ++ci) {
                ++scanned;
                if (seg_data[ci] != seg_code) continue;
                ++emitted;
                int64_t ck = ckey_data[ci];
                if (ck > 0 && static_cast<size_t>(ck) < max_custkey) {
                    // Atomic not needed: each custkey maps to a unique slot.
                    // Different threads may write the same slot to 1 (benign race:
                    // uint8 store is atomic on x86 for aligned addresses).
                    cust_qualifies[static_cast<size_t>(ck)] = 1;
                }
            }
            t_scanned[tid] = scanned;
            t_emitted[tid] = emitted;
        });

        for (int t = 0; t < n_threads; ++t) {
            cust_rows_scanned += t_scanned[t];
            cust_rows_emitted += t_emitted[t];
        }
    }

    // ── 4. Scan qualifying orders (with early exit via binary-search cutoff) ──
    (void)li_csr;

    struct QualOrd {
        int32_t orderdate;
        int32_t shippriority;
        int32_t ord_row;      // row index in orders table
        int32_t _pad;
        int64_t orderkey;
    };
    static_assert(sizeof(QualOrd) == 24, "QualOrd must be 24 bytes");

    // Per-thread local qual_orders lists, then concatenate
    const int n_threads = pool.num_threads;
    std::vector<std::vector<QualOrd>> t_qual_orders(n_threads);
    std::vector<int64_t> t_ord_scanned(n_threads, 0);
    std::vector<int64_t> t_ord_emitted(n_threads, 0);

    // Compute cutoff once (orders table sorted by o_orderdate ASC)
    const int32_t* odate_col_g = ord.o_orderdate.data();
    int64_t n_ord = ord.num_rows;
    int64_t cutoff = static_cast<int64_t>(
        std::upper_bound(odate_col_g, odate_col_g + n_ord, date_days - 1) - odate_col_g);
    TRACE_COUNT("q3_orders_cutoff", cutoff);

    {
        PROFILE_SCOPE("q3_scan_orders");

        pool.parallel_for([&](int tid, int n_t) {
            const int32_t* ckey_col  = ord.o_custkey_i32.data();
            const int32_t* odate_col = ord.o_orderdate.data();
            const int64_t* okey_col  = ord.o_orderkey.data();
            const int32_t* spri_col  = ord.o_shippriority.data();
            const uint8_t* cq        = cust_qualifies.data();

            int64_t chunk = (cutoff + n_t - 1) / n_t;
            int64_t lo    = tid * chunk;
            int64_t hi    = std::min(lo + chunk, cutoff);

            auto& local = t_qual_orders[tid];
            local.reserve((size_t)((cutoff / n_t) * 2 / 3));  // rough estimate

            int64_t scanned = 0, emitted = 0;
            for (int64_t oi = lo; oi < hi; ++oi) {
                ++scanned;
                int32_t ck = ckey_col[oi];
                if (ck <= 0 || static_cast<size_t>(ck) >= max_custkey) continue;
                if (!cq[static_cast<size_t>(ck)]) continue;
                ++emitted;
                local.push_back({odate_col[oi], spri_col[oi],
                                 static_cast<int32_t>(oi), 0,
                                 okey_col[oi]});
            }
            t_ord_scanned[tid] = scanned;
            t_ord_emitted[tid] = emitted;
        });

        for (int t = 0; t < n_threads; ++t) {
            ord_rows_scanned += t_ord_scanned[t];
            ord_rows_emitted += t_ord_emitted[t];
        }
    }

    // Merge per-thread qual_orders into a single flat vector
    size_t total_qo = 0;
    for (int t = 0; t < n_threads; ++t) total_qo += t_qual_orders[t].size();
    std::vector<QualOrd> qual_orders;
    qual_orders.reserve(total_qo);
    // Compute prefix offsets for parallel revenue accumulation
    std::vector<size_t> thread_qo_offset(n_threads + 1, 0);
    for (int t = 0; t < n_threads; ++t) {
        thread_qo_offset[t + 1] = thread_qo_offset[t] + t_qual_orders[t].size();
        for (auto& qo : t_qual_orders[t]) qual_orders.push_back(qo);
    }
    { for (auto& v : t_qual_orders) std::vector<QualOrd>().swap(v); }

    { std::vector<uint8_t>().swap(cust_qualifies); }

    const size_t nqo = qual_orders.size();
    TRACE_COUNT("q3_qual_orders_count", static_cast<long long>(nqo));

    // Separate revenue accumulator: keeps writes separate from the read-hot QualOrd struct.
    std::vector<int64_t> rev_arr(nqo, 0LL);

    {
        PROFILE_SCOPE("q3_scan_lineitem");

        std::vector<int64_t> t_li_scanned(n_threads, 0);
        std::vector<int64_t> t_li_emitted(n_threads, 0);

        pool.parallel_for([&](int tid, int n_t) {
            const int32_t* __restrict__ csr_shipdate = db->li_by_orderkey_shipdate.data();
            const int32_t* __restrict__ csr_rev_i32  = db->li_by_orderkey_rev_i32.data();
            const int32_t* __restrict__ csr_lo_col   = ord.o_li_csr_lo.data();
            const int32_t* __restrict__ csr_hi_col   = ord.o_li_csr_hi.data();

            // Partition nqo work across threads
            size_t chunk = (nqo + (size_t)n_t - 1) / (size_t)n_t;
            size_t lo_qi  = (size_t)tid * chunk;
            size_t hi_qi  = std::min(lo_qi + chunk, nqo);

            int64_t li_scanned = 0, li_emitted = 0;

            for (size_t qi = lo_qi; qi < hi_qi; ++qi) {
                const QualOrd& qo = qual_orders[qi];
                int32_t ord_row  = qo.ord_row;
                int32_t l_start = csr_lo_col[ord_row];
                int32_t l_end   = csr_hi_col[ord_row];
                if (l_start >= l_end) continue;

                // Prefetch next order's CSR data to hide memory latency
                if (qi + 1 < hi_qi) {
                    int32_t next_row = qual_orders[qi + 1].ord_row;
                    int32_t next_lo  = csr_lo_col[next_row];
                    __builtin_prefetch(csr_shipdate + next_lo, 0, 1);
                    __builtin_prefetch(csr_rev_i32  + next_lo, 0, 1);
                }

                // Binary-search for first slot with shipdate > date_days.
                {
                    int32_t blo = l_start, bhi = l_end;
                    while (blo < bhi) {
                        int32_t mid = blo + (bhi - blo) / 2;
                        if (csr_shipdate[mid] <= date_days) blo = mid + 1;
                        else bhi = mid;
                    }
                    li_scanned += (l_end - l_start);
                    l_start = blo;
                }

                // Revenue accumulation
                int64_t rev = 0;
                int32_t count = l_end - l_start;
                li_emitted += count;
                const int32_t* rp = csr_rev_i32 + l_start;
                for (int32_t k = 0; k < count; ++k) rev += static_cast<int64_t>(rp[k]);
                rev_arr[qi] = rev;
            }
            t_li_scanned[tid] = li_scanned;
            t_li_emitted[tid] = li_emitted;
        });

        for (int t = 0; t < n_threads; ++t) {
            li_rows_scanned += t_li_scanned[t];
            li_rows_emitted += t_li_emitted[t];
        }
    }

    TRACE_COUNT("q3_cust_rows_scanned", cust_rows_scanned);
    TRACE_COUNT("q3_cust_rows_emitted", cust_rows_emitted);
    TRACE_COUNT("q3_ord_rows_scanned",  ord_rows_scanned);
    TRACE_COUNT("q3_ord_rows_emitted",  ord_rows_emitted);
    TRACE_COUNT("q3_li_rows_scanned",   li_rows_scanned);
    TRACE_COUNT("q3_li_rows_emitted",   li_rows_emitted);

    struct ResultRow {
        int64_t orderkey;
        int64_t rev_int4;
        int32_t orderdate;
        int32_t shippriority;
    };
    std::vector<ResultRow> result;
    result.reserve(nqo);
    for (size_t qi = 0; qi < nqo; ++qi) {
        if (rev_arr[qi] != 0)
            result.push_back({qual_orders[qi].orderkey, rev_arr[qi],
                               qual_orders[qi].orderdate, qual_orders[qi].shippriority});
    }

    TRACE_COUNT("q3_agg_rows_in",      li_rows_emitted);
    TRACE_COUNT("q3_groups_created",   static_cast<long long>(result.size()));
    TRACE_COUNT("q3_agg_rows_emitted", static_cast<long long>(result.size()));

    {
        PROFILE_SCOPE("q3_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.rev_int4 != b.rev_int4)
                return a.rev_int4 > b.rev_int4;        // revenue DESC (exact 4dp)
            if (a.orderdate != b.orderdate)
                return a.orderdate < b.orderdate;       // o_orderdate ASC
            return a.orderkey < b.orderkey;             // l_orderkey ASC (tiebreaker)
        });
    }
    TRACE_COUNT("q3_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q3_sort_rows_out", static_cast<long long>(result.size()));

    // ── 7. Format output ─────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"l_orderkey", "revenue", "o_orderdate", "o_shippriority"});

    {
        PROFILE_SCOPE("q3_format_output");

        std::unordered_map<int32_t, std::string> date_cache;
        date_cache.reserve(4096);
        for (const auto& r : result) {
            if (date_cache.find(r.orderdate) == date_cache.end()) {
                date_cache.emplace(r.orderdate, q3_days_to_date(r.orderdate));
            }
        }

        std::unordered_map<int32_t, std::string> spri_cache;
        spri_cache.reserve(8);
        for (const auto& r : result) {
            if (spri_cache.find(r.shippriority) == spri_cache.end()) {
                spri_cache.emplace(r.shippriority, std::to_string(r.shippriority));
            }
        }

        // Fast int-to-str buffers: avoid heap allocation per row.
        char okey_buf[22];
        for (const auto& r : result) {
            rows.push_back({
                std::string(q3_i64_to_str(r.orderkey, okey_buf)),
                q3_fmt_rev(r.rev_int4),
                date_cache[r.orderdate],
                spri_cache[r.shippriority]
            });
        }
    }
    TRACE_COUNT("q3_query_output_rows", static_cast<long long>(rows.size()) - 1); // exclude header

    return rows;
}