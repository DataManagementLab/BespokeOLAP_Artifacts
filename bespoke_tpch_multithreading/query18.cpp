#include "query18.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
#include <mutex>
#include <atomic>
static ThreadPool& pool = get_query_pool();
#include <immintrin.h>  // AVX2 (_mm256_sad_epu8, _mm256_cmpgt_epi64, etc.)
#include <cstring>  // memcpy

#include <algorithm>
#include <array>
#include <cstdio>
#include <cstdint>
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
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice,
    sum(l_quantity)
from
    customer,
    orders,
    lineitem
where
    o_orderkey in (
        select
            l_orderkey
        from
            lineitem
        group by
            l_orderkey having
                sum(l_quantity) > [QUANTITY]
    )
    and c_custkey = o_custkey
    and o_orderkey = l_orderkey
group by
    c_name,
    c_custkey,
    o_orderkey,
    o_orderdate,
    o_totalprice
order by
    o_totalprice desc,
    o_orderdate; */

std::vector<std::vector<std::string>> run_q18(Database* db, const Q18Args& args) {
    if (!db) {
        throw std::runtime_error("run_q18: db is null");
    }

    // ── 1. Parse [QUANTITY] threshold ────────────────────────────────────────
    // QUANTITY is a string like "313"; the subquery condition is sum(l_quantity) > QUANTITY.
    // l_quantity_int8 stores integer values 1..50; we use integer sums to avoid
    // floating-point hash-map operations and compare as uint32_t.
    uint32_t qty_threshold_int = 0;
    try {
        // Use stod to handle both integer ("313") and decimal ("313.00") formats,
        // then truncate to uint32_t (the query uses strict >, so truncation is safe
        // since l_quantity values are integers and threshold is always integer).
        double qty_d = std::stod(args.QUANTITY);
        qty_threshold_int = static_cast<uint32_t>(qty_d);
    } catch (...) {
        throw std::runtime_error("run_q18: failed to parse QUANTITY: " + args.QUANTITY);
    }

    PROFILE_SCOPE("q18_total");

    const LineitemTable& li  = db->lineitem;
    const OrdersTable&   ord = db->orders;
    const CustomerTable& cst = db->customer;

    TRACE_COUNT("q18_total_lineitem_rows", li.num_rows);
    TRACE_COUNT("q18_total_order_rows",    ord.num_rows);

    const size_t n_orders = static_cast<size_t>(ord.num_rows);

    // ── 2. Subquery: scan lineitems (qty CSR) + main join ─────────────────────
    //
    // GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) is
    // effectively per-order because o_orderkey is unique per order row.

    const size_t cust_sz = db->cust_row_by_custkey.size();

    auto fmt_date = [](int32_t days) -> std::string {
        // days since Unix epoch → "YYYY-MM-DD"
        int32_t z   = days + 719468;
        int32_t era = (z >= 0 ? z : z - 146096) / 146097;
        int32_t doe = z - era * 146097;
        int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        int32_t y   = static_cast<int32_t>(yoe) + era * 400;
        int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        int32_t mp  = (5 * doy + 2) / 153;
        int32_t m   = mp < 10 ? mp + 3 : mp - 9;
        int32_t d   = doy - (153 * mp + 2) / 5 + 1;
        if (m <= 2) y += 1;
        char buf[11];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
        return std::string(buf);
    };

    auto fmt_price = [](double v) -> std::string {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << v;
        return oss.str();
    };

    struct ResultRow {
        std::string c_name;
        int64_t     c_custkey;
        int64_t     o_orderkey;
        int32_t     o_orderdate;
        double      o_totalprice;
        double      sum_qty;
    };

    std::vector<ResultRow> result;
    result.reserve(64);

    std::atomic<int64_t> n_qualifying{0};
    std::atomic<int64_t> cust_lookup_ok{0};

    // ── Fused parallel scan: o_li_qty_flat + AVX2 + customer join ───────────
    //
    // o_li_qty_flat stores 8 bytes per order (in o_orderdate-sorted order):
    //   bytes 0..cnt-1 = individual l_quantity values (uint8_t, 1-50)
    //   bytes cnt..7   = 0 (zero padding)
    //
    // No count byte: horizontal byte-sum of all 8 bytes = correct total.
    //
    // AVX2 path: _mm256_sad_epu8 processes 32 bytes (4 orders) per instruction.
    //   SAD gives per-8-byte sums in 4 × 64-bit lanes.
    //   _mm256_cmpgt_epi64 tests all 4 sums in one instruction.
    //   _mm256_testz_si256 skips the fallback when none qualify.
    // Scalar SWAR fallback for the tail (remaining < 4 orders).
    {
    PROFILE_SCOPE("q18_subquery_scan");

    const int n_threads = pool.num_threads;
    // Per-thread local result vectors to avoid contention
    std::vector<std::vector<ResultRow>> local_results(n_threads);
    for (auto& v : local_results) v.reserve(64 / n_threads + 1);

    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q18_subquery_scan_thread");

        const uint8_t* __restrict__ flat_ptr = ord.o_li_qty_flat.data();
        const int64_t* __restrict__ ck_ptr   = ord.o_custkey.data();
        const int64_t* __restrict__ ok_ptr   = ord.o_orderkey.data();
        const int32_t* __restrict__ od_ptr   = ord.o_orderdate.data();
        const double*  __restrict__ tp_ptr   = ord.o_totalprice.data();
        const int32_t* __restrict__ crk_ptr  = db->cust_row_by_custkey.data();
        const int64_t  cst_nrows             = cst.num_rows;
        const int64_t  cust_sz_i64           = static_cast<int64_t>(cust_sz);

        // Thread-local result accumulator
        std::vector<ResultRow>& local_result = local_results[tid];
        int64_t local_qualifying   = 0;
        int64_t local_cust_lookup_ok = 0;

        // Compute this thread's range [oi_begin, oi_end)
        const size_t chunk = (n_orders + static_cast<size_t>(nt) - 1) / static_cast<size_t>(nt);
        const size_t oi_begin = static_cast<size_t>(tid) * chunk;
        const size_t oi_end   = std::min(oi_begin + chunk, n_orders);

        // Helper: emit a single qualifying order into thread-local vector
        auto emit_order = [&](size_t oi, uint32_t sq) {
            ++local_qualifying;
            const int64_t custkey = ck_ptr[oi];
            if (__builtin_expect(custkey < 0 || custkey >= cust_sz_i64, 0)) return;
            const int32_t cust_row = crk_ptr[static_cast<size_t>(custkey)];
            if (__builtin_expect(cust_row < 0 || cust_row >= static_cast<int32_t>(cst_nrows), 0)) return;
            ++local_cust_lookup_ok;
            local_result.push_back({
                cst.c_name[static_cast<size_t>(cust_row)],
                custkey,
                ok_ptr[oi],
                od_ptr[oi],
                tp_ptr[oi],
                static_cast<double>(sq)
            });
        };

#ifdef __AVX2__
        // AVX2 vectorised path: 4 orders per iteration (32 bytes).
        // _mm256_sad_epu8(v, zero) gives per-8-byte horizontal sums in 4 × 64-bit lanes:
        //   lane 0: sum of order oi+0 qty bytes (bytes 0-7)
        //   lane 1: sum of order oi+1 qty bytes (bytes 8-15)
        //   lane 2: sum of order oi+2 qty bytes (bytes 16-23)
        //   lane 3: sum of order oi+3 qty bytes (bytes 24-31)
        const __m256i zero256  = _mm256_setzero_si256();
        const __m256i thresh256 = _mm256_set1_epi64x(static_cast<int64_t>(qty_threshold_int));

        size_t oi = oi_begin;
        const size_t n4 = oi_end & ~size_t(3);  // round down to multiple of 4 within this thread's range

        for (; oi < n4; oi += 4) {
            // Load 32 bytes = 4 orders
            __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(flat_ptr + oi * 8));
            // Horizontal byte sums per 8-byte lane (4 sums in 4 × 64-bit lanes)
            __m256i sums = _mm256_sad_epu8(v, zero256);
            // Compare sums > threshold
            __m256i cmp  = _mm256_cmpgt_epi64(sums, thresh256);
            // Fast path: if none qualify, skip (99.997% of iterations at typical thresholds)
            if (__builtin_expect(_mm256_testz_si256(cmp, cmp), 1)) continue;

            // At least one of the 4 orders qualifies — check individually
            // Extract sums to scalar for individual processing
            alignas(32) int64_t sums_arr[4];
            _mm256_store_si256(reinterpret_cast<__m256i*>(sums_arr), sums);
            for (int k = 0; k < 4; ++k) {
                if (static_cast<uint64_t>(sums_arr[k]) > qty_threshold_int)
                    emit_order(oi + static_cast<size_t>(k),
                               static_cast<uint32_t>(sums_arr[k]));
            }
        }

        // Scalar SWAR tail for remaining 0-3 orders
        for (; oi < oi_end; ++oi) {
            uint64_t word;
            memcpy(&word, flat_ptr + oi * 8, 8);
            uint64_t w = word;
            w = (w & UINT64_C(0x00FF00FF00FF00FF)) + ((w >> 8)  & UINT64_C(0x00FF00FF00FF00FF));
            w = (w & UINT64_C(0x0000FFFF0000FFFF)) + ((w >> 16) & UINT64_C(0x0000FFFF0000FFFF));
            w =  w + (w >> 32);
            const uint32_t sq = static_cast<uint32_t>(w & 0xFFFF);
            if (__builtin_expect(sq > qty_threshold_int, 0))
                emit_order(oi, sq);
        }

#else
        // Scalar SWAR fallback (no AVX2): 3-step horizontal byte-sum.
        // New layout: [q0..q6, 0] — no count byte; sum all 8 bytes directly.
        for (size_t oi = oi_begin; oi < oi_end; ++oi) {
            uint64_t word;
            memcpy(&word, flat_ptr + oi * 8, 8);
            uint64_t w = word;
            w = (w & UINT64_C(0x00FF00FF00FF00FF)) + ((w >> 8)  & UINT64_C(0x00FF00FF00FF00FF));
            w = (w & UINT64_C(0x0000FFFF0000FFFF)) + ((w >> 16) & UINT64_C(0x0000FFFF0000FFFF));
            w =  w + (w >> 32);
            const uint32_t sq = static_cast<uint32_t>(w & 0xFFFF);
            if (__builtin_expect(sq > qty_threshold_int, 0))
                emit_order(oi, sq);
        }
#endif

        // Accumulate thread-local counters into atomics
        n_qualifying.fetch_add(local_qualifying, std::memory_order_relaxed);
        cust_lookup_ok.fetch_add(local_cust_lookup_ok, std::memory_order_relaxed);
        TRACE_COUNT("q18_thread_qualifying", local_qualifying);
    }); // end parallel_for

    // Merge local results into global result vector
    {
        PROFILE_SCOPE("q18_merge_results");
        size_t total = 0;
        for (const auto& v : local_results) total += v.size();
        result.reserve(total);
        for (auto& v : local_results) {
            for (auto& r : v) result.push_back(std::move(r));
        }
    }

    } // end q18_subquery_scan (flat + AVX2 SIMD parallel)

    TRACE_COUNT("q18_cust_lookup_ok",   cust_lookup_ok.load());
    TRACE_COUNT("q18_qualifying_orderkeys", n_qualifying.load());
    TRACE_COUNT("q18_result_rows",      static_cast<long long>(result.size()));

    // ── 4. Sort: o_totalprice DESC, o_orderdate ASC ───────────────────────────
    {
    PROFILE_SCOPE("q18_sort");
    std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {  // NOLINT
        if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
        return a.o_orderdate < b.o_orderdate;
    });
    } // end q18_sort
    TRACE_COUNT("q18_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q18_sort_rows_out", static_cast<long long>(result.size()));

    // ── 5. Build output ───────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"c_name", "c_custkey", "o_orderkey", "o_orderdate", "o_totalprice", "sum(l_quantity)"});

    {
    PROFILE_SCOPE("q18_format_output");
    for (const auto& r : result) {
        rows.push_back({
            r.c_name,
            std::to_string(r.c_custkey),
            std::to_string(r.o_orderkey),
            fmt_date(r.o_orderdate),
            fmt_price(r.o_totalprice),
            fmt_price(r.sum_qty)
        });
    }
    } // end q18_format_output
    TRACE_COUNT("q18_query_output_rows", static_cast<long long>(rows.size()) - 1); // exclude header

    return rows;
}