#include "query6.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
#include <atomic>
#include <cstdint>
static ThreadPool& pool = get_query_pool();

#include <immintrin.h>
#include <cstring>
#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <cmath>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// SQL:
/**
select
    sum(l_extendedprice*l_discount) as revenue
from
    lineitem
where
    l_shipdate >= date '[DATE]'
    and l_shipdate < date '[DATE]' + interval '1' year
    and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    and l_quantity < [QUANTITY];
 */

// Convert "YYYY-MM-DD" to days since epoch (1970-01-01)
static int32_t date_to_days_q6(const std::string& s) {
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

// Add one year to a "YYYY-MM-DD" date string, returning days-since-epoch.
static int32_t date_plus_one_year_q6(const std::string& s) {
    int y = std::stoi(s.substr(0, 4));
    int m = std::stoi(s.substr(5, 2));
    int d = std::stoi(s.substr(8, 2));
    y += 1;
    char buf[11];
    std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
    return date_to_days_q6(std::string(buf));
}

// Format a double value as fixed-point with 2 decimal places
static std::string fmt_revenue(double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << v;
    return oss.str();
}

// ── AVX-512 hot kernel ─────────────────────────────────────────────────────────
// #pragma GCC push_options / target enables AVX-512 only for this function,
// without affecting header includes or the rest of the translation unit.
//
// Processes 32 rows per outer iteration (two 16-row groups, A and B) for ILP.
// Per 16-row group:
//   _mm512_cvtepi8_epi32   : load/widen 16 int8 disc → 16 int32  (1 XMM load)
//   _mm512_cvtepu8_epi32   : load/widen 16 uint8 qty → 16 int32  (1 XMM load)
//   _mm512_loadu_si512     : load 16 int32 ep                     (1 ZMM load)
//   _mm512_cmpge/le/lt_mask: 3 compares → k-register mask
//   _mm512_mullo_epi32     : ep × disc → int32  (product ≤ 104,950,000 < 2^31)
//   _mm512_mask_mov_epi32  : zero-blend (non-matching lanes → 0)
//   _mm512_cvtepi32_epi64  : widen 8 int32 → 8 int64 (×2, low and high halves)
//   _mm512_add_epi64       : accumulate into independent ZMM accumulators
// Row counting uses __builtin_popcount on the 16-bit k-mask (1 instruction).
#pragma GCC push_options
#pragma GCC target("avx512f,avx512bw,avx512vl,avx512dq,avx2,bmi,bmi2,popcnt")
#pragma GCC optimize("O3,unroll-loops")

static int64_t q6_avx512_kernel(
    const int8_t*  __restrict__ disc_ptr,
    const uint8_t* __restrict__ qty_ptr,
    const int32_t* __restrict__ ep_ptr,
    int64_t n,
    int32_t dlo, int32_t dhi, int32_t qmax,
    int64_t& rows_out)
{
    // Four ZMM int64 accumulator vectors (32 independent partial sums total).
    // Pairs (vacc0,vacc1) and (vacc2,vacc3) cover the two 16-row groups,
    // allowing the out-of-order engine to overlap their execution.
    __m512i vacc0 = _mm512_setzero_si512();
    __m512i vacc1 = _mm512_setzero_si512();
    __m512i vacc2 = _mm512_setzero_si512();
    __m512i vacc3 = _mm512_setzero_si512();

    const __m512i vdlo  = _mm512_set1_epi32(dlo);
    const __m512i vdhi  = _mm512_set1_epi32(dhi);
    const __m512i vqmax = _mm512_set1_epi32(qmax);
    const __m512i vzero = _mm512_setzero_si512();

    int64_t i = 0;
    int64_t cnt = 0;  // row count (scalar, incremented via popcount)

    // ── Main loop: 32 rows per iteration ─────────────────────────────────────
    const int64_t n32 = (n / 32) * 32;
    for (; i < n32; i += 32) {
        // ── Group A: rows [i, i+16) ───────────────────────────────────────
        // Load and widen: XMM holds 16 bytes; cvt extends to full ZMM (16 int32)
        __m512i vd32a = _mm512_cvtepi8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(disc_ptr + i)));
        __m512i vq32a = _mm512_cvtepu8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(qty_ptr + i)));
        __m512i vepa  = _mm512_loadu_si512(ep_ptr + i);  // 16 × int32 = 64 bytes

        // Predicate k-mask: disc ∈ [dlo, dhi] AND qty < qmax
        __mmask16 mka = (_mm512_cmpge_epi32_mask(vd32a, vdlo)
                       & _mm512_cmple_epi32_mask(vd32a, vdhi)
                       & _mm512_cmplt_epu32_mask(vq32a, vqmax));

        // ep × disc → int32 (vpmulld; low 32 bits; product fits since max 104M < 2^31)
        // Zero-blend: failing lanes become 0, contributing nothing to the sum.
        __m512i vma = _mm512_mask_mov_epi32(vzero, mka,
                          _mm512_mullo_epi32(vepa, vd32a));
        // Widen int32 → int64: low 8 lanes → vacc0, high 8 lanes → vacc1
        vacc0 = _mm512_add_epi64(vacc0,
                _mm512_cvtepi32_epi64(_mm512_castsi512_si256(vma)));
        vacc1 = _mm512_add_epi64(vacc1,
                _mm512_cvtepi32_epi64(_mm512_extracti32x8_epi32(vma, 1)));

        // Count matching rows using popcount on the 16-bit mask (1 scalar instr)
        cnt += __builtin_popcount(mka);

        // ── Group B: rows [i+16, i+32) ────────────────────────────────────
        __m512i vd32b = _mm512_cvtepi8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(disc_ptr + i + 16)));
        __m512i vq32b = _mm512_cvtepu8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(qty_ptr + i + 16)));
        __m512i vepb  = _mm512_loadu_si512(ep_ptr + i + 16);

        __mmask16 mkb = (_mm512_cmpge_epi32_mask(vd32b, vdlo)
                       & _mm512_cmple_epi32_mask(vd32b, vdhi)
                       & _mm512_cmplt_epu32_mask(vq32b, vqmax));

        __m512i vmb = _mm512_mask_mov_epi32(vzero, mkb,
                          _mm512_mullo_epi32(vepb, vd32b));
        vacc2 = _mm512_add_epi64(vacc2,
                _mm512_cvtepi32_epi64(_mm512_castsi512_si256(vmb)));
        vacc3 = _mm512_add_epi64(vacc3,
                _mm512_cvtepi32_epi64(_mm512_extracti32x8_epi32(vmb, 1)));

        cnt += __builtin_popcount(mkb);
    }

    // ── Remaining 16-row block (if n not a multiple of 32) ────────────────────
    const int64_t n16 = (n / 16) * 16;
    for (; i < n16; i += 16) {
        __m512i vd32 = _mm512_cvtepi8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(disc_ptr + i)));
        __m512i vq32 = _mm512_cvtepu8_epi32(
            _mm_loadu_si128(reinterpret_cast<const __m128i*>(qty_ptr + i)));
        __m512i vep  = _mm512_loadu_si512(ep_ptr + i);
        __mmask16 mk = (_mm512_cmpge_epi32_mask(vd32, vdlo)
                      & _mm512_cmple_epi32_mask(vd32, vdhi)
                      & _mm512_cmplt_epu32_mask(vq32, vqmax));
        __m512i vm = _mm512_mask_mov_epi32(vzero, mk,
                         _mm512_mullo_epi32(vep, vd32));
        vacc0 = _mm512_add_epi64(vacc0,
                _mm512_cvtepi32_epi64(_mm512_castsi512_si256(vm)));
        vacc1 = _mm512_add_epi64(vacc1,
                _mm512_cvtepi32_epi64(_mm512_extracti32x8_epi32(vm, 1)));
        cnt += __builtin_popcount(mk);
    }

    // ── Horizontal reduce: sum all 32 int64 partial sums ─────────────────────
    int64_t revenue_scaled = _mm512_reduce_add_epi64(
        _mm512_add_epi64(_mm512_add_epi64(vacc0, vacc1),
                         _mm512_add_epi64(vacc2, vacc3)));
    rows_out = cnt;

    // ── Scalar tail for remaining < 16 rows ───────────────────────────────────
    for (; i < n; ++i) {
        int32_t d = (int32_t)disc_ptr[i];
        int32_t q = (int32_t)qty_ptr[i];
        int64_t m = (int64_t)((d >= dlo) & (d <= dhi) & (q < qmax));
        revenue_scaled += m * ((int64_t)ep_ptr[i] * d);
        rows_out += m;
    }

    return revenue_scaled;
}

#pragma GCC pop_options
// ─────────────────────────────────────────────────────────────────────────────

std::vector<std::vector<std::string>> run_q6(Database* db, const Q6Args& args) {
    if (!db) {
        throw std::runtime_error("run_q6: db is null");
    }

    PROFILE_SCOPE("q6_total");

    // Parse arguments
    int32_t date_start = date_to_days_q6(args.DATE);
    int32_t date_end   = date_plus_one_year_q6(args.DATE);  // exclusive upper bound

    double discount_mid = std::stod(args.DISCOUNT);
    // Convert discount range to integer (×100) for branchless int32 comparison.
    // e.g. DISCOUNT=0.09 → disc_lo_i=8, disc_hi_i=10  (inclusive on both ends)
    int32_t disc_lo_i = static_cast<int32_t>(std::round((discount_mid - 0.01) * 100.0));
    int32_t disc_hi_i = static_cast<int32_t>(std::round((discount_mid + 0.01) * 100.0));

    // l_quantity < QUANTITY (strict). Quantity is integer-valued 1-50.
    int32_t qty_max_i = static_cast<int32_t>(std::stod(args.QUANTITY));

    const LineitemTable& li = db->lineitem;

    // ── Binary-search the sorted l_shipdate array for the date range ──────────
    int64_t lo_idx = 0, hi_idx = 0;
    {
        PROFILE_SCOPE("q6_scan_bisect");
        auto lo_it = std::lower_bound(li.l_shipdate.begin(), li.l_shipdate.end(), date_start);
        auto hi_it = std::lower_bound(li.l_shipdate.begin(), li.l_shipdate.end(), date_end);
        lo_idx = static_cast<int64_t>(lo_it - li.l_shipdate.begin());
        hi_idx = static_cast<int64_t>(hi_it - li.l_shipdate.begin());
    }

    int64_t total_li_rows    = static_cast<int64_t>(li.l_shipdate.size());
    int64_t li_rows_in_range = hi_idx - lo_idx;
    TRACE_COUNT("q6_total_lineitem_rows",   total_li_rows);
    TRACE_COUNT("q6_li_rows_in_range",      li_rows_in_range);
    TRACE_COUNT("q6_li_rows_filtered_out",  total_li_rows - li_rows_in_range);
    TRACE_COUNT("q6_disc_lo_i",  (long long)disc_lo_i);
    TRACE_COUNT("q6_disc_hi_i",  (long long)disc_hi_i);
    TRACE_COUNT("q6_qty_max_i",  (long long)qty_max_i);

    // ── Predicate scan and revenue accumulation ────────────────────────────────
    // Parallel morsel-driven approach:
    //   - Divide the date-range rows [lo_idx, hi_idx) across all threads.
    //   - Each thread accumulates a local int64 partial sum using the AVX-512 kernel.
    //   - Final merge: sum all per-thread partial sums (sequential, trivial cost).
    // No synchronization within the hot path; lock-free by design.

    const int n_threads = pool.num_threads;
    const int64_t n_rows = hi_idx - lo_idx;

    // Per-thread accumulators (cache-line aligned to avoid false sharing)
    struct alignas(64) ThreadResult {
        int64_t revenue_scaled = 0;
        int64_t rows_emitted   = 0;
    };
    std::vector<ThreadResult> thread_results(n_threads);

    const int8_t*  disc_base = li.l_discount_int8.data()       + lo_idx;
    const uint8_t* qty_base  = li.l_quantity_int8.data()       + lo_idx;
    const int32_t* ep_base   = li.l_extendedprice_int32.data() + lo_idx;

    {
        PROFILE_SCOPE("q6_agg_loop_parallel");

        pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q6_worker_task");

            // Partition rows evenly across threads.
            // Align chunk boundaries to 32 (AVX-512 kernel processes 32 rows/iter)
            // so no thread ever straddles a 32-row group straddling boundary.
            const int64_t chunk = (n_rows + n_thr - 1) / n_thr;
            // Align chunk to 32-row boundary to keep AVX-512 loop efficient.
            const int64_t aligned_chunk = ((chunk + 31) / 32) * 32;

            const int64_t row_start = static_cast<int64_t>(tid) * aligned_chunk;
            if (row_start >= n_rows) {
                thread_results[tid].revenue_scaled = 0;
                thread_results[tid].rows_emitted   = 0;
                return;
            }
            const int64_t row_end = std::min(row_start + aligned_chunk, n_rows);
            const int64_t count   = row_end - row_start;

            int64_t local_rows = 0;
            int64_t local_rev  = q6_avx512_kernel(
                disc_base + row_start,
                qty_base  + row_start,
                ep_base   + row_start,
                count,
                disc_lo_i, disc_hi_i, qty_max_i,
                local_rows);

            thread_results[tid].revenue_scaled = local_rev;
            thread_results[tid].rows_emitted   = local_rows;

            TRACE_COUNT("q6_worker_rows_scanned", (long long)count);
            TRACE_COUNT("q6_worker_rows_emitted", (long long)local_rows);
        });
    }

    // Sequential merge of per-thread results
    int64_t revenue_scaled  = 0;
    int64_t li_rows_emitted = 0;
    for (int t = 0; t < n_threads; ++t) {
        revenue_scaled  += thread_results[t].revenue_scaled;
        li_rows_emitted += thread_results[t].rows_emitted;
    }

    TRACE_COUNT("q6_li_rows_scanned", li_rows_in_range);
    TRACE_COUNT("q6_li_rows_emitted", li_rows_emitted);

    // Convert: ep×100 * disc×100 = rev×10000 → divide by 10000.
    double revenue = static_cast<double>(revenue_scaled) / 10000.0;

    // Build output
    std::vector<std::vector<std::string>> rows;
    {
        PROFILE_SCOPE("q6_format_output");
        rows.push_back({"revenue"});
        rows.push_back({fmt_revenue(revenue)});
    }
    TRACE_COUNT("q6_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}