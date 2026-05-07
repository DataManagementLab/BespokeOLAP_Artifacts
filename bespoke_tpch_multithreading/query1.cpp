#include "query1.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
#include <mutex>
static ThreadPool& pool = get_query_pool();

#include <immintrin.h>
#include <cstring>
#include <algorithm>
#include <array>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <cmath>

// SQL:
// select
//     l_returnflag, l_linestatus,
//     sum(l_quantity) as sum_qty,
//     sum(l_extendedprice) as sum_base_price,
//     sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
//     sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
//     avg(l_quantity) as avg_qty,
//     avg(l_extendedprice) as avg_price,
//     avg(l_discount) as avg_disc,
//     count(*) as count_order
// from lineitem
// where l_shipdate <= date '1998-12-01' - interval '[DELTA]' day
// group by l_returnflag, l_linestatus
// order by l_returnflag, l_linestatus

// Helper: convert "YYYY-MM-DD" to days since epoch (1970-01-01)
static int32_t date_to_days(const std::string& s) {
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

static std::string fmt_money(double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << v;
    return oss.str();
}

static std::string fmt_avg(double v) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(6) << v;
    return oss.str();
}

// ─────────────────────────────────────────────────────────────────────────────
// Narrow disc+tax AVX-512 aggregation kernel — 2×-unrolled.
//
// Bandwidth reduction vs. the original all-doubles kernel:
//   Old: rf(1)+ls(1)+qty(1)+ep_dbl(8)+disc_dbl(8)+tax_dbl(8) = 27 B/row → ~3.2 GB
//   New: rf(1)+ls(1)+qty(1)+ep_dbl(8)+disc_i8(1)+tax_i8(1)   = 13 B/row → ~1.6 GB
//   ~2.1× bandwidth reduction; l_extendedprice kept as double for precision.
//
// Technique: disc/tax are loaded as int8/uint8 and converted to double (×0.01)
// before the FNM/FMA.  The conversion chain (i8→i32→pd + fmul) has ~10 cycles
// of latency.  The 2× loop unroll hides this: iteration B's conversions are
// issued while iteration A's FNM/FMA is executing, so the two chains overlap.
//
// Accumulator layout: same 36-zmm structure as the original FP kernel
// (6 slots × 6 zmm each).  The per-slot masked-add strategy is unchanged;
// only the disc/tax source narrowed from double loads to i8 + convert.
// ─────────────────────────────────────────────────────────────────────────────

[[gnu::target("avx512f,avx512dq,avx512vl,avx512bw,avx2")]]
__attribute__((noinline))
static void q1_agg_avx512(
    const uint8_t* __restrict__ rf_ptr,
    const uint8_t* __restrict__ ls_ptr,
    const uint8_t* __restrict__ qty_ptr,
    const double*  __restrict__ ep_ptr,
    const int8_t*  __restrict__ disc_ptr,   // l_discount_int8: disc*100 as int8
    const uint8_t* __restrict__ tax_ptr,    // l_tax_int8:      tax*100 as uint8
    int64_t  start_idx,
    int64_t  end_idx,
    double   s_qty [6],
    double   s_ep  [6],
    double   s_dp  [6],
    double   s_ch  [6],
    double   s_disc[6],
    int64_t  s_cnt [6],
    bool     slot_used[6]
)
{
    // 36 zmm FP accumulators (6 slots × 6 fields: qty,ep,dp,ch,disc,cnt)
    __m512d aq0=_mm512_setzero_pd(), aq1=_mm512_setzero_pd(),
            aq2=_mm512_setzero_pd(), aq3=_mm512_setzero_pd(),
            aq4=_mm512_setzero_pd(), aq5=_mm512_setzero_pd();
    __m512d ae0=_mm512_setzero_pd(), ae1=_mm512_setzero_pd(),
            ae2=_mm512_setzero_pd(), ae3=_mm512_setzero_pd(),
            ae4=_mm512_setzero_pd(), ae5=_mm512_setzero_pd();
    __m512d ad0=_mm512_setzero_pd(), ad1=_mm512_setzero_pd(),
            ad2=_mm512_setzero_pd(), ad3=_mm512_setzero_pd(),
            ad4=_mm512_setzero_pd(), ad5=_mm512_setzero_pd();
    __m512d ac0=_mm512_setzero_pd(), ac1=_mm512_setzero_pd(),
            ac2=_mm512_setzero_pd(), ac3=_mm512_setzero_pd(),
            ac4=_mm512_setzero_pd(), ac5=_mm512_setzero_pd();
    __m512d ai0=_mm512_setzero_pd(), ai1=_mm512_setzero_pd(),
            ai2=_mm512_setzero_pd(), ai3=_mm512_setzero_pd(),
            ai4=_mm512_setzero_pd(), ai5=_mm512_setzero_pd();
    __m512i cn0=_mm512_setzero_si512(), cn1=_mm512_setzero_si512(),
            cn2=_mm512_setzero_si512(), cn3=_mm512_setzero_si512(),
            cn4=_mm512_setzero_si512(), cn5=_mm512_setzero_si512();

    const __m512d v_inv100 = _mm512_set1_pd(0.01);   // 1/100: int8 → fraction
    const __m512i v_ones   = _mm512_set1_epi64(1LL);
    const __m512i vs0 = _mm512_set1_epi64(0LL);
    const __m512i vs1 = _mm512_set1_epi64(1LL);
    const __m512i vs2 = _mm512_set1_epi64(2LL);
    const __m512i vs3 = _mm512_set1_epi64(3LL);
    const __m512i vs4 = _mm512_set1_epi64(4LL);
    const __m512i vs5 = _mm512_set1_epi64(5LL);

    // Helper macros: load 8 int8/uint8 → double × 0.01
    // disc (int8, signed, range 0-10): cvtepi8→epi32 → cvtepi32→pd → ×0.01
    // tax  (uint8, unsigned, range 0-8): cvtepu8→epi32 → cvtepu32→pd → ×0.01
    #define LOAD_DISC8(ptr, off) \
        _mm512_mul_pd( \
            _mm512_cvtepi32_pd(_mm256_cvtepi8_epi32( \
                _mm_loadl_epi64((const __m128i*)((ptr)+(off))))), \
            v_inv100)
    #define LOAD_TAX8(ptr, off) \
        _mm512_mul_pd( \
            _mm512_cvtepu32_pd(_mm256_cvtepu8_epi32( \
                _mm_loadl_epi64((const __m128i*)((ptr)+(off))))), \
            v_inv100)

    // Per-slot masked accumulation macro (identical to original)
    #define DO_SLOT(S, vslot, vqty, vep, vdp, vch, vdisc) \
    { __mmask8 m = _mm512_cmpeq_epi64_mask(vslot, vs##S); \
      if (m) { \
          slot_used[S]  = true; \
          aq##S = _mm512_mask_add_pd(aq##S, m, aq##S, vqty); \
          ae##S = _mm512_mask_add_pd(ae##S, m, ae##S, vep); \
          ad##S = _mm512_mask_add_pd(ad##S, m, ad##S, vdp); \
          ac##S = _mm512_mask_add_pd(ac##S, m, ac##S, vch); \
          ai##S = _mm512_mask_add_pd(ai##S, m, ai##S, vdisc); \
          cn##S = _mm512_mask_add_epi64(cn##S, m, cn##S, v_ones); \
      } \
    }

    int64_t i    = start_idx;
    int64_t endN = end_idx - 15;   // safe for 2×8-row unrolled block

    // Advance i to a multiple of 8 for aligned AVX loads (optional but clean)
    // Actually keep start_idx as-is; the kernels use unaligned loads already.

    for (; i < endN; i += 16) {
        __builtin_prefetch(ep_ptr   + i + 64, 0, 0);
        __builtin_prefetch(rf_ptr   + i + 64, 0, 0);
        __builtin_prefetch(disc_ptr + i + 64, 0, 0);

        // ── Iteration A: rows i..i+7 ────────────────────────────────────────
        __m512i va_rf   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(rf_ptr  + i)));
        __m512i va_ls   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(ls_ptr  + i)));
        __m512i va_slot = _mm512_add_epi64(_mm512_slli_epi64(va_rf, 1), va_ls);
        __m512d va_qty  = _mm512_cvtepu32_pd(
            _mm256_cvtepu8_epi32(_mm_loadl_epi64((const __m128i*)(qty_ptr + i))));
        __m512d va_ep   = _mm512_loadu_pd(ep_ptr + i);
        // Start A's disc/tax conversions early (long latency chain: ~10 cycles)
        __m512d va_disc = LOAD_DISC8(disc_ptr, i);
        __m512d va_tax  = LOAD_TAX8(tax_ptr,   i);

        // ── Iteration B: rows i+8..i+15 — issued while A's conversions finish ─
        __m512i vb_rf   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(rf_ptr  + i+8)));
        __m512i vb_ls   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(ls_ptr  + i+8)));
        __m512i vb_slot = _mm512_add_epi64(_mm512_slli_epi64(vb_rf, 1), vb_ls);
        __m512d vb_qty  = _mm512_cvtepu32_pd(
            _mm256_cvtepu8_epi32(_mm_loadl_epi64((const __m128i*)(qty_ptr + i+8))));
        __m512d vb_ep   = _mm512_loadu_pd(ep_ptr + i+8);
        // Start B's conversions — overlaps with A's FNM/FMA below
        __m512d vb_disc = LOAD_DISC8(disc_ptr, i+8);
        __m512d vb_tax  = LOAD_TAX8(tax_ptr,   i+8);

        // ── Compute A ────────────────────────────────────────────────────────
        __m512d va_dp = _mm512_fnmadd_pd(va_ep, va_disc, va_ep);   // ep - ep*disc
        __m512d va_ch = _mm512_fmadd_pd(va_dp, va_tax,  va_dp);    // dp + dp*tax

        // ── Accumulate A (6 slots) ───────────────────────────────────────────
        DO_SLOT(0, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)
        DO_SLOT(1, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)
        DO_SLOT(2, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)
        DO_SLOT(3, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)
        DO_SLOT(4, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)
        DO_SLOT(5, va_slot, va_qty, va_ep, va_dp, va_ch, va_disc)

        // ── Compute B ────────────────────────────────────────────────────────
        __m512d vb_dp = _mm512_fnmadd_pd(vb_ep, vb_disc, vb_ep);
        __m512d vb_ch = _mm512_fmadd_pd(vb_dp, vb_tax,  vb_dp);

        // ── Accumulate B (6 slots) ───────────────────────────────────────────
        DO_SLOT(0, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
        DO_SLOT(1, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
        DO_SLOT(2, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
        DO_SLOT(3, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
        DO_SLOT(4, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
        DO_SLOT(5, vb_slot, vb_qty, vb_ep, vb_dp, vb_ch, vb_disc)
    }

    // Handle any remaining 8-row block (0 or 1)
    for (; i < end_idx - 7; i += 8) {
        __m512i v_rf   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(rf_ptr  + i)));
        __m512i v_ls   = _mm512_cvtepu8_epi64(_mm_loadl_epi64((const __m128i*)(ls_ptr  + i)));
        __m512i v_slot = _mm512_add_epi64(_mm512_slli_epi64(v_rf, 1), v_ls);
        __m512d v_qty  = _mm512_cvtepu32_pd(
            _mm256_cvtepu8_epi32(_mm_loadl_epi64((const __m128i*)(qty_ptr + i))));
        __m512d v_ep   = _mm512_loadu_pd(ep_ptr + i);
        __m512d v_disc = LOAD_DISC8(disc_ptr, i);
        __m512d v_tax  = LOAD_TAX8(tax_ptr,   i);
        __m512d v_dp   = _mm512_fnmadd_pd(v_ep, v_disc, v_ep);
        __m512d v_ch   = _mm512_fmadd_pd(v_dp, v_tax, v_dp);
        DO_SLOT(0, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
        DO_SLOT(1, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
        DO_SLOT(2, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
        DO_SLOT(3, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
        DO_SLOT(4, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
        DO_SLOT(5, v_slot, v_qty, v_ep, v_dp, v_ch, v_disc)
    }

    #undef LOAD_DISC8
    #undef LOAD_TAX8
    #undef DO_SLOT

    // Horizontal reduce: sum 8 zmm lanes → scalar per slot
    s_qty [0] += _mm512_reduce_add_pd(aq0);  s_ep  [0] += _mm512_reduce_add_pd(ae0);
    s_dp  [0] += _mm512_reduce_add_pd(ad0);  s_ch  [0] += _mm512_reduce_add_pd(ac0);
    s_disc[0] += _mm512_reduce_add_pd(ai0);  s_cnt [0] += _mm512_reduce_add_epi64(cn0);
    s_qty [1] += _mm512_reduce_add_pd(aq1);  s_ep  [1] += _mm512_reduce_add_pd(ae1);
    s_dp  [1] += _mm512_reduce_add_pd(ad1);  s_ch  [1] += _mm512_reduce_add_pd(ac1);
    s_disc[1] += _mm512_reduce_add_pd(ai1);  s_cnt [1] += _mm512_reduce_add_epi64(cn1);
    s_qty [2] += _mm512_reduce_add_pd(aq2);  s_ep  [2] += _mm512_reduce_add_pd(ae2);
    s_dp  [2] += _mm512_reduce_add_pd(ad2);  s_ch  [2] += _mm512_reduce_add_pd(ac2);
    s_disc[2] += _mm512_reduce_add_pd(ai2);  s_cnt [2] += _mm512_reduce_add_epi64(cn2);
    s_qty [3] += _mm512_reduce_add_pd(aq3);  s_ep  [3] += _mm512_reduce_add_pd(ae3);
    s_dp  [3] += _mm512_reduce_add_pd(ad3);  s_ch  [3] += _mm512_reduce_add_pd(ac3);
    s_disc[3] += _mm512_reduce_add_pd(ai3);  s_cnt [3] += _mm512_reduce_add_epi64(cn3);
    s_qty [4] += _mm512_reduce_add_pd(aq4);  s_ep  [4] += _mm512_reduce_add_pd(ae4);
    s_dp  [4] += _mm512_reduce_add_pd(ad4);  s_ch  [4] += _mm512_reduce_add_pd(ac4);
    s_disc[4] += _mm512_reduce_add_pd(ai4);  s_cnt [4] += _mm512_reduce_add_epi64(cn4);
    s_qty [5] += _mm512_reduce_add_pd(aq5);  s_ep  [5] += _mm512_reduce_add_pd(ae5);
    s_dp  [5] += _mm512_reduce_add_pd(ad5);  s_ch  [5] += _mm512_reduce_add_pd(ac5);
    s_disc[5] += _mm512_reduce_add_pd(ai5);  s_cnt [5] += _mm512_reduce_add_epi64(cn5);

    // Scalar tail (< 8 rows remaining)
    for (; i < end_idx; ++i) {
        int    slot = (int)rf_ptr[i] * 2 + (int)ls_ptr[i];
        double ep   = ep_ptr[i];
        double d    = (uint8_t)disc_ptr[i] * 0.01;   // cast via uint8 to avoid sign issues
        double t    = tax_ptr[i] * 0.01;
        double dp   = ep * (1.0 - d);
        s_qty [slot] += (double)qty_ptr[i];
        s_ep  [slot] += ep;
        s_dp  [slot] += dp;
        s_ch  [slot] += dp * (1.0 + t);
        s_disc[slot] += d;
        s_cnt [slot] += 1;
        slot_used[slot] = true;
    }
}

std::vector<std::vector<std::string>> run_q1(Database* db, const Q1Args& args) {
    if (!db) throw std::runtime_error("run_q1: db is null");

    PROFILE_SCOPE("q1_total");

    int32_t delta     = std::stoi(args.DELTA);
    int32_t base_date = date_to_days("1998-12-01");
    int32_t cutoff    = base_date - delta;

    const LineitemTable& li = db->lineitem;

    // l_shipdate is sorted ascending; binary search for upper bound of cutoff.
    int64_t end_idx = 0;
    {
        PROFILE_SCOPE("q1_scan_bisect");
        auto it = std::upper_bound(li.l_shipdate.begin(), li.l_shipdate.end(), cutoff);
        end_idx = static_cast<int64_t>(it - li.l_shipdate.begin());
    }
    int64_t total_li_rows = static_cast<int64_t>(li.l_shipdate.size());
    TRACE_COUNT("q1_rows_scanned",      total_li_rows);
    TRACE_COUNT("q1_rows_emitted",      end_idx);
    TRACE_COUNT("q1_rows_filtered_out", total_li_rows - end_idx);

    static constexpr int NUM_SLOTS = 6;

    int n_threads = pool.num_threads;

    // Per-thread local accumulators (padded to 64B to avoid false sharing)
    struct alignas(64) ThreadAcc {
        double  s_qty [NUM_SLOTS] = {};
        double  s_ep  [NUM_SLOTS] = {};
        double  s_dp  [NUM_SLOTS] = {};
        double  s_ch  [NUM_SLOTS] = {};
        double  s_disc[NUM_SLOTS] = {};
        int64_t s_cnt [NUM_SLOTS] = {};
        bool    slot_used[NUM_SLOTS] = {};
    };
    std::vector<ThreadAcc> thread_accs(n_threads);

    {
        PROFILE_SCOPE("q1_agg_loop");
        // Parallel morsel-driven aggregation:
        // Split [0, end_idx) into n_threads equal-sized ranges.
        // Each thread runs the AVX-512 kernel on its range with thread-local accumulators.
        // Final merge is sequential (only 6 slots × 6 fields = 36 values).
        pool.parallel_for([&](int tid, int n) {
            PROFILE_SCOPE("q1_agg_thread");
            // Compute this thread's range
            int64_t chunk = (end_idx + n - 1) / n;
            int64_t t_start = tid * chunk;
            int64_t t_end   = std::min(t_start + chunk, end_idx);
            if (t_start >= end_idx) return;

            ThreadAcc& acc = thread_accs[tid];
            q1_agg_avx512(
                li.l_returnflag.data(),
                li.l_linestatus.data(),
                li.l_quantity_int8.data(),
                li.l_extendedprice.data(),
                li.l_discount_int8.data(),
                li.l_tax_int8.data(),
                t_start,
                t_end,
                acc.s_qty, acc.s_ep, acc.s_dp, acc.s_ch, acc.s_disc, acc.s_cnt, acc.slot_used
            );
            TRACE_COUNT("q1_agg_thread_rows", t_end - t_start);
        });
    }

    // Sequential merge of per-thread accumulators
    alignas(64) double  s_qty [NUM_SLOTS] = {};
    alignas(64) double  s_ep  [NUM_SLOTS] = {};
    alignas(64) double  s_dp  [NUM_SLOTS] = {};
    alignas(64) double  s_ch  [NUM_SLOTS] = {};
    alignas(64) double  s_disc[NUM_SLOTS] = {};
    alignas(64) int64_t s_cnt [NUM_SLOTS] = {};
    bool slot_used[NUM_SLOTS] = {};
    {
        PROFILE_SCOPE("q1_merge");
        for (int t = 0; t < n_threads; ++t) {
            const ThreadAcc& acc = thread_accs[t];
            for (int s = 0; s < NUM_SLOTS; ++s) {
                s_qty [s] += acc.s_qty [s];
                s_ep  [s] += acc.s_ep  [s];
                s_dp  [s] += acc.s_dp  [s];
                s_ch  [s] += acc.s_ch  [s];
                s_disc[s] += acc.s_disc[s];
                s_cnt [s] += acc.s_cnt [s];
                slot_used[s] = slot_used[s] || acc.slot_used[s];
            }
        }
    }

    int num_groups = 0;
    for (int s = 0; s < NUM_SLOTS; ++s) if (slot_used[s]) ++num_groups;

    TRACE_COUNT("q1_agg_rows_in",      end_idx);
    TRACE_COUNT("q1_groups_created",   num_groups);
    TRACE_COUNT("q1_agg_rows_emitted", num_groups);

    struct Agg {
        double  sum_qty=0, sum_base_price=0, sum_disc_price=0, sum_charge=0, sum_disc=0;
        int64_t count=0;
    };
    Agg agg_arr[NUM_SLOTS];
    for (int s = 0; s < NUM_SLOTS; ++s)
        agg_arr[s] = {s_qty[s], s_ep[s], s_dp[s], s_ch[s], s_disc[s], s_cnt[s]};

    std::vector<std::vector<std::string>> result_rows;
    result_rows.push_back({
        "l_returnflag","l_linestatus",
        "sum_qty","sum_base_price","sum_disc_price","sum_charge",
        "avg_qty","avg_price","avg_disc","count_order"
    });

    struct GroupRow { std::string rf_name, ls_name; Agg agg; };
    std::vector<GroupRow> groups;
    groups.reserve(NUM_SLOTS);

    {
        PROFILE_SCOPE("q1_sort");
        for (int s = 0; s < NUM_SLOTS; ++s) {
            if (!slot_used[s]) continue;
            groups.push_back({li.returnflag_names[s/2], li.linestatus_names[s%2], agg_arr[s]});
        }
        std::sort(groups.begin(), groups.end(), [](const GroupRow& a, const GroupRow& b){
            return (a.rf_name != b.rf_name) ? a.rf_name < b.rf_name : a.ls_name < b.ls_name;
        });
    }
    TRACE_COUNT("q1_sort_rows_in",  (long long)groups.size());
    TRACE_COUNT("q1_sort_rows_out", (long long)groups.size());
    {
        PROFILE_SCOPE("q1_format_output");
        for (const auto& g : groups) {
            const Agg& a = g.agg;
            double cnt = (double)a.count;
            result_rows.push_back({
                g.rf_name, g.ls_name,
                fmt_money(a.sum_qty),        fmt_money(a.sum_base_price),
                fmt_money(a.sum_disc_price), fmt_money(a.sum_charge),
                fmt_avg(a.sum_qty/cnt),      fmt_avg(a.sum_base_price/cnt),
                fmt_avg(a.sum_disc/cnt),     std::to_string(a.count)
            });
        }
    }
    TRACE_COUNT("q1_query_output_rows", (long long)result_rows.size()-1);

    return result_rows;
}