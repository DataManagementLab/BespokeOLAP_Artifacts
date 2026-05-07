#include "query14.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();
#include <immintrin.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <numeric>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

// SQL:
/** select 
    100.00 * sum(case  
        when p_type like 'PROMO%' 
        then l_extendedprice*(1-l_discount) 
        else 0 
    end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue 
from  
    lineitem,  
    part 
where  
    l_partkey = p_partkey 
    and l_shipdate >= date '[DATE]' 
    and l_shipdate < date '[DATE]' + interval '1' month;  */

std::vector<std::vector<std::string>> run_q14(Database* db, const Q14Args& args) {
    if (!db) {
        throw std::runtime_error("run_q14: db is null");
    }

    // 1. Parse the date argument
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

    // Add one calendar month to an epoch-days value.
    auto add_months = [](int32_t days, int months) -> int32_t {
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

        int nm = static_cast<int>(mon) + months;
        int ny = static_cast<int>(y);
        while (nm > 12) { nm -= 12; ny += 1; }
        while (nm <  1) { nm += 12; ny -= 1; }

        static const int dim_table[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
        auto is_leap = [](int yr) -> bool {
            return (yr % 4 == 0 && yr % 100 != 0) || (yr % 400 == 0);
        };
        int dim = dim_table[nm];
        if (nm == 2 && is_leap(ny)) dim = 29;
        int nd = static_cast<int>(dom) > dim ? dim : static_cast<int>(dom);

        if (nm <= 2) { ny -= 1; nm += 12; }
        int era2 = (ny >= 0 ? ny : ny - 399) / 400;
        int yoe2 = ny - era2 * 400;
        int doy2 = (153 * (nm - 3) + 2) / 5 + nd - 1;
        int doe2 = yoe2 * 365 + yoe2 / 4 - yoe2 / 100 + doy2;
        return static_cast<int32_t>(era2 * 146097 + doe2 - 719468);
    };

    int32_t date_lo = date_to_days(args.DATE);   // l_shipdate >= DATE
    int32_t date_hi = add_months(date_lo, 1);    // l_shipdate <  DATE + 1 month

    // 2. Unpack table references
    const LineitemTable& li = db->lineitem;

    PROFILE_SCOPE("q14_total");

    int64_t nrows = li.num_rows;
    TRACE_COUNT("q14_total_lineitem_rows", nrows);

    // 3. Binary-search date range in sorted l_shipdate (sorted ascending by db_loader).
    const int32_t* shipdate_data = li.l_shipdate.data();

    int64_t lo_idx = static_cast<int64_t>(
        std::lower_bound(shipdate_data, shipdate_data + nrows, date_lo) - shipdate_data);
    int64_t hi_idx = static_cast<int64_t>(
        std::lower_bound(shipdate_data + lo_idx, shipdate_data + nrows, date_hi) - shipdate_data);

    TRACE_COUNT("q14_date_range_lo_idx", lo_idx);
    TRACE_COUNT("q14_date_range_hi_idx", hi_idx);
    int64_t range_sz = hi_idx - lo_idx;
    TRACE_COUNT("q14_date_range_size", range_sz);

    // 4. Scan date-filtered range in parallel using pre-joined SoA arrays.
    //
    //    Three separate SoA arrays, all purely sequential in l_shipdate order:
    //      ep_int32[i]    = l_extendedprice * 100 (int32, ~6MB for 1.5M rows at SF=20)
    //      dis_int8[i]    = l_discount * 100 (int8, ~1.5MB)
    //      is_promo_u8[i] = 1 if p_type LIKE 'PROMO%', else 0 (pre-joined at load, ~1.5MB)
    //
    //    Parallelism: each thread owns a disjoint sub-range [t_lo, t_hi).
    //    Per-thread accumulators are merged sequentially after parallel_for.
    //
    //    Integer arithmetic (no FP multiply): val = ep * (100-dis) [1e-4 dollars]
    //    The ratio 100*sum_promo/sum_total is scale-invariant.
    //
    //    AVX2 path: 8 rows per iteration using 256-bit ep load + 64-bit dis/pro loads.
    //      - 3 loads per 8 rows (32B ep + 8B dis + 8B pro = 48 bytes / 8 rows = 6 B/row)
    //      - val8 = ep8 * (100 - dis8) as int32x8 (max 1.05e9 < 2^30, no overflow)
    //      - Widen val to int64x4+int64x4 for safe accumulation
    //      - mask8 = -(int32)pro8 => 0 or -1 per lane (branchless promo selection)
    //    Scalar fallback for tail (<8 rows) and non-AVX2 systems.
    int64_t sum_promo_i64 = 0;
    int64_t sum_total_i64 = 0;

    {
        PROFILE_SCOPE("q14_scan_loop");

        // Per-thread accumulators (cache-line separated to avoid false sharing)
        const int n_threads = pool.num_threads;
        struct alignas(64) ThreadAcc {
            int64_t sum_promo = 0;
            int64_t sum_total = 0;
            int64_t promo_pass = 0;
        };
        std::vector<ThreadAcc> accs(static_cast<size_t>(n_threads));

        const int32_t* ep_base  = li.l_extendedprice_int32.data() + lo_idx;
        const int8_t*  dis_base = li.l_discount_int8.data()       + lo_idx;
        const uint8_t* pro_base = li.li_is_promo_u8.data()        + lo_idx;
        const int64_t count = range_sz;

        pool.parallel_for([&](int tid, int n) {
            // Compute this thread's disjoint sub-range.
            int64_t t_lo = (count * tid)     / n;
            int64_t t_hi = (count * (tid+1)) / n;
            int64_t t_count = t_hi - t_lo;

            if (t_count <= 0) return;

            const int32_t* __restrict__ ep_data  = ep_base  + t_lo;
            const int8_t*  __restrict__ dis_data = dis_base + t_lo;
            const uint8_t* __restrict__ pro_data = pro_base + t_lo;

            int64_t loc_promo = 0;
            int64_t loc_total = 0;
            int64_t loc_promo_pass = 0;

#ifdef __AVX2__
            // ── AVX2 vectorized SoA path per thread ─────────────────────────
            {
                int64_t i = 0;
                __m256i vtot_a = _mm256_setzero_si256();
                __m256i vtot_b = _mm256_setzero_si256();
                __m256i vpro_a = _mm256_setzero_si256();
                __m256i vpro_b = _mm256_setzero_si256();

                const __m256i c100v   = _mm256_set1_epi32(100);
                const __m256i zero256 = _mm256_setzero_si256();

                for (; i + 8 <= t_count; i += 8) {
                    // Load 8 x ep_int32 (32 bytes)
                    __m256i ep8 = _mm256_loadu_si256(
                        reinterpret_cast<const __m256i*>(ep_data + i));

                    // Load 8 x dis_int8 and sign-extend to int32x8
                    __m128i dis_raw = _mm_loadl_epi64(
                        reinterpret_cast<const __m128i*>(dis_data + i));
                    __m128i dis_lo4 = _mm_cvtepi8_epi32(dis_raw);
                    __m128i dis_hi4 = _mm_cvtepi8_epi32(_mm_srli_si128(dis_raw, 4));
                    __m256i dis8    = _mm256_set_m128i(dis_hi4, dis_lo4);

                    // Load 8 x is_promo_u8 and zero-extend to int32x8
                    __m128i pro_raw = _mm_loadl_epi64(
                        reinterpret_cast<const __m128i*>(pro_data + i));
                    __m128i pro_lo4 = _mm_cvtepu8_epi32(pro_raw);
                    __m128i pro_hi4 = _mm_cvtepu8_epi32(_mm_srli_si128(pro_raw, 4));
                    __m256i pro8    = _mm256_set_m128i(pro_hi4, pro_lo4);

                    // val = ep * (100 - dis)
                    __m256i dis8n = _mm256_sub_epi32(c100v, dis8);
                    __m256i val8  = _mm256_mullo_epi32(ep8, dis8n);

                    // Widen int32x8 => two int64x4
                    __m128i val_lo  = _mm256_castsi256_si128(val8);
                    __m128i val_hi  = _mm256_extracti128_si256(val8, 1);
                    __m256i val64a  = _mm256_cvtepi32_epi64(val_lo);
                    __m256i val64b  = _mm256_cvtepi32_epi64(val_hi);

                    // Promo mask
                    __m256i mask8   = _mm256_sub_epi32(zero256, pro8);
                    __m128i msk_lo  = _mm256_castsi256_si128(mask8);
                    __m128i msk_hi  = _mm256_extracti128_si256(mask8, 1);
                    __m256i msk64a  = _mm256_cvtepi32_epi64(msk_lo);
                    __m256i msk64b  = _mm256_cvtepi32_epi64(msk_hi);

                    vtot_a = _mm256_add_epi64(vtot_a, val64a);
                    vtot_b = _mm256_add_epi64(vtot_b, val64b);
                    vpro_a = _mm256_add_epi64(vpro_a, _mm256_and_si256(val64a, msk64a));
                    vpro_b = _mm256_add_epi64(vpro_b, _mm256_and_si256(val64b, msk64b));
                }

                // Horizontal reduce
                auto hsum256_i64 = [](const __m256i v) -> int64_t {
                    __m128i lo = _mm256_castsi256_si128(v);
                    __m128i hi = _mm256_extracti128_si256(v, 1);
                    __m128i s  = _mm_add_epi64(lo, hi);
                    __m128i t  = _mm_unpackhi_epi64(s, s);
                    __m128i r  = _mm_add_epi64(s, t);
                    return _mm_cvtsi128_si64(r);
                };
                loc_total += hsum256_i64(vtot_a) + hsum256_i64(vtot_b);
                loc_promo += hsum256_i64(vpro_a) + hsum256_i64(vpro_b);

                // Scalar tail for remaining < 8 rows
                for (; i < t_count; ++i) {
                    int64_t val      = static_cast<int64_t>(ep_data[i]) * (100 - dis_data[i]);
                    loc_total       += val;
                    int64_t is_promo = static_cast<int64_t>(pro_data[i]);
                    loc_promo       += val & (-is_promo);
                    loc_promo_pass  += is_promo;
                }
            }
#else
            // ── Scalar fallback path per thread ─────────────────────────────
            {
                for (int64_t i = 0; i < t_count; ++i) {
                    int64_t val      = static_cast<int64_t>(ep_data[i]) * (100 - dis_data[i]);
                    loc_total       += val;
                    int64_t is_promo = static_cast<int64_t>(pro_data[i]);
                    loc_promo       += val & (-is_promo);
                    loc_promo_pass  += is_promo;
                }
            }
#endif

            accs[static_cast<size_t>(tid)].sum_promo  = loc_promo;
            accs[static_cast<size_t>(tid)].sum_total  = loc_total;
            accs[static_cast<size_t>(tid)].promo_pass = loc_promo_pass;
        }); // end parallel_for

        // Sequential merge of per-thread accumulators
        int64_t li_promo_pass = 0;
        for (int t = 0; t < n_threads; ++t) {
            sum_promo_i64 += accs[static_cast<size_t>(t)].sum_promo;
            sum_total_i64 += accs[static_cast<size_t>(t)].sum_total;
            li_promo_pass += accs[static_cast<size_t>(t)].promo_pass;
        }
        TRACE_COUNT("q14_avx2_rows", count);
        TRACE_COUNT("q14_li_promo_pass_merged", li_promo_pass);
    } // end q14_scan_loop

    double sum_total = static_cast<double>(sum_total_i64);
    double sum_promo = static_cast<double>(sum_promo_i64);

    TRACE_COUNT("q14_li_rows_scanned", range_sz);
    TRACE_COUNT("q14_sum_total_cents", static_cast<long long>(sum_total / 10000.0 + 0.5));
    TRACE_COUNT("q14_sum_promo_cents", static_cast<long long>(sum_promo / 10000.0 + 0.5));

    // 5. Compute final result and format output
    double promo_revenue = 0.0;
    if (sum_total != 0.0) {
        promo_revenue = 100.0 * sum_promo / sum_total;
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(6) << promo_revenue;

    {
        PROFILE_SCOPE("q14_format_output");
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"promo_revenue"});
        rows.push_back({oss.str()});
        TRACE_COUNT("q14_query_output_rows", static_cast<long long>(rows.size()) - 1);
        return rows;
    } // end q14_format_output
}