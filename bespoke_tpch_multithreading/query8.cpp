#include "query8.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

// SQL:
/** select
    o_year,
    sum(case
        when nation = '[NATION]'
        then volume
        else 0
    end) / sum(volume) as mkt_share
from (
    select
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1-l_discount) as volume,
        n2.n_name as nation
    from
        part,
        supplier,
        lineitem,
        orders,
        customer,
        nation n1,
        nation n2,
        region
    where
        p_partkey = l_partkey
        and s_suppkey = l_suppkey
        and l_orderkey = o_orderkey
        and o_custkey = c_custkey
        and c_nationkey = n1.n_nationkey
        and n1.n_regionkey = r_regionkey
        and r_name = '[REGION]'
        and s_nationkey = n2.n_nationkey
        and o_orderdate between date '1995-01-01' and date '1996-12-31'
        and p_type = '[TYPE]'
    ) as all_nations
group by
    o_year
order by
    o_year; */

std::vector<std::vector<std::string>> run_q8(Database* db, const Q8Args& args) {
    if (!db) return {{"o_year", "mkt_share"}};

    PROFILE_SCOPE("q8_total");

    // ── 2. Parse arguments ────────────────────────────────────────────────────
    const std::string& target_nation = args.NATION;
    const std::string& target_region = args.REGION;
    const std::string& target_type   = args.TYPE;

    // ── 3. Resolve region and nation keys ────────────────────────────────────
    const RegionTable& reg = db->region;
    const NationTable& nat = db->nation;

    auto reg_it = reg.name_to_regionkey.find(target_region);
    if (reg_it == reg.name_to_regionkey.end()) {
        return {{"o_year", "mkt_share"}};
    }
    const int32_t target_regionkey = reg_it->second;

    // is_region_nation[nk] = true if nation nk belongs to the target region
    std::array<bool, 25> is_region_nation{};
    is_region_nation.fill(false);
    for (int32_t i = 0; i < nat.num_rows; ++i) {
        const int32_t nk = nat.n_nationkey[static_cast<size_t>(i)];
        if (nk >= 0 && nk < 25
                && nat.nationkey_to_regionkey[static_cast<size_t>(nk)] == target_regionkey) {
            is_region_nation[static_cast<size_t>(nk)] = true;
        }
    }

    auto nat_it = nat.name_to_nationkey.find(target_nation);
    const int32_t target_nation_key = (nat_it != nat.name_to_nationkey.end())
                                      ? nat_it->second : -1;
    // encode as uint8 for fast comparison in inner loop
    const uint8_t tgt_nk_u8 = (target_nation_key >= 0 && target_nation_key < 25)
                               ? static_cast<uint8_t>(target_nation_key) : 0xFF;

    // ── 4. Table references ───────────────────────────────────────────────────
    const PartTable& part = db->part;

    // ── 5. Per-thread accumulators ────────────────────────────────────────────
    // Cache-line aligned to avoid false sharing between threads.
    const int n_threads = pool.num_threads;

    TRACE_COUNT("q8_total_orders", db->orders.num_rows);

    struct alignas(64) ThreadAccum {
        double  vol_nation[2];  // [0]=year 1995, [1]=year 1996
        double  vol_total[2];   // [0]=year 1995, [1]=year 1996
        int64_t rows_scanned;
        int64_t rows_emitted;
    };
    std::vector<ThreadAccum> thread_accum(static_cast<size_t>(n_threads));
    for (auto& a : thread_accum) {
        a.vol_nation[0] = a.vol_nation[1] = 0.0;
        a.vol_total[0]  = a.vol_total[1]  = 0.0;
        a.rows_scanned  = 0;
        a.rows_emitted  = 0;
    }

    // ── 6. Parallel parts-first loop using li_by_partkey companion arrays ────
    //
    // Access pattern (per thread):
    //   Outer: sequential scan of part.p_type_enum on thread's chunk (~0.67% match)
    //   Inner: iterate li_by_partkey CSR slots j in [l_start, l_end) with
    //          purely sequential reads from pre-joined companion arrays:
    //     li_by_pk_year[j]       -> order year as int16_t  (pre-joined at load)
    //     li_by_pk_nation_u16[j] -> (suppnk<<8)|custnk     (pre-joined at load)
    //     li_by_pk_ep[j]         -> l_extendedprice float  (pre-joined at load)
    //     li_by_pk_disc[j]       -> l_discount float       (pre-joined at load)
    //   Zero random accesses in hot loop -- pure streaming memory access.
    {
        PROFILE_SCOPE("q8_agg_loop");

        // ── Step A: resolve p_type enum (tiny linear search over type_names) ─
        int target_type_enum = -1;
        {
            const auto& tnames = part.type_names;
            for (int i = 0; i < static_cast<int>(tnames.size()); ++i) {
                if (tnames[static_cast<size_t>(i)] == target_type) {
                    target_type_enum = i;
                    break;
                }
            }
        }

        if (target_type_enum >= 0) {
            // ── Step B: parallel hot loop — parts -> li_by_partkey ───────────
            PROFILE_SCOPE("q8_part_li_loop");

            const CsrIndex& li_pk_csr    = db->li_by_partkey;
            const size_t    li_pk_csr_sz = li_pk_csr.offsets.size();
            const uint8_t   tgt_type_u8  = static_cast<uint8_t>(target_type_enum);

            // Pointers into CSR-order companion arrays (purely sequential access)
            const int16_t*  __restrict__ yr_arr      = db->li_by_pk_year.data();
            const uint16_t* __restrict__ nk16_arr    = db->li_by_pk_nation_u16.data();
            const float*    __restrict__ ep_arr      = db->li_by_pk_ep.data();
            const float*    __restrict__ disc_arr    = db->li_by_pk_disc.data();
            const int32_t*  __restrict__ csr_offsets = li_pk_csr.offsets.data();
            const uint8_t*  __restrict__ ptype_arr   = part.p_type_enum.data();
            const int64_t*  __restrict__ pkey_arr    = part.p_partkey.data();

            // Year range as int16 (matches li_by_pk_year encoding)
            const int16_t yr_lo = 1995, yr_hi = 1996;
            const int64_t  n_parts = part.num_rows;

            // Dispatch parallel work: each thread owns a disjoint range of part rows.
            // Parts are iterated to find matching p_type rows (~0.67% pass rate),
            // then lineitem CSR slots for each matching part are processed.
            // Per-thread accumulators avoid any synchronization in the hot loop.
            pool.parallel_for([&](int tid, int nt) {
                PROFILE_SCOPE("q8_part_li_loop_thread");

                ThreadAccum& acc = thread_accum[static_cast<size_t>(tid)];

                // Compute this thread's disjoint range of part rows
                const int64_t chunk    = (n_parts + static_cast<int64_t>(nt) - 1)
                                          / static_cast<int64_t>(nt);
                const int64_t pi_start = static_cast<int64_t>(tid) * chunk;
                const int64_t pi_end   = std::min(pi_start + chunk, n_parts);

                for (int64_t pi = pi_start; pi < pi_end; ++pi) {
                    // Part type filter: sequential uint8 scan (~0.67% pass rate)
                    if (ptype_arr[static_cast<size_t>(pi)] != tgt_type_u8) continue;

                    const int64_t partkey = pkey_arr[static_cast<size_t>(pi)];
                    if (partkey < 0 || static_cast<size_t>(partkey) + 1 >= li_pk_csr_sz) continue;

                    const int32_t l_start = csr_offsets[static_cast<size_t>(partkey)];
                    const int32_t l_end   = csr_offsets[static_cast<size_t>(partkey) + 1];

                    for (int32_t j = l_start; j < l_end; ++j) {
                        ++acc.rows_scanned;

                        // Year filter -- sequential read from pre-joined array
                        const int16_t yr = yr_arr[static_cast<size_t>(j)];
                        if (yr < yr_lo || yr > yr_hi) continue;
                        const int yidx = (yr == yr_lo) ? 0 : 1;  // 0=1995, 1=1996

                        // Customer region + supplier nation -- single uint16 read
                        // low byte  = cust_nationkey, high byte = supp_nationkey
                        const uint16_t nk16 = nk16_arr[static_cast<size_t>(j)];
                        const uint8_t  cnk  = static_cast<uint8_t>(nk16 & 0xFF);
                        if (cnk >= 25 || !is_region_nation[cnk]) continue;

                        const uint8_t snk = static_cast<uint8_t>(nk16 >> 8);
                        if (snk == 0xFF) continue;

                        // Volume -- two sequential float reads
                        const float ep   = ep_arr[static_cast<size_t>(j)];
                        const float disc = disc_arr[static_cast<size_t>(j)];
                        const double vol = static_cast<double>(ep) *
                                           (1.0 - static_cast<double>(disc));

                        acc.vol_total[static_cast<size_t>(yidx)] += vol;
                        ++acc.rows_emitted;
                        if (snk == tgt_nk_u8) {
                            acc.vol_nation[static_cast<size_t>(yidx)] += vol;
                        }
                    }
                }
            }); // pool.parallel_for
        } // target_type_enum >= 0
    } // q8_agg_loop

    // ── Merge per-thread accumulators (sequential, O(n_threads)) ─────────────
    double  vol_nation[2] = {0.0, 0.0};
    double  vol_total [2] = {0.0, 0.0};
    int64_t li_rows_scanned = 0;
    int64_t li_rows_emitted = 0;
    {
        PROFILE_SCOPE("q8_merge");
        for (int t = 0; t < n_threads; ++t) {
            const ThreadAccum& a = thread_accum[static_cast<size_t>(t)];
            vol_nation[0]   += a.vol_nation[0];
            vol_nation[1]   += a.vol_nation[1];
            vol_total[0]    += a.vol_total[0];
            vol_total[1]    += a.vol_total[1];
            li_rows_scanned += a.rows_scanned;
            li_rows_emitted += a.rows_emitted;
        }
    }

    TRACE_COUNT("q8_ord_date_pass",   0LL);
    TRACE_COUNT("q8_ord_region_pass", 0LL);
    TRACE_COUNT("q8_li_rows_scanned", li_rows_scanned);
    TRACE_COUNT("q8_li_rows_emitted", li_rows_emitted);

    int64_t groups_with_vol = 0;
    for (int y = 0; y < 2; ++y)
        if (vol_total[y] != 0.0) ++groups_with_vol;
    TRACE_COUNT("q8_agg_rows_in",      li_rows_emitted);
    TRACE_COUNT("q8_groups_created",   groups_with_vol);
    TRACE_COUNT("q8_agg_rows_emitted", groups_with_vol);

    // ── 7. Assemble output ────────────────────────────────────────────────────
    auto fmt_share = [](double numerator, double denominator) -> std::string {
        const double share = (denominator != 0.0) ? (numerator / denominator) : 0.0;
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(6) << share;
        return oss.str();
    };

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"o_year", "mkt_share"});

    {
        PROFILE_SCOPE("q8_format_output");
        const int32_t years[2] = {1995, 1996};
        for (int y = 0; y < 2; ++y) {
            if (vol_total[y] != 0.0) {
                rows.push_back({
                    std::to_string(years[y]),
                    fmt_share(vol_nation[y], vol_total[y])
                });
            }
        }
    }
    TRACE_COUNT("q8_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}