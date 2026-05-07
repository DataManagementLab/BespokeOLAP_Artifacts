#include "query9.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// SQL:
/** select
    nation,
    o_year,
    sum(amount) as sum_profit
from (
    select
        n_name as nation,
        extract(year from o_orderdate) as o_year,
        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
    from
        part,
        supplier,
        lineitem,
        partsupp,
        orders,
        nation
    where
        s_suppkey = l_suppkey
        and ps_suppkey = l_suppkey
        and ps_partkey = l_partkey
        and p_partkey = l_partkey
        and o_orderkey = l_orderkey
        and s_nationkey = n_nationkey
        and p_name like '%[COLOR]%'
    ) as profit
group by
    nation,
    o_year
order by
    nation,
    o_year desc; */

std::vector<std::vector<std::string>> run_q9(Database* db, const Q9Args& args) {
    if (!db) throw std::runtime_error("run_q9: db is null");

    PROFILE_SCOPE("q9_total");

    // ── 1. Parse args ──────────────────────────────────────────────────────
    const std::string& color = args.COLOR;

    // ── 2. Shorthand references ─────────────────────────────────────────────
    const PartTable&     part      = db->part;
    const NationTable&   nat       = db->nation;
    const CsrIndex&      li_pk_csr = db->li_by_partkey;

    // ── 3. Nation name lookup: nationkey (0..24) → name ────────────────────
    std::array<std::string, 25> nation_name_by_key;
    for (int32_t i = 0; i < nat.num_rows; ++i) {
        int32_t nk = nat.n_nationkey[static_cast<size_t>(i)];
        if (nk >= 0 && nk < 25)
            nation_name_by_key[static_cast<size_t>(nk)] = nat.n_name[static_cast<size_t>(i)];
    }

    // ── 4. Collect matching partkeys ────────────────────────────────────────
    const size_t part_row_sz = db->part_row_by_partkey.size();
    std::vector<int64_t> matching_partkeys;
    matching_partkeys.reserve(64 * 1024);

    // ── 4a. Parallel part scan ──────────────────────────────────────────────
    // Each thread scans a range of part rows and collects matching partkeys
    // into a thread-local vector; then we concatenate results sequentially.
    int64_t part_match_count = 0;
    const int64_t part_rows = part.num_rows;

    {
        PROFILE_SCOPE("q9_build_part_bitmap");

        const int n_threads = pool.num_threads;
        std::vector<std::vector<int64_t>> local_pkeys(n_threads);

        pool.parallel_for([&](int tid, int n) {
            const int64_t chunk = (part_rows + n - 1) / n;
            const int64_t begin = tid * chunk;
            const int64_t end   = std::min(begin + chunk, part_rows);
            local_pkeys[tid].reserve(chunk / 16);
            for (int64_t i = begin; i < end; ++i) {
                const std::string& pname = part.p_name[static_cast<size_t>(i)];
                if (pname.find(color) != std::string::npos) {
                    int64_t pk = part.p_partkey[static_cast<size_t>(i)];
                    if (pk >= 0 && static_cast<size_t>(pk) < part_row_sz) {
                        local_pkeys[tid].push_back(pk);
                    }
                }
            }
        });

        // Merge thread-local results
        for (int t = 0; t < n_threads; ++t) {
            part_match_count += static_cast<int64_t>(local_pkeys[t].size());
            for (int64_t pk : local_pkeys[t])
                matching_partkeys.push_back(pk);
        }
    }

    TRACE_COUNT("q9_part_match_count", part_match_count);

    // ── 5. Pre-fetch companion array pointers ───────────────────────────────
    // Two arrays cover the entire hot loop: packed key + pre-computed amount.
    const size_t   li_pk_off_sz = li_pk_csr.offsets.size();
    const int32_t* li_pk_off    = li_pk_csr.offsets.data();
    // Pre-computed per-slot: packed (suppnk<<8)|yr_idx and full profit amount
    const uint16_t* li_pk_snk_yr = db->li_by_pk_snk_yr.data();   // (suppnk<<8)|yr_idx
    const double*   li_pk_amount = db->li_by_pk_amount.data();    // pre-computed amount

    TRACE_COUNT("q9_total_lineitem_rows", db->lineitem.num_rows);

    // ── 6. Flat 2D aggregation array: profit[nationkey][year_slot] ──────────
    // TPC-H order dates span 1992-1998. Direct-indexed 25×10 array, no hashing.
    static constexpr int32_t BASE_YEAR = 1992;
    static constexpr int32_t N_YEARS   = 10;   // 1992..2001
    static constexpr int32_t N_NATIONS = 25;
    double profit[N_NATIONS][N_YEARS];
    std::memset(profit, 0, sizeof(profit));

    // ── 7. Parallel main scan ───────────────────────────────────────────────
    // Split matching_partkeys across threads. Each thread keeps its own
    // local profit[25][10] accumulator to avoid synchronisation.
    // After all threads finish, merge local accumulators into the global array.
    {
        const int64_t n_pk   = static_cast<int64_t>(matching_partkeys.size());
        const int      n_thr  = pool.num_threads;

        // Per-thread local profit accumulators (flat arrays for alignment)
        // Laid out as [thread][nation][year] = flat index t*N_NATIONS*N_YEARS + n*N_YEARS + y
        const int stride = N_NATIONS * N_YEARS;
        std::vector<double> local_profit(static_cast<size_t>(n_thr) * stride, 0.0);

        // Parallel scan: each thread owns a contiguous chunk of matching_partkeys
        PROFILE_SCOPE("q9_scan_loop");

        pool.parallel_for([&](int tid, int n) {
            PROFILE_SCOPE("q9_scan_loop_thread");

            // Each thread processes a contiguous range of matching_partkeys
            const int64_t chunk = (n_pk + n - 1) / n;
            const int64_t begin = static_cast<int64_t>(tid) * chunk;
            const int64_t end   = std::min(begin + chunk, n_pk);

            double* lp = local_profit.data() + tid * stride;

            for (int64_t pi = begin; pi < end; ++pi) {
                const int64_t partkey = matching_partkeys[static_cast<size_t>(pi)];
                const size_t  pk_sz   = static_cast<size_t>(partkey);

                if (pk_sz + 1 >= li_pk_off_sz) continue;
                const int32_t li_start = li_pk_off[pk_sz];
                const int32_t li_end   = li_pk_off[pk_sz + 1];

                // Inner loop: 2 loads per slot — packed key + pre-computed amount
                for (int32_t ci = li_start; ci < li_end; ++ci) {
                    const size_t ci_sz = static_cast<size_t>(ci);

                    // Single 16-bit load decodes validity and both indices
                    const uint16_t snk_yr = li_pk_snk_yr[ci_sz];
                    const uint8_t  snk    = static_cast<uint8_t>(snk_yr >> 8);
                    const uint8_t  yr_u8  = static_cast<uint8_t>(snk_yr & 0xFFu);
                    if (snk  >= static_cast<uint8_t>(N_NATIONS)) continue;
                    if (yr_u8 >= static_cast<uint8_t>(N_YEARS))  continue;

                    lp[static_cast<size_t>(snk) * N_YEARS + static_cast<size_t>(yr_u8)]
                        += li_pk_amount[ci_sz];
                }
            }
        });

        // Merge per-thread profit into global profit array
        {
            PROFILE_SCOPE("q9_merge_profit");
            for (int t = 0; t < n_thr; ++t) {
                const double* lp = local_profit.data() + t * stride;
                for (int n2 = 0; n2 < N_NATIONS; ++n2)
                    for (int y = 0; y < N_YEARS; ++y)
                        profit[n2][y] += lp[n2 * N_YEARS + y];
            }
        }
    }

    TRACE_COUNT("q9_li_part_pass",    part_match_count);
    TRACE_COUNT("q9_li_rows_emitted", part_match_count);

    // ── 8. Collect non-zero groups ──────────────────────────────────────────
    struct ResultRow {
        std::string nation;
        int32_t     o_year;
        double      sum_profit;
    };
    std::vector<ResultRow> result;
    result.reserve(N_NATIONS * N_YEARS);
    for (int32_t n = 0; n < N_NATIONS; ++n) {
        const std::string& nname = nation_name_by_key[static_cast<size_t>(n)];
        if (nname.empty()) continue;
        for (int32_t y = 0; y < N_YEARS; ++y) {
            if (profit[n][y] != 0.0)
                result.push_back({nname, BASE_YEAR + y, profit[n][y]});
        }
    }
    TRACE_COUNT("q9_groups_created", static_cast<long long>(result.size()));

    // ── 9. Sort: nation ASC, o_year DESC ────────────────────────────────────
    {
        PROFILE_SCOPE("q9_sort");
        std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.o_year > b.o_year;
        });
    }
    TRACE_COUNT("q9_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q9_sort_rows_out", static_cast<long long>(result.size()));

    // ── 10. Format output ───────────────────────────────────────────────────
    auto fmt_profit = [](double v) -> std::string {
        std::ostringstream oss;
        oss << std::fixed << std::setprecision(2) << v;
        return oss.str();
    };

    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"nation", "o_year", "sum_profit"});
    {
        PROFILE_SCOPE("q9_format_output");
        for (const auto& r : result)
            rows.push_back({r.nation, std::to_string(r.o_year), fmt_profit(r.sum_profit)});
    }
    TRACE_COUNT("q9_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}