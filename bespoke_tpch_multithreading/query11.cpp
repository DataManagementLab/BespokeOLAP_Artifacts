#include "query11.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <unordered_map>
#include <vector>

// SQL:
/** select 
    ps_partkey,  
    sum(ps_supplycost * ps_availqty) as value 
from  
    partsupp,  
    supplier,  
    nation 
where  
    ps_suppkey = s_suppkey 
    and s_nationkey = n_nationkey 
    and n_name = '[NATION]' 
group by  
    ps_partkey having  
        sum(ps_supplycost * ps_availqty) > ( 
            select  
                sum(ps_supplycost * ps_availqty) * [FRACTION] 
            from  
                partsupp,  
                supplier,  
                nation 
            where  
                ps_suppkey = s_suppkey 
                and s_nationkey = n_nationkey 
                and n_name = '[NATION]'
        ) 
order by 
    value desc; */

std::vector<std::vector<std::string>> run_q11(Database* db, const Q11Args& args) {
    if (!db) {
        throw std::runtime_error("run_q11: db is null");
    }

    PROFILE_SCOPE("q11_total");

    // ── 1. Resolve the target nation key ─────────────────────────────────────
    const NationTable&   nat  = db->nation;
    const SupplierTable& sup  = db->supplier;

    auto nat_it = nat.name_to_nationkey.find(args.NATION);
    if (nat_it == nat.name_to_nationkey.end()) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"ps_partkey", "value"});
        return rows;
    }
    int32_t target_nationkey = nat_it->second;
    double fraction = std::stod(args.FRACTION);

    // ── 2. Collect suppkeys belonging to target nation ────────────────────────
    // At SF=50: ~20K suppkeys belong to the target nation.
    std::vector<int32_t> nation_suppkeys;
    {
        PROFILE_SCOPE("q11_build_suppkey_filter");
        for (int64_t i = 0; i < sup.num_rows; ++i) {
            if (sup.s_nationkey[static_cast<size_t>(i)] == target_nationkey)
                nation_suppkeys.push_back(static_cast<int32_t>(
                    sup.s_suppkey[static_cast<size_t>(i)]));
        }
    }
    TRACE_COUNT("q11_nation_suppkeys",     (long long)nation_suppkeys.size());
    TRACE_COUNT("q11_total_supplier_rows", (long long)sup.num_rows);
    TRACE_COUNT("q11_total_partsupp_rows", (long long)db->partsupp.num_rows);

    const CsrIndex&      csr        = db->ps_by_suppkey;
    using Slot = Database::PsBySuppkeySlot;
    const Slot* __restrict__ slots  = db->ps_bysuppkey_slots.data();
    const size_t csr_off_sz         = csr.offsets.size();
    const size_t max_partkey        = db->ps_by_partkey.offsets.size();
    const int64_t n_sk              = static_cast<int64_t>(nation_suppkeys.size());
    const int     n_threads         = pool.num_threads;

    TRACE_COUNT("q11_n_threads", (long long)n_threads);

    // ── 3. Two-pass parallel aggregation ─────────────────────────────────────
    //
    // PASS 1 — parallel scatter by suppkey range:
    //   Partition nation_suppkeys into n_threads ranges.
    //   Each thread emits raw (partkey, value) pairs into a private vector.
    //   No deduplication, no synchronization in the hot loop.
    //   Per-thread output: ~(n_sk/n_t) * 80 pairs ≈ 16K entries at SF=50 / 96 threads.
    //
    // PASS 2 — parallel aggregation by partkey range:
    //   Divide the partkey space [0, max_partkey) into n_threads bands.
    //   Each thread iterates all pass-1 pair vectors (shared L3 cache reads)
    //   and accumulates into its private band array (~833KB at SF=50, fits in L2).
    //   Applies the HAVING filter inline and emits passing {partkey, sum} pairs.
    //   Writes to final_sum are to disjoint partkey ranges — no synchronization.
    //
    // Memory: n_threads × (16K × 8B) = ~12MB pass-1 vectors
    //       + n_threads × (833K × 8B) = ~64MB band arrays (each ~833KB)
    //   vs. sequential: 80MB flat array — similar total but better parallelism.

    // ── Pass-1 data structures ────────────────────────────────────────────────
    // Packed slot format: same layout as Database::PsBySuppkeySlot (partkey, value).
    struct PairEntry { int32_t partkey; int32_t value; };  // 8 bytes

    struct alignas(64) ThreadPairs {
        std::vector<PairEntry> pairs;
        int64_t local_total = 0LL;
    };
    std::vector<ThreadPairs> tpairs(n_threads);

    {
        PROFILE_SCOPE("q11_pass1_reserve");
        const size_t est = static_cast<size_t>(
            (n_sk / std::max(n_threads, 1) + 1) * 85);
        for (int t = 0; t < n_threads; ++t)
            tpairs[t].pairs.reserve(est);
    }

    // ── Pass 1: scatter into per-thread pair vectors ──────────────────────────
    {
        PROFILE_SCOPE("q11_pass1_parallel");
        pool.parallel_for([&](int tid, int n_t) {
            PROFILE_SCOPE("q11_pass1_worker");
            ThreadPairs& tp = tpairs[tid];
            auto& pairs = tp.pairs;

            const int64_t chunk = (n_sk + n_t - 1) / n_t;
            const int64_t lo    = static_cast<int64_t>(tid) * chunk;
            const int64_t hi    = std::min(lo + chunk, n_sk);

            int64_t local_total = 0LL;
            for (int64_t si = lo; si < hi; ++si) {
                const int32_t sk = nation_suppkeys[static_cast<size_t>(si)];
                if (sk < 0 || static_cast<size_t>(sk) + 1 >= csr_off_sz) continue;
                const int32_t slot_lo = csr.offsets[static_cast<size_t>(sk)];
                const int32_t slot_hi = csr.offsets[static_cast<size_t>(sk) + 1];

                for (int32_t j = slot_lo; j < slot_hi; ++j) {
                    const Slot s  = slots[static_cast<size_t>(j)];
                    pairs.push_back({s.partkey, s.value});
                    local_total += static_cast<int64_t>(s.value);
                }
            }
            tp.local_total = local_total;
            TRACE_COUNT("q11_pass1_pairs", (long long)pairs.size());
        });
    }

    // ── Compute global total (scalar sum of per-thread totals) ────────────────
    int64_t global_total_cents = 0LL;
    {
        PROFILE_SCOPE("q11_merge_totals");
        for (int t = 0; t < n_threads; ++t)
            global_total_cents += tpairs[t].local_total;
    }
    TRACE_COUNT("q11_global_total_cents", global_total_cents);

    // ── Compute HAVING threshold (used by pass-2 to inline-filter) ────────────
    const int64_t threshold_cents = static_cast<int64_t>(
        static_cast<double>(global_total_cents) * fraction);
    TRACE_COUNT("q11_threshold_cents", threshold_cents);

    // ── Pass-2 data structures ────────────────────────────────────────────────
    struct PartGroup {
        int32_t partkey;
        int64_t value_cents;
    };
    struct alignas(64) ThreadResult {
        std::vector<PartGroup> results;
    };
    std::vector<ThreadResult> tresults(n_threads);

    // ── Pass 2: aggregate by partkey band + inline HAVING filter ─────────────
    {
        PROFILE_SCOPE("q11_pass2_parallel");
        const size_t pk_chunk = (max_partkey + static_cast<size_t>(n_threads) - 1)
                                / static_cast<size_t>(n_threads);

        pool.parallel_for([&](int tid, int n_t) {
            PROFILE_SCOPE("q11_pass2_worker");

            const size_t pk_lo = static_cast<size_t>(tid) * pk_chunk;
            const size_t pk_hi = std::min(pk_lo + pk_chunk, max_partkey);
            if (pk_lo >= pk_hi) return;
            const size_t band_size = pk_hi - pk_lo;

            // Private band accumulator — calloc for lazy zero-init.
            // At SF=50: ~833K entries × 8B ≈ 6.6MB per thread.
            // This fits in L3 cache per thread; the hot inner loop writes
            // sequentially (relative to the band offset) with good cache reuse.
            int64_t* band = static_cast<int64_t*>(
                std::calloc(band_size, sizeof(int64_t)));
            if (!band) return;

            // Bitmap for first-touch dirty tracking within the band.
            const size_t bm_words = (band_size + 63) / 64;
            std::vector<uint64_t> bm(bm_words, 0ULL);
            std::vector<int32_t>  dirty_band;
            dirty_band.reserve(band_size / 4 + 64);

            // Iterate all pass-1 pair vectors; accumulate pairs falling in [pk_lo, pk_hi)
            for (int t = 0; t < n_t; ++t) {
                for (const PairEntry& pe : tpairs[t].pairs) {
                    const size_t pk = static_cast<size_t>(pe.partkey);
                    if (pk < pk_lo || pk >= pk_hi) continue;
                    const size_t idx = pk - pk_lo;
                    band[idx] += static_cast<int64_t>(pe.value);

                    // Track first-touch for dirty list
                    const size_t   word = idx >> 6;
                    const uint64_t bit  = 1ULL << (idx & 63u);
                    if (!(bm[word] & bit)) {
                        bm[word] |= bit;
                        dirty_band.push_back(static_cast<int32_t>(pk));
                    }
                }
            }

            // Apply HAVING filter inline: only emit partkeys with sum > threshold
            auto& res = tresults[tid].results;
            for (int32_t pk : dirty_band) {
                const int64_t v = band[static_cast<size_t>(pk) - pk_lo];
                if (v > threshold_cents)
                    res.push_back({pk, v});
            }

            std::free(band);
            TRACE_COUNT("q11_pass2_dirty_band",  (long long)dirty_band.size());
            TRACE_COUNT("q11_pass2_having_pass", (long long)res.size());
        });
    }

    // ── Collect per-thread HAVING results + sort ──────────────────────────────
    std::vector<PartGroup> result;
    {
        PROFILE_SCOPE("q11_collect_results");
        size_t total = 0;
        for (int t = 0; t < n_threads; ++t) total += tresults[t].results.size();
        result.reserve(total);
        for (int t = 0; t < n_threads; ++t)
            for (const auto& r : tresults[t].results)
                result.push_back(r);
    }
    TRACE_COUNT("q11_having_pass", (long long)result.size());

    {
        PROFILE_SCOPE("q11_sort");
        std::sort(result.begin(), result.end(),
                  [](const PartGroup& a, const PartGroup& b) {
                      return a.value_cents > b.value_cents;
                  });
    }
    TRACE_COUNT("q11_sort_rows_in",  (long long)result.size());
    TRACE_COUNT("q11_sort_rows_out", (long long)result.size());

    // ── 6. Format output ──────────────────────────────────────────────────────
    auto fmt_cents = [](int64_t cents) -> std::string {
        int64_t dollars = cents / 100LL;
        int     frac    = static_cast<int>(cents % 100LL);
        char buf[32];
        int len = std::snprintf(buf, sizeof(buf), "%lld.%02d",
                                static_cast<long long>(dollars), frac);
        return std::string(buf, static_cast<size_t>(len));
    };

    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"ps_partkey", "value"});

    {
        PROFILE_SCOPE("q11_format_output");
        for (const auto& r : result) {
            rows.push_back({
                std::to_string(static_cast<int64_t>(r.partkey)),
                fmt_cents(r.value_cents)
            });
        }
    }
    TRACE_COUNT("q11_query_output_rows", (long long)rows.size() - 1);

    return rows;
}