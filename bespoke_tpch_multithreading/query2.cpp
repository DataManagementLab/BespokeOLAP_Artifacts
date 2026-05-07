#include "query2.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <iomanip>
#include <limits>
#include <bitset>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <mutex>

// SQL:
/** select
    s_acctbal,
    s_name,
    n_name,
    p_partkey,
    p_mfgr,
    s_address,
    s_phone,
    s_comment
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    p_partkey = ps_partkey
    and s_suppkey = ps_suppkey
    and p_size = [SIZE]
    and p_type like '%[TYPE]'
    and s_nationkey = n_nationkey
    and n_regionkey = r_regionkey
    and r_name = '[REGION]'
    and ps_supplycost = (
        select
            min (ps_supplycost)
        from
            partsupp, supplier,
            nation, region
        where
            p_partkey = ps_partkey
            and s_suppkey = ps_suppkey
            and s_nationkey = n_nationkey
            and n_regionkey = r_regionkey
            and r_name = '[REGION]'
        )
order by
    s_acctbal desc,
    n_name,
    s_name,
    p_partkey; */

std::vector<std::vector<std::string>> run_q2(Database* db, const Q2Args& args) {
    if (!db) {
        throw std::runtime_error("run_q2: db is null");
    }

    // Parse args
    int32_t target_size            = std::stoi(args.SIZE);
    const std::string& type_suffix = args.TYPE;   // p_type LIKE '%TYPE'
    const std::string& region_name = args.REGION;

    PROFILE_SCOPE("q2_total");

    auto reg_it = db->region.name_to_regionkey.find(region_name);
    if (reg_it == db->region.name_to_regionkey.end()) {
        return {{"s_acctbal","s_name","n_name","p_partkey","p_mfgr",
                 "s_address","s_phone","s_comment"}};
    }
    int32_t target_regionkey = reg_it->second;

    // ── 2. Build nation lookup arrays (25 rows, indexed by nationkey) ─────────
    const NationTable& nat = db->nation;
    bool        nation_in_region[25]  = {};
    const std::string* nation_name_by_key[25] = {};  // pointers into nat.n_name (zero-copy)
    // Pre-compute alphabetical sort rank for each nation (only 25 rows — trivial cost).
    // This converts string comparisons in the sort comparator to cheap uint8 comparisons.
    uint8_t nation_sort_rank[25] = {};
    {
        PROFILE_SCOPE("q2_nation_build");
        for (int32_t ni = 0; ni < nat.num_rows; ++ni) {
            int32_t nk = nat.n_nationkey[static_cast<size_t>(ni)];
            int32_t rk = nat.n_regionkey[static_cast<size_t>(ni)];
            if (nk >= 0 && nk < 25) {
                nation_name_by_key[nk] = &nat.n_name[static_cast<size_t>(ni)];
                if (rk == target_regionkey)
                    nation_in_region[nk] = true;
            }
        }
        // Build nation alphabetical rank (only 25 entries — sort once).
        // Collect (name pointer, nation key) pairs, sort by name, assign rank.
        struct NkPair { const std::string* name; uint8_t nk; };
        NkPair nk_pairs[25];
        int32_t np = 0;
        for (int32_t ni = 0; ni < 25; ++ni) {
            if (nation_name_by_key[ni]) {
                nk_pairs[np++] = {nation_name_by_key[ni], static_cast<uint8_t>(ni)};
            }
        }
        std::sort(nk_pairs, nk_pairs + np, [](const NkPair& a, const NkPair& b) {
            return *a.name < *b.name;
        });
        for (int32_t r = 0; r < np; ++r)
            nation_sort_rank[nk_pairs[r].nk] = static_cast<uint8_t>(r);
    }

    // ── 3. Handy references ───────────────────────────────────────────────────
    const SupplierTable&  sup      = db->supplier;
    const PartTable&      pt       = db->part;
    const CsrIndex&       ps_csr   = db->ps_by_partkey;
    const CsrIndex&       sz_csr   = db->part_by_size;
    // Use AoS packed struct for ps_by_partkey inner loop: 4 entries (=64B cache line) per part
    const Database::PsByPkPacked* __restrict__ ps_packed = db->ps_by_pk_packed.data();

    // Companion arrays for part_by_size — sequential type_enum/partkey/mfgr access
    const uint8_t* __restrict__ psz_type_enum   = db->psz_type_enum.data();
    const int32_t* __restrict__ psz_partkey_i32 = db->psz_partkey_i32.data();
    const int32_t* __restrict__ psz_ps_start    = db->psz_ps_start.data();
    const int32_t* __restrict__ psz_ps_end      = db->psz_ps_end.data();
    // We still need the raw part row IDs for output (p_partkey_str lookup)
    const int32_t* __restrict__ sz_row_ids      = sz_csr.row_ids.data();

    // ── 3b. Pre-compute which type_enum values match '%type_suffix' ───────────
    // p_type_enum has at most 256 distinct values; check each decode string once.
    // This turns per-row string suffix checks into a single bitset[] lookup.
    const size_t n_type_enums = pt.type_names.size();
    std::vector<bool> type_enum_matches(n_type_enums, false);
    {
        PROFILE_SCOPE("q2_type_enum_build");
        for (size_t ti = 0; ti < n_type_enums; ++ti) {
            const std::string& tname = pt.type_names[ti];
            if (tname.size() >= type_suffix.size() &&
                tname.compare(tname.size() - type_suffix.size(),
                              type_suffix.size(), type_suffix) == 0) {
                type_enum_matches[ti] = true;
            }
        }
    }
    // Compact 256-bit bitset for type_enum matching (4 × uint64).
    // Fits in one cache line; one load + shift + bit-test replaces a byte array read.
    uint64_t type_enum_bits[4] = {0, 0, 0, 0};
    for (size_t ti = 0; ti < n_type_enums && ti < 256; ++ti) {
        if (type_enum_matches[ti])
            type_enum_bits[ti >> 6] |= (uint64_t(1) << (ti & 63));
    }

    const size_t  ps_row_ids_sz  = ps_csr.row_ids.size();

    // ── Compact result row: store indices, not strings ────────────────────────
    // Defer all string materialization to the output phase.
    struct ResultRow {
        int32_t  sr;           // supplier row index
        int32_t  pi;           // part row index
        int32_t  nkey;         // nation key (for sort key via nation_name_by_key)
        int64_t  p_partkey;    // for sort key
        double   s_acctbal;    // for sort key
    };

    // ── 4. Main loop: use part_by_size CSR to only visit parts with matching size ──
    // Use part_by_size CSR: only visit rows with matching p_size
    const int32_t sz_range = static_cast<int32_t>(sz_csr.offsets.size());
    int32_t sz_start = 0, sz_end = 0;
    if (target_size >= 0 && target_size < sz_range - 1) {
        sz_start = sz_csr.offsets[static_cast<size_t>(target_size)];
        sz_end   = sz_csr.offsets[static_cast<size_t>(target_size) + 1];
    }

    int64_t part_rows_scanned   = static_cast<int64_t>(sz_end - sz_start);
    int64_t part_rows_emitted   = 0;
    int64_t ps_probe_rows       = 0;
    int64_t join_rows_emitted   = 0;

    // Per-thread local result vectors to avoid contention
    const int n_threads = pool.num_threads;
    std::vector<std::vector<ResultRow>> thread_results(n_threads);
    for (auto& tr : thread_results) tr.reserve(4096 / n_threads + 64);

    // Atomic counters for stats (shared across threads)
    std::atomic<int64_t> atomic_part_rows_emitted{0};
    std::atomic<int64_t> atomic_ps_probe_rows{0};
    std::atomic<int64_t> atomic_join_rows_emitted{0};

    {
        PROFILE_SCOPE("q2_main_loop");

        pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q2_main_loop_thread");

            // Per-part scratch buffer: TPC-H guarantees ≤4 suppliers per part.
            // Stack-allocated fixed array avoids heap alloc per part.
            struct PsEntry { int32_t sr; uint8_t nkey; double cost; };
            PsEntry entries[8];
            int     nentries;

            // Each thread gets a disjoint range of the part_by_size CSR slice
            int32_t total = sz_end - sz_start;
            int32_t chunk = (total + n_thr - 1) / n_thr;
            int32_t t_start = sz_start + tid * chunk;
            int32_t t_end   = std::min(t_start + chunk, sz_end);

            std::vector<ResultRow>& local_result = thread_results[tid];

            int64_t local_emitted   = 0;
            int64_t local_ps_probe  = 0;
            int64_t local_join_rows = 0;

            for (int32_t szi = t_start; szi < t_end; ++szi) {
                // Sequential companion array reads — no random access into part table
                uint8_t te = psz_type_enum[static_cast<size_t>(szi)];
                // Bitset check: one word load + bit test (branch-free check)
                if (!(type_enum_bits[te >> 6] & (uint64_t(1) << (te & 63)))) continue;
                ++local_emitted;

                // Read partkey from sequential companion (was random access into 800K part table)
                int32_t partkey_i32 = psz_partkey_i32[static_cast<size_t>(szi)];
                if (partkey_i32 <= 0) continue;

                // Sequential companion array lookup — was random access into 16MB ps_csr.offsets
                int32_t ps_start = psz_ps_start[static_cast<size_t>(szi)];
                int32_t ps_end   = psz_ps_end[static_cast<size_t>(szi)];
                if (ps_start >= ps_end) continue;
                if (static_cast<size_t>(ps_end) > ps_row_ids_sz) continue;

                // Single pass: collect qualifying (supplier, cost) pairs AND find min cost.
                // TPC-H: ≤4 suppliers per part — tiny inner loop, already in cache.
                nentries = 0;
                local_ps_probe += (ps_end - ps_start);
                double min_cost = std::numeric_limits<double>::max();
                for (int32_t psi = ps_start; psi < ps_end; ++psi) {
                    // AoS: one 16-byte load covers all 3 fields; 4 entries fit in 1 cache line
                    const Database::PsByPkPacked& p = ps_packed[static_cast<size_t>(psi)];
                    uint8_t nk = p.suppnk;
                    if (nk == 0xFF || !nation_in_region[nk]) continue;
                    int32_t sr = p.sr;
                    if (sr < 0) continue;
                    double cost = p.cost;
                    if (cost < min_cost) min_cost = cost;
                    entries[nentries++] = {sr, nk, cost};
                    if (nentries >= 8) break;  // safety
                }
                if (min_cost == std::numeric_limits<double>::max()) continue;

                // Need part row index for output
                int32_t pi = sz_row_ids[static_cast<size_t>(szi)];
                int64_t partkey = static_cast<int64_t>(partkey_i32);

                for (int k = 0; k < nentries; ++k) {
                    const PsEntry& e = entries[k];
                    if (e.cost != min_cost) continue;
                    ++local_join_rows;
                    local_result.push_back({
                        e.sr,
                        pi,
                        static_cast<int32_t>(e.nkey),
                        partkey,
                        sup.s_acctbal[static_cast<size_t>(e.sr)]
                    });
                }
            }

            TRACE_COUNT("q2_thread_part_rows_emitted", local_emitted);
            TRACE_COUNT("q2_thread_ps_probe_rows", local_ps_probe);
            TRACE_COUNT("q2_thread_join_rows_emitted", local_join_rows);

            atomic_part_rows_emitted.fetch_add(local_emitted, std::memory_order_relaxed);
            atomic_ps_probe_rows.fetch_add(local_ps_probe, std::memory_order_relaxed);
            atomic_join_rows_emitted.fetch_add(local_join_rows, std::memory_order_relaxed);
        });
    }

    part_rows_emitted = atomic_part_rows_emitted.load(std::memory_order_relaxed);
    ps_probe_rows     = atomic_ps_probe_rows.load(std::memory_order_relaxed);
    join_rows_emitted = atomic_join_rows_emitted.load(std::memory_order_relaxed);

    // Scan / join counters
    TRACE_COUNT("q2_part_rows_scanned",   part_rows_scanned);
    TRACE_COUNT("q2_part_rows_emitted",   part_rows_emitted);
    TRACE_COUNT("q2_probe_rows_in",       ps_probe_rows);
    TRACE_COUNT("q2_join_rows_emitted",   join_rows_emitted);

    // ── 4b. Merge thread-local results ────────────────────────────────────────
    std::vector<ResultRow> result;
    {
        PROFILE_SCOPE("q2_merge");
        size_t total_rows = 0;
        for (const auto& tr : thread_results) total_rows += tr.size();
        result.reserve(total_rows);
        for (auto& tr : thread_results) {
            result.insert(result.end(), tr.begin(), tr.end());
        }
    }
    TRACE_COUNT("q2_merged_rows", static_cast<long long>(result.size()));

    // ── 5. Sort: s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC ───────
    // Sort result directly — ResultRow is now small (28 bytes), cheap to move.
    // Comparator uses pre-fetched pointers for nation/supplier names.
    {
        PROFILE_SCOPE("q2_sort");
        std::sort(result.begin(), result.end(),
                  [&nation_sort_rank](const ResultRow& a, const ResultRow& b) {
                      if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
                      // Use pre-computed rank for nation comparison (cheap uint8 compare)
                      uint8_t ank = nation_sort_rank[a.nkey];
                      uint8_t bnk = nation_sort_rank[b.nkey];
                      if (ank != bnk) return ank < bnk;
                      // TPC-H supplier names are "Supplier#%09d" (zero-padded).
                      // The supplier table is sorted by suppkey, so row index order ==
                      // alphabetical name order. Compare row indices directly.
                      if (a.sr != b.sr) return a.sr < b.sr;
                      return a.p_partkey < b.p_partkey;
                  });
    }
    TRACE_COUNT("q2_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q2_sort_rows_out", static_cast<long long>(result.size()));

    // ── 6. Format output ──────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"s_acctbal","s_name","n_name","p_partkey","p_mfgr",
                    "s_address","s_phone","s_comment"});

    {
        PROFILE_SCOPE("q2_format_output");
        for (const ResultRow& r : result) {
            const size_t sr = static_cast<size_t>(r.sr);
            const size_t pi_s = static_cast<size_t>(r.pi);
            // Use pre-formatted strings — no snprintf needed at output time
            rows.emplace_back();
            auto& row = rows.back();
            row.reserve(8);
            row.push_back(sup.s_acctbal_str[sr]);
            row.push_back(sup.s_name       [sr]);
            row.push_back(*nation_name_by_key[r.nkey]);
            row.push_back(pt.p_partkey_str [pi_s]);
            row.push_back(pt.mfgr_names    [pt.p_mfgr[pi_s]]);
            row.push_back(sup.s_address    [sr]);
            row.push_back(sup.s_phone      [sr]);
            row.push_back(sup.s_comment    [sr]);
        }
    }
    TRACE_COUNT("q2_query_output_rows", static_cast<long long>(rows.size()) - 1); // exclude header

    return rows;
}