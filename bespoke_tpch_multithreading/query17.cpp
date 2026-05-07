#include "query17.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <cstdint>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

// SQL:
/** select
    sum (l_extendedprice) / 7.0 as avg_yearly
from
    lineitem,
    part
where
    p_partkey = l_partkey
    and p_brand = '[BRAND]'
    and p_container = '[CONTAINER]'
    and l_quantity < (
        select
            0.2 * avg(l_quantity)
        from
            lineitem
        where
            l_partkey = p_partkey
    ); */

std::vector<std::vector<std::string>> run_q17(Database* db, const Q17Args& args) {
    if (!db) {
        throw std::runtime_error("run_q17: db is null");
    }

    const PartTable&     pt  = db->part;
    const LineitemTable& li  = db->lineitem;

    PROFILE_SCOPE("q17_total");

    // ── 1. Resolve brand and container enum codes ────────────────────────────
    //
    // p_brand and p_container are stored as uint8 enum codes.
    // Walk the decode tables to find the code that matches the argument string.
    // If not found in the dictionary the result will be empty (no parts match).

    int brand_code     = -1;
    int container_code = -1;

    {
    PROFILE_SCOPE("q17_enum_lookup");

    for (int i = 0; i < static_cast<int>(pt.brand_names.size()); ++i) {
        if (pt.brand_names[static_cast<size_t>(i)] == args.BRAND) {
            brand_code = i;
            break;
        }
    }
    for (int i = 0; i < static_cast<int>(pt.container_names.size()); ++i) {
        if (pt.container_names[static_cast<size_t>(i)] == args.CONTAINER) {
            container_code = i;
            break;
        }
    }
    } // end enum_lookup

    // ── 2. Identify qualifying parts and precompute per-partkey avg quantity ─
    //
    // For each part with the requested brand+container:
    //   a) Locate all lineitem rows via the li_by_partkey CSR index.
    //   b) Compute avg_qty = avg(l_quantity) over those lineitems.
    //   c) The correlated-subquery threshold is 0.2 * avg_qty.
    //   d) Sum l_extendedprice for lineitems where l_quantity < threshold.
    //
    // li_by_partkey.offsets[pk]   → start offset into li_by_partkey.row_ids
    // li_by_partkey.offsets[pk+1] → exclusive end offset
    // li_by_partkey.row_ids[...]  → lineitem row indices for that partkey
    //
    // Optimization: use sequential companion arrays li_by_pk_qty[j] and
    // li_by_pk_ep[j] (float, indexed by CSR slot j directly) instead of
    // scattered row_ids[] indirection into the large lineitem column arrays.
    // This converts random memory access into sequential streaming reads,
    // which maximizes cache-line utilization and enables auto-vectorization.

    double sum_extprice = 0.0;

    if (brand_code < 0 || container_code < 0) {
        // No parts can match; emit zero counts and skip computation
        TRACE_COUNT("q17_parts_matched",      0LL);
        TRACE_COUNT("q17_parts_with_li",      0LL);
        TRACE_COUNT("q17_li_entries_visited", 0LL);
        TRACE_COUNT("q17_li_below_threshold", 0LL);
        TRACE_COUNT("q17_sum_extprice_cents", 0LL);
        TRACE_COUNT("q17_bc_parts_in_bucket", 0LL);
    } else {
        // Only proceed if both enum codes were found
        const uint8_t brand_u8     = static_cast<uint8_t>(brand_code);
        const uint8_t container_u8 = static_cast<uint8_t>(container_code);

        const CsrIndex& li_pk = db->li_by_partkey;

        // ── Use the part_by_brand_container CSR index to skip full 4M part scan ──
        // Composite key = brand_code * 256 + container_code
        const CsrIndex& bc_csr = db->part_by_brand_container;
        const int32_t bc_key = static_cast<int32_t>(brand_u8) * 256 + static_cast<int32_t>(container_u8);
        const size_t bc_key_sz = bc_csr.offsets.size();

        int64_t parts_matched      = 0;
        int64_t parts_with_li      = 0;
        int64_t li_entries_visited = 0;
        int64_t li_below_threshold = 0;

        // Sequential companion arrays (CSR-slot indexed, no row_ids indirection):
        //   li_by_pk_qty[j]: l_quantity as float  — 4 bytes/entry, sequential
        //   li_by_pk_ep[j]:  l_extendedprice as float — 4 bytes/entry, sequential
        const float*    pk_qty     = db->li_by_pk_qty.data();
        const float*    pk_ep      = db->li_by_pk_ep.data();
        const int32_t*  offsets    = li_pk.offsets.data();
        const int32_t*  bc_row_ids = bc_csr.row_ids.data();
        const int64_t*  partkey    = pt.p_partkey.data();

        {
        PROFILE_SCOPE("q17_brand_container_lookup");

        // Guard: if bc_key is out of range, no parts match
        if (static_cast<size_t>(bc_key) + 1 < bc_key_sz) {
            const int32_t bc_begin = bc_csr.offsets[static_cast<size_t>(bc_key)];
            const int32_t bc_end   = bc_csr.offsets[static_cast<size_t>(bc_key) + 1];

            TRACE_COUNT("q17_bc_parts_in_bucket", static_cast<int64_t>(bc_end - bc_begin));

            // ── Parallel scan over parts in bc bucket ──────────────────────
            // Each thread processes a disjoint range [bci_lo, bci_hi) of the
            // brand×container CSR bucket.  Thread-local accumulators are merged
            // after the parallel_for barrier.
            const int32_t bc_count = bc_end - bc_begin;
            const int     n_threads = pool.num_threads;

            // Per-thread accumulators (cache-line padded to avoid false sharing)
            struct alignas(64) ThreadAcc {
                double  sum_ep          = 0.0;
                int64_t parts_matched   = 0;
                int64_t parts_with_li   = 0;
                int64_t li_visited      = 0;
                int64_t li_below        = 0;
            };
            std::vector<ThreadAcc> accs(static_cast<size_t>(n_threads));

            pool.parallel_for([&](int tid, int nt) {
                PROFILE_SCOPE("q17_parallel_part_scan");

                // Divide the bc bucket range evenly across threads
                const int32_t chunk = (bc_count + nt - 1) / nt;
                const int32_t lo    = bc_begin + tid * chunk;
                const int32_t hi    = std::min(lo + chunk, bc_end);

                ThreadAcc& acc = accs[static_cast<size_t>(tid)];

                for (int32_t bci = lo; bci < hi; ++bci) {
                    const size_t pidx = static_cast<size_t>(bc_row_ids[static_cast<size_t>(bci)]);
                    ++acc.parts_matched;

                    const int64_t pk = partkey[pidx];
                    if (pk < 0 || static_cast<size_t>(pk) + 1 >= static_cast<size_t>(li_pk.offsets.size())) continue;

                    const int32_t csr_begin = offsets[static_cast<size_t>(pk)];
                    const int32_t csr_end   = offsets[static_cast<size_t>(pk) + 1];
                    const int32_t cnt       = csr_end - csr_begin;

                    if (cnt == 0) continue;

                    ++acc.parts_with_li;
                    acc.li_visited += cnt;

                    const float* __restrict__ qty_ptr = pk_qty + csr_begin;
                    const float* __restrict__ ep_ptr  = pk_ep  + csr_begin;

                    // Pass 1: sum quantities
                    float qty_sum_f = 0.0f;
                    for (int32_t i = 0; i < cnt; ++i) {
                        qty_sum_f += qty_ptr[i];
                    }

                    // threshold = 0.2 * avg(qty) = qty_sum / (5 * cnt)
                    const float threshold = qty_sum_f * (1.0f / 5.0f) / static_cast<float>(cnt);

                    // Pass 2: accumulate extended price for rows below threshold
                    float ep_sum = 0.0f;
                    for (int32_t i = 0; i < cnt; ++i) {
                        const float below_f = static_cast<float>(qty_ptr[i] < threshold);
                        ep_sum += ep_ptr[i] * below_f;
                    }
#ifdef TRACE
                    for (int32_t i = 0; i < cnt; ++i) {
                        acc.li_below += static_cast<int64_t>(qty_ptr[i] < threshold);
                    }
#endif
                    acc.sum_ep += static_cast<double>(ep_sum);
                }

                TRACE_COUNT("q17_thread_parts_matched", acc.parts_matched);
                TRACE_COUNT("q17_thread_li_visited",    acc.li_visited);
            });

            // ── Merge thread-local accumulators ────────────────────────────
            {
            PROFILE_SCOPE("q17_merge");
            for (int t = 0; t < n_threads; ++t) {
                const ThreadAcc& acc = accs[static_cast<size_t>(t)];
                sum_extprice       += acc.sum_ep;
                parts_matched      += acc.parts_matched;
                parts_with_li      += acc.parts_with_li;
                li_entries_visited += acc.li_visited;
                li_below_threshold += acc.li_below;
            }
            } // end merge
        } else {
            TRACE_COUNT("q17_bc_parts_in_bucket", 0LL);
        }
        } // end q17_brand_container_lookup

        TRACE_COUNT("q17_parts_matched",      parts_matched);
        TRACE_COUNT("q17_parts_with_li",      parts_with_li);
        TRACE_COUNT("q17_li_entries_visited", li_entries_visited);
        TRACE_COUNT("q17_li_below_threshold", li_below_threshold);
        // Sum as integer cents for sanity-checking
        TRACE_COUNT("q17_sum_extprice_cents", static_cast<long long>(sum_extprice * 100.0 + 0.5));
    }

    // ── 3. Format output ─────────────────────────────────────────────────────
    //
    // avg_yearly = sum(l_extendedprice) / 7.0, formatted to 2 decimal places
    const double avg_yearly = sum_extprice / 7.0;
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << avg_yearly;

    // avg_yearly as integer cents for sanity-checking
    TRACE_COUNT("q17_avg_yearly_cents", static_cast<long long>(avg_yearly * 100.0 + 0.5));
    TRACE_COUNT("q17_query_output_rows", 1LL);  // always exactly one result row

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"avg_yearly"});
    rows.push_back({oss.str()});
    return rows;
}