#include "query19.hpp"
#include "db_loader.hpp"
#include "thread_pool.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();
#include <immintrin.h>
#include <algorithm>

#include <cstdint>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

// SQL:
/** select
    sum(l_extendedprice * (1 - l_discount) ) as revenue
from
    lineitem,
    part
where
    (
        p_partkey = l_partkey
        and p_brand = '[BRAND1]'
        and p_container in ( 'SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
        and l_quantity >= [QUANTITY1] and l_quantity <= [QUANTITY1] + 10
        and p_size between 1 and 5
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = '[BRAND2]'
        and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
        and l_quantity >= [QUANTITY2] and l_quantity <= [QUANTITY2] + 10
        and p_size between 1 and 10
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    )
    or
    (
        p_partkey = l_partkey
        and p_brand = '[BRAND3]'
        and p_container in ( 'LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
        and l_quantity >= [QUANTITY3] and l_quantity <= [QUANTITY3] + 10
        and p_size between 1 and 15
        and l_shipmode in ('AIR', 'AIR REG')
        and l_shipinstruct = 'DELIVER IN PERSON'
    ); */

// ─── Helper: look up a single enum code by name ───────────────────────────
static int find_enum(const std::vector<std::string>& names, const std::string& target) {
    for (int i = 0; i < static_cast<int>(names.size()); ++i) {
        if (names[static_cast<size_t>(i)] == target) return i;
    }
    return -1;
}

std::vector<std::vector<std::string>> run_q19(Database* db, const Q19Args& args) {
    if (!db) {
        throw std::runtime_error("run_q19: db is null");
    }

    const PartTable&     pt = db->part;
    const LineitemTable& li = db->lineitem;
    const CsrIndex&      li_pk_csr = db->li_by_partkey;

    PROFILE_SCOPE("q19_total");

    // ── 0. Basic counts ──────────────────────────────────────────────────────
    TRACE_COUNT("q19_total_part_rows",     pt.num_rows);
    TRACE_COUNT("q19_total_lineitem_rows", li.num_rows);

    // ── 1. Parse quantity thresholds ─────────────────────────────────────────
    // l_quantity is stored as double; the SQL uses integer comparison bounds.
    const double qty1 = std::stod(args.QUANTITY1);
    const double qty2 = std::stod(args.QUANTITY2);
    const double qty3 = std::stod(args.QUANTITY3);

    // ── 2. Resolve brand enum codes ──────────────────────────────────────────
    const int brand1_code = find_enum(pt.brand_names, args.BRAND1);
    const int brand2_code = find_enum(pt.brand_names, args.BRAND2);
    const int brand3_code = find_enum(pt.brand_names, args.BRAND3);

    // ── 3. Resolve container enum codes ─────────────────────────────────────
    // Branch 1 containers: SM CASE, SM BOX, SM PACK, SM PKG
    const int cont_sm_case = find_enum(pt.container_names, "SM CASE");
    const int cont_sm_box  = find_enum(pt.container_names, "SM BOX");
    const int cont_sm_pack = find_enum(pt.container_names, "SM PACK");
    const int cont_sm_pkg  = find_enum(pt.container_names, "SM PKG");

    // Branch 2 containers: MED BAG, MED BOX, MED PKG, MED PACK
    const int cont_med_bag  = find_enum(pt.container_names, "MED BAG");
    const int cont_med_box  = find_enum(pt.container_names, "MED BOX");
    const int cont_med_pkg  = find_enum(pt.container_names, "MED PKG");
    const int cont_med_pack = find_enum(pt.container_names, "MED PACK");

    // Branch 3 containers: LG CASE, LG BOX, LG PACK, LG PKG
    const int cont_lg_case = find_enum(pt.container_names, "LG CASE");
    const int cont_lg_box  = find_enum(pt.container_names, "LG BOX");
    const int cont_lg_pack = find_enum(pt.container_names, "LG PACK");
    const int cont_lg_pkg  = find_enum(pt.container_names, "LG PKG");

    // ── 4. Resolve shipmode enum codes for AIR and AIR REG ───────────────────
    const int ship_air     = find_enum(li.shipmode_names, "AIR");
    const int ship_air_reg = find_enum(li.shipmode_names, "AIR REG");


    // ── 5. Resolve shipinstruct enum code for DELIVER IN PERSON ──────────────
    const int instruct_dip = find_enum(li.shipinstruct_names, "DELIVER IN PERSON");

    // ── 6. Classify parts into branch masks and collect CSR slices ──────────
    // Instead of scanning all 120M lineitem rows, we use li_by_partkey CSR to
    // iterate only the lineitem rows belonging to eligible parts (~10K rows vs 120M).
    // For each eligible part, store: (csr_lo, csr_hi, branch_mask, qty_lo, qty_hi).

    struct EligiblePart {
        int32_t csr_lo;
        int32_t csr_hi;
        uint8_t bmask;
        uint8_t qty_lo;   // lower quantity bound (uint8)
        uint8_t qty_hi;   // upper quantity bound (uint8)
    };

    // Precompute integer quantity bounds
    const uint8_t iqty1_lo = static_cast<uint8_t>(static_cast<int>(qty1));
    const uint8_t iqty1_hi = static_cast<uint8_t>(static_cast<int>(qty1) + 10);
    const uint8_t iqty2_lo = static_cast<uint8_t>(static_cast<int>(qty2));
    const uint8_t iqty2_hi = static_cast<uint8_t>(static_cast<int>(qty2) + 10);
    const uint8_t iqty3_lo = static_cast<uint8_t>(static_cast<int>(qty3));
    const uint8_t iqty3_hi = static_cast<uint8_t>(static_cast<int>(qty3) + 10);

    // ── 6b. Use part_by_brand_container CSR to skip the full 400K-part scan ──
    // Instead of iterating all parts, directly look up the 12 brand×container
    // combinations (4 per branch).  Only the p_size filter remains to apply.
    const CsrIndex& bc_csr = db->part_by_brand_container;
    const int32_t bc_max_key = static_cast<int32_t>(bc_csr.offsets.size()) - 2;

    auto bc_key = [](int brand, int container) -> int32_t {
        return brand * 256 + container;
    };
    auto bc_slice = [&](int32_t key, int32_t& lo, int32_t& hi) {
        if (key < 0 || key > bc_max_key) { lo = hi = 0; return; }
        lo = bc_csr.offsets[static_cast<size_t>(key)];
        hi = bc_csr.offsets[static_cast<size_t>(key) + 1];
    };

    const int32_t csr_max_key = static_cast<int32_t>(li_pk_csr.offsets.size()) - 2;

    std::vector<EligiblePart> eligible_parts;
    // Reserve ~1% of total parts: 12 brand×container combos × avg 10% size filter ×
    // total parts. At SF=20 this is ~30K; over-allocating is cheap vs realloc cost.
    eligible_parts.reserve(static_cast<size_t>(pt.num_rows / 25) + 1024);

    int64_t parts_branch1 = 0, parts_branch2 = 0, parts_branch3 = 0;

    const int32_t* __restrict__ psize_ptr    = pt.p_size.data();
    const int64_t* __restrict__ ppartkey_ptr = pt.p_partkey.data();
    const int32_t* __restrict__ bc_row_ids   = bc_csr.row_ids.data();
    const int32_t* __restrict__ li_pk_offs   = li_pk_csr.offsets.data();

    {
        PROFILE_SCOPE("q19_part_classification");

        // Branch 1: brand1 × {SM CASE, SM BOX, SM PACK, SM PKG}, p_size 1..5
        if (brand1_code >= 0) {
            const int bc_containers[4] = {cont_sm_case, cont_sm_box, cont_sm_pack, cont_sm_pkg};
            for (int ci = 0; ci < 4; ++ci) {
                if (bc_containers[ci] < 0) continue;
                int32_t lo, hi;
                bc_slice(bc_key(brand1_code, bc_containers[ci]), lo, hi);
                for (int32_t j = lo; j < hi; ++j) {
                    const int32_t pi = bc_row_ids[j];
                    const int32_t ps = psize_ptr[pi];
                    if (ps < 1 || ps > 5) continue;
                    const int64_t pk = ppartkey_ptr[pi];
                    if (pk < 1 || pk > csr_max_key) continue;
                    const int32_t csr_lo = li_pk_offs[static_cast<size_t>(pk)];
                    const int32_t csr_hi = li_pk_offs[static_cast<size_t>(pk) + 1];
                    if (csr_lo >= csr_hi) continue;
                    eligible_parts.push_back({csr_lo, csr_hi, 1u, iqty1_lo, iqty1_hi});
                    ++parts_branch1;
                }
            }
        }

        // Branch 2: brand2 × {MED BAG, MED BOX, MED PKG, MED PACK}, p_size 1..10
        if (brand2_code >= 0) {
            const int bc_containers[4] = {cont_med_bag, cont_med_box, cont_med_pkg, cont_med_pack};
            for (int ci = 0; ci < 4; ++ci) {
                if (bc_containers[ci] < 0) continue;
                int32_t lo, hi;
                bc_slice(bc_key(brand2_code, bc_containers[ci]), lo, hi);
                for (int32_t j = lo; j < hi; ++j) {
                    const int32_t pi = bc_row_ids[j];
                    const int32_t ps = psize_ptr[pi];
                    if (ps < 1 || ps > 10) continue;
                    const int64_t pk = ppartkey_ptr[pi];
                    if (pk < 1 || pk > csr_max_key) continue;
                    const int32_t csr_lo = li_pk_offs[static_cast<size_t>(pk)];
                    const int32_t csr_hi = li_pk_offs[static_cast<size_t>(pk) + 1];
                    if (csr_lo >= csr_hi) continue;
                    eligible_parts.push_back({csr_lo, csr_hi, 2u, iqty2_lo, iqty2_hi});
                    ++parts_branch2;
                }
            }
        }

        // Branch 3: brand3 × {LG CASE, LG BOX, LG PACK, LG PKG}, p_size 1..15
        if (brand3_code >= 0) {
            const int bc_containers[4] = {cont_lg_case, cont_lg_box, cont_lg_pack, cont_lg_pkg};
            for (int ci = 0; ci < 4; ++ci) {
                if (bc_containers[ci] < 0) continue;
                int32_t lo, hi;
                bc_slice(bc_key(brand3_code, bc_containers[ci]), lo, hi);
                for (int32_t j = lo; j < hi; ++j) {
                    const int32_t pi = bc_row_ids[j];
                    const int32_t ps = psize_ptr[pi];
                    if (ps < 1 || ps > 15) continue;
                    const int64_t pk = ppartkey_ptr[pi];
                    if (pk < 1 || pk > csr_max_key) continue;
                    const int32_t csr_lo = li_pk_offs[static_cast<size_t>(pk)];
                    const int32_t csr_hi = li_pk_offs[static_cast<size_t>(pk) + 1];
                    if (csr_lo >= csr_hi) continue;
                    eligible_parts.push_back({csr_lo, csr_hi, 4u, iqty3_lo, iqty3_hi});
                    ++parts_branch3;
                }
            }
        }
    }

    TRACE_COUNT("q19_parts_branch1_eligible", parts_branch1);
    TRACE_COUNT("q19_parts_branch2_eligible", parts_branch2);
    TRACE_COUNT("q19_parts_branch3_eligible", parts_branch3);
    TRACE_COUNT("q19_eligible_parts_total",   static_cast<int64_t>(eligible_parts.size()));

    // ── 7. Sort eligible parts for sequential CSR access ─────────────────────
    // Sort eligible_parts by csr_lo so all companion-array accesses are in
    // strictly increasing order — enabling hardware prefetch.

    {
        PROFILE_SCOPE("q19_sort_eligible_parts");
        std::sort(eligible_parts.begin(), eligible_parts.end(),
                  [](const EligiblePart& a, const EligiblePart& b) {
                      return a.csr_lo < b.csr_lo;
                  });
    }

    // ── 8. Probe lineitems in parallel ───────────────────────────────────────
    // Each thread processes a disjoint range of eligible_parts.
    // Per-thread local revenue avoids any synchronization in the hot loop.
    // Sequential companion arrays (indexed directly by CSR slot j):
    const float*    __restrict__ ep_csr      = db->li_by_pk_ep.data();
    const float*    __restrict__ disc_csr    = db->li_by_pk_disc.data();
    const float*    __restrict__ qty_csr     = db->li_by_pk_qty.data();
    const uint8_t*  __restrict__ sm_si_csr   = db->li_by_pk_sm_si.data();

    // Build packed accept table: sm_si_accept[packed] = 1 if the entry passes
    // both shipmode (AIR or AIR REG) and shipinstruct (DELIVER IN PERSON) filters.
    // packed = (shipmode & 0x0F) | ((shipinstruct & 0x0F) << 4)
    // 256-entry table, fits in a cache line.
    const uint8_t sm_air_u8     = (ship_air     >= 0) ? static_cast<uint8_t>(ship_air     & 0x0F) : 0xFF;
    const uint8_t sm_air_reg_u8 = (ship_air_reg >= 0) ? static_cast<uint8_t>(ship_air_reg & 0x0F) : 0xFF;
    const uint8_t si_dip_u8     = (instruct_dip >= 0) ? static_cast<uint8_t>(instruct_dip & 0x0F) : 0xFF;

    uint8_t sm_si_accept[256] = {};
    for (int sm = 0; sm < 16; ++sm) {
        for (int si = 0; si < 16; ++si) {
            uint8_t packed = static_cast<uint8_t>((sm & 0x0F) | ((si & 0x0F) << 4));
            bool sm_ok = (sm == sm_air_u8 || sm == sm_air_reg_u8);
            bool si_ok = (si == si_dip_u8);
            sm_si_accept[packed] = (sm_ok && si_ok) ? 1u : 0u;
        }
    }

    const int n_threads = pool.num_threads;
    const size_t n_parts = eligible_parts.size();

    // Per-thread local accumulators — cache-line padded to avoid false sharing.
    struct alignas(64) LocalAcc {
        double   revenue         = 0.0;
        int64_t  li_sm_pass      = 0;
        int64_t  li_inst_pass    = 0;
        int64_t  li_qualifying   = 0;
        char     _pad[64 - sizeof(double) - 3*sizeof(int64_t)] = {};
    };
    std::vector<LocalAcc> local(static_cast<size_t>(n_threads));

    // Copy sm_si_accept into a local array captured by value so each thread
    // has its own copy on the stack (avoids the shared read from a pointer).
    uint8_t sm_si_accept_copy[256];
    std::copy(sm_si_accept, sm_si_accept + 256, sm_si_accept_copy);

    {
        PROFILE_SCOPE("q19_lineitem_scan");
        pool.parallel_for([&](int tid, int n_t) {
            PROFILE_SCOPE("q19_lineitem_scan_thread");
            // Each thread handles a contiguous slice of eligible_parts.
            const size_t chunk = (n_parts + static_cast<size_t>(n_t) - 1)
                                 / static_cast<size_t>(n_t);
            const size_t p_begin = static_cast<size_t>(tid) * chunk;
            const size_t p_end   = std::min(p_begin + chunk, n_parts);

            double   rev       = 0.0;
            int64_t  sm_pass   = 0;
            int64_t  inst_pass = 0;
            int64_t  qual      = 0;

            // Tile sm_si_accept into thread-local stack for L1 cache locality.
            uint8_t local_accept[256];
            std::copy(sm_si_accept_copy, sm_si_accept_copy + 256, local_accept);

            for (size_t pi = p_begin; pi < p_end; ++pi) {
                const EligiblePart& ep  = eligible_parts[pi];
                const uint8_t qty_lo    = ep.qty_lo;
                const uint8_t qty_hi    = ep.qty_hi;
                const int32_t ep_lo     = ep.csr_lo;
                const int32_t ep_hi     = ep.csr_hi;

                for (int32_t j = ep_lo; j < ep_hi; ++j) {
                    const uint8_t sm_si = sm_si_csr[j];
                    if (!local_accept[sm_si]) continue;
                    ++sm_pass;
                    ++inst_pass;

                    const auto qty8 = static_cast<uint8_t>(qty_csr[j]);
                    if (qty8 < qty_lo || qty8 > qty_hi) continue;

                    rev += static_cast<double>(ep_csr[j]) *
                           (1.0 - static_cast<double>(disc_csr[j]));
                    ++qual;
                }
            }

            local[tid].revenue       = rev;
            local[tid].li_sm_pass    = sm_pass;
            local[tid].li_inst_pass  = inst_pass;
            local[tid].li_qualifying = qual;
        });
    } // end q19_lineitem_scan

    // Merge per-thread results
    double  revenue          = 0.0;
    int64_t li_shipmode_pass = 0;
    int64_t li_instruct_pass = 0;
    int64_t li_qualifying    = 0;
    for (int t = 0; t < n_threads; ++t) {
        revenue          += local[t].revenue;
        li_shipmode_pass += local[t].li_sm_pass;
        li_instruct_pass += local[t].li_inst_pass;
        li_qualifying    += local[t].li_qualifying;
    }

    TRACE_COUNT("q19_li_shipmode_pass", li_shipmode_pass);
    TRACE_COUNT("q19_li_instruct_pass", li_instruct_pass);
    TRACE_COUNT("q19_li_part_eligible", static_cast<int64_t>(eligible_parts.size()));
    TRACE_COUNT("q19_li_qualifying",    li_qualifying);
    TRACE_COUNT("q19_revenue_cents",    static_cast<long long>(revenue * 100.0 + 0.5));

    // ── 8. Format output ─────────────────────────────────────────────────────
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2) << revenue;

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"revenue"});
    rows.push_back({oss.str()});
    TRACE_COUNT("q19_query_output_rows", 1LL);  // always exactly one result row

    return rows;
}