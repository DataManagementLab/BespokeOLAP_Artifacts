#include "query16.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

// SQL:
/** select
    p_brand,
    p_type,
    p_size,
    count(distinct ps_suppkey) as supplier_cnt
from
    partsupp,
    part
where
    p_partkey = ps_partkey
    and p_brand <> '[BRAND]'
    and p_type not like '[TYPE]%'
    and p_size in ([SIZE1], [SIZE2], [SIZE3], [SIZE4], [SIZE5], [SIZE6], [SIZE7], [SIZE8])
    and ps_suppkey not in (
        select
            s_suppkey
        from
            supplier
        where
            s_comment like '%Customer%Complaints%'
    )
group by
    p_brand,
    p_type,
    p_size
order by
    supplier_cnt desc,
    p_brand,
    p_type,
    p_size; */

// ---------------------------------------------------------------------------
// Parallel implementation:
//
// Phase 1 (parallel): Each thread scans a disjoint range of part rows, emitting
//   (group_key<<20|suppkey) pairs into a thread-local vector.
//
// Phase 2 (parallel): Merge thread-local pairs into one contiguous global array
//   using parallel memcpy with pre-computed per-thread offsets.
//
// Phase 3 (parallel): 4-pass 11-bit parallel radix sort.
//   Each pass: parallel histogram build (1KB/thread, L1) → sequential prefix-sum
//   → parallel scatter. L1-cached histograms eliminate the 65536×96 = 6.3M-op
//   merge overhead of 16-bit radix, halving sort time vs the 3-pass 16-bit variant.
//
// Phase 4 (sequential): Count distinct suppkeys per group (linear scan).
// Phase 5 (sequential): Sort output rows + format.
// ---------------------------------------------------------------------------

std::vector<std::vector<std::string>> run_q16(Database* db, const Q16Args& args) {
    if (!db) {
        throw std::runtime_error("run_q16: db is null");
    }

    const PartTable&     pt  = db->part;
    const PartsuppTable& ps  = db->partsupp;
    const SupplierTable& sup = db->supplier;

    const std::string& excl_brand       = args.BRAND;
    const std::string& excl_type_prefix = args.TYPE;

    PROFILE_SCOPE("q16_total");

    // ── 1. Build complaint suppkey bitmap ─────────────────────────────────────
    const int64_t max_suppkey = sup.num_rows;
    TRACE_COUNT("q16_max_suppkey", max_suppkey);

    std::vector<uint8_t> is_complaint(static_cast<size_t>(max_suppkey + 1), 0u);
    {
    PROFILE_SCOPE("q16_build_complaint");
    for (int64_t i = 0; i < sup.num_rows; ++i) {
        const size_t sidx = static_cast<size_t>(i);
        if (sup.s_has_complaint[sidx])
            is_complaint[static_cast<size_t>(sup.s_suppkey[sidx])] = 1u;
    }
    }
    TRACE_COUNT("q16_total_supplier_rows", sup.num_rows);
    TRACE_COUNT("q16_total_part_rows",     pt.num_rows);
    TRACE_COUNT("q16_total_partsupp_rows", ps.num_rows);

    // ── 2. Build enum-based filter tables ─────────────────────────────────────
    std::array<bool, 256> size_ok{};
    size_ok.fill(false);
    {
        auto add_size = [&](const std::string& s) {
            if (s != "<<NULL>>") {
                int v = std::stoi(s);
                if (v >= 0 && v < 256) size_ok[static_cast<size_t>(v)] = true;
            }
        };
        add_size(args.SIZE1); add_size(args.SIZE2);
        add_size(args.SIZE3); add_size(args.SIZE4);
        add_size(args.SIZE5); add_size(args.SIZE6);
        add_size(args.SIZE7); add_size(args.SIZE8);
    }

    uint8_t excl_brand_code = 0xFF;
    for (size_t b = 0; b < pt.brand_names.size(); ++b) {
        if (pt.brand_names[b] == excl_brand) {
            excl_brand_code = static_cast<uint8_t>(b);
            break;
        }
    }

    std::array<bool, 256> excl_type_code{};
    excl_type_code.fill(false);
    for (size_t t = 0; t < pt.type_names.size(); ++t) {
        const std::string& tn = pt.type_names[t];
        if (tn.size() >= excl_type_prefix.size() &&
            tn.compare(0, excl_type_prefix.size(), excl_type_prefix) == 0)
            excl_type_code[t] = true;
    }

    // ── 3. Pre-compute sort ranks for output ─────────────────────────────────
    std::array<uint8_t, 256> brand_sort_rank{};
    std::array<uint8_t, 256> type_sort_rank{};
    {
        const size_t nb = pt.brand_names.size();
        std::vector<uint8_t> bidx(nb);
        std::iota(bidx.begin(), bidx.end(), uint8_t(0));
        std::sort(bidx.begin(), bidx.end(),
            [&](uint8_t a, uint8_t b){ return pt.brand_names[a] < pt.brand_names[b]; });
        for (size_t r = 0; r < nb; ++r) brand_sort_rank[bidx[r]] = static_cast<uint8_t>(r);
        const size_t nt = pt.type_names.size();
        std::vector<uint8_t> tidx(nt);
        std::iota(tidx.begin(), tidx.end(), uint8_t(0));
        std::sort(tidx.begin(), tidx.end(),
            [&](uint8_t a, uint8_t b){ return pt.type_names[a] < pt.type_names[b]; });
        for (size_t r = 0; r < nt; ++r) type_sort_rank[tidx[r]] = static_cast<uint8_t>(r);
    }

    // ── 4 + 5. Parallel combined part scan + partsupp access ──────────────────
    const CsrIndex& ps_csr    = db->ps_by_partkey;
    const int32_t* __restrict__ csr_offs    = ps_csr.offsets.data();
    const int32_t* __restrict__ ps_sk_ptr   = ps.ps_suppkey_i32.data();
    const uint8_t* __restrict__ compl_ptr   = is_complaint.data();
    const int64_t* __restrict__ p_pk_ptr    = pt.p_partkey.data();
    const uint8_t* __restrict__ p_brand_ptr = pt.p_brand.data();
    const uint8_t* __restrict__ p_type_ptr  = pt.p_type_enum.data();
    const int32_t* __restrict__ p_size_ptr  = pt.p_size.data();
    const size_t csr_max_pk = ps_csr.offsets.size() >= 1 ? ps_csr.offsets.size() - 1 : 0;
    const int32_t max_sk_i32 = static_cast<int32_t>(max_suppkey);
    const int64_t pt_n = pt.num_rows;
    const int n_threads = pool.num_threads;
    const size_t n_threads_sz = static_cast<size_t>(n_threads);
    // Note: ps_csr.row_ids is identity for TPC-H data (partsupp sorted by partkey)

    std::vector<std::vector<uint64_t>> thread_pairs(n_threads_sz);
    std::atomic<int64_t> total_parts_qualified{0};
    {
    PROFILE_SCOPE("q16_parallel_scan");
    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q16_scan_worker");
        const int64_t chunk = (pt_n + static_cast<int64_t>(nt) - 1) / static_cast<int64_t>(nt);
        const int64_t pi_lo = static_cast<int64_t>(tid) * chunk;
        const int64_t pi_hi = std::min(pi_lo + chunk, pt_n);

        auto& local_pairs = thread_pairs[static_cast<size_t>(tid)];
        local_pairs.reserve(static_cast<size_t>((pi_hi - pi_lo) / 2 + 16));

        int64_t parts_qual_local = 0;
        for (int64_t pi = pi_lo; pi < pi_hi; ++pi) {
            const size_t idx = static_cast<size_t>(pi);

            const uint8_t bc = p_brand_ptr[idx];
            if (bc == excl_brand_code) continue;
            const uint8_t tc = p_type_ptr[idx];
            if (excl_type_code[tc]) continue;
            const int32_t sz = p_size_ptr[idx];
            if (sz < 0 || sz >= 256 || !size_ok[static_cast<size_t>(sz)]) continue;

            const uint32_t gk = (static_cast<uint32_t>(bc) << 16) |
                                (static_cast<uint32_t>(tc) <<  8) |
                                 static_cast<uint32_t>(static_cast<uint8_t>(sz));
            ++parts_qual_local;

            const int64_t pk64 = p_pk_ptr[idx];
            if (pk64 < 1 || static_cast<size_t>(pk64) > csr_max_pk) continue;
            const size_t pk = static_cast<size_t>(pk64);

            const int32_t lo = csr_offs[pk];
            const int32_t hi = csr_offs[pk + 1];

            // Direct index: row_ids is identity for partkey-sorted partsupp
            for (int32_t ps_row = lo; ps_row < hi; ++ps_row) {
                const int32_t sk32 = ps_sk_ptr[ps_row];
                if (sk32 <= 0 || sk32 > max_sk_i32) continue;
                if (compl_ptr[static_cast<size_t>(sk32)]) continue;

                local_pairs.push_back((static_cast<uint64_t>(gk) << 20) |
                        static_cast<uint64_t>(static_cast<uint32_t>(sk32) & 0xFFFFFu));
            }
        }
        total_parts_qualified.fetch_add(parts_qual_local, std::memory_order_relaxed);
    });
    }

    // Compute prefix sums for placement in global array
    std::vector<size_t> thread_offsets(n_threads_sz + 1, 0);
    for (int t = 0; t < n_threads; ++t) {
        thread_offsets[static_cast<size_t>(t) + 1] =
            thread_offsets[static_cast<size_t>(t)] + thread_pairs[static_cast<size_t>(t)].size();
    }
    const size_t total_pairs_cnt = thread_offsets[n_threads_sz];

    TRACE_COUNT("q16_parts_qualified", total_parts_qualified.load());
    TRACE_COUNT("q16_ps_pairs_added", static_cast<long long>(total_pairs_cnt));

    // ── 6. Parallel memcpy + 4-pass 11-bit radix sort ─────────────────────────
    std::vector<uint64_t> pairs(total_pairs_cnt);
    {
    PROFILE_SCOPE("q16_parallel_copy");
    pool.parallel_for([&](int tid, int /*nt*/) {
        const size_t off = thread_offsets[static_cast<size_t>(tid)];
        const auto& src = thread_pairs[static_cast<size_t>(tid)];
        if (!src.empty())
            std::memcpy(pairs.data() + off, src.data(), src.size() * sizeof(uint64_t));
    });
    for (auto& v : thread_pairs) { std::vector<uint64_t>().swap(v); }
    }

    {
    PROFILE_SCOPE("q16_radix_sort");
    const size_t N = total_pairs_cnt;
    if (N > 0) {
        std::vector<uint64_t> buf(N);
        const size_t NBUCKETS = 2048;  // 11 bits = 8KB/thread (L1 cached)
        std::vector<uint32_t> th_hist(n_threads_sz * NBUCKETS);

        // 4 passes: 11+11+11+11 = 44 bits exactly
        const int pass_shifts[4] = {0, 11, 22, 33};
        const uint64_t pass_masks[4] = {0x7FFu, 0x7FFu, 0x7FFu, 0x7FFu};

        for (int pass = 0; pass < 4; ++pass) {
            PROFILE_SCOPE("q16_radix_pass");
            const int shift     = pass_shifts[pass];
            const uint64_t mask = pass_masks[pass];

            // Step A: parallel histogram (each thread zeros + fills its own 2KB slice)
            pool.parallel_for([&](int tid, int nt) {
                uint32_t* hist = th_hist.data() + static_cast<size_t>(tid) * NBUCKETS;
                std::memset(hist, 0, NBUCKETS * sizeof(uint32_t));
                const size_t chunk = (N + static_cast<size_t>(nt) - 1) / static_cast<size_t>(nt);
                const size_t lo = static_cast<size_t>(tid) * chunk;
                const size_t hi = std::min(lo + chunk, N);
                for (size_t i = lo; i < hi; ++i)
                    hist[(pairs[i] >> shift) & mask]++;
            });

            // Step B: sequential prefix-sum → per-thread per-digit start offsets
            {
                uint32_t running = 0u;
                for (size_t d = 0; d < NBUCKETS; ++d) {
                    for (size_t t = 0; t < n_threads_sz; ++t) {
                        uint32_t c = th_hist[t * NBUCKETS + d];
                        th_hist[t * NBUCKETS + d] = running;
                        running += c;
                    }
                }
            }

            // Step C: parallel scatter
            pool.parallel_for([&](int tid, int nt) {
                uint32_t* offsets = th_hist.data() + static_cast<size_t>(tid) * NBUCKETS;
                const size_t chunk = (N + static_cast<size_t>(nt) - 1) / static_cast<size_t>(nt);
                const size_t lo = static_cast<size_t>(tid) * chunk;
                const size_t hi = std::min(lo + chunk, N);
                for (size_t i = lo; i < hi; ++i) {
                    const size_t digit = (pairs[i] >> shift) & mask;
                    buf[offsets[digit]++] = pairs[i];
                }
            });

            std::swap(pairs, buf);
        }
    }
    }
    TRACE_COUNT("q16_pairs_sorted", static_cast<long long>(pairs.size()));

    // ── 7. Aggregate: count distinct suppkeys per group (linear scan) ─────────
    struct AggRow {
        uint64_t sort_key;
        uint32_t group_key;
        int32_t  supplier_cnt;
    };
    std::vector<AggRow> agg_result;
    {
    PROFILE_SCOPE("q16_aggregate");
    const size_t n = pairs.size();
    agg_result.reserve(512);
    size_t j = 0;
    while (j < n) {
        const uint64_t cur    = pairs[j];
        const uint32_t cur_gk = static_cast<uint32_t>(cur >> 20);
        uint32_t prev_sk      = static_cast<uint32_t>(cur & 0xFFFFFu);
        int32_t  cnt          = 1;
        ++j;
        while (j < n && static_cast<uint32_t>(pairs[j] >> 20) == cur_gk) {
            const uint32_t sk = static_cast<uint32_t>(pairs[j] & 0xFFFFFu);
            if (sk != prev_sk) { ++cnt; prev_sk = sk; }
            ++j;
        }
        const uint8_t bc = static_cast<uint8_t>((cur_gk >> 16) & 0xFF);
        const uint8_t tc = static_cast<uint8_t>((cur_gk >>  8) & 0xFF);
        const uint8_t sz = static_cast<uint8_t>( cur_gk         & 0xFF);
        const uint64_t sk =
            (static_cast<uint64_t>(static_cast<uint16_t>(65535 - cnt)) << 48) |
            (static_cast<uint64_t>(brand_sort_rank[bc]) << 40) |
            (static_cast<uint64_t>(type_sort_rank[tc])  << 32) |
            static_cast<uint64_t>(static_cast<uint32_t>(sz));
        agg_result.push_back({sk, cur_gk, cnt});
    }
    TRACE_COUNT("q16_groups_created", static_cast<long long>(agg_result.size()));
    }

    // ── 8. Sort and format output ─────────────────────────────────────────────
    {
    PROFILE_SCOPE("q16_sort");
    std::sort(agg_result.begin(), agg_result.end(),
        [](const AggRow& a, const AggRow& b) { return a.sort_key < b.sort_key; });
    }
    TRACE_COUNT("q16_sort_rows", static_cast<long long>(agg_result.size()));

    std::vector<std::vector<std::string>> rows;
    rows.reserve(agg_result.size() + 1);
    rows.push_back({"p_brand", "p_type", "p_size", "supplier_cnt"});
    {
    PROFILE_SCOPE("q16_format_output");
    for (const auto& ar : agg_result) {
        const uint8_t bc = static_cast<uint8_t>((ar.group_key >> 16) & 0xFF);
        const uint8_t tc = static_cast<uint8_t>((ar.group_key >>  8) & 0xFF);
        const uint8_t sz = static_cast<uint8_t>( ar.group_key         & 0xFF);
        rows.push_back({
            pt.brand_names[bc],
            pt.type_names[tc],
            std::to_string(static_cast<int32_t>(sz)),
            std::to_string(ar.supplier_cnt)
        });
    }
    }
    TRACE_COUNT("q16_query_output_rows", static_cast<long long>(rows.size()) - 1);
    return rows;
}