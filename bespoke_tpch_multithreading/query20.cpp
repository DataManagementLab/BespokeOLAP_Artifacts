#include "query20.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <array>
#include <atomic>
#include <cstdint>
#include <climits>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <algorithm>
#include <vector>
#include <mutex>

// SQL:
/** select
    s_name,
    s_address
from
    supplier, nation
where
    s_suppkey in (
        select
            ps_suppkey
        from
            partsupp
        where
            ps_partkey in (
                select
                    p_partkey
                from
                    part
                where
                    p_name like '[COLOR]%'
                    )
        and ps_availqty > (
            select
                0.5 * sum(l_quantity)
            from
                lineitem
            where
                l_partkey = ps_partkey
                and l_suppkey = ps_suppkey
                and l_shipdate >= date('[DATE]')
                and l_shipdate < date('[DATE]') + interval '1' year
        )
    )
    and s_nationkey = n_nationkey
    and n_name = '[NATION]'
order by
    s_name; */

std::vector<std::vector<std::string>> run_q20(Database* db, const Q20Args& args) {
    if (!db) {
        throw std::runtime_error("run_q20: db is null");
    }

    // ── Local date helpers ────────────────────────────────────────────────────
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

    auto date_plus_one_year = [&date_to_days](const std::string& s) -> int32_t {
        int y = std::stoi(s.substr(0, 4));
        int m = std::stoi(s.substr(5, 2));
        int d = std::stoi(s.substr(8, 2));
        y += 1;
        if (m == 2 && d == 29) d = 28;
        char buf[11];
        std::snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
        return date_to_days(std::string(buf));
    };

    const LineitemTable&  li  = db->lineitem;
    const PartsuppTable&  ps  = db->partsupp;
    const PartTable&      pt  = db->part;
    const SupplierTable&  sup = db->supplier;
    const NationTable&    nat = db->nation;

    PROFILE_SCOPE("q20_total");

    TRACE_COUNT("q20_total_part_rows",     pt.num_rows);
    TRACE_COUNT("q20_total_partsupp_rows", ps.num_rows);
    TRACE_COUNT("q20_total_lineitem_rows", li.num_rows);
    TRACE_COUNT("q20_total_supplier_rows", sup.num_rows);

    const std::string& color_prefix = args.COLOR;
    int32_t date_lo = date_to_days(args.DATE);
    int32_t date_hi = date_plus_one_year(args.DATE);

    // ── 2. Resolve nation key ─────────────────────────────────────────────────
    auto nat_it = nat.name_to_nationkey.find(args.NATION);
    if (nat_it == nat.name_to_nationkey.end()) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"s_name", "s_address"});
        TRACE_COUNT("q20_query_output_rows", 0LL);
        return rows;
    }
    int32_t target_nationkey = nat_it->second;

    const int64_t max_partkey = pt.num_rows;
    const int64_t max_suppkey = sup.num_rows;
    const int32_t n_threads   = pool.num_threads;

    // ── 3. Collect qualifying part keys (p_name LIKE 'COLOR%') ──────────────
    //   Parallel scan: each thread scans a disjoint range of part rows and
    //   appends to a thread-local list. Results are merged sequentially.
    //   partkey_qualifies[] is written atomically at byte level — no races since
    //   partkey is unique per row (each partkey written by exactly one thread).
    std::vector<uint8_t> partkey_qualifies(static_cast<size_t>(max_partkey + 1), 0);
    std::vector<int32_t> qualifying_partkey_list;
    qualifying_partkey_list.reserve(60000);

    {
    PROFILE_SCOPE("q20_part_scan");
    const auto& w1_to_code  = pt.p_name_word1_to_code;
    const uint8_t* w1_arr   = pt.p_name_word1_u8.data();
    auto w1_it = w1_to_code.find(color_prefix);

    const int64_t nr = pt.num_rows;
    // Per-thread local lists to avoid any contention on qualifying_partkey_list
    std::vector<std::vector<int32_t>> tl_pklists(static_cast<size_t>(n_threads));
    for (auto& v : tl_pklists) v.reserve(60000 / n_threads + 64);

    if (pt.p_name_word1_u8.empty() || w1_it == w1_to_code.end()) {
        // Fallback: string prefix comparison
        const size_t prefix_len = color_prefix.size();
        const char*  prefix_ptr = color_prefix.data();
        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q20_part_scan_fallback");
            int64_t lo = (nr * tid)       / nt;
            int64_t hi = (nr * (tid + 1)) / nt;
            auto& local = tl_pklists[static_cast<size_t>(tid)];
            for (int64_t i = lo; i < hi; ++i) {
                const std::string& pname = pt.p_name[static_cast<size_t>(i)];
                if (pname.size() >= prefix_len &&
                    std::memcmp(pname.data(), prefix_ptr, prefix_len) == 0) {
                    int32_t pk = static_cast<int32_t>(pt.p_partkey[static_cast<size_t>(i)]);
                    partkey_qualifies[static_cast<size_t>(pk)] = 1;
                    local.push_back(pk);
                }
            }
        });
    } else {
        // Fast path: single uint8_t word1-code comparison per row
        const uint8_t target_code = w1_it->second;
        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q20_part_scan_fast");
            int64_t lo = (nr * tid)       / nt;
            int64_t hi = (nr * (tid + 1)) / nt;
            auto& local = tl_pklists[static_cast<size_t>(tid)];
            for (int64_t i = lo; i < hi; ++i) {
                if (w1_arr[static_cast<size_t>(i)] == target_code) {
                    int32_t pk = static_cast<int32_t>(pt.p_partkey[static_cast<size_t>(i)]);
                    partkey_qualifies[static_cast<size_t>(pk)] = 1;
                    local.push_back(pk);
                }
            }
        });
    }

    // Merge thread-local lists (disjoint ranges → no duplicates)
    for (int t = 0; t < n_threads; ++t)
        for (int32_t pk : tl_pklists[static_cast<size_t>(t)])
            qualifying_partkey_list.push_back(pk);
    } // end q20_part_scan

    TRACE_COUNT("q20_qualifying_partkeys", static_cast<long long>(qualifying_partkey_list.size()));

    if (qualifying_partkey_list.empty()) {
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"s_name", "s_address"});
        TRACE_COUNT("q20_query_output_rows", 0LL);
        return rows;
    }

    // ── 4. Build compact per-pk partsupp index ────────────────────────────────
    //   Sequential: computes the slot layout (prefix sums of per-pk counts)
    //   needed to assign disjoint qty_sum_arr ranges to each qualifying partkey.
    //   This remains sequential because total_ps_slots is needed for allocation.

    const int32_t n_qual = static_cast<int32_t>(qualifying_partkey_list.size());

    struct PkSlots {
        int32_t sk[4];    // suppkey for each slot (i32, TPC-H ≤ 200K)
        int32_t row[4];   // ps_row index for each slot
        int32_t base;     // start index in qty_sum_arr for this pk
        int32_t cnt;      // number of valid slots (≤ 4)
    };
    std::vector<PkSlots> pk_slots(static_cast<size_t>(n_qual));

    int32_t total_ps_slots = 0;

    {
    PROFILE_SCOPE("q20_partsupp_index_build");
    const CsrIndex& ps_csr          = db->ps_by_partkey;
    const int32_t*  ps_suppkey_data  = ps.ps_suppkey_i32.data();

    for (int32_t qi = 0; qi < n_qual; ++qi) {
        int32_t pk     = qualifying_partkey_list[static_cast<size_t>(qi)];
        int32_t off_lo = ps_csr.offsets[static_cast<size_t>(pk)];
        int32_t off_hi = ps_csr.offsets[static_cast<size_t>(pk) + 1];
        PkSlots& pks   = pk_slots[static_cast<size_t>(qi)];
        pks.base = total_ps_slots;
        int32_t cnt = 0;
        for (int32_t r = off_lo; r < off_hi && cnt < 4; ++r) {
            int32_t ps_row  = ps_csr.row_ids[static_cast<size_t>(r)];
            pks.sk [cnt]    = ps_suppkey_data[static_cast<size_t>(ps_row)];
            pks.row[cnt]    = ps_row;
            ++cnt;
        }
        pks.cnt       = cnt;
        total_ps_slots += cnt;
    }
    } // end q20_partsupp_index_build

    // Compact qty accumulator: ~173K uints ≈ 700 KB — fits in LLC
    std::vector<uint32_t> qty_sum_arr(static_cast<size_t>(total_ps_slots), 0u);

    // ── 5. Lineitem scan: parallel qty accumulation per (pk,sk) slot ──────────
    //   Strategy: partition qualifying partkeys across threads.
    //   Each partkey's slots occupy a contiguous range in qty_sum_arr
    //   (pk_slots[qi].base..base+cnt), and partkeys are disjoint across threads,
    //   so each thread writes to a completely non-overlapping sub-array.
    //   Zero locking, zero false sharing (slots per thread are large cache lines).
    //   Binary search per partkey finds the 1-year date window in O(log n).

    std::atomic<int64_t> li_partkey_matches_a{0};
    std::atomic<int64_t> li_rows_visited_a{0};

    {
    PROFILE_SCOPE("q20_lineitem_scan");
    const int32_t base_date = Database::LI_PK_DATE_BASE;
    const int16_t sd_lo = static_cast<int16_t>(std::max(date_lo - base_date, (int32_t)INT16_MIN));
    const int16_t sd_hi = static_cast<int16_t>(std::min(date_hi - base_date, (int32_t)INT16_MAX));
    const int16_t* __restrict__ sd16_arr = db->li_by_pk_shipdate_i16.data();
    const int32_t* __restrict__ sk_arr   = db->li_by_pk_suppkey_i32.data();
    const uint8_t* __restrict__ qty8_arr = db->li_by_pk_qty_i8.data();
    const CsrIndex& li_csr = db->li_by_partkey;
    uint32_t* __restrict__ qa = qty_sum_arr.data();

    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q20_lineitem_scan_parallel");
        int64_t local_pm = 0;
        int64_t local_rv = 0;

        // Each thread processes a contiguous range of qualifying partkeys.
        // Since pk_slots[qi].base is monotonically increasing and each pk's
        // slots are disjoint, each thread writes to a disjoint range of qa[].
        int32_t qi_lo = static_cast<int32_t>((static_cast<int64_t>(n_qual) * tid)       / nt);
        int32_t qi_hi = static_cast<int32_t>((static_cast<int64_t>(n_qual) * (tid + 1)) / nt);

        for (int32_t qi = qi_lo; qi < qi_hi; ++qi) {
            int32_t pk     = qualifying_partkey_list[static_cast<size_t>(qi)];
            int32_t off_lo = li_csr.offsets[static_cast<size_t>(pk)];
            int32_t off_hi = li_csr.offsets[static_cast<size_t>(pk) + 1];

            // Binary search for [sd_lo, sd_hi) window in this partkey's sorted slice.
            const int16_t* slice_begin = sd16_arr + off_lo;
            const int16_t* slice_end   = sd16_arr + off_hi;
            const int16_t* win_lo_ptr  = std::lower_bound(slice_begin, slice_end, sd_lo);
            const int16_t* win_hi_ptr  = std::lower_bound(win_lo_ptr,  slice_end, sd_hi);
            const int32_t  r_lo        = off_lo + static_cast<int32_t>(win_lo_ptr - slice_begin);
            const int32_t  r_hi        = off_lo + static_cast<int32_t>(win_hi_ptr - slice_begin);

            const PkSlots& pks      = pk_slots[static_cast<size_t>(qi)];
            const int32_t  slot_cnt = pks.cnt;
            const int32_t  base_idx = pks.base;

            local_rv += (off_hi - off_lo);
            local_pm += (r_hi   - r_lo);

            // Iterate only rows in the date window — no date check needed
            for (int32_t r = r_lo; r < r_hi; ++r) {
                const int32_t sk = sk_arr[static_cast<size_t>(r)];
                for (int32_t s = 0; s < slot_cnt; ++s) {
                    if (pks.sk[s] == sk) {
                        qa[static_cast<size_t>(base_idx + s)] +=
                            static_cast<uint32_t>(qty8_arr[static_cast<size_t>(r)]);
                        break;
                    }
                }
            }
        }

        li_partkey_matches_a.fetch_add(local_pm, std::memory_order_relaxed);
        li_rows_visited_a.fetch_add(local_rv,    std::memory_order_relaxed);
    });
    } // end q20_lineitem_scan

    TRACE_COUNT("q20_li_rows_visited",        li_rows_visited_a.load());
    TRACE_COUNT("q20_li_partkey_matches",     li_partkey_matches_a.load());
    TRACE_COUNT("q20_qty_sum_distinct_pairs", static_cast<long long>(total_ps_slots));

    // ── 6. Partsupp scan: parallel check ps_availqty > 0.5 * qty_sum ─────────
    //   Each thread handles a disjoint range of qualifying partkeys.
    //   Writes to suppkey_qualifies[] are safe: on x86 byte stores are atomic,
    //   and the value written is idempotent (always 1 → no torn partial writes).

    std::vector<uint8_t> suppkey_qualifies(static_cast<size_t>(max_suppkey + 1), 0);

    std::atomic<int64_t> ps_partkey_pass_a{0};
    std::atomic<int64_t> ps_qty_sum_found_a{0};
    std::atomic<int64_t> ps_availqty_pass_a{0};

    {
    PROFILE_SCOPE("q20_partsupp_scan");
    const int32_t* ps_availqty_data = ps.ps_availqty.data();
    uint8_t* sq = suppkey_qualifies.data();

    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q20_partsupp_scan_parallel");
        int64_t local_pp  = 0;
        int64_t local_qsf = 0;
        int64_t local_ap  = 0;

        int32_t qi_lo = static_cast<int32_t>((static_cast<int64_t>(n_qual) * tid)       / nt);
        int32_t qi_hi = static_cast<int32_t>((static_cast<int64_t>(n_qual) * (tid + 1)) / nt);

        for (int32_t qi = qi_lo; qi < qi_hi; ++qi) {
            const PkSlots& pks = pk_slots[static_cast<size_t>(qi)];
            for (int32_t s = 0; s < pks.cnt; ++s) {
                ++local_pp;
                uint32_t qs = qty_sum_arr[static_cast<size_t>(pks.base + s)];
                if (qs == 0u) continue;
                ++local_qsf;
                int32_t avail = ps_availqty_data[static_cast<size_t>(pks.row[s])];
                // avail > 0.5 * qs  ⟺  2*avail > qs (integer arithmetic, no float rounding)
                if (avail > 0 && static_cast<uint32_t>(avail) * 2u > qs) {
                    ++local_ap;
                    // x86 byte store: atomic at hardware level; idempotent value=1
                    sq[static_cast<size_t>(pks.sk[s])] = 1;
                }
            }
        }

        ps_partkey_pass_a.fetch_add(local_pp,  std::memory_order_relaxed);
        ps_qty_sum_found_a.fetch_add(local_qsf, std::memory_order_relaxed);
        ps_availqty_pass_a.fetch_add(local_ap,  std::memory_order_relaxed);
    });
    } // end q20_partsupp_scan

    TRACE_COUNT("q20_ps_partkey_pass",  ps_partkey_pass_a.load());
    TRACE_COUNT("q20_ps_qty_sum_found", ps_qty_sum_found_a.load());
    TRACE_COUNT("q20_ps_availqty_pass", ps_availqty_pass_a.load());

    // Count qualifying suppkeys (for trace only)
    int64_t num_qualifying_suppkeys = 0;
    for (size_t i = 1; i <= static_cast<size_t>(max_suppkey); ++i) {
        if (suppkey_qualifies[i]) ++num_qualifying_suppkeys;
    }
    TRACE_COUNT("q20_qualifying_suppkeys", num_qualifying_suppkeys);

    // ── 7. Collect supplier rows: parallel filter by nation + suppkey ─────────
    //   Each thread handles a disjoint range of supplier rows and accumulates
    //   into a thread-local result vector; merged sequentially afterwards.
    struct ResultRow {
        std::string s_name;
        std::string s_address;
    };

    std::vector<std::vector<ResultRow>> tl_results(static_cast<size_t>(n_threads));
    for (auto& v : tl_results) v.reserve(16);

    {
    PROFILE_SCOPE("q20_supplier_scan");
    const int64_t nsup = sup.num_rows;
    const uint8_t* sq = suppkey_qualifies.data();
    pool.parallel_for([&](int tid, int nt) {
        PROFILE_SCOPE("q20_supplier_scan_parallel");
        int64_t lo = (nsup * tid)       / nt;
        int64_t hi = (nsup * (tid + 1)) / nt;
        auto& local = tl_results[static_cast<size_t>(tid)];
        for (int64_t i = lo; i < hi; ++i) {
            size_t idx = static_cast<size_t>(i);
            if (sup.s_nationkey[idx] != target_nationkey) continue;
            int64_t sk = sup.s_suppkey[idx];
            if (sk < 1 || sk > max_suppkey) continue;
            if (!sq[static_cast<size_t>(sk)]) continue;
            local.push_back({sup.s_name[idx], sup.s_address[idx]});
        }
    });
    } // end q20_supplier_scan

    // Merge thread-local supplier results
    std::vector<ResultRow> result;
    result.reserve(static_cast<size_t>(num_qualifying_suppkeys));
    for (int t = 0; t < n_threads; ++t)
        for (auto& r : tl_results[static_cast<size_t>(t)])
            result.push_back(std::move(r));

    // ── 8. Sort by s_name ASC ─────────────────────────────────────────────────
    {
    PROFILE_SCOPE("q20_sort");
    std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
        return a.s_name < b.s_name;
    });
    } // end q20_sort

    TRACE_COUNT("q20_result_rows", static_cast<long long>(result.size()));

    // ── 9. Format output ─────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"s_name", "s_address"});

    for (const auto& r : result) {
        rows.push_back({r.s_name, r.s_address});
    }

    TRACE_COUNT("q20_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}