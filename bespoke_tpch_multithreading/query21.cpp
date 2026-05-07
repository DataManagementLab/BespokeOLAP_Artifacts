#include "query21.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

// SQL:
/** select
    s_name,
    count(*) as numwait
from
    supplier,
    lineitem l1,
    orders,
    nation
where
    s_suppkey = l1.l_suppkey
    and o_orderkey = l1.l_orderkey
    and o_orderstatus = 'F'
    and l1.l_receiptdate > l1.l_commitdate
    and exists (
        select *
        from
            lineitem l2
        where
            l2.l_orderkey = l1.l_orderkey
            and l2.l_suppkey <> l1.l_suppkey
    )
    and not exists (
        select *
        from
            lineitem l3
        where
            l3.l_orderkey = l1.l_orderkey
            and l3.l_suppkey <> l1.l_suppkey
            and l3.l_receiptdate > l3.l_commitdate
    )
    and s_nationkey = n_nationkey
    and n_name = '[NATION]'
group by
    s_name
order by
    numwait desc,
    s_name; */

std::vector<std::vector<std::string>> run_q21(Database* db, const Q21Args& args) {
    if (!db) {
        throw std::runtime_error("run_q21: db is null");
    }

    const LineitemTable& li  = db->lineitem;
    const OrdersTable&   ord = db->orders;
    const SupplierTable& sup = db->supplier;
    const NationTable&   nat = db->nation;

    PROFILE_SCOPE("q21_total");

    TRACE_COUNT("q21_total_lineitem_rows", li.num_rows);
    TRACE_COUNT("q21_total_order_rows",    ord.num_rows);
    TRACE_COUNT("q21_total_supplier_rows", sup.num_rows);

    // ── 1. Resolve nation key ─────────────────────────────────────────────────
    auto nat_it = nat.name_to_nationkey.find(args.NATION);
    if (nat_it == nat.name_to_nationkey.end()) {
        TRACE_COUNT("q21_query_output_rows", 0LL);
        return {{"s_name", "numwait"}};
    }
    int32_t target_nationkey = nat_it->second;

    // ── 2. Resolve orderstatus code for 'F' ───────────────────────────────────
    uint8_t status_F = 255;
    for (size_t i = 0; i < ord.orderstatus_names.size(); ++i) {
        if (ord.orderstatus_names[i] == "F") {
            status_F = static_cast<uint8_t>(i);
            break;
        }
    }
    if (status_F == 255) {
        TRACE_COUNT("q21_query_output_rows", 0LL);
        return {{"s_name", "numwait"}};
    }

    // ── 3. Build flat boolean array: in_nation[sk] = 1 if supplier in nation ──
    int32_t max_suppkey = 0;
    for (int64_t i = 0; i < sup.num_rows; ++i) {
        int32_t sk = static_cast<int32_t>(sup.s_suppkey[static_cast<size_t>(i)]);
        if (sk > max_suppkey) max_suppkey = sk;
    }
    std::vector<uint8_t> in_nation(static_cast<size_t>(max_suppkey + 1), 0);
    for (int64_t i = 0; i < sup.num_rows; ++i) {
        size_t idx = static_cast<size_t>(i);
        if (sup.s_nationkey[idx] == target_nationkey)
            in_nation[static_cast<size_t>(sup.s_suppkey[idx])] = 1;
    }
    const size_t sk_sz = static_cast<size_t>(max_suppkey + 1);

    // ── 4. Main loop: iterate all orders, process F-status ones ──────────────
    //
    // Parallelisation strategy:
    //   - Partition the orders array into per-thread ranges (morsel-driven).
    //   - Each thread accumulates into a thread-local unordered_map (no sharing).
    //   - Sequential merge of per-thread maps after parallel_for completes.
    //
    // The fast path (both precomputed arrays available) is embarrassingly
    // parallel: the per-order work is read-only on shared arrays and writes
    // only to the thread-local map.

    std::unordered_map<int32_t, int64_t> count_by_suppkey;
    count_by_suppkey.reserve(static_cast<size_t>(sup.num_rows / 5));

    int64_t orders_status_F       = 0;
    int64_t orders_exist_pass     = 0;
    int64_t orders_not_exist_pass = 0;
    int64_t orders_nation_pass    = 0;
    int64_t li_rows_counted       = 0;

    // Sequential CSR companion arrays for the counting scan
    const int32_t* __restrict__ csr_sk   = db->li_by_orderkey_suppkey_i32.data();
    const uint8_t* __restrict__ csr_late = db->li_by_orderkey_clt_rd.data();
    // Dense per-order CSR bounds
    const int32_t* __restrict__ o_csr_lo = ord.o_li_csr_lo.data();
    const int32_t* __restrict__ o_csr_hi = ord.o_li_csr_hi.data();
    const uint8_t* __restrict__ o_status = ord.o_orderstatus.data();
    const int64_t  num_orders            = ord.num_rows;

    // Precomputed companion arrays (may be empty if built with old loader)
    const bool has_ms_array  = !ord.o_has_multi_suppkey.empty() &&
                               static_cast<int64_t>(ord.o_has_multi_suppkey.size()) >= num_orders;
    const bool has_lus_array = !ord.o_late_unique_suppkey.empty() &&
                               static_cast<int64_t>(ord.o_late_unique_suppkey.size()) >= num_orders;

    TRACE_COUNT("q21_has_ms_array",  has_ms_array  ? 1 : 0);
    TRACE_COUNT("q21_has_lus_array", has_lus_array ? 1 : 0);

    const int n_threads = pool.num_threads;

    const uint8_t* __restrict__ o_has_ms = has_ms_array
        ? ord.o_has_multi_suppkey.data() : nullptr;
    const int32_t* __restrict__ o_lus    = has_lus_array
        ? ord.o_late_unique_suppkey.data() : nullptr;

    // Per-thread accumulator struct (cache-line aligned to avoid false sharing)
    struct alignas(64) LocalAcc {
        std::unordered_map<int32_t, int64_t> counts;
        int64_t sf     = 0;   // orders_status_F
        int64_t ep     = 0;   // orders_exist_pass
        int64_t nep    = 0;   // orders_not_exist_pass
        int64_t np     = 0;   // orders_nation_pass
        int64_t li_cnt = 0;   // li_rows_counted
    };

    // Check if we can use the fast path (both precomputed arrays available)
    if (has_ms_array && has_lus_array) {
        // ── Fast path: both precomputed arrays available ───────────────────────
        PROFILE_SCOPE("q21_main_loop_fast_parallel");

        std::vector<LocalAcc> locals(static_cast<size_t>(n_threads));
        for (auto& l : locals) l.counts.reserve(static_cast<size_t>(sup.num_rows / 5));

        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q21_fast_worker");
            LocalAcc& acc = locals[static_cast<size_t>(tid)];

            // Range-based partition: each thread owns a contiguous slice of orders
            const int64_t chunk = (num_orders + nt - 1) / nt;
            const int64_t oi_lo = static_cast<int64_t>(tid) * chunk;
            const int64_t oi_hi = std::min(oi_lo + chunk, num_orders);

            for (int64_t oi = oi_lo; oi < oi_hi; ++oi) {
                if (o_status[static_cast<size_t>(oi)] != status_F) continue;
                ++acc.sf;

                if (!o_has_ms[static_cast<size_t>(oi)]) continue;
                ++acc.ep;

                const int32_t lone_late = o_lus[static_cast<size_t>(oi)];
                if (lone_late <= 0) continue;
                ++acc.nep;

                if (static_cast<size_t>(lone_late) >= sk_sz ||
                    !in_nation[static_cast<size_t>(lone_late)])
                    continue;
                ++acc.np;

                // Count qualifying l1 rows (only ~400K orders reach here at SF=50)
                const int32_t li_start = o_csr_lo[static_cast<size_t>(oi)];
                const int32_t li_end   = o_csr_hi[static_cast<size_t>(oi)];
                int64_t row_count = 0;
                for (int32_t j = li_start; j < li_end; ++j) {
                    row_count += static_cast<int64_t>(
                        csr_late[static_cast<size_t>(j)] != 0 &&
                        csr_sk[static_cast<size_t>(j)] == lone_late);
                }
                if (row_count > 0) {
                    acc.counts[lone_late] += row_count;
                    acc.li_cnt += row_count;
                }
            }
        });

        // Sequential merge of thread-local results
        {
            PROFILE_SCOPE("q21_fast_merge");
            for (auto& l : locals) {
                orders_status_F       += l.sf;
                orders_exist_pass     += l.ep;
                orders_not_exist_pass += l.nep;
                orders_nation_pass    += l.np;
                li_rows_counted       += l.li_cnt;
                for (auto& kv : l.counts)
                    count_by_suppkey[kv.first] += kv.second;
            }
        }
        TRACE_COUNT("q21_fast_path_threads", n_threads);
    } else {
        // ── Fallback path: use original algorithm (no precomputed arrays) ──────
        const uint8_t* __restrict__ o_has_lt = ord.o_has_clt_rd.empty() ? nullptr
            : ord.o_has_clt_rd.data();
        PROFILE_SCOPE("q21_main_loop_fallback_parallel");

        std::vector<LocalAcc> locals(static_cast<size_t>(n_threads));
        for (auto& l : locals) l.counts.reserve(static_cast<size_t>(sup.num_rows / 5));

        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q21_fallback_worker");
            LocalAcc& acc = locals[static_cast<size_t>(tid)];

            const int64_t chunk = (num_orders + nt - 1) / nt;
            const int64_t oi_lo = static_cast<int64_t>(tid) * chunk;
            const int64_t oi_hi = std::min(oi_lo + chunk, num_orders);

            for (int64_t oi = oi_lo; oi < oi_hi; ++oi) {
                if (o_status[static_cast<size_t>(oi)] != status_F) continue;
                ++acc.sf;

                const int32_t li_start = o_csr_lo[static_cast<size_t>(oi)];
                const int32_t li_end   = o_csr_hi[static_cast<size_t>(oi)];
                if (__builtin_expect(li_start >= li_end, 0)) continue;

                // EXISTS check
                if (has_ms_array) {
                    if (!o_has_ms[static_cast<size_t>(oi)]) continue;
                } else {
                    const int32_t fsk = csr_sk[static_cast<size_t>(li_start)];
                    bool has_second = false;
                    for (int32_t li_i = li_start + 1; li_i < li_end; ++li_i) {
                        if (csr_sk[static_cast<size_t>(li_i)] != fsk) { has_second = true; break; }
                    }
                    if (!has_second) continue;
                }
                ++acc.ep;

                // Pre-filter: skip if no late lineitems
                if (o_has_lt && !o_has_lt[static_cast<size_t>(oi)]) continue;

                // NOT-EXISTS scan
                int32_t lone_late = -1;
                bool    two_late  = false;
                for (int32_t li_i = li_start; li_i < li_end; ++li_i) {
                    if (csr_late[static_cast<size_t>(li_i)]) {
                        const int32_t sk = csr_sk[static_cast<size_t>(li_i)];
                        if (lone_late == -1) {
                            lone_late = sk;
                            if (static_cast<size_t>(sk) >= sk_sz || !in_nation[static_cast<size_t>(sk)]) {
                                two_late = true;
                                break;
                            }
                        } else if (sk != lone_late) { two_late = true; break; }
                    }
                }
                if (lone_late == -1 || two_late) continue;
                ++acc.nep;

                if (static_cast<size_t>(lone_late) >= sk_sz || !in_nation[static_cast<size_t>(lone_late)]) continue;
                ++acc.np;

                int64_t row_count = 0;
                for (int32_t j = li_start; j < li_end; ++j) {
                    row_count += static_cast<int64_t>(
                        csr_late[static_cast<size_t>(j)] != 0 &&
                        csr_sk[static_cast<size_t>(j)] == lone_late);
                }
                if (row_count > 0) {
                    acc.counts[lone_late] += row_count;
                    acc.li_cnt += row_count;
                }
            }
        });

        // Sequential merge of thread-local results
        {
            PROFILE_SCOPE("q21_fallback_merge");
            for (auto& l : locals) {
                orders_status_F       += l.sf;
                orders_exist_pass     += l.ep;
                orders_not_exist_pass += l.nep;
                orders_nation_pass    += l.np;
                li_rows_counted       += l.li_cnt;
                for (auto& kv : l.counts)
                    count_by_suppkey[kv.first] += kv.second;
            }
        }
        TRACE_COUNT("q21_fallback_path_threads", n_threads);
    }

    TRACE_COUNT("q21_orders_status_F",        orders_status_F);
    TRACE_COUNT("q21_orders_exist_pass",      orders_exist_pass);
    TRACE_COUNT("q21_orders_not_exist_pass",  orders_not_exist_pass);
    TRACE_COUNT("q21_orders_nation_pass",     orders_nation_pass);
    TRACE_COUNT("q21_li_rows_counted",        li_rows_counted);
    TRACE_COUNT("q21_distinct_suppliers",     static_cast<long long>(count_by_suppkey.size()));

    // ── 5. Build result: join suppkey → s_name ────────────────────────────────
    struct ResultRow {
        std::string s_name;
        int64_t     numwait;
    };
    std::vector<ResultRow> result;
    result.reserve(count_by_suppkey.size());

    {
    PROFILE_SCOPE("q21_result_build");
    for (int64_t i = 0; i < sup.num_rows; ++i) {
        size_t idx = static_cast<size_t>(i);
        int32_t sk = static_cast<int32_t>(sup.s_suppkey[idx]);
        auto it = count_by_suppkey.find(sk);
        if (it != count_by_suppkey.end()) {
            result.push_back({sup.s_name[idx], it->second});
        }
    }
    }  // end q21_result_build

    // ── 6. Sort: numwait DESC, s_name ASC ─────────────────────────────────────
    {
    PROFILE_SCOPE("q21_sort");
    std::sort(result.begin(), result.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.numwait != b.numwait) return a.numwait > b.numwait;
        return a.s_name < b.s_name;
    });
    }  // end q21_sort

    TRACE_COUNT("q21_result_rows", static_cast<long long>(result.size()));

    // ── 7. Format output ──────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"s_name", "numwait"});
    for (const auto& r : result) {
        rows.push_back({r.s_name, std::to_string(r.numwait)});
    }
    TRACE_COUNT("q21_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}