#include "query13.hpp"
#include "db_loader.hpp"
#include "query_pool.hpp"
#include "trace.hpp"
static ThreadPool& pool = get_query_pool();
#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <cstring>
#include <atomic>

// SQL:
// select c_count, count(*) as custdist
// from (
//   select c_custkey, count(o_orderkey)
//   from customer left outer join orders on
//       c_custkey = o_custkey
//       and o_comment not like '%WORD1%WORD2%'
//   group by c_custkey
// ) as c_orders (c_custkey, c_count)
// group by c_count
// order by custdist desc, c_count desc;

std::vector<std::vector<std::string>> run_q13(Database* db, const Q13Args& args) {
    if (!db) {
        throw std::runtime_error("run_q13: db is null");
    }

    PROFILE_SCOPE("q13_total");

    const CustomerTable& cust = db->customer;
    const OrdersTable&   ord  = db->orders;

    const std::string& word1 = args.WORD1;
    const std::string& word2 = args.WORD2;

    bool pattern_valid = (word1 != "<<NULL>>" && word2 != "<<NULL>>");

    const int64_t total_ord_rows = ord.num_rows;
    const int64_t num_customers  = cust.num_rows;

    TRACE_COUNT("q13_total_order_rows",    total_ord_rows);
    TRACE_COUNT("q13_total_customer_rows", num_customers);

    const int64_t max_ck = num_customers;
    TRACE_COUNT("q13_max_custkey", max_ck);

    const int n_threads = pool.num_threads;
    TRACE_COUNT("q13_num_threads", n_threads);

    const char*     flat      = ord.o_comment_flat.data();
    const uint64_t* offs      = ord.o_comment_offs.data();
    const int32_t*  cks       = ord.o_custkey_i32.data();
    const int64_t   N         = total_ord_rows;
    const size_t    flat_size = ord.o_comment_flat.size();

    // Per-custkey qualifying order count (TPC-H custkeys dense in [1..max_ck]).
    // Use per-thread local arrays to avoid false sharing / atomic ops,
    // then merge them at the end.
    const size_t ocpc_size = static_cast<size_t>(max_ck + 2);
    std::vector<std::vector<int32_t>> thread_ocpc(n_threads,
                                                   std::vector<int32_t>(ocpc_size, 0));

    std::atomic<int64_t> orders_excluded_atomic{0};
    std::atomic<int64_t> orders_counted_atomic{0};

    if (pattern_valid) {
        // ── Two-pass parallel pattern filter + count ──────────────────────
        //
        // Pass 1 (pattern scan, parallel by row range):
        //   Each thread scans its share of rows [row_begin, row_end).
        //   The flat buffer slice for that range is [offs[row_begin], offs[row_end]).
        //   Matching rows are marked in a shared atomic bitset.
        //   Threads may share the same 64-bit word at range boundaries, so we use
        //   std::atomic<uint64_t> with fetch_or (relaxed ordering is fine because
        //   the bitset is only read after all threads finish via parallel_for barrier).
        //
        // Pass 2 (count pass, parallel):
        //   Each thread processes its own row range, writing into per-thread
        //   thread_ocpc[tid] (no contention).
        //
        const char*  w1_data = word1.data();
        const size_t w1_len  = word1.size();
        const char*  w2_data = word2.data();
        const size_t w2_len  = word2.size();

        // Shared bitset: bit[row]=1 means the order row is excluded by the filter.
        const size_t nwords = (static_cast<size_t>(N) + 63) / 64;
        std::vector<std::atomic<uint64_t>> excluded_bits(nwords);
        for (size_t i = 0; i < nwords; ++i) excluded_bits[i].store(0, std::memory_order_relaxed);
        std::atomic<uint64_t>* ebits = excluded_bits.data();

        // Pass 1: parallel pattern scan
        {
            PROFILE_SCOPE("q13_pattern_scan_parallel");
            pool.parallel_for([&](int tid, int nt) {
                PROFILE_SCOPE("q13_pattern_scan_thread");
                const int64_t rows_per_thread = (N + nt - 1) / nt;
                const int64_t row_begin = tid * rows_per_thread;
                const int64_t row_end   = std::min(row_begin + rows_per_thread, N);
                if (row_begin >= row_end) return;

                const char* chunk_start     = flat + offs[row_begin];
                const char* chunk_end_ptr   = flat + offs[row_end];

                int64_t local_excluded = 0;

                const char* p = chunk_start;
                size_t   row_cursor     = static_cast<size_t>(row_begin);
                uint64_t row_cursor_end = offs[row_cursor + 1];

                while (p + w1_len <= chunk_end_ptr) {
                    const char* p1 = (const char*)memmem(
                        p, static_cast<size_t>(chunk_end_ptr - p),
                        w1_data, w1_len);
                    if (!p1) break;

                    const uint64_t p1_pos = static_cast<uint64_t>(p1 - flat);
                    while (row_cursor_end <= p1_pos) {
                        ++row_cursor;
                        row_cursor_end = offs[row_cursor + 1];
                    }
                    const size_t   row         = row_cursor;
                    const uint64_t row_end_off = row_cursor_end;

                    if (p1_pos + static_cast<uint64_t>(w1_len) <= row_end_off) {
                        const char*  s2 = p1 + w1_len;
                        const size_t l2 = static_cast<size_t>(flat + row_end_off - s2);
                        if (l2 >= w2_len && memmem(s2, l2, w2_data, w2_len)) {
                            // Mark row as excluded (atomic OR)
                            const size_t   word_idx = row >> 6;
                            const uint64_t bit      = uint64_t(1) << (row & 63);
                            ebits[word_idx].fetch_or(bit, std::memory_order_relaxed);
                            ++local_excluded;
                            p = flat + row_end_off;  // skip to next row
                            continue;
                        }
                    }
                    p = p1 + 1;
                }
                orders_excluded_atomic.fetch_add(local_excluded, std::memory_order_relaxed);
            });
        }
        TRACE_COUNT("q13_excluded_rows", orders_excluded_atomic.load());

        // Pass 2: parallel count pass with per-thread ocpc accumulators
        {
            PROFILE_SCOPE("q13_count_pass_parallel");
            pool.parallel_for([&](int tid, int nt) {
                PROFILE_SCOPE("q13_count_pass_thread");
                const int64_t rows_per_thread = (N + nt - 1) / nt;
                const int64_t row_begin = tid * rows_per_thread;
                const int64_t row_end_l = std::min(row_begin + rows_per_thread, N);
                if (row_begin >= row_end_l) return;

                int32_t* __restrict__ locpc = thread_ocpc[tid].data();
                int64_t local_counted = 0;

                // Process 64-row bitset blocks within this thread's range
                const int64_t wi_begin = row_begin / 64;
                const int64_t wi_end   = (row_end_l + 63) / 64;

                for (int64_t wi = wi_begin; wi < wi_end; ++wi) {
                    const int64_t base      = wi * 64;
                    const int64_t blk_start = std::max(base, row_begin);
                    const int64_t blk_end   = std::min(base + 64, row_end_l);
                    const uint64_t word = ebits[static_cast<size_t>(wi)].load(std::memory_order_relaxed);

                    if (blk_start == base && blk_end == base + 64) {
                        // Full 64-row block
                        if (__builtin_expect(word == 0, 1)) {
                            for (int i = 0; i < 64; ++i)
                                ++locpc[static_cast<size_t>(cks[static_cast<size_t>(base + i)])];
                            local_counted += 64;
                        } else {
                            uint64_t incl = ~word;
                            while (incl) {
                                const int bit = __builtin_ctzll(incl);
                                ++locpc[static_cast<size_t>(cks[static_cast<size_t>(base + bit)])];
                                ++local_counted;
                                incl &= incl - 1;
                            }
                        }
                    } else {
                        // Partial block (boundary block)
                        for (int64_t oi = blk_start; oi < blk_end; ++oi) {
                            const int bit_pos = static_cast<int>(oi - base);
                            if (__builtin_expect((word >> bit_pos) & 1, 0))
                                continue;
                            ++locpc[static_cast<size_t>(cks[static_cast<size_t>(oi)])];
                            ++local_counted;
                        }
                    }
                }
                orders_counted_atomic.fetch_add(local_counted, std::memory_order_relaxed);
            });
        }

    } else {
        // No filter: parallel scan of o_custkey_i32 into per-thread accumulators
        PROFILE_SCOPE("q13_nofilter_count_parallel");
        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q13_nofilter_count_thread");
            const int64_t rows_per_thread = (N + nt - 1) / nt;
            const int64_t row_begin = tid * rows_per_thread;
            const int64_t row_end_l = std::min(row_begin + rows_per_thread, N);
            if (row_begin >= row_end_l) return;

            int32_t* __restrict__ locpc = thread_ocpc[tid].data();
            for (int64_t oi = row_begin; oi < row_end_l; ++oi) {
                ++locpc[static_cast<size_t>(cks[static_cast<size_t>(oi)])];
            }
            orders_counted_atomic.fetch_add(row_end_l - row_begin, std::memory_order_relaxed);
        });
    }

    const int64_t orders_excluded = orders_excluded_atomic.load();
    const int64_t orders_counted  = orders_counted_atomic.load();

    TRACE_COUNT("q13_orders_excluded", orders_excluded);
    TRACE_COUNT("q13_orders_counted",  orders_counted);

    // ── Merge per-thread ocpc arrays in parallel ──────────────────────────────
    // Thread t handles a disjoint range of custkey indices; sums all thread arrays
    // into thread_ocpc[0] for that range.
    if (n_threads > 1) {
        PROFILE_SCOPE("q13_merge_ocpc");
        pool.parallel_for([&](int tid, int nt) {
            PROFILE_SCOPE("q13_merge_ocpc_thread");
            const int64_t range = static_cast<int64_t>(ocpc_size);
            const int64_t chunk = (range + nt - 1) / nt;
            const int64_t begin = tid * chunk;
            const int64_t end   = std::min(begin + chunk, range);
            if (begin >= end) return;

            int32_t* __restrict__ dst = thread_ocpc[0].data() + begin;
            for (int t = 1; t < nt; ++t) {
                const int32_t* __restrict__ src = thread_ocpc[t].data() + begin;
                const int64_t len = end - begin;
                for (int64_t i = 0; i < len; ++i)
                    dst[i] += src[i];
            }
        });
    }

    const std::vector<int32_t>& order_count_per_cust = thread_ocpc[0];

    // ── Aggregate: c_count → custdist ─────────────────────────────────────────
    int32_t max_count = 0;
    {
        PROFILE_SCOPE("q13_find_max_count");
        const int32_t* ocpc = order_count_per_cust.data();
        for (int64_t ci = 1; ci <= max_ck; ++ci)
            if (ocpc[ci] > max_count) max_count = ocpc[ci];
    }
    TRACE_COUNT("q13_max_count", max_count);

    std::vector<int32_t> dist_arr(static_cast<size_t>(max_count + 1), 0);
    {
        PROFILE_SCOPE("q13_dist_agg");
        const int32_t* ocpc = order_count_per_cust.data();
        int32_t*       darr = dist_arr.data();
        for (int64_t ck = 1; ck <= max_ck; ++ck)
            ++darr[static_cast<size_t>(ocpc[static_cast<size_t>(ck)])];
    }
    TRACE_COUNT("q13_groups_created", static_cast<long long>(max_count + 1));

    // ── Collect and sort results ──────────────────────────────────────────────
    std::vector<std::pair<int32_t, int32_t>> result;
    result.reserve(static_cast<size_t>(max_count + 1));
    for (int32_t cnt = 0; cnt <= max_count; ++cnt) {
        const int32_t dist = dist_arr[static_cast<size_t>(cnt)];
        if (dist > 0)
            result.emplace_back(cnt, dist);
    }
    {
        PROFILE_SCOPE("q13_sort");
        std::sort(result.begin(), result.end(),
            [](const std::pair<int32_t,int32_t>& a, const std::pair<int32_t,int32_t>& b) {
                if (a.second != b.second) return a.second > b.second;
                return a.first > b.first;
            });
    }
    TRACE_COUNT("q13_sort_rows_in",  static_cast<long long>(result.size()));
    TRACE_COUNT("q13_sort_rows_out", static_cast<long long>(result.size()));

    // ── Format output ─────────────────────────────────────────────────────────
    std::vector<std::vector<std::string>> rows;
    rows.reserve(result.size() + 1);
    rows.push_back({"c_count", "custdist"});
    {
        PROFILE_SCOPE("q13_format_output");
        for (auto& [cnt, dist] : result)
            rows.push_back({std::to_string(cnt), std::to_string(dist)});
    }
    TRACE_COUNT("q13_query_output_rows", static_cast<long long>(rows.size()) - 1);

    return rows;
}