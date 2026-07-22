#include "query2a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
static ThreadPool& pool = get_query_pool();

#include <climits>
#include <cmath>
#include <algorithm>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <memory>

// ---------------------------------------------------------------------------
// Packed 64-bit bitset — 5M IDs fit in ~78 KB (L2-resident)
// ---------------------------------------------------------------------------
struct PackedBitset {
    std::vector<uint64_t> words;
    uint32_t cap = 0;
    void init(uint32_t max_val) {
        cap = max_val;
        words.assign((size_t)(max_val / 64) + 1, 0ULL);
    }
    inline void set(uint32_t id) noexcept { words[id >> 6] |= 1ULL << (id & 63u); }
    inline bool test(uint32_t id) const noexcept {
        return id <= cap && ((words[id >> 6] >> (id & 63u)) & 1u);
    }
    inline void clear_all() noexcept { std::fill(words.begin(), words.end(), 0ULL); }
};

// ---------------------------------------------------------------------------
// Atomic bitset for parallel writes (fetch_or on 64-bit words, lock-free)
// ---------------------------------------------------------------------------
struct AtomicBitset {
    std::unique_ptr<std::atomic<uint64_t>[]> words;
    size_t n_words = 0;
    uint32_t cap = 0;
    void init(uint32_t max_val) {
        cap = max_val;
        n_words = (size_t)(max_val / 64) + 1;
        words = std::make_unique<std::atomic<uint64_t>[]>(n_words);
    }
    inline void set(uint32_t id) noexcept {
        words[id >> 6].fetch_or(1ULL << (id & 63u), std::memory_order_relaxed);
    }
    inline bool test(uint32_t id) const noexcept {
        return id <= cap && ((words[id >> 6].load(std::memory_order_relaxed) >> (id & 63u)) & 1u);
    }
};

// SQL:
/** SELECT COUNT(*) FROM title as t,
kind_type as kt,
info_type as it1,
movie_info as mi1,
movie_info as mi2,
info_type as it2,
cast_info as ci,
role_type as rt,
name as n,
movie_keyword as mk,
keyword as k
WHERE
t.id = ci.movie_id
AND t.id = mi1.movie_id
AND t.id = mi2.movie_id
AND t.id = mk.movie_id
AND k.id = mk.keyword_id
AND mi1.movie_id = mi2.movie_id
AND mi1.info_type_id = it1.id
AND mi2.info_type_id = it2.id
AND (it1.id IN ID1)
AND (it2.id IN ID2)
AND t.kind_id = kt.id
AND ci.person_id = n.id
AND ci.role_id = rt.id
AND (mi1.info IN INFO1)
AND (mi2.info IN INFO2)
AND (kt.kind IN KIND)
AND (rt.role IN ROLE)
AND (n.gender IN GENDER)
AND (t.production_year <= YEAR1)
AND (t.production_year >= YEAR2) */

std::vector<std::vector<std::string>> run_q2a(Database* db, const Q2aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q2a: db is null");
    }
    PROFILE_SCOPE("q2a_total");

    // Parse year bounds
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty()) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty()) year2 = std::stoi(args.YEAR2);

    // -----------------------------------------------------------------------
    // info_type_id sets
    // -----------------------------------------------------------------------
    auto parse_id_set = [](const std::vector<std::string>& sv) {
        std::unordered_set<int32_t> s;
        for (const auto& v : sv)
            s.insert(v == "<<NULL>>" ? -1 : std::stoi(v));
        return s;
    };
    auto id1_set = parse_id_set(args.ID1);
    auto id2_set = parse_id_set(args.ID2);

    // -----------------------------------------------------------------------
    // Valid role_ids
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_role_ids;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i])) valid_role_ids.insert(rt.id[i]);
        if (role_null_ok) valid_role_ids.insert(-1);
    }

    // -----------------------------------------------------------------------
    // Valid kind_ids
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> kind_set(args.KIND.begin(), args.KIND.end());
    bool kind_null_ok = kind_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_kind_ids;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_set.count(kt.kind[i])) valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
    }

    // -----------------------------------------------------------------------
    // Gender filter
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> gender_set(args.GENDER.begin(), args.GENDER.end());
    bool gender_null_ok = gender_set.count("<<NULL>>") > 0;
    gender_set.erase("NULL");

    // -----------------------------------------------------------------------
    // INFO1/INFO2 reconstruction using global info_dict_map.
    //
    // The original approach scanned type_unique_info to find comma-containing
    // strings. Instead, we check the global info_dict_map directly: if a
    // concatenated token string exists anywhere in the dict, it's a valid
    // re-join. Type filtering happens downstream in the inverted index lookup
    // (which only scans rows for the correct type_id), so false positives from
    // other types are harmless — they just don't match any (type, iid) pair.
    //
    // This eliminates the expensive 20M-cycle type_unique_info scan entirely,
    // replacing it with O(tokens * MAX_WIN) O(1) hash map lookups.
    // -----------------------------------------------------------------------
    static constexpr int MAX_WIN = 8;

    std::unordered_set<std::string> info1_set(args.INFO1.begin(), args.INFO1.end());
    bool info1_null_ok = info1_set.count("<<NULL>>") > 0;
    std::unordered_set<std::string> info2_set(args.INFO2.begin(), args.INFO2.end());
    bool info2_null_ok = info2_set.count("<<NULL>>") > 0;

    {
        PROFILE_SCOPE("q2a_info_reconstruction");
        const auto& dict_map = db->movie_info.info_dict_map;

        // Direct reconstruction using global dict_map: check if concatenated
        // token forms exist in the dict. No full scan of type_unique_info needed.
        auto reconstruct_tokens_fast = [&](std::unordered_set<std::string>& s,
                                           const std::vector<std::string>& tokens) {
            if (tokens.empty()) return;
            // Check if any token contains comma already → skip reconstruction
            bool any_comma = false;
            for (const auto& tok : tokens)
                if (tok.find(',') != std::string::npos) { any_comma = true; break; }
            if (any_comma) return; // tokens already re-joined (or have literal commas)

            // Check if any multi-token concatenation exists in the dict
            bool any_multi = false;
            int32_t n = (int32_t)tokens.size();
            for (int32_t i = 0; i < n && !any_multi; ++i) {
                for (int32_t len = 2; len <= std::min((int32_t)MAX_WIN, n - i) && !any_multi; ++len) {
                    for (const char* sep : {", ", ","}) {
                        std::string c = tokens[i];
                        for (int32_t k = 1; k < len; ++k) { c += sep; c += tokens[i+k]; }
                        if (dict_map.count(c)) { any_multi = true; break; }
                    }
                }
            }
            if (!any_multi) return; // no multi-token strings; no reconstruction needed

            // Rebuild s from scratch using dict-based greedy matching
            s.clear();
            int32_t i = 0;
            while (i < n) {
                bool matched = false;
                for (int32_t len = std::min((int32_t)MAX_WIN, n - i); len >= 2 && !matched; --len) {
                    for (const char* sep : {", ", ","}) {
                        std::string c = tokens[i];
                        for (int32_t k = 1; k < len; ++k) { c += sep; c += tokens[i+k]; }
                        if (dict_map.count(c)) {
                            s.insert(c); i += len; matched = true; break;
                        }
                    }
                }
                if (!matched) { s.insert(tokens[i]); ++i; }
            }
        };
        reconstruct_tokens_fast(info1_set, args.INFO1);
        reconstruct_tokens_fast(info2_set, args.INFO2);

        TRACE_COUNT("q2a_info1_values", (int64_t)info1_set.size());
        TRACE_COUNT("q2a_info2_values", (int64_t)info2_set.size());
    }

    // -----------------------------------------------------------------------
    // Convert info sets to intern-ID bitsets for O(1) lookup
    // -----------------------------------------------------------------------
    const auto& mi_glob = db->movie_info;
    const int32_t dict_size = (int32_t)mi_glob.info_dict_vec.size();

    auto make_id_bitset = [&](const std::unordered_set<std::string>& sset)
        -> std::vector<uint8_t> {
        if (dict_size <= 0) return {};
        std::vector<uint8_t> bs((size_t)dict_size, 0);
        for (const auto& sv : sset) {
            auto it = mi_glob.info_dict_map.find(sv);
            if (it != mi_glob.info_dict_map.end())
                bs[(size_t)it->second] = 1;
        }
        return bs;
    };
    auto info1_bs = make_id_bitset(info1_set);
    auto info2_bs = make_id_bitset(info2_set);

    std::vector<int32_t> info1_iids, info2_iids;
    info1_iids.reserve(info1_set.size());
    info2_iids.reserve(info2_set.size());
    for (int32_t i = 0; i < dict_size; ++i) {
        if (info1_bs[(size_t)i]) info1_iids.push_back(i);
        if (info2_bs[(size_t)i]) info2_iids.push_back(i);
    }

    bool need_id1_scan = !id1_set.empty();
    bool need_id2_scan = !id2_set.empty();
    bool same_type = (id1_set == id2_set);
    TRACE_COUNT("q2a_same_type", same_type ? 1 : 0);
    TRACE_COUNT("q2a_need_id1", need_id1_scan ? 1 : 0);
    TRACE_COUNT("q2a_need_id2", need_id2_scan ? 1 : 0);

    // -----------------------------------------------------------------------
    // Step 1: Scan title for kind+year filter — parallelized.
    // Use AtomicBitset for parallel writes, then copy to PackedBitset for reads.
    // -----------------------------------------------------------------------
    const uint32_t max_title_id = (uint32_t)((int32_t)db->title.id_to_row.size() - 1);
    AtomicBitset valid_title_atomic;
    { PROFILE_SCOPE("q2a_alloc_valid_title"); valid_title_atomic.init(max_title_id); }
    PackedBitset valid_title;

    {
        PROFILE_SCOPE("q2a_title_scan");
        const auto& t = db->title;
        const int32_t* __restrict__ py_ptr = t.production_year.data();
        const int32_t* __restrict__ id_ptr = t.id.data();
        const int32_t yr_lo = (year2 >= 0) ? year2 : INT_MIN;
        const int32_t yr_hi = (year1 >= 0) ? year1 : INT_MAX;
        const int32_t kps_size = (int32_t)t.kind_part_start.size();
        auto compute_range = [&](int32_t part_beg, int32_t part_end) -> std::pair<int32_t,int32_t> {
            if (part_beg >= part_end) return {part_beg, part_beg};
            int32_t lo = part_beg, hi = part_end;
            { int32_t a=part_beg,b=part_end; while(a<b){int32_t m=a+(b-a)/2; if(py_ptr[m]<yr_lo)a=m+1; else b=m;} lo=a; }
            { int32_t a=lo,b=part_end;       while(a<b){int32_t m=a+(b-a)/2; if(py_ptr[m]<=yr_hi)a=m+1; else b=m;} hi=a; }
            return {lo, hi};
        };

        std::vector<std::pair<int32_t,int32_t>> scan_ranges;
        scan_ranges.reserve(valid_kind_ids.size() + 1);
        for (int32_t kid : valid_kind_ids) {
            if (kid < 0 || kid >= kps_size) continue;
            auto [lo, hi] = compute_range(t.kind_part_start[(size_t)kid],
                                          t.kind_part_end[(size_t)kid]);
            if (lo < hi) scan_ranges.push_back({lo, hi});
        }
        if (kind_null_ok) {
            const int32_t* __restrict__ kid_ptr = t.kind_id.data();
            const int32_t N = (int32_t)t.id.size();
            int32_t null_end = 0;
            while (null_end < N && kid_ptr[null_end] < 0) ++null_end;
            auto [lo, hi] = compute_range(0, null_end);
            if (lo < hi) scan_ranges.push_back({lo, hi});
        }

        // Split into 16K-row chunks for even 96-thread load distribution
        const int32_t CHUNK_ROWS = 16384;
        std::vector<std::pair<int32_t,int32_t>> fine_ranges;
        fine_ranges.reserve(scan_ranges.size() * 16);
        for (auto [lo, hi] : scan_ranges)
            for (int32_t r = lo; r < hi; r += CHUNK_ROWS)
                fine_ranges.push_back({r, std::min(r + CHUNK_ROWS, hi)});
        const int32_t n_fine = (int32_t)fine_ranges.size();

        std::atomic<int64_t> total_scanned{0}, total_emitted{0};
        std::atomic<int32_t> range_idx{0};

        pool.parallel_for([&](int /*thr*/, int /*n_threads*/) {
            PROFILE_SCOPE("q2a_title_scan_parallel");
            int64_t rows_scanned = 0, rows_emitted = 0;
            while (true) {
                int32_t ri = range_idx.fetch_add(1, std::memory_order_relaxed);
                if (ri >= n_fine) break;
                auto [lo, hi] = fine_ranges[ri];
                for (int32_t r = lo; r < hi; ++r) {
                    ++rows_scanned;
                    uint32_t mid_id = (uint32_t)id_ptr[r];
                    if (mid_id <= max_title_id) {
                        valid_title_atomic.set(mid_id);
                        ++rows_emitted;
                    }
                }
            }
            total_scanned.fetch_add(rows_scanned, std::memory_order_relaxed);
            total_emitted.fetch_add(rows_emitted, std::memory_order_relaxed);
        });

        TRACE_COUNT("q2a_title_rows_scanned", total_scanned.load());
        TRACE_COUNT("q2a_title_rows_emitted", total_emitted.load());

        // Copy AtomicBitset -> PackedBitset for fast downstream sequential reads
        {
            PROFILE_SCOPE("q2a_title_bitset_copy");
            size_t nw = valid_title_atomic.n_words;
            valid_title.words.resize(nw);
            valid_title.cap = max_title_id;
            for (size_t i = 0; i < nw; ++i)
                valid_title.words[i] = valid_title_atomic.words[i].load(std::memory_order_relaxed);
        }
    }

    // -----------------------------------------------------------------------
    // Step 2: Build mi1 movie list and counts using inverted index.
    // Different-type path: parallel mi1 scan, per-thread raw lists.
    // -----------------------------------------------------------------------
    const int32_t vtl_count = (int32_t)max_title_id + 2;
    std::vector<int32_t> mi1_movies;
    std::vector<int32_t> mi1_c1;
    std::vector<int32_t> mi1_flat, mi2_flat, mi2_movies;
    PackedBitset mi1_seen_bs_same, mi2_seen_bs;
    bool mi2_scanned = false;

    {
        PROFILE_SCOPE("q2a_movie_info_scan");
        const auto& mi = db->movie_info;
        std::atomic<int64_t> total_scanned{0}, total_mi1_emitted{0}, total_mi2_emitted{0};

        if (same_type) {
            mi1_flat.assign(vtl_count, 0);
            mi2_flat.assign(vtl_count, 0);
            mi1_movies.reserve(16384);
            mi2_movies.reserve(262144);
            mi1_seen_bs_same.init(max_title_id);
            mi2_seen_bs.init(max_title_id);
            mi2_scanned = true;
            int64_t rows_scanned = 0, mi1_rows_emitted = 0, mi2_rows_emitted = 0;
            int32_t* m1f = mi1_flat.data();
            int32_t* m2f = mi2_flat.data();

            auto process_flat = [&](const int32_t* __restrict__ data, int32_t len,
                                    int32_t* __restrict__ flat, PackedBitset& seen_bs,
                                    std::vector<int32_t>& movies, int64_t& emitted) {
                for (int32_t i = 0; i < len; ++i) {
                    ++rows_scanned;
                    uint32_t mid = (uint32_t)data[i];
                    if (!valid_title.test(mid)) continue;
                    ++flat[mid]; ++emitted;
                    if (!seen_bs.test(mid)) { seen_bs.set(mid); movies.push_back((int32_t)mid); }
                }
            };
            auto lookup_iid_flat = [&](int32_t tid_p, int32_t iid,
                                       int32_t* __restrict__ flat, PackedBitset& seen_bs,
                                       std::vector<int32_t>& movies, int64_t& emitted) {
                if (tid_p < 0 || tid_p >= (int32_t)mi.type_iid_keys.size()) return;
                const auto& keys    = mi.type_iid_keys[(size_t)tid_p];
                const auto& offsets = mi.type_iid_offsets[(size_t)tid_p];
                const auto& rows_v  = mi.type_iid_rows[(size_t)tid_p];
                auto it = std::lower_bound(keys.begin(), keys.end(), iid);
                if (it == keys.end() || *it != iid) return;
                int32_t local = (int32_t)(it - keys.begin());
                process_flat(rows_v.data() + offsets[(size_t)local],
                             offsets[(size_t)local+1] - offsets[(size_t)local],
                             flat, seen_bs, movies, emitted);
            };
            auto scan_null_flat = [&](int32_t tid_p,
                                      int32_t* __restrict__ flat, PackedBitset& seen_bs,
                                      std::vector<int32_t>& movies, int64_t& emitted) {
                if (tid_p < 0 || tid_p >= (int32_t)mi.type_part_start.size()) return;
                int32_t beg = mi.type_part_start[(size_t)tid_p];
                int32_t end = mi.type_part_end[(size_t)tid_p];
                const int32_t* __restrict__ mi_mid = mi.movie_id.data();
                const int32_t* __restrict__ mi_iid = mi.info_id.data();
                for (int32_t r = beg; r < end; ++r) {
                    ++rows_scanned;
                    if (mi_iid[r] != -1) continue;
                    uint32_t mid = (uint32_t)mi_mid[r];
                    if (!valid_title.test(mid)) continue;
                    ++flat[mid]; ++emitted;
                    if (!seen_bs.test(mid)) { seen_bs.set(mid); movies.push_back((int32_t)mid); }
                }
            };
            for (int32_t tid : id1_set) {
                if (info1_null_ok) scan_null_flat(tid, m1f, mi1_seen_bs_same, mi1_movies, mi1_rows_emitted);
                if (info2_null_ok) scan_null_flat(tid, m2f, mi2_seen_bs,      mi2_movies, mi2_rows_emitted);
                for (int32_t iid : info1_iids) lookup_iid_flat(tid, iid, m1f, mi1_seen_bs_same, mi1_movies, mi1_rows_emitted);
                for (int32_t iid : info2_iids) lookup_iid_flat(tid, iid, m2f, mi2_seen_bs,      mi2_movies, mi2_rows_emitted);
            }
            total_scanned.store(rows_scanned);
            total_mi1_emitted.store(mi1_rows_emitted);
            total_mi2_emitted.store(mi2_rows_emitted);
        } else {
            // Different-type: parallel mi1 scan, each thread owns its raw list
            struct MiWork { int32_t type_id; int32_t iid; };
            std::vector<MiWork> mi_work;
            if (need_id1_scan) {
                for (int32_t tid_p : id1_set) {
                    if (info1_null_ok) mi_work.push_back({tid_p, -2});
                    for (int32_t iid : info1_iids) mi_work.push_back({tid_p, iid});
                }
            }
            const int32_t n_mi_work = (int32_t)mi_work.size();
            const int n_thr_mi = pool.num_threads;
            std::vector<std::vector<int32_t>> per_thread_raw(n_thr_mi);
            for (auto& v : per_thread_raw) v.reserve(8192);

            pool.parallel_for([&](int thr, int n_threads) {
                PROFILE_SCOPE("q2a_mi1_scan_parallel");
                auto& raw_out = per_thread_raw[thr];
                int64_t local_scanned = 0, local_emitted = 0;
                int32_t chunk = (n_mi_work + n_threads - 1) / n_threads;
                int32_t wi_beg = thr * chunk;
                int32_t wi_end = std::min(wi_beg + chunk, n_mi_work);
                for (int32_t wi = wi_beg; wi < wi_end; ++wi) {
                    int32_t tid_p = mi_work[wi].type_id;
                    int32_t iid   = mi_work[wi].iid;
                    if (iid == -2) {
                        if (tid_p < 0 || tid_p >= (int32_t)mi.type_part_start.size()) continue;
                        int32_t beg = mi.type_part_start[(size_t)tid_p];
                        int32_t end = mi.type_part_end[(size_t)tid_p];
                        const int32_t* __restrict__ mi_mid = mi.movie_id.data();
                        const int32_t* __restrict__ mi_iid = mi.info_id.data();
                        for (int32_t r = beg; r < end; ++r) {
                            ++local_scanned;
                            if (mi_iid[r] != -1) continue;
                            uint32_t mid = (uint32_t)mi_mid[r];
                            if (!valid_title.test(mid)) continue;
                            raw_out.push_back((int32_t)mid);
                            ++local_emitted;
                        }
                    } else {
                        if (tid_p < 0 || tid_p >= (int32_t)mi.type_iid_keys.size()) continue;
                        const auto& keys    = mi.type_iid_keys[(size_t)tid_p];
                        const auto& offsets = mi.type_iid_offsets[(size_t)tid_p];
                        const auto& rows_v  = mi.type_iid_rows[(size_t)tid_p];
                        auto it = std::lower_bound(keys.begin(), keys.end(), iid);
                        if (it == keys.end() || *it != iid) continue;
                        int32_t local_idx = (int32_t)(it - keys.begin());
                        int32_t list_beg = offsets[(size_t)local_idx];
                        int32_t list_end = offsets[(size_t)local_idx + 1];
                        const int32_t* __restrict__ data = rows_v.data() + list_beg;
                        int32_t len = list_end - list_beg;
                        for (int32_t i = 0; i < len; ++i) {
                            ++local_scanned;
                            uint32_t mid = (uint32_t)data[i];
                            if (!valid_title.test(mid)) continue;
                            raw_out.push_back((int32_t)mid);
                            ++local_emitted;
                        }
                    }
                }
                total_scanned.fetch_add(local_scanned, std::memory_order_relaxed);
                total_mi1_emitted.fetch_add(local_emitted, std::memory_order_relaxed);
                // Sort this thread's raw list in-place (parallel sort phase)
                std::sort(raw_out.begin(), raw_out.end());
            });

            {
                PROFILE_SCOPE("q2a_sort_mi1");
                // k-way merge of per-thread sorted lists into mi1_movies + mi1_c1
                // O(N log k) where k = n_thr_mi and N = total_raw (~153K)
                size_t total_raw = 0;
                for (auto& v : per_thread_raw) total_raw += v.size();

                // Min-heap: (value, thread_id, pos)
                struct HeapEntry { int32_t val; int32_t tid; int32_t pos; };
                auto heap_gt = [](const HeapEntry& a, const HeapEntry& b){ return a.val > b.val; };
                std::vector<HeapEntry> heap;
                heap.reserve((size_t)n_thr_mi);
                for (int t = 0; t < n_thr_mi; ++t)
                    if (!per_thread_raw[t].empty())
                        heap.push_back({per_thread_raw[t][0], t, 0});
                std::make_heap(heap.begin(), heap.end(), heap_gt);

                mi1_movies.reserve(total_raw / 2 + 1);
                mi1_c1.reserve(total_raw / 2 + 1);

                int32_t prev_val = INT32_MIN;
                int32_t cur_cnt  = 0;
                while (!heap.empty()) {
                    std::pop_heap(heap.begin(), heap.end(), heap_gt);
                    auto& e = heap.back();
                    int32_t val = e.val;
                    int32_t t   = e.tid;
                    int32_t p   = e.pos + 1;

                    if (val == prev_val) {
                        ++cur_cnt;
                    } else {
                        if (prev_val != INT32_MIN) { mi1_movies.push_back(prev_val); mi1_c1.push_back(cur_cnt); }
                        prev_val = val; cur_cnt = 1;
                    }

                    if (p < (int32_t)per_thread_raw[t].size()) {
                        e.val = per_thread_raw[t][p]; e.pos = p;
                        std::push_heap(heap.begin(), heap.end(), heap_gt);
                    } else {
                        heap.pop_back();
                    }
                }
                if (prev_val != INT32_MIN) { mi1_movies.push_back(prev_val); mi1_c1.push_back(cur_cnt); }
            }
        }
        TRACE_COUNT("q2a_movie_info_rows_scanned", total_scanned.load());
        TRACE_COUNT("q2a_mi1_rows_emitted",  total_mi1_emitted.load());
        TRACE_COUNT("q2a_mi2_rows_emitted",  total_mi2_emitted.load());
        TRACE_COUNT("q2a_mi1_movies",  (int64_t)mi1_movies.size());
        TRACE_COUNT("q2a_mi2_movies",  (int64_t)mi2_movies.size());
    }

    // -----------------------------------------------------------------------
    // Step 3: mi2 merge-join — parallelized
    // -----------------------------------------------------------------------
    const auto& mi_ref = db->movie_info;
    const int32_t mi1_sz = (int32_t)mi1_movies.size();

    std::unique_ptr<std::atomic<int32_t>[]> c2_atomic{new std::atomic<int32_t>[mi1_sz]};
    for (int32_t i = 0; i < mi1_sz; ++i) c2_atomic[i].store(0, std::memory_order_relaxed);

    if (!mi2_scanned && need_id2_scan) {
        PROFILE_SCOPE("q2a_mi2_merge_join");
        struct Mi2Work { int32_t type_id; int32_t iid; };
        std::vector<Mi2Work> mi2_work;
        for (int32_t tid_p : id2_set) {
            if (info2_null_ok) mi2_work.push_back({tid_p, -2});
            for (int32_t iid : info2_iids) {
                if (tid_p >= 0 && tid_p < (int32_t)mi_ref.type_iid_keys.size())
                    mi2_work.push_back({tid_p, iid});
            }
        }
        const int32_t n_mi2_work = (int32_t)mi2_work.size();
        const int32_t* __restrict__ m1 = mi1_movies.data();

        pool.parallel_for([&](int thr, int n_threads) {
            PROFILE_SCOPE("q2a_mi2_merge_join_parallel");
            int32_t chunk = (n_mi2_work + n_threads - 1) / n_threads;
            int32_t wi_beg = thr * chunk;
            int32_t wi_end = std::min(wi_beg + chunk, n_mi2_work);
            for (int32_t wi = wi_beg; wi < wi_end; ++wi) {
                int32_t tid_p = mi2_work[wi].type_id;
                int32_t iid   = mi2_work[wi].iid;
                if (iid == -2) {
                    if (tid_p < 0 || tid_p >= (int32_t)mi_ref.type_part_start.size()) continue;
                    int32_t beg = mi_ref.type_part_start[(size_t)tid_p];
                    int32_t end_p = mi_ref.type_part_end[(size_t)tid_p];
                    const int32_t* mi_mid = mi_ref.movie_id.data();
                    const int32_t* mi_iid = mi_ref.info_id.data();
                    for (int32_t r = beg; r < end_p; ++r) {
                        if (mi_iid[r] != -1) continue;
                        int32_t mid = mi_mid[r];
                        if (!valid_title.test((uint32_t)mid)) continue;
                        auto it = std::lower_bound(m1, m1 + mi1_sz, mid);
                        if (it != m1 + mi1_sz && *it == mid)
                            c2_atomic[(int32_t)(it - m1)].fetch_add(1, std::memory_order_relaxed);
                    }
                } else {
                    const auto& keys    = mi_ref.type_iid_keys[(size_t)tid_p];
                    const auto& offsets = mi_ref.type_iid_offsets[(size_t)tid_p];
                    const auto& rows_v  = mi_ref.type_iid_rows[(size_t)tid_p];
                    auto kit = std::lower_bound(keys.begin(), keys.end(), iid);
                    if (kit == keys.end() || *kit != iid) continue;
                    int32_t local = (int32_t)(kit - keys.begin());
                    int32_t list_beg = offsets[(size_t)local];
                    int32_t list_end = offsets[(size_t)local + 1];
                    const int32_t* __restrict__ list_data = rows_v.data() + list_beg;
                    int32_t list_len = list_end - list_beg;
                    int32_t pi = 0, qi = 0;
                    while (pi < mi1_sz && qi < list_len) {
                        int32_t a = m1[pi];
                        while (qi + 8 <= list_len && list_data[qi + 7] < a) qi += 8;
                        while (qi < list_len && list_data[qi] < a) ++qi;
                        if (qi >= list_len) break;
                        int32_t b = list_data[qi];
                        if (a == b) {
                            int32_t cnt = 0;
                            while (qi < list_len && list_data[qi] == b) { ++cnt; ++qi; }
                            c2_atomic[pi].fetch_add(cnt, std::memory_order_relaxed);
                            ++pi;
                        } else {
                            ++pi;
                        }
                    }
                }
            }
        });
    }

    std::vector<int32_t> c2_small(mi1_sz);
    for (int32_t i = 0; i < mi1_sz; ++i)
        c2_small[i] = c2_atomic[i].load(std::memory_order_relaxed);

    // -----------------------------------------------------------------------
    // Step 4: movie_keyword probe — sequential
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int64_t> valid_movies;
    {
        PROFILE_SCOPE("q2a_movie_keyword_probe");
        const auto& mk_csr = db->movie_keyword.movie_id_csr;
        int64_t mk_rows_probed = 0, movies_emitted = 0;
        valid_movies.reserve(8192);
        if (mi2_scanned) {
            const int32_t* __restrict__ large_flat = mi2_flat.data();
            const int32_t lf_size = (int32_t)mi2_flat.size();
            for (int32_t mid : mi1_movies) {
                if (__builtin_expect((uint32_t)mid >= (uint32_t)lf_size, 0)) continue;
                int32_t c2 = large_flat[(size_t)mid];
                if (!c2) continue;
                int32_t c1 = mi1_flat[(size_t)mid];
                auto [beg, end] = mk_csr.range(mid);
                int32_t mk_cnt = end - beg;
                mk_rows_probed += mk_cnt;
                if (mk_cnt == 0) continue;
                valid_movies[mid] = (int64_t)c1 * c2 * mk_cnt;
                ++movies_emitted;
            }
        } else {
            const bool has_c1 = !mi1_c1.empty();
            for (int32_t i = 0; i < mi1_sz; ++i) {
                int32_t c2 = c2_small[i];
                if (!c2) continue;
                int32_t mid = mi1_movies[i];
                int32_t c1 = has_c1 ? mi1_c1[(size_t)i] : 1;
                auto [beg, end] = mk_csr.range(mid);
                int32_t mk_cnt = end - beg;
                mk_rows_probed += mk_cnt;
                if (mk_cnt == 0) continue;
                valid_movies[mid] = (int64_t)c1 * c2 * mk_cnt;
                ++movies_emitted;
            }
        }
        TRACE_COUNT("q2a_mk_rows_probed", mk_rows_probed);
        TRACE_COUNT("q2a_valid_movies_after_mk", movies_emitted);
    }

    // -----------------------------------------------------------------------
    // Step 5: cast_info probe — parallelized over valid_movies
    // -----------------------------------------------------------------------
    int64_t count = 0;
    {
        PROFILE_SCOPE("q2a_cast_info_probe");
        const auto& ci  = db->cast_info;
        const auto& nm  = db->name;
        const auto& csr = ci.movie_id_csr;
        const int32_t* __restrict__ ci_role_id    = ci.role_id.data();
        const int32_t* __restrict__ ci_person_id  = ci.person_id.data();
        const int32_t* __restrict__ nm_id_to_row  = nm.id_to_row.data();
        const uint8_t* __restrict__ nm_gender_byte = nm.gender_byte.data();
        const int32_t nm_itr_size = (int32_t)nm.id_to_row.size();
        const int32_t nm_gb_size  = (int32_t)nm.gender_byte.size();

        bool role_ok[32] = {};
        bool role_large_ok = false;
        for (int32_t rid : valid_role_ids) {
            if (rid >= 0 && rid < 32) role_ok[rid] = true;
            else if (rid >= 32) role_large_ok = true;
        }
        bool gender_byte_ok[256] = {};
        if (gender_null_ok) gender_byte_ok[0] = true;
        for (const auto& gs : gender_set) {
            if (!gs.empty()) gender_byte_ok[(uint8_t)gs[0]] = true;
        }

        std::vector<std::pair<int32_t, int64_t>> vm_vec(valid_movies.begin(), valid_movies.end());
        const int32_t n_vm = (int32_t)vm_vec.size();
        std::atomic<int64_t> total_count{0}, total_probe{0}, total_emit{0};

        pool.parallel_for([&](int thr, int n_threads) {
            PROFILE_SCOPE("q2a_cast_info_probe_parallel");
            int64_t local_count = 0, probe_rows_in = 0, join_rows_emitted = 0;
            int32_t chunk = (n_vm + n_threads - 1) / n_threads;
            int32_t beg_vm = thr * chunk;
            int32_t end_vm = std::min(beg_vm + chunk, n_vm);
            for (int32_t vi = beg_vm; vi < end_vm; ++vi) {
                int32_t mid    = vm_vec[vi].first;
                int64_t mi_mult = vm_vec[vi].second;
                auto [beg, end] = csr.range(mid);
                for (int32_t r = beg; r < end; ++r) {
                    ++probe_rows_in;
                    int32_t rid = ci_role_id[r];
                    bool role_pass;
                    if (rid >= 0 && rid < 32) role_pass = role_ok[rid];
                    else if (rid >= 32) role_pass = role_large_ok && valid_role_ids.count(rid);
                    else role_pass = false;
                    if (!role_pass) continue;
                    int32_t pid = ci_person_id[r];
                    if (__builtin_expect(pid < 0 || pid >= nm_itr_size, 0)) continue;
                    int32_t nrow = nm_id_to_row[pid];
                    if (nrow < 0) continue;
                    if (__builtin_expect(nrow >= nm_gb_size, 0)) continue;
                    uint8_t gb = nm_gender_byte[nrow];
                    if (!gender_byte_ok[gb]) continue;
                    local_count += mi_mult;
                    ++join_rows_emitted;
                }
            }
            total_count.fetch_add(local_count, std::memory_order_relaxed);
            total_probe.fetch_add(probe_rows_in, std::memory_order_relaxed);
            total_emit.fetch_add(join_rows_emitted, std::memory_order_relaxed);
        });

        count = total_count.load();
        TRACE_COUNT("q2a_cast_probe_rows_in", total_probe.load());
        TRACE_COUNT("q2a_join_rows_emitted", total_emit.load());
    }

    TRACE_COUNT("q2a_valid_movies", (int64_t)valid_movies.size());
    TRACE_COUNT("q2a_query_output_rows", 1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}