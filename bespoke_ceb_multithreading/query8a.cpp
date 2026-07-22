#include "query8a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstring>
#include <cstdint>
#include <climits>
#include <stdexcept>
#include <atomic>
#include <string>
#include <numeric>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
// SELECT COUNT(*) FROM title as t,
// kind_type as kt,
// info_type as it1,
// movie_info as mi1,
// cast_info as ci,
// role_type as rt,
// name as n,
// movie_keyword as mk,
// keyword as k,
// movie_companies as mc,
// company_type as ct,
// company_name as cn
// WHERE
//   t.id = ci.movie_id
//   AND t.id = mc.movie_id
//   AND t.id = mi1.movie_id
//   AND t.id = mk.movie_id
//   AND mc.company_type_id = ct.id
//   AND mc.company_id = cn.id
//   AND k.id = mk.keyword_id
//   AND mi1.info_type_id = it1.id
//   AND t.kind_id = kt.id
//   AND ci.person_id = n.id
//   AND ci.role_id = rt.id
//   AND (it1.id IN ID)
//   AND (mi1.info IN INFO)
//   AND (kt.kind IN KIND1)
//   AND (rt.role IN ROLE)
//   AND (n.gender IN GENDER)
//   AND (n.name_pcode_cf IN NAME_PCODE_CF)
//   AND (t.production_year <= YEAR1)
//   AND (t.production_year >= YEAR2)
//   AND (cn.name IN NAME)
//   AND (ct.kind IN KIND2)

std::vector<std::vector<std::string>> run_q8a(Database* db, const Q8aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q8a: db is null");
    }
    PROFILE_SCOPE("q8a_total");

    // -----------------------------------------------------------------------
    // Null-sentinel helper
    // -----------------------------------------------------------------------
    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };

    // -----------------------------------------------------------------------
    // Parse year bounds  (YEAR1 = upper bound, YEAR2 = lower bound)
    // -----------------------------------------------------------------------
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty() && !is_null(args.YEAR1)) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty() && !is_null(args.YEAR2)) year2 = std::stoi(args.YEAR2);

    // -----------------------------------------------------------------------
    // Resolve valid kind_ids from kind_type  (kt.kind IN KIND1)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_kind_ids;
    {
        std::unordered_set<std::string> kind_str_set;
        bool kind_null_ok = false;
        for (const auto& s : args.KIND1) {
            if (is_null(s)) kind_null_ok = true;
            else            kind_str_set.insert(s);
        }
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_str_set.count(kt.kind[i]))
                valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
        TRACE_COUNT("q8a_valid_kind_ids", (int64_t)valid_kind_ids.size());
    }
    // Flat bool array for kind_id lookup
    int32_t max_kind_id = 0;
    for (int32_t id : valid_kind_ids) if (id > max_kind_id) max_kind_id = id;
    std::vector<uint8_t> valid_kind_arr(max_kind_id + 1, 0);
    for (int32_t id : valid_kind_ids)
        if (id >= 0) valid_kind_arr[id] = 1;
    bool kind_null_ok_flag = valid_kind_ids.count(-1) > 0;

    // -----------------------------------------------------------------------
    // Resolve valid info_type_ids  (it1.id IN ID)
    // ID is already numeric strings -- just parse them directly.
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_info_type_ids;
    for (const auto& s : args.ID)
        if (!is_null(s)) valid_info_type_ids.insert(std::stoi(s));
    TRACE_COUNT("q8a_valid_info_type_ids", (int64_t)valid_info_type_ids.size());

    // -----------------------------------------------------------------------
    // Resolve valid role_ids from role_type  (rt.role IN ROLE)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_role_ids;
    {
        std::unordered_set<std::string> role_str_set;
        for (const auto& s : args.ROLE)
            if (!is_null(s)) role_str_set.insert(s);
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_str_set.count(rt.role[i]))
                valid_role_ids.insert(rt.id[i]);
        TRACE_COUNT("q8a_valid_role_ids", (int64_t)valid_role_ids.size());
    }

    // -----------------------------------------------------------------------
    // Resolve valid company_type_ids  (ct.kind IN KIND2)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_ct_ids;
    int32_t max_ct_id = 0;
    {
        std::unordered_set<std::string> ct_str_set;
        for (const auto& s : args.KIND2)
            if (!is_null(s)) ct_str_set.insert(s);
        const auto& ct = db->company_type;
        for (size_t i = 0; i < ct.id.size(); ++i)
        {
            if (ct.id[i] > max_ct_id) max_ct_id = ct.id[i];
            if (ct_str_set.count(ct.kind[i])) valid_ct_ids.insert(ct.id[i]);
        }
        TRACE_COUNT("q8a_valid_ct_ids", (int64_t)valid_ct_ids.size());
    }
    std::vector<uint8_t> valid_ct_arr(max_ct_id + 1, 0);
    for (int32_t id : valid_ct_ids) valid_ct_arr[id] = 1;

    // -----------------------------------------------------------------------
    // Resolve valid company_ids from company_name  (cn.name IN NAME)
    // Use pre-built name_to_ids reverse index for O(|NAME|) lookup instead
    // of O(N) linear scan over 234K rows.
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_cn_ids;
    int32_t max_cn_id = 0;
    {
        PROFILE_SCOPE("q8a_setup_cn_scan");
        const auto& cn = db->company_name;
        for (const auto& s : args.NAME) {
            if (is_null(s)) continue;
            auto it = cn.name_to_ids.find(s);
            if (it == cn.name_to_ids.end()) continue;
            for (int32_t cid : it->second) {
                valid_cn_ids.insert(cid);
            }
        }
        // Compute max_cn_id from id_to_row size (avoids linear scan)
        if (!cn.id_to_row.empty())
            max_cn_id = (int32_t)cn.id_to_row.size() - 1;
        TRACE_COUNT("q8a_valid_cn_ids", (int64_t)valid_cn_ids.size());
    }
    std::vector<uint8_t> valid_cn_arr(max_cn_id + 1, 0);
    for (int32_t id : valid_cn_ids) valid_cn_arr[id] = 1;

    // -----------------------------------------------------------------------
    // Build flat array for role_id lookup
    // -----------------------------------------------------------------------
    int32_t max_role_id = 0;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (rt.id[i] > max_role_id) max_role_id = rt.id[i];
    }
    std::vector<uint8_t> valid_role_arr(max_role_id + 1, 0);
    for (int32_t id : valid_role_ids) valid_role_arr[id] = 1;

    // -----------------------------------------------------------------------
    // Gender filter set  (n.gender IN GENDER)
    // NULL entries in GENDER never match (SQL semantics).
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> gender_set;
    for (const auto& s : args.GENDER)
        if (!is_null(s)) gender_set.insert(s);
    TRACE_COUNT("q8a_gender_set_size", (int64_t)gender_set.size());

    // -----------------------------------------------------------------------
    // name_pcode_cf filter set  (n.name_pcode_cf IN NAME_PCODE_CF)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> pcode_cf_set;
    for (const auto& s : args.NAME_PCODE_CF)
        if (!is_null(s)) pcode_cf_set.insert(s);
    TRACE_COUNT("q8a_pcode_cf_set_size", (int64_t)pcode_cf_set.size());

    // -----------------------------------------------------------------------
    // INFO string set for mi1  (mi1.info IN INFO)
    //
    // The args parser splits on commas, so compound values like
    // "Dolby Digital, Stereo" may arrive as separate tokens.
    // We reconstruct by scanning the movie_info table for multi-token joins
    // that actually exist.
    // -----------------------------------------------------------------------
    static constexpr int MAX_WIN = 8;
    std::unordered_set<std::string> info_set;
    bool info_null_ok = false;
    for (const auto& s : args.INFO) {
        if (is_null(s)) info_null_ok = true;
        else            info_set.insert(s);
    }
    {
        PROFILE_SCOPE("q8a_info_reconstruct");
        const std::vector<std::string>& tokens = args.INFO;
        if (!tokens.empty() && !valid_info_type_ids.empty()) {
            std::unordered_set<std::string> cands;
            int32_t n_tok = (int32_t)tokens.size();
            for (int32_t i = 0; i < n_tok; ++i) {
                if (is_null(tokens[i])) continue;
                for (const char* sep : {", ", ","}) {
                    std::string c = tokens[i];
                    for (int32_t j = i + 1; j < n_tok && j <= i + MAX_WIN - 1; ++j) {
                        if (is_null(tokens[j])) break;
                        c += sep;
                        c += tokens[j];
                        cands.insert(c);
                    }
                }
            }
            if (!cands.empty()) {
                std::unordered_set<std::string> found;
                const auto& mi = db->movie_info;
                for (int32_t it_id : valid_info_type_ids) {
                    if (it_id < 0 || it_id >= (int32_t)mi.type_unique_info.size()) continue;
                    const auto& unique_set = mi.type_unique_info[it_id];
                    for (const auto& cand : cands) {
                        if (unique_set.count(cand)) found.insert(cand);
                    }
                }
                if (!found.empty()) {
                    info_set.clear();
                    int32_t i = 0;
                    while (i < n_tok) {
                        if (is_null(tokens[i])) { ++i; continue; }
                        bool matched = false;
                        for (int32_t len = std::min((int32_t)MAX_WIN, n_tok - i);
                             len >= 2 && !matched; --len) {
                            for (const char* sep : {", ", ","}) {
                                std::string c = tokens[i];
                                for (int32_t k = 1; k < len; ++k) {
                                    c += sep;
                                    c += tokens[i + k];
                                }
                                if (found.count(c)) {
                                    info_set.insert(c);
                                    i += len;
                                    matched = true;
                                    break;
                                }
                            }
                        }
                        if (!matched) {
                            info_set.insert(tokens[i]);
                            ++i;
                        }
                    }
                }
            }
        }
    }
    TRACE_COUNT("q8a_info_set_size", (int64_t)info_set.size());

    // -----------------------------------------------------------------------
    // Compact data structures to reduce memory footprint.
    //
    // Instead of flat arrays of size max_movie_id (~2.5M), we use:
    //  - mc_idx_arr[max_movie_id+1]: maps movie_id -> index in mc_movies (-1=absent)
    //    This replaces mc_arr (10MB) but is same size; saves 30MB by eliminating
    //    mi1_arr and movie_contrib_arr as full-size flat arrays.
    //  - mc_counts[i]: mc row count for mc_movies[i]  (132K * 4 = 528KB)
    //  - mi1_counts[i]: mi1 row count for mc_movies[i] (128K * 4 = 528KB)
    //  - contrib[i]: final contribution for mc_movies[i] (128K * 8 = 1MB)
    //
    // mc_idx_arr needs one parallel memset (10MB = 1/4 of previous 40MB).
    // -----------------------------------------------------------------------
    const int32_t max_movie_id = (int32_t)db->title.id_to_row.size() - 1;
    const int32_t n_elems = max_movie_id + 1;

    // mc_idx_arr: persistent reusable buffer to avoid repeated mmap/page-fault overhead.
    // Uses a static vector that grows to needed size and is reused across invocations.
    // Between invocations, only touched positions need to be reset (not the full array).
    // Zero = "not in mc" sentinel; 1-based index for non-zero.
    static std::vector<int32_t> mc_idx_static; // persistent allocation
    if ((int32_t)mc_idx_static.size() < n_elems) {
        mc_idx_static.assign(n_elems, 0); // zero-initialize on first use or resize
    }
    int32_t* const mc_idx_arr = mc_idx_static.data(); // movie_id -> mc_movies index+1, 0=absent

    // Map info strings to intern IDs for O(1) lookup
    const auto& mi_dict_map  = db->movie_info.info_dict_map;
    const int32_t mi_dict_size = (int32_t)db->movie_info.info_dict_vec.size();
    std::vector<uint8_t> valid_info_id_arr(mi_dict_size, 0);
    for (const auto& inf : info_set) {
        auto it = mi_dict_map.find(inf);
        if (it != mi_dict_map.end() && it->second >= 0 && it->second < mi_dict_size)
            valid_info_id_arr[it->second] = 1;
    }

    // -----------------------------------------------------------------------
    // Combined alloc + mc_build in one parallel pass:
    //   Thread 0: performs mc_build (sequential CSR probe, 4.3ms)
    //   Threads 1-95: perform the mc_idx_arr memset (-1 fill, 10MB)
    //
    // This overlaps alloc with mc_build so total cost ≈ max(1.5ms, 4.3ms) = 4.3ms
    // instead of sequential 1.5ms + 4.3ms = 5.8ms.
    //
    // Thread 0 does the mc_build AFTER the workers have started memset-ing.
    // mc_idx_arr is guaranteed to be -1 in the regions thread 0 touches,
    // because mc_build only touches positions [0..max_movie_id], and the
    // workers memset with 0xFF (=-1) starting from the beginning.
    //
    // CORRECTNESS: Thread 0 initializes mc_idx_arr by overwriting -1 values
    // with positive indices. Workers only write 0xFF (=-1) bytes. Since thread 0
    // reads mc_idx_arr[mid] only AFTER writing it (or the initial -1 from malloc),
    // we need the memset to happen BEFORE thread 0 reads positions.
    //
    // To ensure this, we do a two-phase approach:
    // Phase 1: All threads (incl. thread 0) do the memset in parallel.
    // Phase 2: Thread 0 (main) does mc_build while workers are idle.
    // This uses 2 parallel_for invocations but overlaps memset+mc or not.
    //
    // Actually simpler: just do memset in one parallel_for, then mc_build sequentially.
    // -----------------------------------------------------------------------
    std::vector<int32_t> mc_movies;  // unique movie_ids that pass mc filter
    std::vector<int32_t> mc_counts;  // mc row count for mc_movies[i]
    mc_movies.reserve(200000);
    mc_counts.reserve(200000);
    // mc_idx_arr is zero-initialized by calloc (0 = "not in mc" sentinel)
    TRACE_COUNT("q8a_alloc_arrays_ns", 0); // placeholder
    {
        // Parallel mc_build: each thread processes a stripe of the CSR values
        // for all valid company_ids, accumulating into thread-local hash maps.
        // The parallel scan pre-faults mc_idx_arr pages (by mc_merge) AND
        // keeps workers spinning so mi1_build dispatch is instant.
        PROFILE_SCOPE("q8a_mc_build");
        const auto& mc = db->movie_companies;
        const int32_t* __restrict__ mc_mid_ptr  = mc.movie_id.data();
        const int32_t* __restrict__ mc_ctid_ptr = mc.company_type_id.data();
        const uint8_t* __restrict__ ct_arr_ptr  = valid_ct_arr.data();

        // Collect all CSR row ranges for valid companies into a flat list
        struct McRange { int32_t beg, end; };
        std::vector<McRange> mc_ranges;
        mc_ranges.reserve(valid_cn_ids.size());
        int64_t total_mc_rows = 0;
        const auto& cid_csr = mc.company_id_csr;
        const int32_t* __restrict__ csr_vals_ptr = cid_csr.values.data();
        for (int32_t cn_id : valid_cn_ids) {
            auto [b, e] = cid_csr.range(cn_id);
            if (e > b) { mc_ranges.push_back({b, e}); total_mc_rows += (e - b); }
        }

        const int n_mc_threads = pool.num_threads;
        std::vector<std::vector<std::pair<int32_t,int32_t>>> local_mc(n_mc_threads);
        for (auto& v : local_mc) v.reserve(8192);
        std::atomic<int64_t> mc_total_scanned{0}, mc_total_emitted{0};

        pool.parallel_for([&](int tid, int n) {
            int64_t vrow_beg = (int64_t(tid)     * total_mc_rows) / n;
            int64_t vrow_end = (int64_t(tid + 1) * total_mc_rows) / n;
            int64_t rows_scanned = 0, rows_emitted = 0;

            std::unordered_map<int32_t,int32_t> lmap;
            lmap.reserve(8192);

            int64_t vrow = 0;
            for (auto& rng : mc_ranges) {
                int64_t rlen = rng.end - rng.beg;
                int64_t r_end_v = vrow + rlen;
                if (r_end_v <= vrow_beg) { vrow = r_end_v; continue; }
                if (vrow >= vrow_end) break;
                int64_t lo = std::max(int64_t(0), vrow_beg - vrow);
                int64_t hi = std::min(rlen, vrow_end - vrow);
                for (int64_t r = lo; r < hi; ++r) {
                    int32_t row = csr_vals_ptr[rng.beg + r];
                    ++rows_scanned;
                    int32_t ct_id = mc_ctid_ptr[row];
                    if ((uint32_t)ct_id > (uint32_t)max_ct_id || !ct_arr_ptr[ct_id]) continue;
                    int32_t mid = mc_mid_ptr[row];
                    if ((uint32_t)mid > (uint32_t)max_movie_id) continue;
                    ++lmap[mid];
                    ++rows_emitted;
                }
                vrow = r_end_v;
            }
            auto& lh = local_mc[tid];
            for (auto& [mid, cnt] : lmap) lh.emplace_back(mid, cnt);
            mc_total_scanned.fetch_add(rows_scanned, std::memory_order_relaxed);
            mc_total_emitted.fetch_add(rows_emitted, std::memory_order_relaxed);
        });

        // Sequential merge: build mc_idx_arr (1-based), mc_movies, mc_counts
        {
            PROFILE_SCOPE("q8a_mc_merge");
            for (int t = 0; t < n_mc_threads; ++t) {
                for (auto& [mid, cnt] : local_mc[t]) {
                    int32_t idx1 = mc_idx_arr[mid];
                    if (idx1 == 0) {
                        idx1 = (int32_t)mc_movies.size() + 1;
                        mc_idx_arr[mid] = idx1;
                        mc_movies.push_back(mid);
                        mc_counts.push_back(0);
                    }
                    mc_counts[idx1 - 1] += cnt;
                }
            }
        }
        TRACE_COUNT("q8a_mc_rows_scanned", mc_total_scanned.load());
        TRACE_COUNT("q8a_mc_rows_emitted", mc_total_emitted.load());
    }
    TRACE_COUNT("q8a_mc_merge_ns", 0);
    // Reset mc_idx_arr touched positions to 0 for next query invocation.
    // Only reset the 132K written positions (not the full 10MB array).
    // This is much faster than calloc + page faults on next invocation.
    // Note: reset must happen AFTER mi1_build which reads mc_idx_arr.
    // We defer the reset to after all reads are done (at query end), using
    // a lambda that will be called before function return.
    // For now, we reset immediately since we're done with mc_idx_arr by
    // the time mi1_merge completes.
    // Actually, mc_idx_arr is read during mi1_build (3.3M rows). So we reset
    // AFTER mi1_build completes. We use RAII or defer.

    const int32_t n_mc_movies = (int32_t)mc_movies.size();

    // Compact per-movie arrays (indexed by position in mc_movies)
    std::vector<int32_t> mi1_counts(n_mc_movies, 0);      // mi1 count per mc_movie
    std::vector<int64_t> movie_contrib(n_mc_movies, 0LL); // final contribution per mc_movie

    // -----------------------------------------------------------------------
    // Build mi1_counts: parallel scan of movie_info row ranges.
    // Filter: only accumulate for movies in mc_movies (mc_idx_arr[mid] >= 0).
    // Uses per-thread hash maps, merged into compact mi1_counts array.
    // -----------------------------------------------------------------------
    if (!valid_info_type_ids.empty()) {
        PROFILE_SCOPE("q8a_mi1_build");
        const auto& mi = db->movie_info;

        struct MiRange { int32_t beg, end; };
        std::vector<MiRange> mi_ranges;
        mi_ranges.reserve(valid_info_type_ids.size());
        int32_t total_mi_rows = 0;
        for (int32_t it_id : valid_info_type_ids) {
            if (it_id < 0 || it_id >= (int32_t)mi.type_part_start.size()) continue;
            int32_t beg = mi.type_part_start[it_id];
            int32_t end = mi.type_part_end[it_id];
            if (end > beg) { mi_ranges.push_back({beg, end}); total_mi_rows += (end - beg); }
        }

        const int n_threads_mi = pool.num_threads;
        // Per-thread hash maps: (mc_idx, count) pairs
        std::vector<std::vector<std::pair<int32_t,int32_t>>> local_mi1_hits(n_threads_mi);
        for (auto& v : local_mi1_hits) v.reserve(4096);

        std::atomic<int64_t> mi_scanned{0}, mi_emitted{0};
        const int32_t* __restrict__ mc_idx2 = mc_idx_arr;
        const uint8_t* __restrict__ vii_arr = valid_info_id_arr.data();

        pool.parallel_for([&](int tid, int n) {
            int64_t rows_scanned = 0, rows_emitted = 0;

            int32_t vrow_beg = (int64_t(tid)     * total_mi_rows) / n;
            int32_t vrow_end = (int64_t(tid + 1) * total_mi_rows) / n;

            // Map mc_idx -> count (small: only mc_movies seen in this slice)
            std::unordered_map<int32_t,int32_t> lmap;
            lmap.reserve(4096);

            int32_t vrow = 0;
            for (int32_t ri = 0; ri < (int32_t)mi_ranges.size() && vrow < vrow_end; ++ri) {
                int32_t rlen = mi_ranges[ri].end - mi_ranges[ri].beg;
                int32_t r_global_end = vrow + rlen;
                if (r_global_end <= vrow_beg) { vrow = r_global_end; continue; }
                int32_t lo = std::max(0, vrow_beg - vrow);
                int32_t hi = std::min(rlen, vrow_end - vrow);
                int32_t phys_beg = mi_ranges[ri].beg + lo;
                const int32_t* __restrict__ mid_ptr = mi.movie_id.data() + phys_beg;
                const int32_t* __restrict__ iid_ptr = mi.info_id.data()  + phys_beg;
                for (int32_t r = 0; r < hi - lo; ++r) {
                    ++rows_scanned;
                    int32_t mid = mid_ptr[r];
                    if ((uint32_t)mid > (uint32_t)max_movie_id) continue;
                    int32_t idx1 = mc_idx2[mid]; // 1-based; 0=not in mc
                    if (idx1 == 0) continue;  // not in mc
                    int32_t iid = iid_ptr[r];
                    bool ok = ((uint32_t)iid < (uint32_t)mi_dict_size) ? (bool)vii_arr[iid] : info_null_ok;
                    if (!ok) continue;
                    ++lmap[idx1 - 1]; // convert to 0-based index
                    ++rows_emitted;
                }
                vrow = r_global_end;
            }
            auto& lh = local_mi1_hits[tid];
            lh.reserve(lmap.size());
            for (auto& [idx, cnt] : lmap) lh.emplace_back(idx, cnt);
            mi_scanned.fetch_add(rows_scanned, std::memory_order_relaxed);
            mi_emitted.fetch_add(rows_emitted, std::memory_order_relaxed);
        });

        // Sequential merge into mi1_counts (indexed by mc_movies position)
        {
            PROFILE_SCOPE("q8a_mi1_merge");
            int32_t* __restrict__ mi1c = mi1_counts.data();
            for (int t = 0; t < n_threads_mi; ++t) {
                for (auto& [idx, cnt] : local_mi1_hits[t])
                    mi1c[idx] += cnt;
            }
        }
        TRACE_COUNT("q8a_mi1_rows_scanned", mi_scanned.load());
        TRACE_COUNT("q8a_mi1_rows_emitted", mi_emitted.load());
    }
    // Reset mc_idx_arr touched positions for next invocation (only 132K positions)
    for (int32_t mid : mc_movies) mc_idx_arr[mid] = 0;

    // -----------------------------------------------------------------------
    // Title scan: mc intersect mi1 intersect title(kind/year) -> title_pass_movies
    // Parallelized across threads by splitting mc_movies (by index).
    // Uses compact arrays: mi1_counts[qi] and mc_counts[qi] indexed by mc_movies pos.
    // -----------------------------------------------------------------------
    // qualifying_movies: list of (mid, contrib) for final person join
    struct MovieContrib { int32_t mid; int64_t contrib; };
    std::vector<MovieContrib> qualifying_movies;
    qualifying_movies.reserve(4096);

    // title_pass_movies: list of (mid, partial_contrib=mi1*mc, qi_index) for mk probe
    struct TitlePass { int32_t mid; int64_t partial_contrib; };
    std::vector<TitlePass> title_pass_movies;
    title_pass_movies.reserve(32000);
    {
        // Sequential title scan: 132K movies, ~0.2ms, avoids parallel_for overhead.
        // Workers remain spinning after mi1_build, so person_join wake-up is instant.
        PROFILE_SCOPE("q8a_title_scan");
        const auto& t = db->title;

        const int32_t* __restrict__ id_to_row    = t.id_to_row.data();
        const int32_t  id_to_row_sz              = (int32_t)t.id_to_row.size();
        const int32_t* __restrict__ kind_id_arr  = t.kind_id.data();
        const int32_t* __restrict__ prod_year_arr = t.production_year.data();
        const int32_t* __restrict__ mi1c         = mi1_counts.data();
        const int32_t* __restrict__ mcc          = mc_counts.data();
        const uint8_t* __restrict__ vka          = valid_kind_arr.data();

        int64_t rows_emitted = 0;
        for (int32_t qi = 0; qi < n_mc_movies; ++qi) {
            int32_t mi1_v = mi1c[qi]; if (mi1_v == 0) continue;
            int32_t mc_v  = mcc[qi];
            int32_t mid   = mc_movies[qi];

            if (mid >= id_to_row_sz) continue;
            int32_t r = id_to_row[mid];
            if (r < 0) continue;

            int32_t kid = kind_id_arr[r];
            if (kid < 0) {
                if (!kind_null_ok_flag) continue;
            } else if ((uint32_t)kid > (uint32_t)max_kind_id || !vka[kid]) continue;

            int32_t py = prod_year_arr[r];
            if (py == -1) continue;
            if (year1 >= 0 && py > year1) continue;
            if (year2 >= 0 && py < year2) continue;

            title_pass_movies.push_back({mid, (int64_t)mi1_v * (int64_t)mc_v});
            ++rows_emitted;
        }
        TRACE_COUNT("q8a_title_mc_movies",    (int64_t)n_mc_movies);
        TRACE_COUNT("q8a_title_rows_emitted", rows_emitted);
    }

    // mk probe via CSR for title_pass_movies only
    {
        PROFILE_SCOPE("q8a_mk_build");
        int64_t rows_scanned = 0;
        const auto& mk_csr = db->movie_keyword.movie_id_csr;
        for (auto& tp : title_pass_movies) {
            int32_t mid = tp.mid;
            auto [beg, end] = mk_csr.range(mid);
            int32_t cnt = end - beg;
            rows_scanned += cnt;
            if (cnt > 0) {
                qualifying_movies.push_back({mid, tp.partial_contrib * (int64_t)cnt});
            }
        }
        TRACE_COUNT("q8a_mk_rows_scanned",    rows_scanned);
        TRACE_COUNT("q8a_movie_contrib_size", (int64_t)qualifying_movies.size());
    }

    // -----------------------------------------------------------------------
    // Person join: iterate CI rows for qualifying movies.
    // Parallel by movie partition; thread-local person cache.
    // -----------------------------------------------------------------------
    const auto& nm = db->name;
    const auto& ci = db->cast_info;

    int32_t max_person_id = (int32_t)nm.id_to_row.size() - 1;

    // Gender byte targets
    uint8_t target_gender_bytes[8];
    int n_gender_targets = 0;
    bool gender_empty_ok = false;
    for (const auto& gs : gender_set) {
        if (gs.empty()) { gender_empty_ok = true; continue; }
        if (n_gender_targets < 8) target_gender_bytes[n_gender_targets++] = (uint8_t)gs[0];
    }

    int64_t count = 0;
    int64_t ci_rows_probed = 0, ci_rows_matched = 0;
    int64_t persons_gender_pass = 0, persons_pcode_pass = 0;
    {
        PROFILE_SCOPE("q8a_person_join");
        const int n_threads_pj = pool.num_threads;
        const int32_t n_qm = (int32_t)qualifying_movies.size();

        std::vector<int64_t> local_count(n_threads_pj, 0);
        std::vector<int64_t> local_ci_probed(n_threads_pj, 0);
        std::vector<int64_t> local_ci_matched(n_threads_pj, 0);
        std::vector<int64_t> local_gender_pass(n_threads_pj, 0);
        std::vector<int64_t> local_pcode_pass(n_threads_pj, 0);

        const auto& csr                           = ci.movie_id_csr;
        const int32_t* __restrict__ ci_role_ptr   = ci.role_id.data();
        const int32_t* __restrict__ ci_person_ptr = ci.person_id.data();
        const uint8_t* __restrict__ nm_gbytes     = nm.gender_byte.data();
        const int32_t* __restrict__ nm_id_to_row  = nm.id_to_row.data();
        const uint8_t* __restrict__ role_arr      = valid_role_arr.data();

        pool.parallel_for([&](int tid, int n) {
            std::unordered_map<int32_t, uint8_t> pcache;
            pcache.reserve(2048);

            int64_t lcount = 0, lprobed = 0, lmatched = 0, lgpass = 0, lppass = 0;

            int32_t qm_beg = (tid * n_qm) / n;
            int32_t qm_end = ((tid + 1) * n_qm) / n;

            for (int32_t qi = qm_beg; qi < qm_end; ++qi) {
                int32_t mid    = qualifying_movies[qi].mid;
                int64_t contrib = qualifying_movies[qi].contrib;

                auto [ci_beg, ci_end] = csr.range(mid);
                for (int32_t row = ci_beg; row < ci_end; ++row) {
                    ++lprobed;

                    int32_t role_id = ci_role_ptr[row];
                    if ((uint32_t)role_id > (uint32_t)max_role_id || !role_arr[role_id]) continue;

                    int32_t pid = ci_person_ptr[row];
                    if ((uint32_t)pid > (uint32_t)max_person_id) continue;

                    auto it = pcache.find(pid);
                    uint8_t cached;
                    if (it != pcache.end()) {
                        cached = it->second;
                    } else {
                        int32_t nrow = nm_id_to_row[pid];
                        bool ok = false;
                        if (nrow >= 0) {
                            uint8_t gb = nm_gbytes[nrow];
                            bool gender_ok = false;
                            if (gb == 0) {
                                gender_ok = gender_empty_ok;
                            } else {
                                for (int gi = 0; gi < n_gender_targets; ++gi) {
                                    if (gb == target_gender_bytes[gi]) { gender_ok = true; break; }
                                }
                            }
                            if (gender_ok) {
                                ++lgpass;
                                if (pcode_cf_set.count(nm.name_pcode_cf[nrow])) {
                                    ++lppass;
                                    ok = true;
                                }
                            }
                        }
                        cached = ok ? 1 : 2;
                        pcache.emplace(pid, cached);
                    }
                    if (cached != 1) continue;

                    lcount += contrib;
                    ++lmatched;
                }
            }

            local_count[tid]       = lcount;
            local_ci_probed[tid]   = lprobed;
            local_ci_matched[tid]  = lmatched;
            local_gender_pass[tid] = lgpass;
            local_pcode_pass[tid]  = lppass;
        });

        for (int t = 0; t < n_threads_pj; ++t) {
            count               += local_count[t];
            ci_rows_probed      += local_ci_probed[t];
            ci_rows_matched     += local_ci_matched[t];
            persons_gender_pass += local_gender_pass[t];
            persons_pcode_pass  += local_pcode_pass[t];
        }
    }
    TRACE_COUNT("q8a_name_rows_total",     (int64_t)nm.id.size());
    TRACE_COUNT("q8a_persons_gender_pass", persons_gender_pass);
    TRACE_COUNT("q8a_persons_pcode_pass",  persons_pcode_pass);
    TRACE_COUNT("q8a_ci_rows_probed",    ci_rows_probed);
    TRACE_COUNT("q8a_ci_rows_matched",   ci_rows_matched);
    TRACE_COUNT("q8a_query_output_rows", 1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
// force recompile
