#include "query2b.hpp" // v8 - adaptive seq/par based on data size
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>

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
AND (t.production_year >= YEAR2)
AND (k.keyword IN KEYWORD) */

std::vector<std::vector<std::string>> run_q2b(Database* db, const Q2bArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q2b: db is null");
    }
    PROFILE_SCOPE("q2b_total");

    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty()) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty()) year2 = std::stoi(args.YEAR2);

    auto parse_id_set = [](const std::vector<std::string>& sv) {
        std::unordered_set<int32_t> s;
        for (const auto& v : sv)
            s.insert(v == "<<NULL>>" ? -1 : std::stoi(v));
        return s;
    };
    auto id1_set = parse_id_set(args.ID1);
    auto id2_set = parse_id_set(args.ID2);

    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    int32_t max_role_id = 0;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (rt.id[i] > max_role_id) max_role_id = rt.id[i];
    }
    std::vector<uint8_t> role_ok((size_t)(max_role_id + 2), 0);
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i])) role_ok[(size_t)rt.id[i]] = 1;
        if (role_null_ok) role_ok[0] = 1;
    }
    const int32_t role_ok_bound = (int32_t)role_ok.size() - 1;

    std::unordered_set<std::string> kind_set(args.KIND.begin(), args.KIND.end());
    bool kind_null_ok = kind_set.count("<<NULL>>") > 0;
    int32_t max_kind_id = 0;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kt.id[i] > max_kind_id) max_kind_id = kt.id[i];
    }
    std::vector<uint8_t> kind_ok((size_t)(max_kind_id + 2), 0);
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_set.count(kt.kind[i])) kind_ok[(size_t)kt.id[i]] = 1;
    }

    std::unordered_set<std::string> gender_set(args.GENDER.begin(), args.GENDER.end());
    bool gender_null_ok = gender_set.count("<<NULL>>") > 0;
    gender_set.erase("<<NULL>>");
    uint8_t gender_byte_ok[256] = {};
    for (const auto& gs : gender_set)
        if (!gs.empty()) gender_byte_ok[(uint8_t)gs[0]] = 1;
    if (gender_null_ok) gender_byte_ok[0] = 1;

    std::unordered_set<std::string> keyword_set(args.KEYWORD.begin(), args.KEYWORD.end());
    bool keyword_null_ok = keyword_set.count("<<NULL>>") > 0;
    std::vector<int32_t> valid_keyword_ids_vec;
    std::unordered_set<int32_t> valid_keyword_ids;
    {
        PROFILE_SCOPE("q2b_setup_keyword");
        const auto& kw = db->keyword;
        TRACE_COUNT("q2b_kw_map_size", (int64_t)kw.str_to_ids.size());
        if (!kw.str_to_ids.empty()) {
            for (const auto& kname : args.KEYWORD) {
                if (kname == "<<NULL>>") continue;
                auto it = kw.str_to_ids.find(kname);
                if (it != kw.str_to_ids.end()) {
                    for (int32_t kid : it->second) {
                        valid_keyword_ids.insert(kid);
                        valid_keyword_ids_vec.push_back(kid);
                    }
                }
            }
            if (keyword_null_ok) valid_keyword_ids.insert(-1);
        } else {
            for (size_t i = 0; i < kw.id.size(); ++i) {
                if (keyword_set.count(kw.keyword_str[i])) {
                    valid_keyword_ids.insert(kw.id[i]);
                    valid_keyword_ids_vec.push_back(kw.id[i]);
                }
            }
            if (keyword_null_ok) valid_keyword_ids.insert(-1);
        }
        TRACE_COUNT("q2b_kw_fast_ids", (int64_t)valid_keyword_ids_vec.size());
    }

    static constexpr int MAX_WIN = 8;

    auto apply_reconstruction = [&db](
        std::unordered_set<std::string>& s,
        const std::vector<std::string>& tokens,
        const std::unordered_set<int32_t>& type_ids)
    {
        if (tokens.empty()) return;
        std::unordered_set<std::string> cands;
        int32_t n = (int32_t)tokens.size();
        for (int32_t i = 0; i < n; ++i) {
            for (const char* sep : {", ", ","}) {
                std::string c = tokens[i];
                for (int32_t j = i+1; j < n && j <= i+MAX_WIN-1; ++j) {
                    c += sep; c += tokens[j]; cands.insert(c);
                }
            }
        }
        if (cands.empty()) return;
        std::unordered_set<std::string> found;
        const auto& mi = db->movie_info;
        const auto& uinfo = mi.type_unique_info;
        for (int32_t tid : type_ids) {
            if (tid < 0 || tid >= (int32_t)uinfo.size()) continue;
            const auto& uset = uinfo[(size_t)tid];
            for (const auto& cand : cands) {
                if (uset.count(cand)) found.insert(cand);
            }
        }
        if (found.empty()) return;
        s.clear();
        int32_t i = 0;
        while (i < n) {
            bool matched = false;
            for (int32_t len = std::min((int32_t)MAX_WIN, n - i); len >= 2 && !matched; --len) {
                for (const char* sep : {", ", ","}) {
                    std::string c = tokens[i];
                    for (int32_t k = 1; k < len; ++k) { c += sep; c += tokens[i + k]; }
                    if (found.count(c)) { s.insert(c); i += len; matched = true; break; }
                }
            }
            if (!matched) { s.insert(tokens[i]); ++i; }
        }
    };

    std::unordered_set<std::string> info1_set(args.INFO1.begin(), args.INFO1.end());
    bool info1_null_ok = info1_set.count("<<NULL>>") > 0;
    std::unordered_set<std::string> info2_set(args.INFO2.begin(), args.INFO2.end());
    bool info2_null_ok = info2_set.count("<<NULL>>") > 0;

    {
        PROFILE_SCOPE("q2b_info_reconstruction");
        apply_reconstruction(info1_set, args.INFO1, id1_set);
        apply_reconstruction(info2_set, args.INFO2, id2_set);
        TRACE_COUNT("q2b_info1_values", (int64_t)info1_set.size());
        TRACE_COUNT("q2b_info2_values", (int64_t)info2_set.size());
    }

    const auto& mi_table = db->movie_info;
    const int32_t max_iid = (int32_t)mi_table.info_dict_vec.size();
    std::vector<uint8_t> info1_iid_arr, info2_iid_arr;
    bool info1_iid_ok = false, info2_iid_ok = false;

    if (!mi_table.info_dict_map.empty() && max_iid > 0) {
        info1_iid_arr.assign(max_iid, 0);
        info2_iid_arr.assign(max_iid, 0);
        for (const auto& s : info1_set) {
            auto it = mi_table.info_dict_map.find(s);
            if (it != mi_table.info_dict_map.end() && it->second >= 0 && it->second < max_iid)
                info1_iid_arr[it->second] = 1;
        }
        for (const auto& s : info2_set) {
            auto it = mi_table.info_dict_map.find(s);
            if (it != mi_table.info_dict_map.end() && it->second >= 0 && it->second < max_iid)
                info2_iid_arr[it->second] = 1;
        }
        info1_iid_ok = true;
        info2_iid_ok = true;
    }

    auto info_matches = [](const std::string& inf,
                           const std::unordered_set<std::string>& s,
                           bool null_ok) -> bool {
        if (inf.empty()) return null_ok;
        return s.count(inf) > 0;
    };

    const int n_threads = pool.num_threads;

    // -----------------------------------------------------------------------
    // Build per-movie count of qualifying movie_keyword rows.
    // Adaptive: sequential for small arrays (avoids parallel dispatch overhead),
    // parallel sort+merge for large arrays.
    // -----------------------------------------------------------------------
    std::vector<int32_t> mk_mid_arr;
    std::vector<int32_t> mk_cnt_arr;
    {
        PROFILE_SCOPE("q2b_movie_keyword_build");
        const auto& mk = db->movie_keyword;

        int64_t rows_in = 0, rows_emitted = 0;
        std::vector<int32_t> movie_ids_flat;

        if (!mk.keyword_id_csr.empty()) {
            int64_t total = 0;
            for (int32_t kid : valid_keyword_ids_vec) {
                auto [beg, end] = mk.keyword_id_csr.range(kid);
                total += (end - beg);
                rows_in += (end - beg);
            }
            rows_emitted = total;
            movie_ids_flat.resize((size_t)total);

            struct KwRange { int32_t csr_beg; int32_t csr_end; int64_t flat_beg; };
            std::vector<KwRange> kw_ranges;
            kw_ranges.reserve(valid_keyword_ids_vec.size());
            int64_t flat_off = 0;
            for (int32_t kid : valid_keyword_ids_vec) {
                auto [beg, end] = mk.keyword_id_csr.range(kid);
                kw_ranges.push_back({beg, end, flat_off});
                flat_off += (end - beg);
            }

            // Adaptive: sequential fill for small arrays.
            static constexpr int64_t PAR_FILL_THRESHOLD = 65536;
            if (total <= PAR_FILL_THRESHOLD) {
                int64_t flat_pos = 0;
                for (const auto& kr : kw_ranges) {
                    for (int32_t ci2 = kr.csr_beg; ci2 < kr.csr_end; ++ci2, ++flat_pos) {
                        movie_ids_flat[flat_pos] = mk.movie_id[mk.keyword_id_csr.values[ci2]];
                    }
                }
            } else {
                pool.parallel_for([&](int tid, int nt) {
                    int64_t lo = (total * tid) / nt;
                    int64_t hi = (total * (tid + 1)) / nt;
                    int kr_idx = (int)(std::upper_bound(kw_ranges.begin(), kw_ranges.end(), lo,
                        [](int64_t v, const KwRange& r){ return v < r.flat_beg; })
                        - kw_ranges.begin()) - 1;
                    if (kr_idx < 0) kr_idx = 0;
                    int64_t flat_pos = lo;
                    while (flat_pos < hi && kr_idx < (int)kw_ranges.size()) {
                        const auto& kr = kw_ranges[kr_idx];
                        int64_t off = flat_pos - kr.flat_beg;
                        int32_t csr_cur = kr.csr_beg + (int32_t)off;
                        int32_t csr_end_local = (int32_t)std::min(
                            (int64_t)kr.csr_end, kr.csr_beg + (hi - kr.flat_beg));
                        for (int32_t ci2 = csr_cur; ci2 < csr_end_local; ++ci2, ++flat_pos) {
                            movie_ids_flat[flat_pos] = mk.movie_id[mk.keyword_id_csr.values[ci2]];
                        }
                        ++kr_idx;
                    }
                });
            }
        } else {
            rows_in = (int64_t)mk.movie_id.size();
            movie_ids_flat.reserve(mk.movie_id.size() / 10);
            for (int32_t r = 0; r < (int32_t)mk.movie_id.size(); ++r) {
                if (!valid_keyword_ids.count(mk.keyword_id[r])) continue;
                movie_ids_flat.push_back(mk.movie_id[r]);
                ++rows_emitted;
            }
        }
        TRACE_COUNT("q2b_mk_scan_rows_in", rows_in);
        TRACE_COUNT("q2b_mk_scan_rows_emitted", rows_emitted);

        const int64_t sz = (int64_t)movie_ids_flat.size();

        // Adaptive sort: sequential for small, parallel for large.
        static constexpr int64_t SEQ_SORT_THRESHOLD = 65536;
        if (sz <= SEQ_SORT_THRESHOLD) {
            PROFILE_SCOPE("q2b_mk_build_sort");
            std::sort(movie_ids_flat.begin(), movie_ids_flat.end());
            TRACE_COUNT("q2b_mk_build_merge_skipped", 1);
        } else {
            {
                PROFILE_SCOPE("q2b_mk_build_sort");
                pool.parallel_for([&](int tid, int nt) {
                    int64_t lo = (sz * tid) / nt;
                    int64_t hi = (sz * (tid + 1)) / nt;
                    std::sort(movie_ids_flat.begin() + lo, movie_ids_flat.begin() + hi);
                });
            }
            {
                PROFILE_SCOPE("q2b_mk_build_merge");
                std::vector<int32_t> buf(movie_ids_flat.size());
                int32_t* src = movie_ids_flat.data();
                int32_t* dst = buf.data();

                std::vector<int64_t> boundaries(n_threads + 1);
                for (int t = 0; t <= n_threads; ++t)
                    boundaries[t] = (sz * t) / n_threads;

                while ((int)boundaries.size() > 2) {
                    int cur_runs = (int)boundaries.size() - 1;
                    int new_runs = (cur_runs + 1) / 2;
                    std::vector<int64_t> new_boundaries(new_runs + 1);
                    new_boundaries[0] = 0;

                    pool.parallel_for([&](int tid, int nt) {
                        for (int pair_idx = tid; pair_idx < new_runs; pair_idx += nt) {
                            int r = pair_idx * 2;
                            int64_t lo1 = boundaries[r];
                            int64_t hi1 = boundaries[r + 1];
                            if (r + 2 < (int)boundaries.size()) {
                                int64_t hi2 = boundaries[r + 2];
                                std::merge(src + lo1, src + hi1,
                                           src + hi1, src + hi2,
                                           dst + lo1);
                                new_boundaries[pair_idx + 1] = hi2;
                            } else {
                                std::copy(src + lo1, src + hi1, dst + lo1);
                                new_boundaries[pair_idx + 1] = hi1;
                            }
                        }
                    });
                    new_boundaries[0] = 0;
                    std::swap(src, dst);
                    boundaries = std::move(new_boundaries);
                }
                if (src == buf.data())
                    std::copy(buf.begin(), buf.end(), movie_ids_flat.begin());
            }
        }

        // Sequential count-runs.
        {
            PROFILE_SCOPE("q2b_mk_build_count_runs");
            mk_mid_arr.reserve((size_t)sz / 6 + 16);
            mk_cnt_arr.reserve((size_t)sz / 6 + 16);
            int64_t i = 0;
            const int32_t* data = movie_ids_flat.data();
            while (i < sz) {
                int32_t mid = data[i];
                int32_t cnt = 0;
                while (i < sz && data[i] == mid) { ++cnt; ++i; }
                mk_mid_arr.push_back(mid);
                mk_cnt_arr.push_back(cnt);
            }
        }
        TRACE_COUNT("q2b_mk_groups_created", (int64_t)mk_mid_arr.size());
    }

    // -----------------------------------------------------------------------
    // Filter title: adaptive sequential/parallel over mk_mid_arr.
    // -----------------------------------------------------------------------
    int32_t mk_max_mid_g = 0;
    std::vector<int32_t> mk_count_flat_g;
    std::vector<std::pair<int32_t,int32_t>> mk_title_vec;
    {
        PROFILE_SCOPE("q2b_title_prefilter");

        mk_max_mid_g = mk_mid_arr.empty() ? 0 :
            (*std::max_element(mk_mid_arr.begin(), mk_mid_arr.end())) + 1;
        mk_count_flat_g.assign(mk_max_mid_g > 0 ? mk_max_mid_g : 1, 0);
        for (size_t ii = 0; ii < mk_mid_arr.size(); ++ii)
            mk_count_flat_g[mk_mid_arr[ii]] = mk_cnt_arr[ii];

        const int32_t mk_max_mid = mk_max_mid_g;
        const auto& mk_count_flat = mk_count_flat_g;

        const auto& t = db->title;
        const int64_t mk_arr_total = (int64_t)mk_mid_arr.size();
        std::atomic<int64_t> tpf_looked_up{0}, tpf_passed{0};

        // Adaptive: for small arrays (< 16K), sequential avoids parallel overhead.
        static constexpr int64_t PAR_TITLE_THRESHOLD = 16384;
        if (mk_arr_total <= PAR_TITLE_THRESHOLD) {
            int64_t looked_up_local = 0, passed_local = 0;
            mk_title_vec.reserve((size_t)mk_arr_total);
            for (int64_t idx = 0; idx < mk_arr_total; ++idx) {
                int32_t mid = mk_mid_arr[idx];
                ++looked_up_local;
                if (mid < 0 || mid >= (int32_t)t.id_to_row.size()) continue;
                int32_t row = t.id_to_row[mid];
                if (row < 0) continue;
                int32_t kid = t.kind_id[row];
                if (kid < 0) { if (!kind_null_ok) continue; }
                else if ((size_t)kid >= kind_ok.size() || !kind_ok[(size_t)kid]) continue;
                int32_t py = t.production_year[row];
                if (py == -1) continue;
                if (year1 >= 0 && py > year1) continue;
                if (year2 >= 0 && py < year2) continue;
                int32_t cnt = (mid < mk_max_mid) ? mk_count_flat[mid] : 0;
                if (cnt == 0) continue;
                mk_title_vec.emplace_back(mid, cnt);
                ++passed_local;
            }
            tpf_looked_up.fetch_add(looked_up_local, std::memory_order_relaxed);
            tpf_passed.fetch_add(passed_local, std::memory_order_relaxed);
        } else {
            std::vector<std::vector<std::pair<int32_t,int32_t>>> tl_title(n_threads);
            pool.parallel_for([&](int tid, int nt) {
                int64_t lo = (mk_arr_total * tid) / nt;
                int64_t hi = (mk_arr_total * (tid + 1)) / nt;
                auto& my_vec = tl_title[tid];
                int64_t looked_up_local = 0, passed_local = 0;
                for (int64_t idx = lo; idx < hi; ++idx) {
                    int32_t mid = mk_mid_arr[idx];
                    ++looked_up_local;
                    if (mid < 0 || mid >= (int32_t)t.id_to_row.size()) continue;
                    int32_t row = t.id_to_row[mid];
                    if (row < 0) continue;
                    int32_t kid = t.kind_id[row];
                    if (kid < 0) { if (!kind_null_ok) continue; }
                    else if ((size_t)kid >= kind_ok.size() || !kind_ok[(size_t)kid]) continue;
                    int32_t py = t.production_year[row];
                    if (py == -1) continue;
                    if (year1 >= 0 && py > year1) continue;
                    if (year2 >= 0 && py < year2) continue;
                    int32_t cnt = (mid < mk_max_mid) ? mk_count_flat[mid] : 0;
                    if (cnt == 0) continue;
                    my_vec.emplace_back(mid, cnt);
                    ++passed_local;
                }
                tpf_looked_up.fetch_add(looked_up_local, std::memory_order_relaxed);
                tpf_passed.fetch_add(passed_local, std::memory_order_relaxed);
            });
            size_t total_sz = 0;
            for (int t2 = 0; t2 < n_threads; ++t2) total_sz += tl_title[t2].size();
            mk_title_vec.reserve(total_sz);
            for (int t2 = 0; t2 < n_threads; ++t2)
                for (auto& p : tl_title[t2]) mk_title_vec.push_back(p);
        }
        TRACE_COUNT("q2b_title_prefilter_looked_up", tpf_looked_up.load());
        TRACE_COUNT("q2b_title_prefilter_passed", tpf_passed.load());
    }

    // -----------------------------------------------------------------------
    // Accumulate mi1/mi2 counts using type_iid_rows inverted index.
    // Fast path: binary-search each qualifying movie_id in per-(type,iid) lists.
    // This is O(Q * K * log(N)) where Q=#qualifying movies (~14), K=#matching iids (~2),
    // instead of O(all_rows_in_type_partition) which is millions.
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t,int32_t> mi1_count, mi2_count;
    {
        PROFILE_SCOPE("q2b_movie_info_scan");
        const auto& mi = mi_table;

        // Build byte array marking title-qualified movie_ids.
        const int32_t q_max_mid = mk_max_mid_g;
        std::vector<uint8_t> qualifying_byte(q_max_mid > 0 ? q_max_mid : 1, 0);
        for (const auto& [mid, cnt] : mk_title_vec)
            if (mid >= 0 && mid < q_max_mid) qualifying_byte[mid] = 1;

        int64_t rows_scanned_total = 0;
        int64_t mi1_emitted = 0, mi2_emitted = 0;

        // Build sorted list of qualifying movie_ids for binary search intersection.
        std::vector<int32_t> qualifying_mids;
        qualifying_mids.reserve(mk_title_vec.size());
        for (const auto& [mid2, cnt2] : mk_title_vec)
            if (mid2 >= 0) qualifying_mids.push_back(mid2);
        std::sort(qualifying_mids.begin(), qualifying_mids.end());
        TRACE_COUNT("q2b_qualifying_mids", (int64_t)qualifying_mids.size());

        // Use type_iid_rows inverted index when available.
        bool use_iid_index = !mi.type_iid_rows.empty() && info1_iid_ok && info2_iid_ok;

        if (use_iid_index) {
            // Fast path: for each matching (type, intern_id), binary-search each
            // qualifying movie_id in the sorted iid_rows list.
            // iid_rows[type][beg..end) is sorted by movie_id (see db_loader).
            auto process_iids = [&](const std::unordered_set<int32_t>& type_ids,
                                    const std::vector<uint8_t>& iid_arr,
                                    bool null_ok,
                                    std::unordered_map<int32_t,int32_t>& out_count,
                                    int64_t& emitted_ref) {
                for (int32_t tid2 : type_ids) {
                    if (tid2 < 0 || (size_t)tid2 >= mi.type_iid_keys.size()) continue;
                    const auto& keys     = mi.type_iid_keys[tid2];
                    const auto& offsets  = mi.type_iid_offsets[tid2];
                    const auto& iid_rows = mi.type_iid_rows[tid2];
                    const int32_t* row_data = iid_rows.data();

                    for (int32_t ki = 0; ki < (int32_t)keys.size(); ++ki) {
                        int32_t iid = keys[ki];
                        if (iid < 0 || iid >= max_iid || !iid_arr[iid]) continue;
                        int32_t beg  = offsets[ki];
                        int32_t end2 = offsets[ki + 1];
                        // Binary-search each qualifying movie_id in iid_rows[beg..end2).
                        // This is O(Q * log(end2-beg)) instead of O(end2-beg).
                        rows_scanned_total += (int64_t)qualifying_mids.size();
                        for (int32_t qmid : qualifying_mids) {
                            auto it = std::lower_bound(row_data + beg, row_data + end2, qmid);
                            if (it == row_data + end2 || *it != qmid) continue;
                            // Count all occurrences (duplicates for same movie in same type)
                            int32_t count = 0;
                            while (it != row_data + end2 && *it == qmid) { ++count; ++it; }
                            out_count[qmid] += count;
                            emitted_ref += count;
                        }
                    }

                    // Handle null info values if null_ok (sequential scan of type partition)
                    if (null_ok && (size_t)tid2 < mi.type_part_start.size()) {
                        int32_t pb = mi.type_part_start[tid2], pe = mi.type_part_end[tid2];
                        rows_scanned_total += (pe - pb);
                        for (int32_t r2 = pb; r2 < pe; ++r2) {
                            if (mi.info_id[r2] >= 0) continue;
                            int32_t mid = mi.movie_id[r2];
                            if (mid < 0 || mid >= q_max_mid || !qualifying_byte[mid]) continue;
                            ++out_count[mid];
                            ++emitted_ref;
                        }
                    }
                }
            };
            process_iids(id1_set, info1_iid_arr, info1_null_ok, mi1_count, mi1_emitted);
            process_iids(id2_set, info2_iid_arr, info2_null_ok, mi2_count, mi2_emitted);
        } else {
            // Fallback: sequential scan of type partitions.
            auto scan_partition = [&](const std::unordered_set<int32_t>& type_ids,
                                      const std::vector<uint8_t>& iid_arr,
                                      bool iid_ok_flag, bool null_ok,
                                      const std::unordered_set<std::string>& str_set,
                                      std::unordered_map<int32_t,int32_t>& out_count,
                                      int64_t& emitted_ref) {
                for (int32_t tid2 : type_ids) {
                    if (tid2 < 0 || (size_t)tid2 >= mi.type_part_start.size()) continue;
                    int32_t pb = mi.type_part_start[tid2], pe = mi.type_part_end[tid2];
                    rows_scanned_total += (pe - pb);
                    for (int32_t r2 = pb; r2 < pe; ++r2) {
                        int32_t mid = mi.movie_id[r2];
                        if (mid < 0 || mid >= q_max_mid || !qualifying_byte[mid]) continue;
                        bool ok;
                        if (iid_ok_flag) {
                            int32_t iid = mi.info_id[r2];
                            ok = (iid >= 0 && iid < max_iid && iid_arr[iid]) || (iid < 0 && null_ok);
                        } else {
                            ok = info_matches(mi.info_str[r2], str_set, null_ok);
                        }
                        if (ok) { ++out_count[mid]; ++emitted_ref; }
                    }
                }
            };
            scan_partition(id1_set, info1_iid_arr, info1_iid_ok, info1_null_ok,
                           info1_set, mi1_count, mi1_emitted);
            scan_partition(id2_set, info2_iid_arr, info2_iid_ok, info2_null_ok,
                           info2_set, mi2_count, mi2_emitted);
        }

        TRACE_COUNT("q2b_movie_info_rows_scanned", rows_scanned_total);
        TRACE_COUNT("q2b_mi1_rows_emitted", mi1_emitted);
        TRACE_COUNT("q2b_mi2_rows_emitted", mi2_emitted);
    }

    // -----------------------------------------------------------------------
    // Build valid_movies flat vector.
    // Adaptive: sequential for small data sizes.
    // -----------------------------------------------------------------------
    std::vector<std::pair<int32_t,int64_t>> valid_movies_vec;
    {
        PROFILE_SCOPE("q2b_valid_movies_build");
        const int64_t total = (int64_t)mk_title_vec.size();
        std::atomic<int64_t> rows_emitted{0};

        static constexpr int64_t PAR_VM_THRESHOLD = 4096;
        if (total <= PAR_VM_THRESHOLD) {
            valid_movies_vec.reserve((size_t)total);
            int64_t local_emitted = 0;
            for (int64_t idx = 0; idx < total; ++idx) {
                int32_t mid = mk_title_vec[idx].first;
                int32_t mk_cnt = mk_title_vec[idx].second;
                auto i1 = mi1_count.find(mid), i2 = mi2_count.find(mid);
                if (i1 == mi1_count.end() || i2 == mi2_count.end()) continue;
                valid_movies_vec.emplace_back(mid, (int64_t)i1->second * i2->second * mk_cnt);
                ++local_emitted;
            }
            rows_emitted.fetch_add(local_emitted, std::memory_order_relaxed);
        } else {
            std::vector<std::vector<std::pair<int32_t,int64_t>>> tl_vm(n_threads);
            pool.parallel_for([&](int tid, int nt) {
                int64_t lo = (total * tid) / nt;
                int64_t hi = (total * (tid + 1)) / nt;
                auto& my_vm = tl_vm[tid];
                int64_t local_emitted = 0;
                for (int64_t idx = lo; idx < hi; ++idx) {
                    int32_t mid = mk_title_vec[idx].first;
                    int32_t mk_cnt = mk_title_vec[idx].second;
                    auto i1 = mi1_count.find(mid), i2 = mi2_count.find(mid);
                    if (i1 == mi1_count.end() || i2 == mi2_count.end()) continue;
                    my_vm.emplace_back(mid, (int64_t)i1->second * i2->second * mk_cnt);
                    ++local_emitted;
                }
                rows_emitted.fetch_add(local_emitted, std::memory_order_relaxed);
            });
            size_t total_sz = 0;
            for (int t = 0; t < n_threads; ++t) total_sz += tl_vm[t].size();
            valid_movies_vec.reserve(total_sz);
            for (int t = 0; t < n_threads; ++t)
                for (auto& p : tl_vm[t]) valid_movies_vec.push_back(p);
        }
        TRACE_COUNT("q2b_title_rows_emitted", rows_emitted.load());
    }

    // -----------------------------------------------------------------------
    // Scan cast_info for valid movies.
    // Adaptive: sequential for small valid_movies_vec.
    // -----------------------------------------------------------------------
    std::atomic<int64_t> atomic_count{0};
    std::atomic<int64_t> atomic_probe_rows{0};
    std::atomic<int64_t> atomic_join_rows{0};
    {
        PROFILE_SCOPE("q2b_cast_info_probe");
        const auto& ci  = db->cast_info;
        const auto& nm  = db->name;
        const auto& csr = ci.movie_id_csr;
        const int64_t vm_total = (int64_t)valid_movies_vec.size();

        static constexpr int64_t PAR_CAST_THRESHOLD = 256;
        if (vm_total <= PAR_CAST_THRESHOLD) {
            int64_t local_count = 0, probe_local = 0, join_local = 0;
            for (int64_t idx = 0; idx < vm_total; ++idx) {
                int32_t mid = valid_movies_vec[idx].first;
                int64_t mi_mult = valid_movies_vec[idx].second;
                auto [beg, end] = csr.range(mid);
                for (int32_t r = beg; r < end; ++r) {
                    ++probe_local;
                    int32_t rid = ci.role_id[r];
                    if (rid < 0 || rid > role_ok_bound || !role_ok[(size_t)rid]) continue;
                    int32_t pid = ci.person_id[r];
                    if (pid < 0 || pid >= (int32_t)nm.id_to_row.size()) continue;
                    int32_t nrow = nm.id_to_row[pid];
                    if (nrow < 0) continue;
                    if (!gender_byte_ok[nm.gender_byte[nrow]]) continue;
                    local_count += mi_mult;
                    ++join_local;
                }
            }
            atomic_count.fetch_add(local_count, std::memory_order_relaxed);
            atomic_probe_rows.fetch_add(probe_local, std::memory_order_relaxed);
            atomic_join_rows.fetch_add(join_local, std::memory_order_relaxed);
        } else {
            pool.parallel_for([&](int tid, int nt) {
                int64_t lo = (vm_total * tid) / nt;
                int64_t hi = (vm_total * (tid + 1)) / nt;
                int64_t local_count = 0;
                int64_t probe_local = 0, join_local = 0;

                for (int64_t idx = lo; idx < hi; ++idx) {
                    int32_t mid = valid_movies_vec[idx].first;
                    int64_t mi_mult = valid_movies_vec[idx].second;
                    auto [beg, end] = csr.range(mid);
                    for (int32_t r = beg; r < end; ++r) {
                        ++probe_local;
                        int32_t rid = ci.role_id[r];
                        if (rid < 0 || rid > role_ok_bound || !role_ok[(size_t)rid]) continue;
                        int32_t pid = ci.person_id[r];
                        if (pid < 0 || pid >= (int32_t)nm.id_to_row.size()) continue;
                        int32_t nrow = nm.id_to_row[pid];
                        if (nrow < 0) continue;
                        if (!gender_byte_ok[nm.gender_byte[nrow]]) continue;
                        local_count += mi_mult;
                        ++join_local;
                    }
                }

                atomic_count.fetch_add(local_count, std::memory_order_relaxed);
                atomic_probe_rows.fetch_add(probe_local, std::memory_order_relaxed);
                atomic_join_rows.fetch_add(join_local, std::memory_order_relaxed);
            });
        }

        TRACE_COUNT("q2b_cast_probe_rows_in", atomic_probe_rows.load());
        TRACE_COUNT("q2b_join_rows_emitted", atomic_join_rows.load());
    }

    TRACE_COUNT("q2b_valid_movies", (int64_t)valid_movies_vec.size());
    TRACE_COUNT("q2b_query_output_rows", 1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(atomic_count.load())});
    return rows;
}
