#include "query5a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <atomic>
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
// SELECT COUNT(*)
// FROM title as t, movie_info as mi1, kind_type as kt,
//      info_type as it1, info_type as it3, info_type as it4,
//      movie_info_idx as mii1, movie_info_idx as mii2,
//      movie_keyword as mk, keyword as k
// WHERE t.id = mi1.movie_id AND t.id = mii1.movie_id
//   AND t.id = mii2.movie_id AND t.id = mk.movie_id
//   AND mk.keyword_id = k.id
//   AND mi1.info_type_id = it1.id  AND (it1.id IN ID1)
//   AND mii1.info_type_id = it3.id AND it3.id = ID2
//   AND mii2.info_type_id = it4.id AND it4.id = ID3
//   AND t.kind_id = kt.id AND (kt.kind IN KIND)
//   AND (t.production_year <= YEAR1) AND (t.production_year >= YEAR2)
//   AND (mi1.info IN INFO1)
//   AND (mii2.info numeric AND INFO3 <= mii2.info::float <= INFO2)
//   AND (mii1.info numeric AND INFO4 <= mii1.info::float <= INFO5)

std::vector<std::vector<std::string>> run_q5a(Database* db, const Q5aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q5a: db is null");
    }
    PROFILE_SCOPE("q5a_total");

    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };

    // Parse year bounds
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty() && !is_null(args.YEAR1)) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty() && !is_null(args.YEAR2)) year2 = std::stoi(args.YEAR2);

    // Parse ID2, ID3
    int32_t id2 = -1, id3 = -1;
    if (!args.ID2.empty() && !is_null(args.ID2)) id2 = std::stoi(args.ID2);
    if (!args.ID3.empty() && !is_null(args.ID3)) id3 = std::stoi(args.ID3);

    // Parse ID1: set of valid info_type_ids for mi1
    std::unordered_set<int32_t> id1_set;
    std::vector<uint8_t> id1_flat;
    for (const auto& s : args.ID1)
        if (!is_null(s)) id1_set.insert(std::stoi(s));
    if (!id1_set.empty()) {
        int32_t max_id1 = *std::max_element(id1_set.begin(), id1_set.end());
        id1_flat.assign(max_id1 + 1, 0);
        for (int32_t v : id1_set) id1_flat[v] = 1;
    }

    // Parse float bounds
    float info2 =  std::numeric_limits<float>::max();
    float info3 = -std::numeric_limits<float>::max();
    float info4 = -std::numeric_limits<float>::max();
    float info5 =  std::numeric_limits<float>::max();
    if (!args.INFO2.empty() && !is_null(args.INFO2)) info2 = std::stof(args.INFO2);
    if (!args.INFO3.empty() && !is_null(args.INFO3)) info3 = std::stof(args.INFO3);
    if (!args.INFO4.empty() && !is_null(args.INFO4)) info4 = std::stof(args.INFO4);
    if (!args.INFO5.empty() && !is_null(args.INFO5)) info5 = std::stof(args.INFO5);

    // Resolve valid kind_ids
    std::unordered_set<std::string> kind_str_set;
    bool kind_null_ok = false;
    for (const auto& s : args.KIND) {
        if (is_null(s)) kind_null_ok = true;
        else            kind_str_set.insert(s);
    }
    std::vector<uint8_t> valid_kind_flat;
    {
        PROFILE_SCOPE("q5a_kind_resolve");
        const auto& kt = db->kind_type;
        int32_t max_kid = 0;
        for (size_t i = 0; i < kt.id.size(); ++i) if (kt.id[i] > max_kid) max_kid = kt.id[i];
        valid_kind_flat.assign(max_kid + 1, 0);
        int64_t nkind = 0;
        for (size_t i = 0; i < kt.id.size(); ++i) {
            if (kind_str_set.count(kt.kind[i])) {
                valid_kind_flat[kt.id[i]] = 1;
                ++nkind;
            }
        }
        TRACE_COUNT("q5a_valid_kind_ids", nkind);
    }

    // INFO1 string set with comma-token reconstruction
    std::unordered_set<std::string> info1_set;
    bool info1_null_ok = false;
    for (const auto& s : args.INFO1) {
        if (is_null(s)) info1_null_ok = true;
        else            info1_set.insert(s);
    }
    static constexpr int MAX_WIN = 8;
    {
        const std::vector<std::string>& tokens = args.INFO1;
        if (!tokens.empty() && !id1_set.empty()) {
            std::unordered_set<std::string> cands;
            int32_t n = (int32_t)tokens.size();
            for (int32_t i = 0; i < n; ++i) {
                if (is_null(tokens[i])) continue;
                for (const char* sep : {", ", ","}) {
                    std::string c = tokens[i];
                    for (int32_t j = i + 1; j < n && j <= i + MAX_WIN - 1; ++j) {
                        if (is_null(tokens[j])) break;
                        c += sep;
                        c += tokens[j];
                        cands.insert(c);
                    }
                }
            }
            if (!cands.empty()) {
                PROFILE_SCOPE("q5a_info1_reconstruct");
                const auto& mi = db->movie_info;
                std::unordered_set<int32_t> cand_intern_ids;
                for (const auto& c : cands) {
                    auto it = mi.info_dict_map.find(c);
                    if (it != mi.info_dict_map.end())
                        cand_intern_ids.insert(it->second);
                }
                int64_t rows_scanned = 0;
                std::unordered_set<int32_t> found_intern_ids;
                if (!cand_intern_ids.empty()) {
                    const int32_t* __restrict__ mi_info_id = mi.info_id.data();
                    for (int32_t itype : id1_set) {
                        int32_t beg = 0, end = (int32_t)mi.movie_id.size();
                        if (itype >= 0 && itype < (int32_t)mi.type_part_start.size()) {
                            beg = mi.type_part_start[itype];
                            end = mi.type_part_end[itype];
                        } else { continue; }
                        for (int32_t r = beg; r < end; ++r) {
                            ++rows_scanned;
                            int32_t iid = mi_info_id[r];
                            if (cand_intern_ids.count(iid)) {
                                found_intern_ids.insert(iid);
                                if (found_intern_ids.size() == cand_intern_ids.size()) {
                                    rows_scanned += (end - r - 1);
                                    goto done_scan;
                                }
                            }
                        }
                    }
                    done_scan:;
                }
                TRACE_COUNT("q5a_info1_reconstruct_rows", rows_scanned);
                if (!found_intern_ids.empty()) {
                    std::unordered_set<std::string> found;
                    for (int32_t iid : found_intern_ids)
                        if (iid >= 0 && iid < (int32_t)mi.info_dict_vec.size())
                            found.insert(mi.info_dict_vec[iid]);
                    if (!found.empty()) {
                        info1_set.clear();
                        int32_t i = 0;
                        while (i < n) {
                            if (is_null(tokens[i])) { ++i; continue; }
                            bool matched = false;
                            for (int32_t len = std::min((int32_t)MAX_WIN, n - i);
                                 len >= 2 && !matched; --len) {
                                for (const char* sep : {", ", ","}) {
                                    std::string c = tokens[i];
                                    for (int32_t k = 1; k < len; ++k) { c += sep; c += tokens[i + k]; }
                                    if (found.count(c)) {
                                        info1_set.insert(c);
                                        i += len; matched = true; break;
                                    }
                                }
                            }
                            if (!matched) { info1_set.insert(tokens[i]); ++i; }
                        }
                    }
                }
            }
        }
    }

    // max_movie_id from id_to_row size (O(1): id_to_row.size() == max_id + 1)
    const int32_t max_movie_id = (int32_t)db->title.id_to_row.size() - 1;

    // -----------------------------------------------------------------------
    // Build mii1 and mii2.
    // -----------------------------------------------------------------------
    struct Mii1Entry { int32_t movie_id; int32_t count; };
    struct Mii2Entry { int32_t movie_id; int32_t count; };
    std::vector<Mii1Entry> mii1_sorted;
    std::vector<Mii2Entry> mii2_sorted;
    std::vector<int32_t> mii1_movies;
    std::vector<int32_t> mii1_counts;

    if (id2 >= 0 || id3 >= 0) {
        PROFILE_SCOPE("q5a_mii_build");
        const auto& mii = db->movie_info_idx;
        const int32_t* __restrict__ mii_mid = mii.movie_id.data();
        const float*   __restrict__ mii_flt = mii.info_float.data();

        int32_t beg2 = 0, end2 = 0;
        int32_t beg3 = 0, end3 = 0;
        if (id2 >= 0 && id2 < (int32_t)mii.type_part_start.size()) {
            beg2 = mii.type_part_start[id2]; end2 = mii.type_part_end[id2];
        }
        if (id3 >= 0 && id3 < (int32_t)mii.type_part_start.size()) {
            beg3 = mii.type_part_start[id3]; end3 = mii.type_part_end[id3];
        }

        // ── Combined parallel build of mii1 + mii2 in ONE parallel_for ──
        //
        // Split the movie_id space [0, max_movie_id] evenly across threads.
        // Each thread independently:
        //   1. Binary-searches the mii1 partition for its movie_id range start.
        //   2. Linearly scans that range, collecting qualifying (movie_id, count)
        //      entries into thread_mii1[tid] (already sorted by movie_id).
        //   3. For each qualifying mii1 movie_id, binary-searches the mii2
        //      partition (also sorted by movie_id) using a monotonic lo_hint.
        //
        // Since each thread owns a disjoint movie_id range, thread_mii1[tid]
        // and thread_mii2[tid] are globally sorted → final concat = O(total).
        // ONE parallel_for call = ONE thread wakeup.

        const int32_t n_threads = pool.num_threads;

        // ── Phase 1: Parallel mii1 scan ───────────────────────────────────
        // Split [beg2, end2) by row range, aligned to movie_id boundaries.
        std::vector<int32_t> t1_beg(n_threads, 0), t1_end(n_threads, 0);
        if (id2 >= 0 && end2 > beg2) {
            const int32_t total1 = end2 - beg2;
            const int32_t rpt1   = std::max(1, (total1 + n_threads - 1) / n_threads);
            for (int32_t t = 0; t < n_threads; ++t) {
                int32_t rb = beg2 + t * rpt1;
                if (rb > end2) rb = end2;
                if (t > 0 && rb < end2 && rb > beg2) {
                    int32_t pm = mii_mid[rb - 1];
                    while (rb < end2 && mii_mid[rb] == pm) ++rb;
                }
                t1_beg[t] = rb;
            }
            for (int32_t t = 0; t < n_threads - 1; ++t) t1_end[t] = t1_beg[t + 1];
            t1_end[n_threads - 1] = end2;
        }

        std::vector<std::vector<Mii1Entry>> thread_mii1(n_threads);
        std::vector<int64_t> thr_r1s(n_threads,0), thr_r1e(n_threads,0);

        pool.parallel_for([&](int tid, int /*n_thr*/) {
            PROFILE_SCOPE("q5a_mii1_parallel_scan");
            if (id2 < 0) return;
            const int32_t my_beg = t1_beg[tid];
            const int32_t my_end = t1_end[tid];
            auto& local1 = thread_mii1[tid];
            local1.reserve(std::max(4, (my_end - my_beg) / 2));
            int64_t r_sc = 0, r_em = 0;
            int32_t prev_mid = -1, cur_cnt = 0;
            for (int32_t r = my_beg; r < my_end; ++r) {
                ++r_sc;
                float fv = mii_flt[r];
                if (std::isnan(fv) || fv < info4 || fv > info5) continue;
                int32_t mid = mii_mid[r];
                ++r_em;
                if (mid == prev_mid) { ++cur_cnt; }
                else {
                    if (prev_mid >= 0) local1.push_back({prev_mid, cur_cnt});
                    prev_mid = mid; cur_cnt = 1;
                }
            }
            if (prev_mid >= 0) local1.push_back({prev_mid, cur_cnt});
            thr_r1s[tid] = r_sc; thr_r1e[tid] = r_em;
        });

        int64_t rows1_scanned = 0, rows1_emitted = 0;
        for (int32_t t = 0; t < n_threads; ++t) {
            rows1_scanned += thr_r1s[t]; rows1_emitted += thr_r1e[t];
        }

        // Merge mii1 (disjoint ranges → concat, fold boundary entries)
        if (id2 >= 0) {
            PROFILE_SCOPE("q5a_mii1_merge");
            size_t total = 0;
            for (int32_t t = 0; t < n_threads; ++t) total += thread_mii1[t].size();
            mii1_sorted.reserve(total);
            for (int32_t t = 0; t < n_threads; ++t) {
                for (auto& e : thread_mii1[t]) {
                    if (!mii1_sorted.empty() && mii1_sorted.back().movie_id == e.movie_id)
                        mii1_sorted.back().count += e.count;
                    else
                        mii1_sorted.push_back(e);
                }
            }
            mii1_movies.reserve(mii1_sorted.size());
            mii1_counts.reserve(mii1_sorted.size());
            for (const auto& e : mii1_sorted) {
                mii1_movies.push_back(e.movie_id);
                mii1_counts.push_back(e.count);
            }
        }

        // ── Phase 2: Parallel mii2 binary search ─────────────────────────
        // Split mii1_movies across threads; each searches the sorted mii2
        // partition monotonically. One parallel_for = one wakeup.
        int64_t rows2_scanned = 0, rows2_emitted = 0;
        if (id3 >= 0) {
            const int32_t nm1 = (int32_t)mii1_movies.size();
            std::vector<std::vector<Mii2Entry>> thread_mii2(n_threads);
            std::vector<int64_t> thr_r2s(n_threads,0), thr_r2e(n_threads,0);

            pool.parallel_for([&](int tid, int n_thr) {
                PROFILE_SCOPE("q5a_mii2_parallel_search");
                auto& local = thread_mii2[tid];
                local.reserve(std::max(4, nm1 / n_thr + 1));
                int64_t r2_sc = 0, r2_em = 0;

                const int32_t m_beg = (int32_t)((int64_t)tid * nm1 / n_thr);
                const int32_t m_end = (int32_t)((int64_t)(tid + 1) * nm1 / n_thr);

                // Find starting position in mii2 partition for this slice
                int32_t lo_hint = beg3;
                if (m_beg > 0 && m_beg < nm1) {
                    int32_t start_mid = mii1_movies[m_beg];
                    int32_t lo2 = beg3, hi2 = end3;
                    while (lo2 < hi2) {
                        int32_t mid2 = (lo2 + hi2) >> 1;
                        if (mii_mid[mid2] < start_mid) lo2 = mid2 + 1;
                        else hi2 = mid2;
                    }
                    lo_hint = lo2;
                }

                for (int32_t i = m_beg; i < m_end; ++i) {
                    int32_t mid = mii1_movies[i];
                    int32_t lo = lo_hint, hi = end3;
                    while (lo < hi) {
                        int32_t m = (lo + hi) >> 1;
                        if (mii_mid[m] < mid) lo = m + 1;
                        else hi = m;
                    }
                    if (lo >= end3 || mii_mid[lo] != mid) continue;
                    lo_hint = lo;
                    int32_t cnt = 0;
                    for (int32_t r = lo; r < end3 && mii_mid[r] == mid; ++r) {
                        ++r2_sc;
                        float fv = mii_flt[r];
                        if (!std::isnan(fv) && fv >= info3 && fv <= info2) {
                            ++cnt; ++r2_em;
                        }
                    }
                    if (cnt > 0) local.push_back({mid, cnt});
                }
                thr_r2s[tid] = r2_sc; thr_r2e[tid] = r2_em;
            });

            for (int32_t t = 0; t < n_threads; ++t) {
                rows2_scanned += thr_r2s[t]; rows2_emitted += thr_r2e[t];
            }
            // Concatenate in order (each thread owns contiguous slice of mii1_movies)
            {
                PROFILE_SCOPE("q5a_mii2_merge");
                size_t total2 = 0;
                for (int32_t t = 0; t < n_threads; ++t) total2 += thread_mii2[t].size();
                mii2_sorted.reserve(total2);
                for (int32_t t = 0; t < n_threads; ++t)
                    for (auto& e : thread_mii2[t])
                        mii2_sorted.push_back(e);
            }
        }

        TRACE_COUNT("q5a_mii1_rows_scanned", rows1_scanned);
        TRACE_COUNT("q5a_mii1_rows_emitted", rows1_emitted);
        TRACE_COUNT("q5a_mii2_rows_scanned", rows2_scanned);
        TRACE_COUNT("q5a_mii2_rows_emitted", rows2_emitted);
        TRACE_COUNT("q5a_mii1_movies",       (int64_t)mii1_movies.size());
    }

    // Build intern-ID flat for info1 values
    std::vector<uint8_t> info1_intern_flat;
    bool use_intern = false;
    {
        PROFILE_SCOPE("q5a_intern_flat_build");
        const auto& mi = db->movie_info;
        if (!info1_set.empty() && !mi.info_dict_map.empty()) {
            int32_t max_iid = (int32_t)mi.info_dict_vec.size();
            info1_intern_flat.assign(max_iid, 0);
            for (const auto& s : info1_set) {
                auto it = mi.info_dict_map.find(s);
                if (it != mi.info_dict_map.end()) {
                    int32_t iid = it->second;
                    if (iid >= 0 && iid < max_iid) info1_intern_flat[iid] = 1;
                }
            }
            use_intern = true;
        }
    }

    // -----------------------------------------------------------------------
    // Pass 1: merge-join mii1_movies with mii2_sorted, probe mk CSR and title.
    // -----------------------------------------------------------------------
    struct CandMovie { int32_t movie_id; int64_t base_count; };
    std::vector<CandMovie> candidates;
    candidates.reserve(4096);

    {
        PROFILE_SCOPE("q5a_title_scan_pass1");
        int64_t rows_scanned = 0, rows_emitted = 0;
        const auto& t = db->title;
        const auto& mk_csr = db->movie_keyword.movie_id_csr;

        if (id2 >= 0 && !mii1_movies.empty()) {
            const int32_t* __restrict__ mc1         = mii1_counts.data();
            const int32_t* __restrict__ mk_off      = mk_csr.offsets.data();
            const int32_t mk_off_size               = (int32_t)mk_csr.offsets.size();
            const int32_t id2row_size               = (int32_t)t.id_to_row.size();
            const int32_t* __restrict__ id2row      = t.id_to_row.data();
            const int32_t* __restrict__ t_kind      = t.kind_id.data();
            const int32_t* __restrict__ t_year      = t.production_year.data();
            const int32_t vkf_sz                    = (int32_t)valid_kind_flat.size();
            const uint8_t* __restrict__ vkf         = valid_kind_flat.data();

#define PROBE_TITLE(mid_, base_) do { \
                if ((mid_) + 1 < mk_off_size) { \
                    int32_t mk_cnt_ = mk_off[(mid_)+1] - mk_off[(mid_)]; \
                    if (mk_cnt_ > 0 && (mid_) < id2row_size) { \
                        int32_t tr_ = id2row[(mid_)]; \
                        if (tr_ >= 0) { \
                            int32_t kid_ = t_kind[tr_]; \
                            bool kt_ok_ = (kid_ < 0) ? kind_null_ok : \
                                         ((uint32_t)kid_ < (uint32_t)vkf_sz && vkf[kid_]); \
                            if (kt_ok_) { \
                                int32_t py_ = t_year[tr_]; \
                                if (py_ != -1 && \
                                    (year1 < 0 || py_ <= year1) && \
                                    (year2 < 0 || py_ >= year2)) { \
                                    candidates.push_back({(mid_), (base_) * mk_cnt_}); \
                                    ++rows_emitted; \
                                } \
                            } \
                        } \
                    } \
                } \
            } while(0)

            if (id3 >= 0 && !mii2_sorted.empty()) {
                int32_t j = 0;
                const int32_t n2 = (int32_t)mii2_sorted.size();
                const Mii2Entry* __restrict__ m2 = mii2_sorted.data();
                const int32_t nm1 = (int32_t)mii1_movies.size();
                for (int32_t i1 = 0; i1 < nm1; ++i1) {
                    int32_t mid = mii1_movies[i1];
                    int32_t mm1 = mc1[i1];
                    ++rows_scanned;
                    while (j < n2 && m2[j].movie_id < mid) ++j;
                    if (j >= n2 || m2[j].movie_id != mid) continue;
                    PROBE_TITLE(mid, (int64_t)mm1 * m2[j].count);
                }
            } else if (id3 < 0) {
                const int32_t nm1 = (int32_t)mii1_movies.size();
                for (int32_t i1 = 0; i1 < nm1; ++i1) {
                    int32_t mid = mii1_movies[i1];
                    int32_t mm1 = mc1[i1];
                    ++rows_scanned;
                    PROBE_TITLE(mid, (int64_t)mm1);
                }
            }
#undef PROBE_TITLE
        } else {
            // Fallback: full title scan
            for (int32_t r = 0; r < (int32_t)t.id.size(); ++r) {
                ++rows_scanned;
                int32_t kid = t.kind_id[r];
                bool kt_ok = (kid < 0) ? kind_null_ok :
                             (kid < (int32_t)valid_kind_flat.size() && valid_kind_flat[kid]);
                if (!kt_ok) continue;
                int32_t py = t.production_year[r];
                if (py == -1) continue;
                if (year1 >= 0 && py > year1) continue;
                if (year2 >= 0 && py < year2) continue;
                int32_t mid = t.id[r];
                if (mid < 0 || mid > max_movie_id) continue;
                int32_t mm1 = 1;
                if (id2 >= 0) {
                    auto it = std::lower_bound(mii1_sorted.begin(), mii1_sorted.end(), mid,
                        [](const Mii1Entry& e, int32_t v){ return e.movie_id < v; });
                    mm1 = (it != mii1_sorted.end() && it->movie_id == mid) ? it->count : 0;
                }
                if (mm1 == 0) continue;
                int32_t mm2 = 1;
                if (id3 >= 0) {
                    if (mii2_sorted.empty()) { mm2 = 0; }
                    else {
                        auto it = std::lower_bound(mii2_sorted.begin(), mii2_sorted.end(), mid,
                            [](const Mii2Entry& e, int32_t v){ return e.movie_id < v; });
                        mm2 = (it != mii2_sorted.end() && it->movie_id == mid) ? it->count : 0;
                    }
                }
                if (mm2 == 0) continue;
                int32_t mk_cnt = 0;
                if (mid + 1 < (int32_t)mk_csr.offsets.size())
                    mk_cnt = mk_csr.offsets[mid + 1] - mk_csr.offsets[mid];
                if (mk_cnt == 0) continue;
                candidates.push_back({mid, (int64_t)mm1 * mm2 * mk_cnt});
                ++rows_emitted;
            }
        }
        TRACE_COUNT("q5a_title_rows_scanned", rows_scanned);
        TRACE_COUNT("q5a_title_rows_emitted", rows_emitted);
        TRACE_COUNT("q5a_title_candidates",   (int64_t)candidates.size());
    }

    // -----------------------------------------------------------------------
    // Pass 2: inverted-index based mi1 probe.
    // -----------------------------------------------------------------------
    int64_t count = 0;
    if (!id1_set.empty() && !candidates.empty()) {
        PROFILE_SCOPE("q5a_mi1_probe");
        int64_t rows_scanned = 0, rows_emitted = 0;
        const auto& mi  = db->movie_info;
        bool used_inv = false;

        if (use_intern && !mi.type_iid_keys.empty() && !info1_intern_flat.empty()) {
            const int32_t iif_sz = (int32_t)info1_intern_flat.size();
            const uint8_t* __restrict__ iif = info1_intern_flat.data();
            const int32_t nc = (int32_t)candidates.size();
            std::vector<int32_t> mi1_cnts(nc, 0);

            for (int32_t itype : id1_set) {
                if (itype < 0 || itype >= (int32_t)mi.type_iid_keys.size()) continue;
                const auto& keys    = mi.type_iid_keys[itype];
                const auto& offsets = mi.type_iid_offsets[itype];
                const auto& mids_v  = mi.type_iid_rows[itype];
                if (keys.empty()) continue;
                const int32_t* __restrict__ mids_ptr = mids_v.data();
                const int32_t n_keys = (int32_t)keys.size();

                for (int32_t ki = 0; ki < n_keys; ++ki) {
                    int32_t iid = keys[ki];
                    if ((uint32_t)iid >= (uint32_t)iif_sz || !iif[iid]) continue;
                    int32_t off_beg = offsets[ki];
                    int32_t off_end = offsets[ki + 1];
                    int32_t list_start = off_beg;
                    for (int32_t ci = 0; ci < nc; ++ci) {
                        int32_t cand_mid = candidates[ci].movie_id;
                        int32_t lo = list_start, hi = off_end;
                        while (lo < hi) {
                            int32_t m2 = (lo + hi) >> 1;
                            if (mids_ptr[m2] < cand_mid) lo = m2 + 1;
                            else hi = m2;
                        }
                        list_start = lo;
                        ++rows_scanned;
                        if (lo < off_end && mids_ptr[lo] == cand_mid) {
                            int32_t cnt = 0;
                            while (lo < off_end && mids_ptr[lo] == cand_mid) {
                                ++lo; ++rows_scanned; ++cnt;
                            }
                            list_start = lo;
                            mi1_cnts[ci] += cnt;
                        }
                    }
                }
            }
            for (int32_t i = 0; i < nc; ++i) {
                if (mi1_cnts[i] == 0) continue;
                count += candidates[i].base_count * mi1_cnts[i];
                ++rows_emitted;
            }
            used_inv = true;
        }

        if (!used_inv) {
            const auto& csr = mi.movie_id_csr;
            const int32_t* __restrict__ mi_itype   = mi.info_type_id.data();
            const int32_t* __restrict__ csr_values = csr.values.data();
            const int32_t id1f_sz                  = (int32_t)id1_flat.size();
            const uint8_t* __restrict__ id1f       = id1_flat.data();
            if (use_intern) {
                const int32_t* __restrict__ mi_iid = mi.info_id.data();
                const int32_t iif_sz               = (int32_t)info1_intern_flat.size();
                const uint8_t* __restrict__ iif    = info1_intern_flat.data();
                for (const auto& cand : candidates) {
                    auto [beg, end] = csr.range(cand.movie_id);
                    int32_t mi1_cnt = 0;
                    for (int32_t ci = beg; ci < end; ++ci) {
                        int32_t r = csr_values[ci];
                        ++rows_scanned;
                        int32_t itype = mi_itype[r];
                        if ((uint32_t)itype >= (uint32_t)id1f_sz || !id1f[itype]) continue;
                        int32_t iid = mi_iid[r];
                        if ((uint32_t)iid >= (uint32_t)iif_sz || !iif[iid]) continue;
                        ++mi1_cnt;
                    }
                    if (mi1_cnt == 0) continue;
                    count += cand.base_count * mi1_cnt;
                    ++rows_emitted;
                }
            } else {
                for (const auto& cand : candidates) {
                    auto [beg, end] = csr.range(cand.movie_id);
                    int32_t mi1_cnt = 0;
                    for (int32_t ci = beg; ci < end; ++ci) {
                        int32_t r = csr_values[ci];
                        ++rows_scanned;
                        int32_t itype = mi_itype[r];
                        if ((uint32_t)itype >= (uint32_t)id1f_sz || !id1f[itype]) continue;
                        const std::string& inf = mi.info_str[r];
                        bool ok = inf.empty() ? info1_null_ok : info1_set.count(inf) > 0;
                        if (!ok) continue;
                        ++mi1_cnt;
                    }
                    if (mi1_cnt == 0) continue;
                    count += cand.base_count * mi1_cnt;
                    ++rows_emitted;
                }
            }
        }

        TRACE_COUNT("q5a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q5a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q5a_mi1_groups",       rows_emitted);
    }

    TRACE_COUNT("q5a_info1_set_size",    (int64_t)info1_set.size());
    TRACE_COUNT("q5a_query_output_rows", 1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
