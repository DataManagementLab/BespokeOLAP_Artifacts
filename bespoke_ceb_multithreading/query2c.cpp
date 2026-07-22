#include "query2c.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include <mutex>
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <unordered_map>
#include <vector>
#include <cstring>
#include <climits>

// SQL:
/**
SELECT COUNT(*) FROM title as t,
kind_type as kt,
info_type as it1,
movie_info as mi1,
movie_info as mi2,
info_type as it2,
cast_info as ci,
role_type as rt,
name as n
WHERE
t.id = ci.movie_id
AND t.id = mi1.movie_id
AND t.id = mi2.movie_id
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
AND (t.title IN TITLE)
*/

std::vector<std::vector<std::string>> run_q2c(Database* db, const Q2cArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q2c: db is null");
    }
    PROFILE_SCOPE("q2c_total");

    // --- Parse year bounds ---
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty()) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty()) year2 = std::stoi(args.YEAR2);

    // --- Resolve info_type_id sets ---
    auto parse_id_set = [](const std::vector<std::string>& sv) {
        std::unordered_set<int32_t> s;
        for (const auto& v : sv)
            s.insert(v == "<<NULL>>" ? -1 : std::stoi(v));
        return s;
    };
    auto id1_set_uset = parse_id_set(args.ID1);
    auto id2_set_uset = parse_id_set(args.ID2);
    std::vector<int32_t> id1_vec, id2_vec;
    for (int32_t v : id1_set_uset) if (v >= 0) id1_vec.push_back(v);
    for (int32_t v : id2_set_uset) if (v >= 0) id2_vec.push_back(v);
    std::sort(id1_vec.begin(), id1_vec.end());
    std::sort(id2_vec.begin(), id2_vec.end());
    const auto& id1_set = id1_set_uset;
    const auto& id2_set = id2_set_uset;

    // --- Valid role_ids ---
    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_role_ids;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i])) valid_role_ids.insert(rt.id[i]);
        if (role_null_ok) valid_role_ids.insert(-1);
    }

    // --- Valid kind_ids ---
    std::unordered_set<std::string> kind_set(args.KIND.begin(), args.KIND.end());
    bool kind_null_ok = kind_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_kind_ids;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_set.count(kt.kind[i])) valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
    }
    // Flat boolean array for O(1) kind_id lookup
    int32_t max_kind_id = 0;
    for (int32_t kid : valid_kind_ids) if (kid > max_kind_id) max_kind_id = kid;
    std::vector<uint8_t> kind_ok_arr(max_kind_id + 2, 0);
    for (int32_t kid : valid_kind_ids) if (kid >= 0) kind_ok_arr[kid] = 1;
    const uint8_t* __restrict__ kind_ok = kind_ok_arr.data();
    const int32_t kind_ok_sz = (int32_t)kind_ok_arr.size();

    // --- Gender filter ---
    std::unordered_set<std::string> gender_set(args.GENDER.begin(), args.GENDER.end());
    bool gender_null_ok = gender_set.count("<<NULL>>") > 0;
    gender_set.erase("NULL");

    uint8_t gender_byte_ok[256] = {};
    bool gender_empty_ok = gender_null_ok;
    for (const auto& gs : gender_set) {
        if (!gs.empty()) gender_byte_ok[(uint8_t)gs[0]] = 1;
    }

    // --- Title filter ---
    std::unordered_set<std::string> title_set(args.TITLE.begin(), args.TITLE.end());
    bool title_null_ok = title_set.count("<<NULL>>") > 0;
    title_set.erase("<<NULL>>");

    // --- INFO1/INFO2 reconstruction (comma-split tokens) ---
    static constexpr int MAX_WIN = 8;

    std::unordered_set<std::string> info1_set(args.INFO1.begin(), args.INFO1.end());
    bool info1_null_ok = info1_set.count("<<NULL>>") > 0;
    std::unordered_set<std::string> info2_set(args.INFO2.begin(), args.INFO2.end());
    bool info2_null_ok = info2_set.count("<<NULL>>") > 0;

    const auto& mi = db->movie_info;
    auto type_range = [&](int32_t type_id) -> std::pair<int32_t,int32_t> {
        if (type_id < 0 || type_id >= (int32_t)mi.type_part_start.size()) return {0,0};
        return {mi.type_part_start[type_id], mi.type_part_end[type_id]};
    };

    auto apply_reconstruction_partitioned = [&](
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

        const int32_t loc_dict_sz = (int32_t)mi.info_dict_vec.size();
        std::unordered_set<int32_t> cand_ids;
        cand_ids.reserve(cands.size());
        for (const auto& c : cands) {
            auto it = mi.info_dict_map.find(c);
            if (it != mi.info_dict_map.end()) cand_ids.insert(it->second);
        }

        std::unordered_set<std::string> found;
        if (!cand_ids.empty()) {
            const int32_t* __restrict__ loc_info_id = mi.info_id.data();
            for (int32_t tid : type_ids) {
                if (tid == -1) continue;
                auto [beg, end] = type_range(tid);
                for (int32_t r = beg; r < end; ++r) {
                    int32_t iid = loc_info_id[r];
                    if (iid >= 0 && iid < loc_dict_sz && cand_ids.count(iid)) {
                        found.insert(mi.info_dict_vec[iid]);
                    }
                }
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
                    for (int32_t k = 1; k < len; ++k) {
                        c += sep; c += tokens[i + k];
                    }
                    if (found.count(c)) {
                        s.insert(c);
                        i += len;
                        matched = true;
                        break;
                    }
                }
            }
            if (!matched) {
                s.insert(tokens[i]);
                ++i;
            }
        }
    };

    {
        PROFILE_SCOPE("q2c_info_reconstruction");
        apply_reconstruction_partitioned(info1_set, args.INFO1, id1_set);
        apply_reconstruction_partitioned(info2_set, args.INFO2, id2_set);
        TRACE_COUNT("q2c_info1_values", (int64_t)info1_set.size());
        TRACE_COUNT("q2c_info2_values", (int64_t)info2_set.size());
    }

    // --- Convert info string sets -> interned IDs ---
    const int32_t dict_sz = (int32_t)mi.info_dict_vec.size();
    std::vector<int32_t> info1_iids, info2_iids;
    {
        PROFILE_SCOPE("q2c_info_intern_lookup");
        const auto& dict_map = mi.info_dict_map;
        for (const auto& s : info1_set) {
            auto it = dict_map.find(s);
            if (it != dict_map.end() && it->second >= 0 && it->second < dict_sz)
                info1_iids.push_back(it->second);
        }
        for (const auto& s : info2_set) {
            auto it = dict_map.find(s);
            if (it != dict_map.end() && it->second >= 0 && it->second < dict_sz)
                info2_iids.push_back(it->second);
        }
    }

    // -----------------------------------------------------------------------
    // STRATEGY: Title-first execution order.
    //
    // 1. Scan title table first using kind-partition index to restrict rows
    //    → produces a tiny set of valid movie_ids (typically ~7).
    // 2. For each valid movie_id, probe movie_info inverted index to count
    //    matching mi1/mi2 rows — O(log N + k) per movie_id.
    // 3. Probe cast_info for surviving movies.
    //
    // Avoids scanning 3.4M movie_info rows; title scan is the only large scan.
    // -----------------------------------------------------------------------

    // PHASE 1: Title scan with kind-partition + year-range binary search optimization
    std::vector<int32_t> title_mids; // movie_ids passing all title filters
    title_mids.reserve(64);

    {
        PROFILE_SCOPE("q2c_title_scan");
        const auto& t = db->title;
        const int32_t* __restrict__ t_id        = t.id.data();
        const int32_t* __restrict__ t_kind_id   = t.kind_id.data();
        const int32_t* __restrict__ t_prod_year = t.production_year.data();
        const int32_t n_title_rows = (int32_t)t.id.size();
        int64_t rows_scanned_title = 0, rows_emitted = 0;

        // Use kind partition index to skip irrelevant kind partitions.
        // kind_part_start/end are indexed by kind_id.
        bool use_kind_partition = !kind_null_ok && !valid_kind_ids.empty()
            && !t.kind_part_start.empty();

        struct Range { int32_t beg, end; };
        std::vector<Range> ranges;
        if (use_kind_partition) {
            int32_t kps_sz = (int32_t)t.kind_part_start.size();
            for (int32_t kid : valid_kind_ids) {
                if (kid >= 0 && kid < kps_sz) {
                    int32_t b = t.kind_part_start[(size_t)kid];
                    int32_t e = t.kind_part_end[(size_t)kid];
                    if (b >= e) continue;

                    // -------------------------------------------------------
                    // Year-range binary search optimization:
                    // Within each kind partition, rows are sorted by
                    // production_year (ascending, with -1/null first).
                    // Binary search narrows the range to [year2, year1]
                    // without scanning O(N) rows in the partition.
                    // This reduces scanned rows significantly when year
                    // constraints are tight (e.g., a 5-year window in a
                    // 100-year span reduces rows by ~95%).
                    // -------------------------------------------------------
                    if (year2 > 0) {
                        // Find first row with production_year >= year2.
                        // Null years (-1 < year2) are naturally skipped.
                        int32_t lo = b, hi = e;
                        while (lo < hi) {
                            int32_t mid = lo + (hi - lo) / 2;
                            if (t_prod_year[mid] < year2) lo = mid + 1;
                            else hi = mid;
                        }
                        b = lo;
                    }
                    if (year1 > 0 && b < e) {
                        // Find first row with production_year > year1.
                        int32_t lo = b, hi = e;
                        while (lo < hi) {
                            int32_t mid = lo + (hi - lo) / 2;
                            if (t_prod_year[mid] <= year1) lo = mid + 1;
                            else hi = mid;
                        }
                        e = lo;
                    }
                    if (b < e) ranges.push_back({b, e});
                }
            }
        } else {
            ranges.push_back({0, n_title_rows});
        }

        const int n_threads = pool.num_threads;

        // Per-thread local accumulators to avoid synchronization in the hot loop.
        // Cache-line aligned to prevent false sharing between threads.
        struct alignas(64) PadVec {
            std::vector<int32_t> mids;
            int64_t scanned = 0;
            int64_t emitted = 0;
        };
        std::vector<PadVec> local(n_threads);

        // Pre-compute title string lengths for fast pre-filtering.
        // Titles in the set have specific lengths; most DB titles have different lengths.
        // Length pre-check avoids hashing strings that can't possibly match.
        size_t title_min_len = SIZE_MAX, title_max_len = 0;
        for (const auto& ts2 : title_set) {
            size_t l = ts2.size();
            if (l < title_min_len) title_min_len = l;
            if (l > title_max_len) title_max_len = l;
        }
        if (title_set.empty()) { title_min_len = 0; title_max_len = 0; }

        // Split each range's rows evenly across threads (contiguous chunks).
        pool.parallel_for([&](int tid, int nt) {
            auto& lc = local[tid];
            lc.mids.reserve(8);

            for (auto [beg, end] : ranges) {
                int32_t len = end - beg;
                // Distribute rows: thread tid gets [chunk_beg, chunk_end)
                int32_t chunk_beg = beg + (int32_t)((int64_t)len * tid / nt);
                int32_t chunk_end = beg + (int32_t)((int64_t)len * (tid + 1) / nt);

                for (int32_t r = chunk_beg; r < chunk_end; ++r) {
                    ++lc.scanned;

                    // Kind check: only needed when not using kind partition
                    if (!use_kind_partition) {
                        int32_t kid = t_kind_id[r];
                        bool kf = (kid == -1) ? kind_null_ok
                                              : (kid < kind_ok_sz && kind_ok[kid]);
                        if (!kf) continue;
                        // Full year check needed when no kind partition
                        int32_t py = t_prod_year[r];
                        if (py == -1) continue;
                        if (year1 >= 0 && py > year1) continue;
                        if (year2 >= 0 && py < year2) continue;
                    } else {
                        // With kind partition + year binary search, range is
                        // already narrowed. Null years (py == -1) may still
                        // appear when year2 == 0 (no lower bound), skip them.
                        int32_t py = t_prod_year[r];
                        if (py < 0) continue;  // null year always excluded
                    }

                    const std::string& ts = t.title_str[r];
                    if (ts.empty()) {
                        if (!title_null_ok) continue;
                    } else {
                        // Fast length pre-filter: skip strings with wrong length.
                        // This avoids the hash computation for titles of wrong length.
                        size_t tl = ts.size();
                        if (tl < title_min_len || tl > title_max_len) continue;
                        // Hash set lookup for exact match.
                        if (!title_set.count(ts)) continue;
                    }

                    lc.mids.push_back(t_id[r]);
                    ++lc.emitted;
                }
            }
        });

        // Merge per-thread results
        for (int t2 = 0; t2 < n_threads; ++t2) {
            rows_scanned_title += local[t2].scanned;
            rows_emitted       += local[t2].emitted;
            title_mids.insert(title_mids.end(),
                              local[t2].mids.begin(), local[t2].mids.end());
        }

        TRACE_COUNT("q2c_title_rows_scanned", rows_scanned_title);
        TRACE_COUNT("q2c_title_rows_emitted", rows_emitted);
    }

    // -----------------------------------------------------------------------
    // PHASE 2: For each title-valid movie_id, count mi1/mi2 matching rows
    // using inverted index (binary search in sorted movie_id sublists).
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int64_t> valid_movies;
    valid_movies.reserve(title_mids.size() * 2 + 1);
    {
        PROFILE_SCOPE("q2c_movie_info_scan");
        int64_t rows_scanned = 0, mi1_emitted = 0, mi2_emitted = 0;

        for (int32_t movie_id : title_mids) {
            // Count mi1 matching rows for this movie_id via inverted index
            int32_t cnt1 = 0;
            for (int32_t tid : id1_vec) {
                if (tid >= (int32_t)mi.type_iid_keys.size()) continue;
                const auto& keys    = mi.type_iid_keys[(size_t)tid];
                const auto& offsets = mi.type_iid_offsets[(size_t)tid];
                const auto& rows    = mi.type_iid_rows[(size_t)tid];
                for (int32_t iid : info1_iids) {
                    // Binary search for intern ID in keys
                    auto kit = std::lower_bound(keys.begin(), keys.end(), iid);
                    if (kit == keys.end() || *kit != iid) continue;
                    size_t kidx = (size_t)(kit - keys.begin());
                    int32_t beg = offsets[kidx], end2 = offsets[kidx + 1];
                    // Binary search for movie_id in sorted sublist
                    const int32_t* lo = rows.data() + beg;
                    const int32_t* hi = rows.data() + end2;
                    lo = std::lower_bound(lo, hi, movie_id);
                    // Count consecutive occurrences of movie_id (multiple mi rows)
                    while (lo != hi && *lo == movie_id) { ++cnt1; ++lo; ++rows_scanned; }
                }
                // Handle null info_ok: scan partition binary-search style
                if (info1_null_ok) {
                    const int32_t* __restrict__ mi_movie_id = mi.movie_id.data();
                    const int32_t* __restrict__ mi_info_id2 = mi.info_id.data();
                    auto [beg, end2] = type_range(tid);
                    const int32_t* lo2 = mi_movie_id + beg;
                    const int32_t* hi2 = mi_movie_id + end2;
                    lo2 = std::lower_bound(lo2, hi2, movie_id);
                    while (lo2 != hi2 && *lo2 == movie_id) {
                        int32_t r = (int32_t)(lo2 - mi_movie_id);
                        if (mi_info_id2[r] < 0) ++cnt1;
                        ++lo2; ++rows_scanned;
                    }
                }
            }
            if (cnt1 == 0) continue;
            mi1_emitted += cnt1;

            // Count mi2 matching rows for this movie_id
            int32_t cnt2 = 0;
            for (int32_t tid : id2_vec) {
                if (tid >= (int32_t)mi.type_iid_keys.size()) continue;
                const auto& keys    = mi.type_iid_keys[(size_t)tid];
                const auto& offsets = mi.type_iid_offsets[(size_t)tid];
                const auto& rows    = mi.type_iid_rows[(size_t)tid];
                for (int32_t iid : info2_iids) {
                    auto kit = std::lower_bound(keys.begin(), keys.end(), iid);
                    if (kit == keys.end() || *kit != iid) continue;
                    size_t kidx = (size_t)(kit - keys.begin());
                    int32_t beg = offsets[kidx], end2 = offsets[kidx + 1];
                    const int32_t* lo = rows.data() + beg;
                    const int32_t* hi = rows.data() + end2;
                    lo = std::lower_bound(lo, hi, movie_id);
                    while (lo != hi && *lo == movie_id) { ++cnt2; ++lo; ++rows_scanned; }
                }
                if (info2_null_ok) {
                    const int32_t* __restrict__ mi_movie_id = mi.movie_id.data();
                    const int32_t* __restrict__ mi_info_id2 = mi.info_id.data();
                    auto [beg, end2] = type_range(tid);
                    const int32_t* lo2 = mi_movie_id + beg;
                    const int32_t* hi2 = mi_movie_id + end2;
                    lo2 = std::lower_bound(lo2, hi2, movie_id);
                    while (lo2 != hi2 && *lo2 == movie_id) {
                        int32_t r = (int32_t)(lo2 - mi_movie_id);
                        if (mi_info_id2[r] < 0) ++cnt2;
                        ++lo2; ++rows_scanned;
                    }
                }
            }
            if (cnt2 == 0) continue;
            mi2_emitted += cnt2;

            valid_movies[movie_id] = (int64_t)cnt1 * cnt2;
        }
        TRACE_COUNT("q2c_movie_info_rows_scanned", rows_scanned);
        TRACE_COUNT("q2c_mi1_rows_emitted", mi1_emitted);
        TRACE_COUNT("q2c_mi2_rows_emitted", mi2_emitted);
    }

    // -----------------------------------------------------------------------
    // PHASE 3: Probe cast_info for each valid movie.
    // -----------------------------------------------------------------------
    int64_t count = 0;
    {
        PROFILE_SCOPE("q2c_cast_info_probe");
        int64_t probe_rows_in = 0, join_rows_emitted = 0;
        const auto& ci  = db->cast_info;
        const auto& nm  = db->name;
        const auto& csr = ci.movie_id_csr;
        const uint8_t* __restrict__ nm_gender_byte = nm.gender_byte.data();
        const int32_t* __restrict__ nm_id_to_row   = nm.id_to_row.data();
        const int32_t nm_id_sz = (int32_t)nm.id_to_row.size();
        const int32_t* __restrict__ ci_role_id   = ci.role_id.data();
        const int32_t* __restrict__ ci_person_id = ci.person_id.data();

        for (const auto& [mid, mi_mult] : valid_movies) {
            auto [beg, end] = csr.range(mid);
            for (int32_t r = beg; r < end; ++r) {
                ++probe_rows_in;
                if (!valid_role_ids.count(ci_role_id[r])) continue;
                int32_t pid = ci_person_id[r];
                if (pid < 0 || pid >= nm_id_sz) continue;
                int32_t nrow = nm_id_to_row[pid];
                if (nrow < 0) continue;
                uint8_t gb = nm_gender_byte[nrow];
                if (gb == 0) {
                    if (!gender_empty_ok) continue;
                } else {
                    if (!gender_byte_ok[gb]) continue;
                }
                count += mi_mult;
                ++join_rows_emitted;
            }
        }
        TRACE_COUNT("q2c_cast_probe_rows_in", probe_rows_in);
        TRACE_COUNT("q2c_join_rows_emitted", join_rows_emitted);
    }

    TRACE_COUNT("q2c_valid_movies", (int64_t)valid_movies.size());
    TRACE_COUNT("q2c_query_output_rows", 1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
