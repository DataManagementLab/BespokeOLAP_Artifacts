#include "query1a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <sys/mman.h>
#include <unordered_map>
#include <unordered_set>
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
AND (t.production_year >= YEAR2) */

std::vector<std::vector<std::string>> run_q1a(Database* db, const Q1aArgs& args) {
    if (!db) throw std::runtime_error("run_q1a: db is null");

    PROFILE_SCOPE("q1a_total");
    // Parse year bounds
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty()) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty()) year2 = std::stoi(args.YEAR2);

    // info_type_id sets (IDs as numeric strings; <<NULL>> -> -1)
    auto parse_id_set = [](const std::vector<std::string>& sv) {
        std::unordered_set<int32_t> s;
        for (const auto& v : sv)
            s.insert(v == "<<NULL>>" ? -1 : std::stoi(v));
        return s;
    };
    auto id1_set = parse_id_set(args.ID1);
    auto id2_set = parse_id_set(args.ID2);

    // Valid role_ids
    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_role_ids;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i])) valid_role_ids.insert(rt.id[i]);
        if (role_null_ok) valid_role_ids.insert(-1);
    }

    // Valid kind_ids
    std::unordered_set<std::string> kind_set(args.KIND.begin(), args.KIND.end());
    bool kind_null_ok = kind_set.count("<<NULL>>") > 0;
    std::unordered_set<int32_t> valid_kind_ids;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_set.count(kt.kind[i])) valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
    }

    // Gender filter
    std::unordered_set<std::string> gender_set(args.GENDER.begin(), args.GENDER.end());
    bool gender_null_ok = gender_set.count("<<NULL>>") > 0;

    // -----------------------------------------------------------------------
    // INFO1 / INFO2 reconstruction helpers
    // -----------------------------------------------------------------------

    auto fp_of = [](const std::string& cv) -> std::string {
        for (const char* sep : {", ", ","}) {
            size_t f = cv.find(sep);
            if (f != std::string::npos) return cv.substr(0, f);
        }
        return cv;
    };
    auto lp_of = [](const std::string& cv) -> std::string {
        size_t f = cv.rfind(", ");
        if (f != std::string::npos) return cv.substr(f + 2);
        f = cv.rfind(",");
        if (f != std::string::npos) return cv.substr(f + 1);
        return cv;
    };

    auto get_parts = [](const std::string& cv,
                        const std::unordered_set<std::string>& orig_s)
                     -> std::vector<std::string> {
        for (const std::string sep : {std::string(", "), std::string(",")}) {
            std::vector<std::string> parts;
            size_t start = 0;
            while (true) {
                size_t f = cv.find(sep, start);
                if (f == std::string::npos) { parts.push_back(cv.substr(start)); break; }
                parts.push_back(cv.substr(start, f - start));
                start = f + sep.size();
            }
            if (parts.size() <= 1) continue;
            bool ok = true;
            for (const auto& p : parts) if (!orig_s.count(p)) { ok = false; break; }
            if (ok) return parts;
        }
        return {};
    };

    auto is_sub_of = [](const std::string& cv, const std::string& other) -> bool {
        if (other.size() <= cv.size()) return false;
        if (other.compare(0, cv.size(), cv) == 0 && other[cv.size()] == ',') return true;
        for (const char* sep : {",", ", "}) {
            std::string needle = std::string(sep) + cv;
            if (other.find(needle) != std::string::npos) return true;
        }
        return false;
    };

    auto finish_reconstruction = [&fp_of, &lp_of, &get_parts, &is_sub_of](
        std::unordered_set<std::string>& s,
        const std::unordered_set<std::string>& found)
        -> std::unordered_set<std::string>
    {
        if (found.empty()) return {};
        const std::unordered_set<std::string> orig_s = s;

        std::unordered_set<std::string> maximal;
        for (const auto& cv : found) {
            bool sub = false;
            for (const auto& other : found)
                if (is_sub_of(cv, other)) { sub = true; break; }
            if (!sub) maximal.insert(cv);
        }

        std::unordered_set<std::string> first_parts, last_parts;
        for (const auto& cv : found) {
            first_parts.insert(fp_of(cv));
            last_parts.insert(lp_of(cv));
        }
        std::unordered_set<std::string> boundary;
        for (const auto& t : last_parts)
            if (first_parts.count(t)) boundary.insert(t);

        std::unordered_set<std::string> trusted;
        for (const auto& cv : maximal) {
            if (!boundary.count(fp_of(cv)) && !boundary.count(lp_of(cv)))
                trusted.insert(cv);
        }
        if (trusted.empty()) return {};

        std::unordered_set<std::string> to_remove;
        for (const auto& cv : trusted) {
            auto parts = get_parts(cv, orig_s);
            for (int i = 0; i < (int)parts.size(); ++i) {
                if (i > 0) {
                    to_remove.insert(parts[i]);
                } else {
                    for (const char* sep : {", ", ","}) {
                        std::string pfx = parts[0] + sep;
                        for (const auto& tc : trusted) {
                            if (tc.compare(0, pfx.size(), pfx) == 0) {
                                to_remove.insert(parts[0]); break;
                            }
                        }
                        if (to_remove.count(parts[0])) break;
                    }
                }
            }
        }
        for (const auto& p : to_remove) s.erase(p);
        std::unordered_set<std::string> newly_added;
        for (const auto& cv : trusted) {
            if (!s.count(cv)) newly_added.insert(cv);
            s.insert(cv);
        }
        return newly_added;
    };

    auto gen_candidates = [](const std::vector<std::string>& tokens)
        -> std::unordered_set<std::string>
    {
        std::unordered_set<std::string> cands;
        int32_t n = (int32_t)tokens.size();
        for (int32_t i = 0; i < n; ++i) {
            for (const char* sep : {", ", ","}) {
                std::string c = tokens[i];
                for (int32_t j = i+1; j < n && j <= i+5; ++j) {
                    c += sep; c += tokens[j]; cands.insert(c);
                }
            }
        }
        return cands;
    };

    std::unordered_set<std::string> info1_set(args.INFO1.begin(), args.INFO1.end());
    bool info1_null_ok = info1_set.count("<<NULL>>") > 0;
    std::unordered_set<std::string> info2_set(args.INFO2.begin(), args.INFO2.end());
    bool info2_null_ok = info2_set.count("<<NULL>>") > 0;
    auto cands1 = gen_candidates(args.INFO1);
    auto cands2 = gen_candidates(args.INFO2);

    // Scratch arrays: mi1_arr[movie_id] counts info1 matches per movie;
    //                 mi2_arr[movie_id] counts info2 matches per movie.
    // Allocated via mmap(MAP_NORESERVE) so pages are NOT faulted at mmap time.
    // A parallel_for at the start of each call faults pages in parallel (first
    // call) or clears them fast (subsequent calls, pages already hot).
    // -----------------------------------------------------------------------
    const int32_t max_movie_id = (int32_t)db->title.id_to_row.size();

    static int32_t* mi1_arr_raw = nullptr;
    static int32_t* mi2_arr_raw = nullptr;
    static int32_t  mi_arr_cap  = 0;
    if (max_movie_id > mi_arr_cap) {
        if (mi1_arr_raw) munmap(mi1_arr_raw, (size_t)mi_arr_cap * sizeof(int32_t));
        if (mi2_arr_raw) munmap(mi2_arr_raw, (size_t)mi_arr_cap * sizeof(int32_t));
        size_t sz = (size_t)max_movie_id * sizeof(int32_t);
        mi1_arr_raw = (int32_t*)mmap(nullptr, sz, PROT_READ|PROT_WRITE,
                                     MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
        mi2_arr_raw = (int32_t*)mmap(nullptr, sz, PROT_READ|PROT_WRITE,
                                     MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
        mi_arr_cap = max_movie_id;
    }
    int32_t* __restrict__ mi1_arr = mi1_arr_raw;
    int32_t* __restrict__ mi2_arr = mi2_arr_raw;

    // Parallel zero-fill: faults pages on first call (parallel page-faults);
    // fast sequential memset on subsequent calls (pages warm).
    {
        PROFILE_SCOPE("q1a_arr_init");
        pool.parallel_for([&](int tid, int nt) {
            int64_t chunk = ((int64_t)max_movie_id + nt - 1) / nt;
            int64_t beg   = (int64_t)tid * chunk;
            int64_t end   = std::min(beg + chunk, (int64_t)max_movie_id);
            if (beg >= end) return;
            std::memset(mi1_arr + beg, 0, (size_t)(end - beg) * sizeof(int32_t));
            std::memset(mi2_arr + beg, 0, (size_t)(end - beg) * sizeof(int32_t));
        });
    }

    std::vector<int32_t> mi1_movies;  // distinct movie_ids with mi1 hits
    mi1_movies.reserve(65536);
    // For mi2, we track distinct count via atomic and skip building the full list
    // (mi2 is almost always larger than mi1, so we never iterate it).
    std::atomic<int64_t> mi2_distinct_count{0};

    // -----------------------------------------------------------------------
    // movie_info access using per-type inverted index.
    // -----------------------------------------------------------------------
    const auto& mi = db->movie_info;
    const int32_t dict_size = (int32_t)mi.info_dict_vec.size();
    const int32_t max_type  = (int32_t)mi.type_part_start.size();

    TRACE_COUNT("q1a_dict_size", (int64_t)dict_size);

    const auto& dict_map = mi.info_dict_map;

    // Resolve a set of strings to their intern IDs
    auto resolve_ids = [&](const std::unordered_set<std::string>& sset) {
        std::vector<int32_t> result;
        result.reserve(sset.size());
        for (const auto& s : sset) {
            auto it = dict_map.find(s);
            if (it != dict_map.end()) result.push_back(it->second);
        }
        return result;
    };

    // Look up row range for intern ID `iid` in type `tp`.
    auto lookup_iid_rows = [&](int32_t tp, int32_t iid,
                                const int32_t*& rows_ptr, int32_t& rows_count) -> bool {
        if (tp < 0 || tp >= (int32_t)mi.type_iid_keys.size()) return false;
        const auto& keys = mi.type_iid_keys[(size_t)tp];
        if (keys.empty()) return false;
        auto it = std::lower_bound(keys.begin(), keys.end(), iid);
        if (it == keys.end() || *it != iid) return false;
        int32_t local_idx = (int32_t)(it - keys.begin());
        const auto& offsets = mi.type_iid_offsets[(size_t)tp];
        const auto& rows    = mi.type_iid_rows[(size_t)tp];
        rows_ptr   = rows.data() + offsets[(size_t)local_idx];
        rows_count = offsets[(size_t)(local_idx + 1)] - offsets[(size_t)local_idx];
        return rows_count > 0;
    };

    // -----------------------------------------------------------------------
    // Phase 1: Candidate scan for reconstruction.
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> found1_str, found2_str;

    {
        PROFILE_SCOPE("q1a_info_reconstruction");
        {
            std::vector<int32_t> cand1_iids = resolve_ids(cands1);
            std::vector<int32_t> cand2_iids = resolve_ids(cands2);

            for (int32_t tid : id1_set) {
                if (tid < 0 || tid >= max_type) continue;
                for (int32_t iid : cand1_iids) {
                    const int32_t* rows_ptr; int32_t rows_count;
                    if (lookup_iid_rows(tid, iid, rows_ptr, rows_count))
                        found1_str.insert(mi.info_dict_vec[iid]);
                }
            }
            for (int32_t tid : id2_set) {
                if (tid < 0 || tid >= max_type) continue;
                for (int32_t iid : cand2_iids) {
                    const int32_t* rows_ptr; int32_t rows_count;
                    if (lookup_iid_rows(tid, iid, rows_ptr, rows_count))
                        found2_str.insert(mi.info_dict_vec[iid]);
                }
            }

            auto newly_added1 = finish_reconstruction(info1_set, found1_str);
            auto newly_added2 = finish_reconstruction(info2_set, found2_str);

            TRACE_COUNT("q1a_info1_values", (int64_t)info1_set.size());
            TRACE_COUNT("q1a_info2_values", (int64_t)info2_set.size());
        }

        // Phase 2: Count rows for FINAL info1_set/info2_set (post-reconstruction).
        // mi1 scan is sequential (small); mi2 scan is parallelised (large).
        // Parallel mi2 scan partitions by movie_id range — sorted rows_ptr means
        // each movie_id belongs to exactly one thread's range, no data races.
        {
            PROFILE_SCOPE("q1a_movie_info_scan");
            int64_t rows_scanned = 0, mi1_emitted = 0, mi2_emitted = 0;

            std::vector<int32_t> info1_iids = resolve_ids(info1_set);
            std::vector<int32_t> info2_iids = resolve_ids(info2_set);

            // ---- INFO1 (small scan, single-threaded) ----
            for (int32_t type_id : id1_set) {
                if (type_id < 0 || type_id >= max_type) continue;
                for (int32_t iid : info1_iids) {
                    const int32_t* rows_ptr; int32_t rows_count;
                    if (!lookup_iid_rows(type_id, iid, rows_ptr, rows_count)) continue;
                    rows_scanned += rows_count;
                    for (int32_t i = 0; i < rows_count; ++i) {
                        int32_t movie = rows_ptr[i];
                        if ((uint32_t)movie < (uint32_t)max_movie_id) {
                            if (mi1_arr[movie] == 0) mi1_movies.push_back(movie);
                            ++mi1_arr[movie];
                            ++mi1_emitted;
                        }
                    }
                }
            }

            // ---- INFO2 (large scan, parallelised by movie_id range) ----
            // Collect all row spans for info2.
            struct RowSpan { const int32_t* ptr; int32_t count; };
            std::vector<RowSpan> info2_spans;
            int64_t info2_total_rows = 0;
            for (int32_t type_id : id2_set) {
                if (type_id < 0 || type_id >= max_type) continue;
                for (int32_t iid : info2_iids) {
                    const int32_t* rp; int32_t rc;
                    if (lookup_iid_rows(type_id, iid, rp, rc)) {
                        info2_spans.push_back({rp, rc});
                        info2_total_rows += rc;
                    }
                }
            }
            rows_scanned += info2_total_rows;

            // Parallel scan: thread t owns movie_ids in [mid_lo, mid_hi).
            // Within each sorted span, use lower_bound to find the thread's slice.
            // No movie_id straddles two threads since partitioning is by movie_id value.
            const int n_tp = pool.num_threads;
            std::vector<int64_t> thr_mi2_emitted(n_tp, 0);
            std::vector<int64_t> thr_mi2_distinct(n_tp, 0);

            pool.parallel_for([&](int tid_p, int nt_p) {
                int64_t mid_chunk = ((int64_t)max_movie_id + nt_p - 1) / nt_p;
                int32_t mid_lo = (int32_t)((int64_t)tid_p * mid_chunk);
                int32_t mid_hi = (int32_t)std::min((int64_t)mid_lo + mid_chunk,
                                                   (int64_t)max_movie_id);

                int64_t local_emitted = 0, local_distinct = 0;
                for (const auto& span : info2_spans) {
                    const int32_t* beg = std::lower_bound(span.ptr, span.ptr + span.count, mid_lo);
                    const int32_t* end = std::lower_bound(beg,      span.ptr + span.count, mid_hi);
                    for (const int32_t* p = beg; p != end; ++p) {
                        int32_t movie = *p;
                        if (mi2_arr[movie] == 0) ++local_distinct;
                        ++mi2_arr[movie];
                        ++local_emitted;
                    }
                }
                thr_mi2_emitted[tid_p]  = local_emitted;
                thr_mi2_distinct[tid_p] = local_distinct;
            });

            for (int t = 0; t < n_tp; ++t) {
                mi2_emitted += thr_mi2_emitted[t];
                mi2_distinct_count.fetch_add(thr_mi2_distinct[t],
                                             std::memory_order_relaxed);
            }

            TRACE_COUNT("q1a_movie_info_rows_scanned", rows_scanned);
            TRACE_COUNT("q1a_mi1_rows_emitted", mi1_emitted);
            TRACE_COUNT("q1a_mi2_rows_emitted", mi2_emitted);
        }
    }

    // -----------------------------------------------------------------------
    // Build valid_movies via flat array intersection + title filter.
    // mi1_movies is typically much smaller than mi2 distinct count; use it
    // as the probe side. mi2_arr[mid] acts as the existence/count lookup.
    // -----------------------------------------------------------------------
    std::vector<int32_t> vm_movie_id;
    std::vector<int64_t> vm_mi_mult;
    vm_movie_id.reserve(4096);
    vm_mi_mult.reserve(4096);

    {
        PROFILE_SCOPE("q1a_title_scan");
        int64_t rows_emitted = 0;
        const auto& t = db->title;

        // mi1_movies is the small list; mi2_arr[mid] checked for existence.
        const int32_t* __restrict__ id_to_row = t.id_to_row.data();
        const int32_t* __restrict__ kind_id   = t.kind_id.data();
        const int32_t* __restrict__ prod_year = t.production_year.data();
        TRACE_COUNT("q1a_title_rows_scanned", (int64_t)mi1_movies.size());
        for (int32_t mid : mi1_movies) {
            int32_t mi2_cnt = mi2_arr[mid];
            if (mi2_cnt == 0) continue;
            int32_t row = id_to_row[mid];
            if (row < 0) continue;
            int32_t kid = kind_id[row];
            if ((kid == -1) ? !kind_null_ok : !valid_kind_ids.count(kid)) continue;
            int32_t py = prod_year[row];
            if (py == -1) continue;
            if (year1 >= 0 && py > year1) continue;
            if (year2 >= 0 && py < year2) continue;
            int32_t mi1_cnt = mi1_arr[mid];
            vm_movie_id.push_back(mid);
            vm_mi_mult.push_back((int64_t)mi1_cnt * mi2_cnt);
            ++rows_emitted;
        }
        TRACE_COUNT("q1a_title_rows_emitted", rows_emitted);
    }

    // -----------------------------------------------------------------------
    // Pre-build flat role_id validity array
    // -----------------------------------------------------------------------
    int32_t max_role_id = 0;
    for (int32_t rid : db->role_type.id)
        if (rid > max_role_id) max_role_id = rid;
    std::vector<bool> role_valid(max_role_id + 2, false);
    for (int32_t rid : valid_role_ids)
        if (rid >= 0 && rid <= max_role_id) role_valid[rid] = true;
    bool role_neg1_ok = valid_role_ids.count(-1) > 0;

    const auto& nm = db->name;
    const int32_t pid_range = (int32_t)nm.id_to_row.size();

    // Build gender-ok lookup. Used inline during cast_info probe.
    uint8_t gender_ok_char[256] = {};
    bool gender_empty_ok = gender_null_ok || gender_set.count("") > 0;
    for (const auto& g : gender_set) {
        if (g.empty()) { gender_empty_ok = true; continue; }
        if (g == "<<NULL>>") continue;
        if (g.size() == 1) gender_ok_char[(uint8_t)g[0]] = 1;
    }
    if (gender_empty_ok) gender_ok_char[0] = 1;

    const int32_t* __restrict__ nm_id_to_row = nm.id_to_row.data();
    const int32_t  nm_row_count              = (int32_t)nm.gender_byte.size();
    const uint8_t* __restrict__ nm_gb        = nm.gender_byte.data();

    auto person_gender_ok = [&](int32_t pid) -> bool {
        if ((uint32_t)pid >= (uint32_t)pid_range) return false;
        int32_t row = nm_id_to_row[pid];
        if ((uint32_t)row >= (uint32_t)nm_row_count) return false;
        return gender_ok_char[nm_gb[row]] != 0;
    };

    TRACE_COUNT("q1a_valid_person_build", 0); // no-op: inline check replaces precompute

    // Scan cast_info: sum up (role + gender matching) * mi_mult
    int64_t count = 0;
    {
        PROFILE_SCOPE("q1a_cast_info_probe");
        int64_t probe_rows_in = 0, join_rows_emitted = 0;
        const auto& ci  = db->cast_info;
        const auto& csr = ci.movie_id_csr;
        const int32_t vm_size = (int32_t)vm_movie_id.size();
        const int32_t* __restrict__ ci_role_id   = ci.role_id.data();
        const int32_t* __restrict__ ci_person_id = ci.person_id.data();
        for (int32_t vi = 0; vi < vm_size; ++vi) {
            int32_t mid = vm_movie_id[vi];
            int64_t mi_mult = vm_mi_mult[vi];
            auto [beg, end] = csr.range(mid);
            probe_rows_in += (end - beg);
            for (int32_t r = beg; r < end; ++r) {
                int32_t rid = ci_role_id[r];
                bool role_ok;
                if (rid < 0) role_ok = role_neg1_ok;
                else role_ok = (rid <= max_role_id) && role_valid[rid];
                if (!role_ok) continue;
                int32_t pid = ci_person_id[r];
                if (!person_gender_ok(pid)) continue;
                count += mi_mult;
                ++join_rows_emitted;
            }
        }
        TRACE_COUNT("q1a_cast_probe_rows_in", probe_rows_in);
        TRACE_COUNT("q1a_join_rows_emitted", join_rows_emitted);
    }

    TRACE_COUNT("q1a_valid_movies", (int64_t)vm_movie_id.size());
    TRACE_COUNT("q1a_query_output_rows", 1);
    TRACE_COUNT("q1a_mi1_movies_size", (int64_t)mi1_movies.size());
    TRACE_COUNT("q1a_mi2_distinct",    mi2_distinct_count.load(std::memory_order_relaxed));

    // mi1_arr/mi2_arr are zeroed at the START of next call (parallel fill),
    // so no cleanup needed here.

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}