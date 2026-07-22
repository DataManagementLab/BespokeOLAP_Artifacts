#include "query11a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& thread_pool = get_query_pool();

#include <queue>
#include <algorithm>
#include <climits>
#include <cstdint>
#include <cctype>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <atomic>

// SQL:
/**
SELECT n.gender, rt.role, cn.name, COUNT(*)
FROM title as t,
movie_companies as mc,
company_name as cn,
company_type as ct,
kind_type as kt,
cast_info as ci,
name as n,
role_type as rt,
movie_info as mi1,
info_type as it
WHERE t.id = mc.movie_id
AND t.id = ci.movie_id
AND t.id = mi1.movie_id
AND mi1.movie_id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND kt.id = t.kind_id
AND ci.person_id = n.id
AND ci.role_id = rt.id
AND mi1.info_type_id = it.id
AND (kt.kind ILIKE KIND)
AND (rt.role IN ROLE)
AND (t.production_year <= YEAR1)
AND (t.production_year >= YEAR2)
AND (it.id IN ID)
AND (mi1.info ILIKE INFO)
AND (cn.name ILIKE NAME)
GROUP BY n.gender, rt.role, cn.name
ORDER BY COUNT(*) DESC
*/

// Fast case-insensitive ilike: both text and pattern must be pre-lowercased.
static bool ilike_lc_impl(const char* tp, size_t tl, const char* pp, size_t pl) {
    bool all_pct = true;
    for (size_t i = 0; i < pl; ++i)
        if (pp[i] != '%') { all_pct = false; break; }
    if (all_pct) return true;

    const char* te = tp + tl;
    const char* pe = pp + pl;
    bool lw = false;

    while (pp <= pe) {
        const char* ss = pp, *pc = pp;
        while (pc < pe && *pc != '%') ++pc;
        size_t sl = (size_t)(pc - ss);

        if (!sl) {
            if (pc < pe) { lw = true; pp = pc + 1; continue; }
            return (lw || tp == te);
        }
        if (!lw) {
            if ((size_t)(te - tp) < sl) return false;
            if (std::memcmp(tp, ss, sl) != 0) return false;
            tp += sl;
        } else {
            bool f = false;
            while ((size_t)(te - tp) >= sl) {
                if (std::memcmp(tp, ss, sl) == 0) { tp += sl; f = true; break; }
                ++tp;
            }
            if (!f) return false;
        }
        pp = pc;
        if (pp < pe && *pp == '%') { lw = true; ++pp; }
        else { lw = false; if (pp == pe) return (tp == te); }
    }
    return (lw || tp == te);
}

// ilike match that lowercases text on the fly using a caller-supplied buffer.
static inline bool ilike_noalloc_buf(const char* tp, size_t tl,
                                     const char* pp, size_t pl,
                                     char* buf) {
    for (size_t i = 0; i < tl; ++i)
        buf[i] = (char)std::tolower((unsigned char)tp[i]);
    return ilike_lc_impl(buf, tl, pp, pl);
}

// ---------------------------------------------------------------------------
// Pattern classification: returns the simplest form of a pre-lowercased ilike
// pattern. Handles single-literal patterns; complex patterns fall back to
// the generic engine.
// ---------------------------------------------------------------------------
enum class IlikeKind { FULL_WILDCARD, CONTAINS, PREFIX, SUFFIX, EXACT, COMPLEX };

struct IlikeMode {
    IlikeKind kind;
    size_t    frag_offset;
    size_t    frag_len;
};

static IlikeMode classify_ilike(const char* pp, size_t pl) {
    const char* pe = pp + pl;
    const char* p = pp;
    bool leading_pct = false, trailing_pct = false;

    while (p < pe && *p == '%') { leading_pct = true; ++p; }
    if (p == pe) return {IlikeKind::FULL_WILDCARD, 0, 0};

    const char* frag_start = p;
    while (p < pe && *p != '%') ++p;
    size_t frag_len = (size_t)(p - frag_start);

    while (p < pe && *p == '%') { trailing_pct = true; ++p; }
    if (p < pe) return {IlikeKind::COMPLEX, 0, 0};

    size_t frag_off = (size_t)(frag_start - pp);
    if (leading_pct && trailing_pct) return {IlikeKind::CONTAINS, frag_off, frag_len};
    if (leading_pct)                 return {IlikeKind::SUFFIX,   frag_off, frag_len};
    if (trailing_pct)                return {IlikeKind::PREFIX,   frag_off, frag_len};
    return {IlikeKind::EXACT, frag_off, frag_len};
}

// Fast case-insensitive CONTAINS: scan text for lc_needle (pre-lowercased).
// Avoids full-string lowercase buffer allocation.
static inline bool ci_contains(const char* __restrict__ tp, size_t tl,
                                const char* __restrict__ needle, size_t nl) {
    if (nl == 0) return true;
    if (tl < nl) return false;
    const size_t limit = tl - nl;
    const char first = needle[0];
    for (size_t i = 0; i <= limit; ++i) {
        if ((char)std::tolower((unsigned char)tp[i]) != first) continue;
        bool ok = true;
        for (size_t j = 1; j < nl; ++j) {
            if ((char)std::tolower((unsigned char)tp[i + j]) != needle[j]) {
                ok = false; break;
            }
        }
        if (ok) return true;
    }
    return false;
}

std::vector<std::vector<std::string>> run_q11a(Database* db, const Q11aArgs& args) {
    if (!db) throw std::runtime_error("run_q11a: db is null");

    PROFILE_SCOPE("q11a_total");

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };
    auto strip_sq = [](const std::string& s) -> std::string {
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
            return s.substr(1, s.size() - 2);
        return s;
    };
    auto to_lower = [](const std::string& s) -> std::string {
        std::string r; r.reserve(s.size());
        for (unsigned char c : s) r += (char)std::tolower(c);
        return r;
    };

    const std::string lc_kind_pat = to_lower(strip_sq(args.KIND));
    const std::string lc_info_pat = to_lower(strip_sq(args.INFO));
    const std::string lc_name_pat = to_lower(strip_sq(args.NAME));

    // Reusable lowercase buffer
    std::vector<char> lc_buf;
    lc_buf.reserve(256);
    auto ilike_noalloc = [&](const char* tp, size_t tl, const char* pp, size_t pl) -> bool {
        if (lc_buf.size() < tl) lc_buf.resize(tl + 128);
        return ilike_noalloc_buf(tp, tl, pp, pl, lc_buf.data());
    };

    // Classify patterns for fast-path dispatch
    const IlikeMode name_mode = classify_ilike(lc_name_pat.c_str(), lc_name_pat.size());
    const IlikeMode info_mode = classify_ilike(lc_info_pat.c_str(), lc_info_pat.size());
    const char*  name_frag     = lc_name_pat.c_str() + name_mode.frag_offset;
    const size_t name_frag_len = name_mode.frag_len;
    const char*  info_frag     = lc_info_pat.c_str() + info_mode.frag_offset;
    const size_t info_frag_len = info_mode.frag_len;

    auto name_match = [&](const char* tp, size_t tl) -> bool {
        if (name_mode.kind == IlikeKind::CONTAINS)      return ci_contains(tp, tl, name_frag, name_frag_len);
        if (name_mode.kind == IlikeKind::FULL_WILDCARD)  return true;
        return ilike_noalloc(tp, tl, lc_name_pat.c_str(), lc_name_pat.size());
    };
    auto info_match = [&](const char* tp, size_t tl) -> bool {
        if (info_mode.kind == IlikeKind::CONTAINS)      return ci_contains(tp, tl, info_frag, info_frag_len);
        if (info_mode.kind == IlikeKind::FULL_WILDCARD)  return true;
        return ilike_noalloc(tp, tl, lc_info_pat.c_str(), lc_info_pat.size());
    };

    int32_t year1 = std::stoi(args.YEAR1);
    int32_t year2 = std::stoi(args.YEAR2);

    // -----------------------------------------------------------------------
    // 1. kind_type filter: build flat ok-array for O(1) lookup.
    // -----------------------------------------------------------------------
    int32_t max_kind_id = 0;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kt.id[i] > max_kind_id) max_kind_id = kt.id[i];
    }
    std::vector<uint8_t> kind_id_ok(max_kind_id + 2, 0);
    int64_t n_valid_kind_ids = 0;
    {
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i) {
            const std::string lc_kind = to_lower(kt.kind[i]);
            if (ilike_lc_impl(lc_kind.c_str(), lc_kind.size(),
                              lc_kind_pat.c_str(), lc_kind_pat.size())) {
                if (kt.id[i] >= 0 && kt.id[i] <= max_kind_id)
                    kind_id_ok[kt.id[i]] = 1;
                ++n_valid_kind_ids;
            }
        }
    }
    TRACE_COUNT("q11a_valid_kind_ids", n_valid_kind_ids);

    // -----------------------------------------------------------------------
    // 2. role_type filter: build flat ok-array + id-to-string map.
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, std::string> role_id_to_str;
    int32_t max_role_id = 0;
    {
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (rt.id[i] > max_role_id) max_role_id = rt.id[i];
    }
    std::vector<uint8_t> role_id_ok(max_role_id + 2, 0);
    {
        std::unordered_set<std::string> role_str_set;
        for (const auto& s : args.ROLE)
            if (!is_null(s)) role_str_set.insert(s);
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i) {
            if (role_str_set.count(rt.role[i])) {
                if (rt.id[i] >= 0 && rt.id[i] <= max_role_id)
                    role_id_ok[rt.id[i]] = 1;
                role_id_to_str[rt.id[i]] = rt.role[i];
            }
        }
    }
    TRACE_COUNT("q11a_valid_role_ids", (int64_t)role_id_to_str.size());

    // -----------------------------------------------------------------------
    // 3. info_type filter.
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_it_ids;
    for (const auto& s : args.ID)
        if (!is_null(s)) valid_it_ids.insert(std::stoi(s));
    TRACE_COUNT("q11a_valid_it_ids", (int64_t)valid_it_ids.size());

    if (role_id_to_str.empty() || n_valid_kind_ids == 0) {
        return {{"gender", "role", "name", "count_star()"}};
    }

    // -----------------------------------------------------------------------
    // 4+5. COMBINED: MI1 inverted index scan + title probe
    //
    //    New pipeline: process MI1 FIRST using the inverted index, then probe
    //    title directly for each candidate movie_id. This avoids the large
    //    sequential title scan (~3.27M rows) entirely.
    //
    //    Steps:
    //    4a. Scan info_dict to find matching intern IDs (2 matches out of 2.7M).
    //    4b. Use type_iid_rows to get sorted movie_id lists for matching interns.
    //    4c. For each candidate movie_id, probe title.id_to_row + kind_id + year.
    //    Result: movie_mi1_count with only title-and-info-filtered movies.
    //
    //    The inverted index gives 126K movie_id candidates (with duplicates from
    //    multiple intern_ids). After title probing, only ~5343 survive.
    //    Cost: 126K random probes into title arrays (vs. 3.27M sequential reads).
    // -----------------------------------------------------------------------
    const int32_t max_title_id = (int32_t)db->title.id_to_row.size() - 1;
    const uint32_t u_max_title = (uint32_t)max_title_id;

    std::unordered_map<int32_t, int32_t> movie_mi1_count;
    movie_mi1_count.reserve(8192);

    std::vector<uint8_t> movie_ok(max_title_id + 2, 0); // built in mi1_row_scan

    int64_t title_rows_scanned = 0;
    {
        PROFILE_SCOPE("q11a_mi1_build");
        const auto& mi   = db->movie_info;
        const int32_t max_type = (int32_t)mi.type_part_start.size() - 1;
        const auto& dict       = mi.info_dict_vec;
        const size_t dict_sz   = dict.size();
        const uint32_t u_max_dict  = dict_sz > 0 ? (uint32_t)(dict_sz - 1) : 0u;

        // 4a: build ok_info_ids from per-type unique-info set.
        std::vector<uint8_t> ok_info_ids(dict_sz, 0);
        int64_t n_dict_matches = 0;
        {
            PROFILE_SCOPE("q11a_mi1_dict_scan");
            for (int32_t it_id : valid_it_ids) {
                if (it_id < 0 || it_id > max_type) continue;
                const auto& uniq = mi.type_unique_info[it_id];
                if (!uniq.empty()) {
                    for (const auto& s : uniq) {
                        if (info_match(s.c_str(), s.size())) {
                            auto it2 = mi.info_dict_map.find(s);
                            if (it2 != mi.info_dict_map.end()) {
                                int32_t iid = it2->second;
                                if ((uint32_t)iid <= u_max_dict) {
                                    ok_info_ids[iid] = 1;
                                    ++n_dict_matches;
                                }
                            }
                        }
                    }
                } else {
                    // Fallback: scan partition rows for unique info strings
                    const int32_t r_beg = mi.type_part_start[it_id];
                    const int32_t r_end = mi.type_part_end[it_id];
                    for (int32_t r = r_beg; r < r_end; ++r) {
                        const int32_t iid = mi.info_id[r];
                        if ((uint32_t)iid > u_max_dict || ok_info_ids[iid]) continue;
                        const std::string& s = dict[iid];
                        if (info_match(s.c_str(), s.size())) {
                            ok_info_ids[iid] = 1;
                            ++n_dict_matches;
                        }
                    }
                }
            }
        }
        TRACE_COUNT("q11a_mi1_dict_total",   (int64_t)dict_sz);
        TRACE_COUNT("q11a_mi1_dict_matches", n_dict_matches);

        // 4b+4c: Use type_iid inverted index to get matching movie_ids, then
        //        probe title directly for kind+year validation.
        //        Avoids scanning 3.27M title rows sequentially.
        int64_t rows_scanned = 0, rows_emitted = 0;
        {
            PROFILE_SCOPE("q11a_mi1_row_scan");
            const int32_t* __restrict__ t_id2row = db->title.id_to_row.data();
            const int32_t* __restrict__ t_kinds  = db->title.kind_id.data();
            const int32_t* __restrict__ t_years  = db->title.production_year.data();
            const uint8_t* __restrict__ kind_ok  = kind_id_ok.data();
            const uint32_t u_max_kind = (uint32_t)max_kind_id;
            const int32_t  t_id2row_sz = (int32_t)db->title.id_to_row.size();

            for (int32_t it_id : valid_it_ids) {
                if (it_id < 0 || it_id > max_type) continue;

                // Check if inverted index is available for this type
                const bool has_inv = (it_id < (int32_t)mi.type_iid_keys.size()) &&
                                     !mi.type_iid_keys[it_id].empty();
                if (has_inv) {
                    // Collect valid intern_id ranges for parallel processing
                    const auto& keys    = mi.type_iid_keys[it_id];
                    const auto& offsets = mi.type_iid_offsets[it_id];
                    const auto& rows_v  = mi.type_iid_rows[it_id];
                    const int32_t* __restrict__ rows_ptr = rows_v.data();
                    const int32_t nkeys = (int32_t)keys.size();

                    // Gather valid intern_id segments
                    struct Segment { int32_t beg; int32_t end; };
                    std::vector<Segment> segments;
                    segments.reserve(16);
                    for (int32_t ki = 0; ki < nkeys; ++ki) {
                        const int32_t iid = keys[ki];
                        if ((uint32_t)iid > u_max_dict || !ok_info_ids[iid]) continue;
                        segments.push_back({offsets[ki], offsets[ki + 1]});
                    }

                    // Sequential scan with hash map
                    uint8_t* __restrict__ mok = movie_ok.data();
                    for (const auto& seg : segments) {
                        for (int32_t mi_r = seg.beg; mi_r < seg.end; ++mi_r) {
                            ++rows_scanned;
                            const int32_t mid = rows_ptr[mi_r];
                            if ((uint32_t)mid >= (uint32_t)t_id2row_sz) continue;
                            const int32_t trow = t_id2row[mid];
                            if (trow < 0) continue;
                            ++title_rows_scanned;
                            const int32_t kid = t_kinds[trow];
                            if ((uint32_t)kid > u_max_kind || !kind_ok[kid]) continue;
                            const int32_t yr = t_years[trow];
                            if (yr < year2 || yr > year1) continue;
                            ++movie_mi1_count[mid];
                            mok[mid] = 1;
                            ++rows_emitted;
                        }
                    }

                } else {
                    // Fallback: sequential scan of the type partition
                    // (also probes title inline)
                    const uint8_t* __restrict__ ok_inf   = ok_info_ids.data();
                    const int32_t* __restrict__ mi_movie = mi.movie_id.data();
                    const int32_t* __restrict__ mi_iid   = mi.info_id.data();
                    const int32_t r_beg = mi.type_part_start[it_id];
                    const int32_t r_end = mi.type_part_end[it_id];
                    uint8_t* __restrict__ mok = movie_ok.data();
                    for (int32_t r = r_beg; r < r_end; ++r) {
                        ++rows_scanned;
                        const int32_t mid = mi_movie[r];
                        if ((uint32_t)mid > u_max_title) continue;
                        const int32_t iid = mi_iid[r];
                        if ((uint32_t)iid > u_max_dict || !ok_inf[iid]) continue;
                        // Probe title
                        const int32_t trow = t_id2row[mid];
                        if (trow < 0) continue;
                        ++title_rows_scanned;
                        const int32_t kid = t_kinds[trow];
                        if ((uint32_t)kid > u_max_kind || !kind_ok[kid]) continue;
                        const int32_t yr = t_years[trow];
                        if (yr < year2 || yr > year1) continue;
                        ++movie_mi1_count[mid];
                        mok[mid] = 1;
                        ++rows_emitted;
                    }
                }
            }
        }
        TRACE_COUNT("q11a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q11a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q11a_mi1_movie_groups", (int64_t)movie_mi1_count.size());
    }
    TRACE_COUNT("q11a_title_rows_scanned", title_rows_scanned);
    // Emit a dummy title_scan profile for continuity (no work done separately)
    {
        PROFILE_SCOPE("q11a_title_scan");
        // Title filtering is now done inline in mi1_row_scan above.
        // This scope is kept for tracing continuity but does no work.
    }

    // -----------------------------------------------------------------------
    // 6+7. Filter company names AND build movie_cid_vec in a single pass over
    //      movie_companies rows for qualifying movies.
    //      - ilike-check company names on first encounter (lazy, O(1) id_to_row)
    //      - simultaneously accumulate (company_id, mc_count) per movie
    //      This is much cheaper than checking all 234K company names upfront.
    // -----------------------------------------------------------------------
    int32_t max_company_id = 0;
    {
        const auto& cn = db->company_name;
        for (size_t i = 0; i < cn.id.size(); ++i)
            if (cn.id[i] > max_company_id) max_company_id = cn.id[i];
    }
    std::vector<uint8_t> company_id_ok(max_company_id + 2, 0);
    std::unordered_map<int32_t, std::string> company_id_to_name;
    company_id_to_name.reserve(256);

    struct CidCount { int32_t company_id; int32_t mc_count; };

    // Build flat vectors for parallel-safe access:
    // mi_movie_keys[i] = movie_id, mc_vecs[i] = CidCount list for that movie
    std::vector<int32_t> mi_movie_keys_mc;
    mi_movie_keys_mc.reserve(movie_mi1_count.size());
    for (const auto& [mid, _] : movie_mi1_count)
        mi_movie_keys_mc.push_back(mid);

    // Build movie_id -> index map for O(1) lookup in cast_info join
    std::vector<int32_t> movie_id_to_mc_idx(
        (size_t)(max_title_id + 2), -1);
    for (size_t i = 0; i < mi_movie_keys_mc.size(); ++i)
        movie_id_to_mc_idx[mi_movie_keys_mc[i]] = (int32_t)i;

    // Parallel-safe: each entry owned by exactly one thread during pass 2
    std::vector<std::vector<CidCount>> mc_vecs(mi_movie_keys_mc.size());

    {
        PROFILE_SCOPE("q11a_company_mc_build");
        const auto& mc = db->movie_companies;
        const auto& cn = db->company_name;
        const uint32_t u_max_cid = (uint32_t)max_company_id;

        // Pass 1: collect unique company_ids seen in qualifying movie MCs
        // (sequential scan, builds company_id set for name matching)
        std::vector<uint8_t> company_seen(max_company_id + 2, 0);
        {
            PROFILE_SCOPE("q11a_mc_pass1_cid_collect");
            // Parallel pass 1: each thread collects unique company_ids from its movie subset
            // Per-thread company_seen to avoid synchronization; merge after.
            const size_t n_mi_pass1 = mi_movie_keys_mc.size();
            std::vector<std::vector<int32_t>> thread_unique_cids(thread_pool.num_threads);
            std::atomic<size_t> next_p1{0};
            constexpr size_t P1_CHUNK = 128;

            thread_pool.parallel_for([&](int tid, int /*n_thr*/) {
                PROFILE_SCOPE("q11a_mc_pass1_thread");
                // Per-thread seen bitmap (re-use main company_seen but with atomics)
                // Simple approach: collect all cids, deduplicate after
                std::vector<uint8_t> local_seen(max_company_id + 2, 0);
                auto& my_cids = thread_unique_cids[tid];
                while (true) {
                    const size_t base = next_p1.fetch_add(P1_CHUNK, std::memory_order_relaxed);
                    if (base >= n_mi_pass1) break;
                    const size_t end_p1 = std::min(base + P1_CHUNK, n_mi_pass1);
                    for (size_t mi = base; mi < end_p1; ++mi) {
                        const int32_t mid = mi_movie_keys_mc[mi];
                        auto [beg, end_r] = mc.movie_id_csr.range(mid);
                        for (int32_t r = beg; r < end_r; ++r) {
                            const int32_t cid  = mc.company_id[r];
                            const int32_t ctid = mc.company_type_id[r];
                            if (ctid == -1) continue;
                            if ((uint32_t)cid > u_max_cid) continue;
                            if (!local_seen[cid]) {
                                local_seen[cid] = 1;
                                my_cids.push_back(cid);
                            }
                        }
                    }
                }
            });

            // Merge unique cids from all threads and do name matching (sequential, fast)
            for (int t = 0; t < thread_pool.num_threads; ++t) {
                for (const int32_t cid : thread_unique_cids[t]) {
                    if (!company_seen[cid]) {
                        company_seen[cid] = 1;
                        const int32_t crow = cn.id_to_row[cid];
                        if (crow >= 0) {
                            const std::string& nm = cn.name_str[crow];
                            if (name_match(nm.c_str(), nm.size())) {
                                company_id_ok[cid] = 1;
                                company_id_to_name[cid] = nm;
                            }
                        }
                    }
                }
            }
        }

        // Pass 2 (parallel): build mc_vecs using pre-computed company_id_ok
        // Each thread processes a disjoint set of movies - thread-safe since
        // mc_vecs entries are indexed by position and distinct movies don't share.
        const size_t n_mi_movies = mi_movie_keys_mc.size();

        std::atomic<int64_t> mc_scanned_total{0}, mc_emitted_total{0};
        const uint8_t* __restrict__ c_ok = company_id_ok.data();
        std::atomic<size_t> next_mc_movie{0};

        thread_pool.parallel_for([&](int /*tid*/, int /*n_thr*/) {
            PROFILE_SCOPE("q11a_mc_pass2_thread");
            int64_t mc_scanned = 0, mc_emitted = 0;
            constexpr size_t MC_CHUNK = 256;
            while (true) {
                const size_t base = next_mc_movie.fetch_add(MC_CHUNK, std::memory_order_relaxed);
                if (base >= n_mi_movies) break;
                const size_t end_chunk = std::min(base + MC_CHUNK, n_mi_movies);
                for (size_t mi = base; mi < end_chunk; ++mi) {
                    const int32_t mid = mi_movie_keys_mc[mi];
                    auto [beg, end_r] = mc.movie_id_csr.range(mid);
                    auto& vec = mc_vecs[mi]; // safe: indexed by position, no sharing
                    for (int32_t r = beg; r < end_r; ++r) {
                        ++mc_scanned;
                        const int32_t cid  = mc.company_id[r];
                        const int32_t ctid = mc.company_type_id[r];
                        if (ctid == -1) continue;
                        if ((uint32_t)cid > u_max_cid || !c_ok[cid]) continue;
                        bool found = false;
                        for (auto& e : vec) {
                            if (e.company_id == cid) { ++e.mc_count; found = true; break; }
                        }
                        if (!found) vec.push_back({cid, 1});
                        ++mc_emitted;
                    }
                }
            }
            mc_scanned_total.fetch_add(mc_scanned, std::memory_order_relaxed);
            mc_emitted_total.fetch_add(mc_emitted, std::memory_order_relaxed);
        });
        int64_t n_mc_movie_groups = 0;
        for (const auto& v : mc_vecs) if (!v.empty()) ++n_mc_movie_groups;
        TRACE_COUNT("q11a_mc_rows_scanned", mc_scanned_total.load());
        TRACE_COUNT("q11a_mc_rows_emitted", mc_emitted_total.load());
        TRACE_COUNT("q11a_mc_movie_groups", n_mc_movie_groups);
    }
    TRACE_COUNT("q11a_valid_company_ids", (int64_t)company_id_to_name.size());

    if (company_id_to_name.empty()) {
        return {{"gender", "role", "name", "count_star()"}};
    }

    // -----------------------------------------------------------------------
    // 8. Cast-info join: for each qualifying movie, look up cast rows and
    //    accumulate GROUP BY (gender, role, company_name) -> count.
    //
    //    Optimization: encode (gender_idx, role_idx, company_idx) as a packed
    //    integer key so per-thread maps use integer keys (cheap hash/compare)
    //    and the merge step is a simple array addition instead of string-key
    //    hash map merge.
    // -----------------------------------------------------------------------

    // Build integer encodings for gender, role, company_name
    // gender: small set (typically 2-3 values: "", "m", "f")
    // role: small set (role_id_to_str.size() values)
    // company: company_id_to_name.size() values

    // Map gender string -> index
    std::unordered_map<std::string, int32_t> gender_to_idx;
    std::vector<std::string> idx_to_gender;
    // Map role_id -> role_idx (compact)
    std::vector<int32_t> role_id_to_idx(max_role_id + 2, -1);
    std::vector<std::string> idx_to_role;
    // Map company_id -> company_idx (compact)
    std::vector<int32_t> cid_to_idx(max_company_id + 2, -1);
    std::vector<std::string> idx_to_company;

    {
        for (const auto& [rid, rname] : role_id_to_str) {
            int32_t idx = (int32_t)idx_to_role.size();
            idx_to_role.push_back(rname);
            if (rid >= 0 && rid <= max_role_id) role_id_to_idx[rid] = idx;
        }
        // Company grouping is by NAME (not id), so deduplicate by name.
        std::unordered_map<std::string, int32_t> cname_to_idx;
        for (const auto& [cid, cname] : company_id_to_name) {
            auto [it, inserted] = cname_to_idx.emplace(cname, (int32_t)idx_to_company.size());
            if (inserted) idx_to_company.push_back(cname);
            int32_t idx = it->second;
            if (cid >= 0 && cid <= max_company_id) cid_to_idx[cid] = idx;
        }
    }

    const int32_t n_roles     = (int32_t)idx_to_role.size();
    const int32_t n_companies = (int32_t)idx_to_company.size();

    // Pre-scan all unique genders from the name table (fast: tiny distinct count).
    // This correctly handles "NaN"/NULL genders loaded as strings.
    {
        const auto& nm_ref = db->name;
        for (size_t i = 0; i < nm_ref.gender.size(); ++i) {
            const std::string& g = nm_ref.gender[i];
            if (gender_to_idx.find(g) == gender_to_idx.end()) {
                int32_t idx = (int32_t)idx_to_gender.size();
                gender_to_idx[g] = idx;
                idx_to_gender.push_back(g);
            }
        }
    }
    const int32_t N_GENDER_PRESEED = (int32_t)idx_to_gender.size();

    // Total number of output groups <= n_gender * n_roles * n_companies
    // Use flat array indexed by (gender_idx * n_roles * n_companies +
    //                            role_idx * n_companies + company_idx)
    // For typical params: 3 * 45 * ~30K = ~4M entries - too large for flat array
    // Use per-thread unordered_map<int64_t, int64_t> (integer keys = fast)
    // Merge is fast: iterate once and add.

    // Pack (gender_idx, role_idx, company_idx) into int64_t
    // role_idx < 64, company_idx < 65536, gender_idx < 8
    // key = gender_idx * (64 * 65536) + role_idx * 65536 + company_idx
    // But we don't know company count upfront; use: key = g * 2^20 + r * 2^16 + c
    // Actually n_companies can be up to ~32K, n_roles up to 12, n_gender = 3
    // key = (int64_t)gender_idx * n_roles * n_companies + role_idx * n_companies + company_idx
    // This gives a dense key in [0, n_gender * n_roles * n_companies)

    // Pre-allocate global counts array (dense, zero-initialized)
    // We discover gender on the fly per-thread; pre-seed 3 genders.
    // Max array size: 3 * n_roles * n_companies (bounded)
    // If a new gender appears, we fall back to extending (rare).
    const int64_t flat_size = (int64_t)N_GENDER_PRESEED * n_roles * n_companies;
    // Per-thread: use unordered_map<int64_t,int64_t> for integer keys
    // Then merge into a flat global array.

    const int32_t max_person_id = (int32_t)db->name.id_to_row.size() - 1;
    const uint32_t u_max_role = (uint32_t)max_role_id;

    // movie_keys for cast_info join: only movies that have valid company entries
    // mi_movie_keys_mc[i] and mc_vecs[i] are already in sync
    // Filter to only movies with non-empty mc_vecs
    std::vector<int32_t> movie_keys;
    std::vector<int32_t> movie_mc_idx; // parallel: movie_keys[i] -> mc_vecs[movie_mc_idx[i]]
    movie_keys.reserve(mi_movie_keys_mc.size());
    movie_mc_idx.reserve(mi_movie_keys_mc.size());
    for (size_t i = 0; i < mi_movie_keys_mc.size(); ++i) {
        if (!mc_vecs[i].empty()) {
            movie_keys.push_back(mi_movie_keys_mc[i]);
            movie_mc_idx.push_back((int32_t)i);
        }
    }

    const int n_threads = thread_pool.num_threads;

    {
        PROFILE_SCOPE("q11a_cast_info_join");
        const auto& ci = db->cast_info;
        const auto& nm = db->name;
        const uint8_t* __restrict__ r_ok = role_id_ok.data();
        const int32_t* __restrict__ ci_role   = ci.role_id.data();
        const int32_t* __restrict__ ci_person = ci.person_id.data();
        const int32_t* __restrict__ ri_to_idx = role_id_to_idx.data();
        const int32_t* __restrict__ ci_to_idx = cid_to_idx.data();

        const size_t n_movies = movie_keys.size();

        // Per-thread local count maps using integer keys (fast hash/compare)
        using CountMap = std::unordered_map<int64_t, int64_t>;
        std::vector<CountMap> local_counts(n_threads);

        // Dynamic work stealing via atomic counter to fix load imbalance
        std::atomic<size_t> next_movie{0};

        std::atomic<int64_t> total_ci_scanned{0}, total_ci_role_pass{0}, total_ci_person_pass{0};

        thread_pool.parallel_for([&](int tid, int n_thr) {
            PROFILE_SCOPE("q11a_cast_info_join_thread");
            int64_t ci_rows_scanned = 0, ci_rows_role_pass = 0, ci_rows_person_pass = 0;

            CountMap& my_counts = local_counts[tid];
            my_counts.reserve(4096);

            // Dynamic work stealing: grab movies in chunks
            constexpr size_t CHUNK_SIZE = 64;
            while (true) {
                const size_t base = next_movie.fetch_add(CHUNK_SIZE, std::memory_order_relaxed);
                if (base >= n_movies) break;
                const size_t end_chunk = std::min(base + CHUNK_SIZE, n_movies);

            for (size_t mi = base; mi < end_chunk; ++mi) {
                const int32_t mid    = movie_keys[mi];
                const auto& mc_vec   = mc_vecs[movie_mc_idx[mi]];
                // mc_vec is guaranteed non-empty (filtered above)
                const int32_t mi1_cnt = movie_mi1_count.at(mid);

                auto [ci_beg, ci_end] = ci.movie_id_csr.range(mid);
                for (int32_t r = ci_beg; r < ci_end; ++r) {
                    ++ci_rows_scanned;
                    const int32_t role_id = ci_role[r];
                    if ((uint32_t)role_id > u_max_role || !r_ok[role_id]) continue;
                    ++ci_rows_role_pass;
                    const int32_t ridx = ri_to_idx[role_id];
                    if (ridx < 0) continue;

                    const int32_t pid = ci_person[r];
                    if (pid < 0 || pid > max_person_id) continue;
                    const int32_t nrow = nm.id_to_row[pid];
                    if (nrow < 0) continue;
                    ++ci_rows_person_pass;

                    // Encode gender as index (pre-seeded for common values)
                    const std::string& gender_str = nm.gender[nrow];
                    int32_t gidx = -1;
                    {
                        auto git = gender_to_idx.find(gender_str);
                        if (git != gender_to_idx.end()) gidx = git->second;
                    }
                    if (gidx < 0) continue; // unknown gender (should not happen after pre-scan)

                    const int64_t role_offset = (int64_t)ridx * n_companies;
                    const int64_t gender_offset = (int64_t)gidx * n_roles * n_companies;
                    const int64_t increment = (int64_t)mi1_cnt;

                    for (const auto& e : mc_vec) {
                        const int32_t cidx = ci_to_idx[e.company_id];
                        if (cidx < 0) continue;
                        const int64_t key = gender_offset + role_offset + cidx;
                        my_counts[key] += increment * e.mc_count;
                    }
                }
            } // end chunk
            } // end while

            total_ci_scanned.fetch_add(ci_rows_scanned, std::memory_order_relaxed);
            total_ci_role_pass.fetch_add(ci_rows_role_pass, std::memory_order_relaxed);
            total_ci_person_pass.fetch_add(ci_rows_person_pass, std::memory_order_relaxed);
        });

        TRACE_COUNT("q11a_ci_rows_scanned",     total_ci_scanned.load());
        TRACE_COUNT("q11a_ci_rows_role_pass",   total_ci_role_pass.load());
        TRACE_COUNT("q11a_ci_rows_person_pass", total_ci_person_pass.load());

        // Merge: accumulate per-thread integer-keyed local maps into a global map.
        // Integer keys are cheap to hash/compare.
        {
            PROFILE_SCOPE("q11a_cast_info_merge");

            // Pre-sorted local maps from parallel sort step
            using KVPair = std::pair<int64_t, int64_t>; // (key, val)
            std::vector<std::vector<KVPair>> sorted_local(n_threads);

            thread_pool.parallel_for([&](int tid, int /*n_thr*/) {
                PROFILE_SCOPE("q11a_cast_info_merge_sort");
                auto& lc = local_counts[tid];
                auto& sv = sorted_local[tid];
                sv.reserve(lc.size());
                for (const auto& [k, v] : lc) sv.push_back({k, v});
                std::sort(sv.begin(), sv.end());
            });

            // Global sort: collect all sorted local vectors, re-sort globally
            std::vector<KVPair> merged;
            {
                PROFILE_SCOPE("q11a_merge_sort_global");
                size_t total = 0;
                for (int t = 0; t < n_threads; ++t) total += sorted_local[t].size();
                merged.reserve(total);
                for (int t = 0; t < n_threads; ++t)
                    for (const auto& kv : sorted_local[t])
                        merged.push_back(kv);
                std::sort(merged.begin(), merged.end());
            }

            // Accumulate consecutive equal keys
            std::vector<KVPair> deduped;
            deduped.reserve(300000);
            for (size_t i = 0; i < merged.size(); ) {
                int64_t key = merged[i].first;
                int64_t sum = 0;
                while (i < merged.size() && merged[i].first == key) {
                    sum += merged[i].second;
                    ++i;
                }
                deduped.push_back({key, sum});
            }

            // -----------------------------------------------------------------------
            // 9. Assemble output rows sorted by COUNT(*) DESC.
            // -----------------------------------------------------------------------
            int64_t n_output_groups = (int64_t)deduped.size();
            TRACE_COUNT("q11a_output_groups", n_output_groups);
            TRACE_COUNT("q11a_query_output_rows", n_output_groups);

            std::vector<std::vector<std::string>> rows;
            rows.reserve((size_t)(n_output_groups + 1));
            rows.push_back({"gender", "role", "name", "count_star()"});

            // Sort by count descending
            std::vector<KVPair> count_sorted;
            count_sorted.reserve((size_t)n_output_groups);
            for (const auto& [key, cnt] : deduped)
                count_sorted.push_back({cnt, key});

            { PROFILE_SCOPE("q11a_merge_sort_count");
            std::sort(count_sorted.begin(), count_sorted.end(),
                      [](const auto& a, const auto& b) { return a.first > b.first; });
            }

            { PROFILE_SCOPE("q11a_merge_output_build");
            for (const auto& [cnt, packed_key] : count_sorted) {
                const int64_t g_off = (int64_t)n_roles * n_companies;
                const int32_t g = (int32_t)(packed_key / g_off);
                const int64_t rem = packed_key % g_off;
                const int32_t r = (int32_t)(rem / n_companies);
                const int32_t c = (int32_t)(rem % n_companies);
                rows.push_back({
                    idx_to_gender[g],
                    idx_to_role[r],
                    idx_to_company[c],
                    std::to_string(cnt)
                });
            }
            }

            return rows;
        }
    }

}
