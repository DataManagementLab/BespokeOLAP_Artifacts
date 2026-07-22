#include "query3b.hpp"  // OPTIMIZED v13-fused
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstring>
#include <cstdint>
#include <mutex>
#include <string_view>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
/** SELECT t.title, n.name, cn.name, COUNT(*)
FROM title as t,
movie_keyword as mk,
keyword as k,
movie_companies as mc,
company_name as cn,
company_type as ct,
kind_type as kt,
cast_info as ci,
name as n,
role_type as rt
WHERE t.id = mk.movie_id
AND t.id = mc.movie_id
AND t.id = ci.movie_id
AND ci.movie_id = mc.movie_id
AND ci.movie_id = mk.movie_id
AND mk.movie_id = mc.movie_id
AND k.id = mk.keyword_id
AND cn.id = mc.company_id
AND ct.id = mc.company_type_id
AND kt.id = t.kind_id
AND ci.person_id = n.id
AND ci.role_id = rt.id
AND (t.title ILIKE TITLE)
AND (n.name_pcode_nf ILIKE NAME_PCODE_NF)
AND (cn.name ILIKE NAME)
AND (kt.kind IN KIND)
AND (rt.role IN ROLE)
GROUP BY t.title, n.name, cn.name
ORDER BY COUNT(*) DESC */

// ---------------------------------------------------------------------------
// Strip surrounding SQL single-quotes
// ---------------------------------------------------------------------------
static std::string strip_sql_quotes(const std::string& s) {
    if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
        return s.substr(1, s.size() - 2);
    return s;
}

// ---------------------------------------------------------------------------
// ILIKE pattern classification
// ---------------------------------------------------------------------------
enum class PatternType { SUBSTRING, PREFIX, GENERAL };
struct PatternInfo {
    PatternType type;
    std::string needle_low;
    char        first_lo = 0;
};

static PatternInfo classify_pattern(const std::string& pat) {
    PatternInfo info;
    if (pat.size() > 2 && pat.front() == '%' && pat.back() == '%') {
        bool no_inner = true;
        for (size_t i = 1; i + 1 < pat.size(); ++i)
            if (pat[i] == '%' || pat[i] == '_') { no_inner = false; break; }
        if (no_inner) {
            info.type = PatternType::SUBSTRING;
            info.needle_low = pat.substr(1, pat.size() - 2);
            for (char& c : info.needle_low) c = (char)std::tolower((unsigned char)c);
            info.first_lo = info.needle_low.empty() ? 0 : info.needle_low[0];
            return info;
        }
    }
    if (!pat.empty() && pat.back() == '%') {
        bool no_other = true;
        for (size_t i = 0; i + 1 < pat.size(); ++i)
            if (pat[i] == '%' || pat[i] == '_') { no_other = false; break; }
        if (no_other) {
            info.type = PatternType::PREFIX;
            info.needle_low = pat.substr(0, pat.size() - 1);
            for (char& c : info.needle_low) c = (char)std::tolower((unsigned char)c);
            info.first_lo = info.needle_low.empty() ? 0 : info.needle_low[0];
            return info;
        }
    }
    info.type = PatternType::GENERAL;
    info.needle_low = pat;
    return info;
}

static bool ilike_match_general(const std::string& text, const std::string& pattern) {
    std::string t_low = text, p_low = pattern;
    for (char& c : t_low) c = (char)std::tolower((unsigned char)c);
    for (char& c : p_low) c = (char)std::tolower((unsigned char)c);
    const int tlen = (int)t_low.size(), plen = (int)p_low.size();
    std::vector<bool> prev(plen+1, false), curr(plen+1, false);
    prev[0] = true;
    for (int j = 1; j <= plen; ++j)
        prev[j] = prev[j-1] && p_low[j-1] == '%';
    for (int i = 1; i <= tlen; ++i) {
        curr[0] = false;
        for (int j = 1; j <= plen; ++j) {
            if (p_low[j-1] == '%')       curr[j] = curr[j-1] || prev[j];
            else if (p_low[j-1] == '_')  curr[j] = prev[j-1];
            else                          curr[j] = prev[j-1] && (t_low[i-1] == p_low[j-1]);
        }
        std::swap(prev, curr);
    }
    return prev[plen];
}

static inline bool match_pattern(const std::string& text, const PatternInfo& pi) {
    const char* tdata = text.data();
    const size_t tlen = text.size();
    switch (pi.type) {
        case PatternType::SUBSTRING: {
            const char* needle = pi.needle_low.data();
            const size_t nlen  = pi.needle_low.size();
            if (nlen == 0) return true;
            if (tlen < nlen) return false;
            const char flo = pi.first_lo;
            const char* end = tdata + tlen - nlen + 1;
            for (const char* p = tdata; p < end; ++p) {
                if ((char)std::tolower((unsigned char)*p) != flo) continue;
                bool ok = true;
                for (size_t k = 1; k < nlen; ++k)
                    if ((char)std::tolower((unsigned char)p[k]) != needle[k]) { ok = false; break; }
                if (ok) return true;
            }
            return false;
        }
        case PatternType::PREFIX: {
            const size_t nlen  = pi.needle_low.size();
            if (tlen < nlen) return false;
            const char* needle = pi.needle_low.data();
            for (size_t i = 0; i < nlen; ++i)
                if ((char)std::tolower((unsigned char)tdata[i]) != needle[i]) return false;
            return true;
        }
        case PatternType::GENERAL:
            return ilike_match_general(text, pi.needle_low);
    }
    return false;
}

// match_pattern_prelowered: text is already fully lowercased.
static inline bool match_pattern_prelowered(const std::string& text_low, const PatternInfo& pi) {
    const char* tdata = text_low.data();
    const size_t tlen = text_low.size();
    switch (pi.type) {
        case PatternType::SUBSTRING:
            return text_low.find(pi.needle_low) != std::string::npos;
        case PatternType::PREFIX: {
            const size_t nlen = pi.needle_low.size();
            if (tlen < nlen) return false;
            return std::memcmp(tdata, pi.needle_low.data(), nlen) == 0;
        }
        case PatternType::GENERAL:
            return ilike_match_general(text_low, pi.needle_low);
    }
    return false;
}

// match_pattern_sv: text_low is a string_view into a flat byte array (already lowercased).
static inline bool match_pattern_sv(std::string_view text_low, const PatternInfo& pi) {
    const char* tdata = text_low.data();
    const size_t tlen = text_low.size();
    switch (pi.type) {
        case PatternType::SUBSTRING: {
            // Use std::string_view::find for SIMD-optimized substring search
            return text_low.find(pi.needle_low) != std::string_view::npos;
        }
        case PatternType::PREFIX: {
            const size_t nlen = pi.needle_low.size();
            if (tlen < nlen) return false;
            return std::memcmp(tdata, pi.needle_low.data(), nlen) == 0;
        }
        case PatternType::GENERAL: {
            // Convert to std::string for general matching
            std::string s(tdata, tlen);
            return ilike_match_general(s, pi.needle_low);
        }
    }
    return false;
}

// ---------------------------------------------------------------------------
// Flat byte array for movie-id membership.
// ---------------------------------------------------------------------------
struct MovieBitset {
    std::vector<uint8_t> bits;
    void init(int32_t max_id) { bits.assign((size_t)max_id + 1, 0); }
    inline void set(int32_t id) {
        if (id >= 0 && id < (int32_t)bits.size()) bits[id] = 1;
    }
    inline bool test(int32_t id) const {
        return (uint32_t)id < (uint32_t)bits.size() && bits[id];
    }
};

std::vector<std::vector<std::string>> run_q3b(Database* db, const Q3bArgs& args) {
    if (!db) throw std::runtime_error("run_q3b: db is null");
    PROFILE_SCOPE("q3b_total");

    const std::string pat_title  = strip_sql_quotes(args.TITLE);
    const std::string pat_nf     = strip_sql_quotes(args.NAME_PCODE_NF);
    const std::string pat_cnname = strip_sql_quotes(args.NAME);

    const PatternInfo pi_title  = classify_pattern(pat_title);
    const PatternInfo pi_nf     = classify_pattern(pat_nf);
    const PatternInfo pi_cnname = classify_pattern(pat_cnname);

    // -----------------------------------------------------------------------
    // Resolve valid kind_ids
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> kind_set(args.KIND.begin(), args.KIND.end());
    bool kind_null_ok = kind_set.count("<<NULL>>") > 0;
    std::vector<uint8_t> valid_kind_flag;
    {
        const auto& kt = db->kind_type;
        int32_t max_kid = 0;
        for (int32_t id : kt.id) if (id > max_kid) max_kid = id;
        valid_kind_flag.assign((size_t)max_kid + 1, 0);
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_set.count(kt.kind[i])) valid_kind_flag[kt.id[i]] = 1;
    }
    const uint32_t kind_flag_sz = (uint32_t)valid_kind_flag.size();

    // -----------------------------------------------------------------------
    // Resolve valid role_ids
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> role_set(args.ROLE.begin(), args.ROLE.end());
    bool role_null_ok = role_set.count("<<NULL>>") > 0;
    std::vector<uint8_t> valid_role_flag;
    {
        const auto& rt = db->role_type;
        int32_t max_rid = 0;
        for (int32_t id : rt.id) if (id > max_rid) max_rid = id;
        valid_role_flag.assign((size_t)max_rid + 1, 0);
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_set.count(rt.role[i])) valid_role_flag[rt.id[i]] = 1;
    }
    const uint32_t role_flag_sz = (uint32_t)valid_role_flag.size();

    // -----------------------------------------------------------------------
    // Determine max_movie_id upfront (needed for mk bitset allocation)
    // -----------------------------------------------------------------------
    // Use id_to_row.size() - 1 instead of scanning title.id (avoids 10M int32 scan)
    const int32_t max_movie_id = (int32_t)db->title.id_to_row.size() - 1;

    // -----------------------------------------------------------------------
    // FUSED PHASE 1: cn_filter + mk_existence in a SINGLE parallel_for.
    // Each thread does its stripe of company_name AND its stripe of mk CSR.
    // Saves 1 thread wakeup/barrier cycle compared to running them separately.
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, std::string> valid_company_name;
    std::vector<uint8_t> valid_cn_flag;
    MovieBitset mk_movie_set;
    mk_movie_set.init(max_movie_id);
    std::vector<uint8_t> in_mc_flag;
    {
        PROFILE_SCOPE("q3b_company_name_filter");
        const auto& cn       = db->company_name;
        const auto& mc_tbl_cn = db->movie_companies;
        const auto& mk_tbl   = db->movie_keyword;
        const auto& mk_csr   = mk_tbl.movie_id_csr;
        const int32_t n_cn   = (int32_t)cn.id.size();
        const int32_t mk_max = (int32_t)mk_csr.offsets.size() - 2;

        // max_cid derived from id_to_row size (avoids scanning cn.id)
        const int32_t max_cid = (int32_t)cn.id_to_row.size() - 1;
        valid_cn_flag.assign((size_t)max_cid + 1, 0);

        const int n_threads = pool.num_threads;

        // Build a compact flag array: in_mc_flag[cid]=1 iff company cid appears in mc.
        // This avoids expensive ILIKE matching for companies not in any movie.
        // We build it from the company_id_csr.offsets array (sequential scan = fast).
        {
            const auto& cid_csr = mc_tbl_cn.company_id_csr;
            const int32_t csr_max = (int32_t)cid_csr.offsets.size() - 2;
            const int32_t flag_max = std::min((int32_t)max_cid, csr_max);
            in_mc_flag.assign((size_t)(max_cid + 1), 0);
            for (int32_t cid = 0; cid < flag_max + 1; ++cid) {
                if (cid_csr.offsets[cid] < cid_csr.offsets[cid + 1]) {
                    in_mc_flag[cid] = 1;
                }
            }
        }

        struct CnLocal { std::vector<std::pair<int32_t, std::string>> matches; };
        std::vector<CnLocal> locals(n_threads);
        std::atomic<int64_t> rows_in_total{0}, rows_emitted_total{0};
        std::atomic<int64_t> mk_movies_total{0};

        pool.parallel_for([&](int tid, int nt) {
            // --- cn stripe ---
            {
                int64_t rows_in = 0, rows_emitted = 0;
                auto& loc = locals[tid];
                const int32_t beg = (int32_t)((int64_t)n_cn * tid / nt);
                const int32_t end = (int32_t)((int64_t)n_cn * (tid + 1) / nt);
                for (int32_t i = beg; i < end; ++i) {
                    ++rows_in;
                    const int32_t cid = cn.id[i];
                    // Skip companies not in any movie (flag array: cache-friendly)
                    if ((uint32_t)cid >= (uint32_t)in_mc_flag.size() || !in_mc_flag[cid]) continue;
                    if (!match_pattern_prelowered(cn.name_str_lower[i], pi_cnname)) continue;
                    loc.matches.emplace_back(cn.id[i], cn.name_str[i]);
                    ++rows_emitted;
                }
                rows_in_total.fetch_add(rows_in, std::memory_order_relaxed);
                rows_emitted_total.fetch_add(rows_emitted, std::memory_order_relaxed);
            }
            // --- mk stripe ---
            if (mk_max >= 0) {
                const int32_t beg = (int32_t)((int64_t)(mk_max + 1) * tid / nt);
                const int32_t end = (int32_t)((int64_t)(mk_max + 1) * (tid + 1) / nt);
                int64_t local_cnt = 0;
                for (int32_t mid = beg; mid < end; ++mid) {
                    if (mk_csr.offsets[mid] < mk_csr.offsets[mid + 1]) {
                        mk_movie_set.bits[mid] = 1;
                        ++local_cnt;
                    }
                }
                mk_movies_total.fetch_add(local_cnt, std::memory_order_relaxed);
            }
        });

        // Merge cn results
        valid_company_name.reserve(512);
        for (auto& loc : locals) {
            for (auto& [cid, nm] : loc.matches) {
                valid_company_name[cid] = std::move(nm);
                valid_cn_flag[cid] = 1;
            }
        }
        TRACE_COUNT("q3b_cn_rows_in", rows_in_total.load());
        TRACE_COUNT("q3b_cn_rows_emitted", rows_emitted_total.load());
        TRACE_COUNT("q3b_mk_existence_rows_in", mk_movies_total.load());
        TRACE_COUNT("q3b_mk_existence_movies", (int64_t)mk_movie_set.bits.size());
    }
    const uint32_t cn_flag_sz = (uint32_t)valid_cn_flag.size();

    // -----------------------------------------------------------------------
    // FUSED PHASE 2: mc_existence_build + title_scan in a SINGLE parallel_for.
    // Thread stripes cover movie_companies rows (sorted by movie_id).
    // Each qualifying movie (in cn + mk + title) is added directly to
    // thread-local valid_movies (no intermediate mc_mk_movie_ids list needed).
    // mc is sorted by movie_id; stripe boundaries are aligned to movie_id runs
    // so each movie_id is fully processed by exactly one thread.
    // -----------------------------------------------------------------------

    struct MovieInfo {
        std::string title_str;
        int32_t     mk_cnt;
    };
    std::unordered_map<int32_t, MovieInfo> valid_movies;
    std::unordered_map<int32_t, std::vector<std::string>> movie_to_cn_names;
    {
        PROFILE_SCOPE("q3b_title_scan");
        const auto& mc     = db->movie_companies;
        const int32_t n_mc = (int32_t)mc.movie_id.size();
        const int32_t* __restrict__ mc_mid = mc.movie_id.data();
        const int32_t* __restrict__ mc_cid = mc.company_id.data();
        const auto& mc_csr = mc.movie_id_csr;
        const auto& t      = db->title;
        const auto& mk     = db->movie_keyword;
        const auto& mk_csr = mk.movie_id_csr;
        const auto& kw     = db->keyword;

        const uint32_t kw_id_to_row_sz = (uint32_t)kw.id_to_row.size();
        const int32_t* __restrict__ kw_id_to_row = kw.id_to_row.data();
        const uint32_t id2row_sz = (uint32_t)t.id_to_row.size();
        const int32_t* __restrict__ id2row = t.id_to_row.data();

        const int n_threads = pool.num_threads;
        struct TitleLocal {
            std::unordered_map<int32_t, MovieInfo>                valid_movies;
            std::unordered_map<int32_t, std::vector<std::string>> movie_to_cn_names;
            int64_t mc_rows_scanned{0};
            int64_t rows_scanned{0};   // title scan rows
            int64_t rows_emitted{0};   // after title ILIKE
        };
        std::vector<TitleLocal> t_locals(n_threads);
        for (auto& loc : t_locals) {
            loc.valid_movies.reserve(64);
            loc.movie_to_cn_names.reserve(64);
        }

        pool.parallel_for([&](int tid, int nt) {
            auto& loc = t_locals[tid];
            // Stripe over mc rows (aligned to movie_id boundaries)
            const int32_t raw_beg = (int32_t)((int64_t)n_mc * tid / nt);
            const int32_t raw_end = (int32_t)((int64_t)n_mc * (tid + 1) / nt);

            int32_t beg = raw_beg;
            if (tid > 0 && beg > 0) {
                int32_t boundary_mid = mc_mid[beg - 1];
                while (beg < n_mc && mc_mid[beg] == boundary_mid) ++beg;
            }
            const int32_t end = raw_end;

            int32_t prev_mid = -1;
            bool prev_mid_cn_ok = false;  // did prev_mid pass cn filter?
            bool prev_mid_added = false;  // was prev_mid already added to valid_movies?

            for (int32_t r = beg; r < end; ++r) {
                ++loc.mc_rows_scanned;
                int32_t cid = mc_cid[r];
                int32_t mid = mc_mid[r];

                // Check cn filter for this row
                if ((uint32_t)cid >= cn_flag_sz || !valid_cn_flag[cid]) continue;

                // Skip if we already processed this movie_id and it passed
                if (mid == prev_mid && prev_mid_added) continue;

                // New movie_id or haven't added yet
                if (mid != prev_mid) {
                    prev_mid = mid;
                    prev_mid_added = false;
                    prev_mid_cn_ok = false;
                    if (!mk_movie_set.test(mid)) continue;
                    prev_mid_cn_ok = true;
                } else if (!prev_mid_cn_ok) {
                    // prev_mid failed mk test
                    continue;
                }

                // Title lookup
                ++loc.rows_scanned;
                if ((uint32_t)mid >= id2row_sz) continue;
                int32_t row = id2row[mid];
                if (row < 0) continue;

                // kind_id check
                int32_t kid = t.kind_id[row];
                bool kt_ok = (kid == -1) ? kind_null_ok
                                         : ((uint32_t)kid < kind_flag_sz && valid_kind_flag[kid]);
                if (!kt_ok) { prev_mid_cn_ok = false; continue; }

                // ILIKE on title (expensive -- after all cheap checks)
                if (!match_pattern(t.title_str[row], pi_title)) { prev_mid_cn_ok = false; continue; }

                // Already checked title -- now build cn_list from all mc rows for this mid
                // (must scan entire mc_csr range for this movie)
                std::vector<std::string> cn_list;
                {
                    auto [mbeg, mend] = mc_csr.range(mid);
                    for (int32_t mr = mbeg; mr < mend; ++mr) {
                        int32_t cid2 = mc.company_id[mr];
                        auto it = valid_company_name.find(cid2);
                        if (it == valid_company_name.end()) continue;
                        cn_list.push_back(it->second);
                    }
                }
                if (cn_list.empty()) { prev_mid_cn_ok = false; continue; }

                // Title lookup via id_to_row
                // Count valid keyword rows
                int32_t mk_cnt = 0;
                {
                    auto [mbeg, mend] = mk_csr.range(mid);
                    for (int32_t mr = mbeg; mr < mend; ++mr) {
                        int32_t kid2 = mk.keyword_id[mr];
                        if ((uint32_t)kid2 < kw_id_to_row_sz && kw_id_to_row[kid2] >= 0)
                            ++mk_cnt;
                    }
                }

                loc.movie_to_cn_names[mid] = std::move(cn_list);
                loc.valid_movies[mid] = MovieInfo{t.title_str[row], mk_cnt};
                ++loc.rows_emitted;
                prev_mid_added = true;
            }
        });

        // Merge thread-local results
        valid_movies.reserve(512);
        movie_to_cn_names.reserve(512);
        int64_t mc_rows = 0;
        for (auto& loc : t_locals) {
            mc_rows += loc.mc_rows_scanned;
            for (auto& [mid, minfo] : loc.valid_movies)
                valid_movies[mid] = std::move(minfo);
            for (auto& [mid, cn_list] : loc.movie_to_cn_names)
                movie_to_cn_names[mid] = std::move(cn_list);
        }
        int64_t ts = 0, te = 0;
        for (auto& loc : t_locals) { ts += loc.rows_scanned; te += loc.rows_emitted; }
        TRACE_COUNT("q3b_mc_existence_rows_in", mc_rows);
        TRACE_COUNT("q3b_mc_existence_movies", (int64_t)valid_company_name.size());
        TRACE_COUNT("q3b_title_rows_scanned", ts);
        TRACE_COUNT("q3b_title_rows_emitted", te);
    }
    TRACE_COUNT("q3b_valid_movies", (int64_t)valid_movies.size());
    TRACE_COUNT("q3b_mc_unique_movies", (int64_t)valid_movies.size());

    // -----------------------------------------------------------------------
    // Flatten valid_movies for parallel access
    // -----------------------------------------------------------------------
    struct MovieEntry {
        int32_t mid;
        const MovieInfo* minfo;
        const std::vector<std::string>* cn_list;
    };
    std::vector<MovieEntry> movie_entries;
    movie_entries.reserve(valid_movies.size());
    for (const auto& [mid, minfo] : valid_movies)
        movie_entries.push_back({mid, &minfo, &movie_to_cn_names.at(mid)});
    const int32_t n_movies_valid = (int32_t)movie_entries.size();

    // -----------------------------------------------------------------------
    // FUSED PHASE 2: collect_candidate_persons + name_filter in one pass.
    // For each valid movie, scan cast_info, check role, get person_id, and
    // immediately filter by name_pcode_nf. Eliminates one parallel_for and
    // the intermediate candidate_person_ids set.
    // Each thread builds a thread-local valid_person_name map.
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, std::string> valid_person_name;
    {
        PROFILE_SCOPE("q3b_collect_candidate_persons");
        const int n_threads = pool.num_threads;
        const auto& ci  = db->cast_info;
        const auto& csr = ci.movie_id_csr;
        const auto& nm  = db->name;
        const int32_t nm_id2row_sz = (int32_t)nm.id_to_row.size();

        struct PhaseLocal {
            // Use vector<pair> to avoid false sharing during writes
            std::vector<std::pair<int32_t,std::string>> valid_persons;
        };
        std::vector<PhaseLocal> phase_locals(n_threads);
        std::atomic<int64_t> rows_scanned_total{0};
        std::atomic<int64_t> name_rows_in_total{0}, name_rows_emitted_total{0};

        pool.parallel_for([&](int tid, int nt) {
            int64_t rows_scanned = 0, name_rows_in = 0, name_rows_emitted = 0;
            auto& loc = phase_locals[tid];
            // Local set to deduplicate person_ids within this thread's movie range
            std::unordered_set<int32_t> seen_pids;

            const int32_t beg = (int32_t)((int64_t)n_movies_valid * tid / nt);
            const int32_t end = (int32_t)((int64_t)n_movies_valid * (tid + 1) / nt);
            for (int32_t i = beg; i < end; ++i) {
                int32_t mid = movie_entries[i].mid;
                auto [rb, re] = csr.range(mid);
                for (int32_t r = rb; r < re; ++r) {
                    ++rows_scanned;
                    int32_t rid = ci.role_id[r];
                    bool rid_ok = (rid == -1) ? role_null_ok
                                              : ((uint32_t)rid < role_flag_sz && valid_role_flag[rid]);
                    if (!rid_ok) continue;
                    int32_t pid = ci.person_id[r];
                    if (!seen_pids.insert(pid).second) continue; // already processed
                    ++name_rows_in;
                    if (pid < 0 || pid >= nm_id2row_sz) continue;
                    int32_t row = nm.id_to_row[pid];
                    if (row < 0) continue;
                    if (!match_pattern(nm.name_pcode_nf[row], pi_nf)) continue;
                    loc.valid_persons.emplace_back(pid, nm.name_str[row]);
                    ++name_rows_emitted;
                }
            }
            rows_scanned_total.fetch_add(rows_scanned, std::memory_order_relaxed);
            name_rows_in_total.fetch_add(name_rows_in, std::memory_order_relaxed);
            name_rows_emitted_total.fetch_add(name_rows_emitted, std::memory_order_relaxed);
        });

        // Merge valid persons
        int64_t total_cands = 0;
        for (auto& loc : phase_locals) {
            total_cands += (int64_t)loc.valid_persons.size();
            for (auto& [pid, nm_str] : loc.valid_persons)
                valid_person_name.emplace(pid, std::move(nm_str));
        }
        TRACE_COUNT("q3b_candidate_ci_rows_scanned", rows_scanned_total.load());
        TRACE_COUNT("q3b_candidate_persons_found", total_cands);
        TRACE_COUNT("q3b_name_rows_in", name_rows_in_total.load());
        TRACE_COUNT("q3b_name_rows_emitted", name_rows_emitted_total.load());
    }

    // -----------------------------------------------------------------------
    // Group by (t.title, n.name, cn.name), accumulate COUNT(*)
    // -----------------------------------------------------------------------
    struct GroupKey {
        std::string title;
        std::string person_name;
        std::string cn_name;
        bool operator==(const GroupKey& o) const {
            return cn_name == o.cn_name && person_name == o.person_name && title == o.title;
        }
    };
    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            size_t h = std::hash<std::string>{}(k.title);
            h ^= std::hash<std::string>{}(k.person_name) + 0x9e3779b9u + (h << 6) + (h >> 2);
            h ^= std::hash<std::string>{}(k.cn_name)     + 0x9e3779b9u + (h << 6) + (h >> 2);
            return h;
        }
    };
    std::unordered_map<GroupKey, int64_t, GroupKeyHash> group_counts;

    // -----------------------------------------------------------------------
    // cast_info_probe: parallel over movies, thread-local group maps
    // -----------------------------------------------------------------------
    {
        PROFILE_SCOPE("q3b_cast_info_probe");
        const auto& ci  = db->cast_info;
        const auto& csr = ci.movie_id_csr;
        const int n_threads = pool.num_threads;

        using GroupMap = std::unordered_map<GroupKey, int64_t, GroupKeyHash>;
        std::vector<GroupMap> thread_groups(n_threads);
        std::atomic<int64_t> probe_rows_total{0}, join_rows_total{0};

        pool.parallel_for([&](int tid, int nt) {
            int64_t probe_rows = 0, join_rows = 0;
            auto& local_groups = thread_groups[tid];

            const int32_t beg = (int32_t)((int64_t)n_movies_valid * tid / nt);
            const int32_t end = (int32_t)((int64_t)n_movies_valid * (tid + 1) / nt);

            for (int32_t i = beg; i < end; ++i) {
                int32_t mid = movie_entries[i].mid;
                const MovieInfo& minfo = *movie_entries[i].minfo;
                const std::vector<std::string>& cn_list = *movie_entries[i].cn_list;
                int32_t mk_cnt = minfo.mk_cnt;

                // Build mc_cn_count for this movie
                std::unordered_map<std::string, int32_t> mc_cn_count;
                for (const auto& cn_nm : cn_list)
                    ++mc_cn_count[cn_nm];

                auto [rb, re] = csr.range(mid);
                for (int32_t r = rb; r < re; ++r) {
                    ++probe_rows;
                    int32_t rid = ci.role_id[r];
                    bool rid_ok = (rid == -1) ? role_null_ok
                                              : ((uint32_t)rid < role_flag_sz && valid_role_flag[rid]);
                    if (!rid_ok) continue;

                    int32_t pid = ci.person_id[r];
                    auto pit = valid_person_name.find(pid);
                    if (pit == valid_person_name.end()) continue;

                    for (const auto& [cn_nm, mc_cnt] : mc_cn_count) {
                        GroupKey gk{minfo.title_str, pit->second, cn_nm};
                        local_groups[gk] += (int64_t)mk_cnt * mc_cnt;
                        ++join_rows;
                    }
                }
            }
            probe_rows_total.fetch_add(probe_rows, std::memory_order_relaxed);
            join_rows_total.fetch_add(join_rows, std::memory_order_relaxed);
        });

        // Merge thread-local group maps
        for (auto& local_groups : thread_groups)
            for (auto& [gk, cnt] : local_groups)
                group_counts[gk] += cnt;

        TRACE_COUNT("q3b_cast_probe_rows_in", probe_rows_total.load());
        TRACE_COUNT("q3b_join_rows_emitted", join_rows_total.load());
    }

    // -----------------------------------------------------------------------
    // Sort and emit results
    // -----------------------------------------------------------------------
    std::vector<std::pair<int64_t, GroupKey>> sorted;
    sorted.reserve(group_counts.size());
    for (const auto& [gk, cnt] : group_counts)
        sorted.push_back({cnt, gk});
    std::sort(sorted.begin(), sorted.end(),
              [](const auto& a, const auto& b) { return a.first > b.first; });

    TRACE_COUNT("q3b_groups_created", (int64_t)group_counts.size());
    TRACE_COUNT("q3b_query_output_rows", (int64_t)sorted.size());

    std::vector<std::vector<std::string>> rows;
    rows.reserve(sorted.size() + 1);
    rows.push_back({"title", "name", "name_1", "count_star()"});
    for (const auto& [cnt, gk] : sorted)
        rows.push_back({gk.title, gk.person_name, gk.cn_name, std::to_string(cnt)});

    return rows;
}
