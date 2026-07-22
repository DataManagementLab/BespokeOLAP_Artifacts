#include "query9a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
#include "thread_pool.hpp"

#include <algorithm>
#include <cmath>
#include <array>
#include <cstdint>
#include <cctype>
#include <cstring>
#include <iomanip>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

static ThreadPool& pool = get_query_pool();

// ---------------------------------------------------------------------------
// Fast case-fold: ASCII-only lower.
// ---------------------------------------------------------------------------
static inline void ascii_tolower_inplace(char* buf, size_t n) noexcept {
    for (size_t i = 0; i < n; ++i)
        buf[i] = (char)((unsigned char)buf[i] | (((unsigned char)buf[i] - 'A' < 26u) ? 0x20u : 0u));
}

// ---------------------------------------------------------------------------
// Compiled ILIKE pattern
// ---------------------------------------------------------------------------
struct IlikePattern {
    struct Seg { std::string storage; uint32_t len; };
    std::vector<Seg> segs;
    bool starts_wild = false;
    bool ends_wild   = false;
    bool all_wild    = false;
    bool is_contains = false;

    void compile(const std::string& lc_pat) {
        segs.clear();
        is_contains = false;
        if (lc_pat.find_first_not_of('%') == std::string::npos) {
            all_wild = true; return;
        }
        starts_wild = (!lc_pat.empty() && lc_pat[0] == '%');
        ends_wild   = (!lc_pat.empty() && lc_pat.back() == '%');
        size_t pos = 0;
        while (pos <= lc_pat.size()) {
            size_t pct = lc_pat.find('%', pos);
            if (pct == std::string::npos) pct = lc_pat.size();
            if (pct > pos) {
                segs.push_back({lc_pat.substr(pos, pct - pos), 0u});
            }
            pos = pct + 1;
        }
        for (auto& sg : segs) sg.len = (uint32_t)sg.storage.size();
        is_contains = (starts_wild && ends_wild && segs.size() == 1);
    }

    bool match_lower(const char* lp, size_t llen) const noexcept {
        if (all_wild || segs.empty()) return true;
        if (is_contains) {
            const char* sp   = segs[0].storage.c_str();
            uint32_t    slen = segs[0].len;
            if (slen == 0) return true;
            if (llen < slen) return false;
            const char first = sp[0];
            const char* p = lp;
            const char* end = lp + llen - slen;
            while (p <= end) {
                p = (const char*)std::memchr(p, first, (size_t)(end - p + 1));
                if (!p) return false;
                if (std::memcmp(p, sp, slen) == 0) return true;
                ++p;
            }
            return false;
        }
        const char* te = lp + llen;
        const char* t_pos = lp;
        bool lead = starts_wild;
        for (size_t si = 0; si < segs.size(); ++si) {
            const char* sp   = segs[si].storage.c_str();
            uint32_t    slen = segs[si].len;
            if (!lead) {
                if ((uint32_t)(te - t_pos) < slen) return false;
                if (std::memcmp(t_pos, sp, slen) != 0) return false;
                t_pos += slen;
            } else {
                bool found = false;
                while ((uint32_t)(te - t_pos) >= slen) {
                    if (std::memcmp(t_pos, sp, slen) == 0) {
                        t_pos += slen; found = true; break;
                    }
                    ++t_pos;
                }
                if (!found) return false;
            }
            lead = (si + 1 < segs.size());
        }
        if (!ends_wild && t_pos != te) return false;
        return true;
    }

    bool match(const std::string& text) const noexcept {
        if (all_wild || segs.empty()) return true;
        char buf_stack[256];
        std::string buf_heap;
        char* buf;
        size_t n = text.size();
        if (n <= 255) {
            buf = buf_stack;
        } else {
            buf_heap.resize(n);
            buf = buf_heap.data();
        }
        std::memcpy(buf, text.c_str(), n);
        ascii_tolower_inplace(buf, n);
        return match_lower(buf, n);
    }
};

std::vector<std::vector<std::string>> run_q9a(Database* db, const Q9aArgs& args) {
    if (!db) throw std::runtime_error("run_q9a: db is null");
    PROFILE_SCOPE("q9a_total");

    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };
    auto lower_str = [](const std::string& s) {
        std::string r; r.reserve(s.size());
        for (unsigned char c : s) r += (char)std::tolower(c);
        return r;
    };
    auto strip_sq = [](const std::string& s) -> std::string {
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
            return s.substr(1, s.size() - 2);
        return s;
    };

    const std::string pat1 = strip_sq(args.INFO1);
    const std::string pat2 = strip_sq(args.INFO2);
    IlikePattern ilike1, ilike2;
    ilike1.compile(lower_str(pat1));
    ilike2.compile(lower_str(pat2));

    // -----------------------------------------------------------------------
    // Resolve valid kind_ids
    // -----------------------------------------------------------------------
    int32_t max_kind_id = 0;
    for (int32_t v : db->kind_type.id) if (v > max_kind_id) max_kind_id = v;
    std::vector<uint8_t> kind_id_valid_v(max_kind_id + 2, 0);
    {
        std::unordered_map<std::string, int32_t> kind_str_set;
        for (const auto& s : args.KIND) kind_str_set[s] = 1;
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_str_set.count(kt.kind[i]))
                kind_id_valid_v[kt.id[i]] = 1;
        TRACE_COUNT("q9a_valid_kind_ids", (int64_t)std::count(kind_id_valid_v.begin(), kind_id_valid_v.end(), 1));
    }
    const uint8_t* kind_id_valid = kind_id_valid_v.data();

    // -----------------------------------------------------------------------
    // Resolve valid role_ids
    // -----------------------------------------------------------------------
    int32_t max_role_id = 0;
    for (int32_t v : db->role_type.id) if (v > max_role_id) max_role_id = v;
    std::vector<uint8_t> role_id_valid_v(max_role_id + 2, 0);
    {
        std::unordered_map<std::string, int32_t> role_str_set;
        for (const auto& s : args.ROLE) role_str_set[s] = 1;
        const auto& rt = db->role_type;
        for (size_t i = 0; i < rt.id.size(); ++i)
            if (role_str_set.count(rt.role[i]))
                role_id_valid_v[rt.id[i]] = 1;
        TRACE_COUNT("q9a_valid_role_ids", (int64_t)std::count(role_id_valid_v.begin(), role_id_valid_v.end(), 1));
    }
    const uint8_t* role_id_valid = role_id_valid_v.data();

    // -----------------------------------------------------------------------
    // Resolve valid it1 ids + it2 ids
    // -----------------------------------------------------------------------
    std::vector<int32_t> it1_ids, it2_ids;
    for (const auto& s : args.ID1) if (!is_null(s)) it1_ids.push_back(std::stoi(s));
    for (const auto& s : args.ID2) if (!is_null(s)) it2_ids.push_back(std::stoi(s));
    int32_t max_it1 = 0;
    for (int32_t id : it1_ids) if (id > max_it1) max_it1 = id;
    std::vector<uint8_t> it1_valid_v(max_it1 + 2, 0);
    for (int32_t id : it1_ids) it1_valid_v[id] = 1;
    const uint8_t* it1_valid = it1_valid_v.data();
    TRACE_COUNT("q9a_valid_it1_ids", (int64_t)it1_ids.size());
    TRACE_COUNT("q9a_valid_it2_ids", (int64_t)it2_ids.size());

    // -----------------------------------------------------------------------
    // Pre-populate movie validity from title table (PARALLEL).
    // Encoding: 0=invalid/absent, 1=kind-valid+unseen, 3=kind-valid+seen
    // 
    // Key optimization: use kind_part_start/kind_part_end to scan ONLY valid-kind
    // title partitions. This reduces the scan from 12.6M to ~7-9M rows.
    // The valid-kind rows form contiguous ranges (title sorted by kind_id),
    // so we can split them evenly across threads without false sharing between
    // different partition ranges.
    // -----------------------------------------------------------------------
    const int32_t max_title_id = (int32_t)db->title.id_to_row.size() - 1;
    const int32_t title_id_to_row_size = (int32_t)db->title.id_to_row.size();
    const int32_t* __restrict__ title_id_to_row = db->title.id_to_row.data();
    const int32_t* __restrict__ title_kind_id   = db->title.kind_id.data();
    const int32_t* __restrict__ title_id_col    = db->title.id.data();

    // movie_state: 0=unseen, 1=kind-valid+unseen (unused now), 3=kind-valid+seen
    // We use lazy population: only entries seen during CI scan are written.
    // This avoids expensive pre-warming of all title rows (cold memory access).
    std::vector<uint8_t> movie_state_v((size_t)(max_title_id + 2), 0);
    uint8_t* movie_state = movie_state_v.data();

    // -----------------------------------------------------------------------
    // Step 1: Scan person_info partitions — PARALLELIZED.
    // -----------------------------------------------------------------------
    std::vector<std::string> pi_info_pool;
    pi_info_pool.reserve(256);
    std::unordered_map<std::string, int32_t> pi_info_intern;
    pi_info_intern.reserve(256);

    struct PiEntry { int32_t person_id; int32_t info_idx; };
    std::vector<PiEntry> pi_entries;
    pi_entries.reserve(1024);

    {
        PROFILE_SCOPE("q9a_pi_build");
        const auto& pi = db->person_info;
        const int32_t* __restrict__ pi_person_id = pi.person_id.data();

        struct Range { int32_t beg, end; };
        std::vector<Range> ranges;
        for (int32_t it2 : it2_ids) {
            if (it2 < 0 || it2 >= (int32_t)pi.type_part_start.size()) continue;
            int32_t b = pi.type_part_start[it2];
            int32_t e = pi.type_part_end[it2];
            if (b < e) ranges.push_back({b, e});
        }

        int64_t total_rows = 0;
        for (auto& rng : ranges) total_rows += (rng.end - rng.beg);

        int n_threads = pool.num_threads;

        struct LocalPI {
            std::vector<PiEntry> entries;
            std::unordered_map<std::string, int32_t> intern;
            std::vector<std::string> pool_vec;
            int64_t rows_scanned = 0, rows_emitted = 0;
        };
        std::vector<LocalPI> locals(n_threads);

        pool.parallel_for([&](int tid, int n) {
            LocalPI& loc = locals[tid];
            int64_t chunk_beg = (total_rows * tid) / n;
            int64_t chunk_end = (total_rows * (tid + 1)) / n;
            int64_t offset = 0;
            for (const auto& rng : ranges) {
                int64_t rlen = rng.end - rng.beg;
                int64_t r_beg_off = std::max((int64_t)0, chunk_beg - offset);
                int64_t r_end_off = std::min(rlen, chunk_end - offset);
                if (r_beg_off < r_end_off) {
                    int32_t row_beg = rng.beg + (int32_t)r_beg_off;
                    int32_t row_end = rng.beg + (int32_t)r_end_off;
                    for (int32_t r = row_beg; r < row_end; ++r) {
                        ++loc.rows_scanned;
                        const std::string& inf = pi.info_str[r];
                        if (!ilike2.match(inf)) continue;
                        auto [iit, ins] = loc.intern.emplace(inf, (int32_t)loc.pool_vec.size());
                        if (ins) loc.pool_vec.push_back(inf);
                        loc.entries.push_back({pi_person_id[r], iit->second});
                        ++loc.rows_emitted;
                    }
                }
                offset += rlen;
                if (offset >= chunk_end) break;
            }
        });

        int64_t rows_scanned = 0, rows_emitted = 0;
        for (int t = 0; t < n_threads; ++t) {
            LocalPI& loc = locals[t];
            rows_scanned += loc.rows_scanned;
            rows_emitted += loc.rows_emitted;
            std::vector<int32_t> remap(loc.pool_vec.size());
            for (int32_t i = 0; i < (int32_t)loc.pool_vec.size(); ++i) {
                auto [iit, ins] = pi_info_intern.emplace(loc.pool_vec[i], (int32_t)pi_info_pool.size());
                if (ins) pi_info_pool.push_back(loc.pool_vec[i]);
                remap[i] = iit->second;
            }
            for (const auto& e : loc.entries)
                pi_entries.push_back({e.person_id, remap[e.info_idx]});
        }
        TRACE_COUNT("q9a_pi_rows_scanned", rows_scanned);
        TRACE_COUNT("q9a_pi_rows_emitted", rows_emitted);
    }

    if (pi_entries.empty()) {
        TRACE_COUNT("q9a_query_output_rows", 0);
        return {{"info", "info_1", "count_star()"}};
    }

    // Group by person_id for cast_info CSR lookup.
    std::unordered_map<int32_t, std::vector<int32_t>> person_to_pi_infos;
    person_to_pi_infos.reserve(pi_entries.size());
    for (const auto& e : pi_entries)
        person_to_pi_infos[e.person_id].push_back(e.info_idx);
    TRACE_COUNT("q9a_pi_person_groups", (int64_t)person_to_pi_infos.size());

    // -----------------------------------------------------------------------
    // Step 2: Walk cast_info via person_id_csr — PARALLELIZED.
    // movie_state is pre-populated (read-only for threads).
    // -----------------------------------------------------------------------
    struct CiMatch {
        int32_t movie_id;
        const std::vector<int32_t>* pi_infos;
    };
    std::vector<CiMatch> qualifying_ci;
    qualifying_ci.reserve(8192);
    std::vector<int32_t> distinct_movie_ids_vec;
    distinct_movie_ids_vec.reserve(4096);

    {
        PROFILE_SCOPE("q9a_ci_scan");
        const auto& ci = db->cast_info;
        const auto& csr = ci.person_id_csr;
        const int32_t* __restrict__ ci_role_id  = ci.role_id.data();
        const int32_t* __restrict__ ci_movie_id = ci.movie_id.data();

        std::vector<std::pair<int32_t, const std::vector<int32_t>*>> pi_pairs;
        pi_pairs.reserve(person_to_pi_infos.size());
        for (const auto& [pid, pi_infos] : person_to_pi_infos)
            pi_pairs.push_back({pid, &pi_infos});

        int n_threads = pool.num_threads;
        struct LocalCI {
            std::vector<CiMatch> qualifying;
            std::vector<int32_t> movie_candidates;
            int64_t rows_visited = 0, rows_role_pass = 0, rows_kind_pass = 0;
        };
        std::vector<LocalCI> ci_locals(n_threads);

        pool.parallel_for([&](int tid, int n) {
            LocalCI& loc = ci_locals[tid];
            size_t total = pi_pairs.size();
            size_t beg = (total * (size_t)tid) / (size_t)n;
            size_t end = (total * (size_t)(tid + 1)) / (size_t)n;
            for (size_t i = beg; i < end; ++i) {
                int32_t pid = pi_pairs[i].first;
                const std::vector<int32_t>* pi_infos = pi_pairs[i].second;
                auto [csr_beg, csr_end] = csr.range(pid);
                for (int32_t ci_pos = csr_beg; ci_pos < csr_end; ++ci_pos) {
                    int32_t ci_row = csr.values[ci_pos];
                    ++loc.rows_visited;
                    int32_t rid = ci_role_id[ci_row];
                    if ((uint32_t)rid > (uint32_t)max_role_id || !role_id_valid[rid]) continue;
                    ++loc.rows_role_pass;
                    int32_t mid = ci_movie_id[ci_row];
                    if ((uint32_t)mid >= (uint32_t)title_id_to_row_size) continue;
                    // Inline kind check: two-level lookup via shared-mem arrays (warm).
                    int32_t title_row = title_id_to_row[mid];
                    if (title_row < 0) continue;
                    int32_t kid = title_kind_id[title_row];
                    if ((uint32_t)kid > (uint32_t)max_kind_id || !kind_id_valid[kid]) continue;
                    ++loc.rows_kind_pass;
                    loc.qualifying.push_back({mid, pi_infos});
                    loc.movie_candidates.push_back(mid);
                }
            }
        });

        int64_t ci_rows_visited = 0, ci_rows_role_pass = 0, ci_rows_kind_pass = 0;
        for (int t = 0; t < n_threads; ++t) {
            LocalCI& loc = ci_locals[t];
            ci_rows_visited   += loc.rows_visited;
            ci_rows_role_pass += loc.rows_role_pass;
            ci_rows_kind_pass += loc.rows_kind_pass;
            for (auto& cm : loc.qualifying)
                qualifying_ci.push_back(cm);
            // Dedup movie candidates via movie_state (cold write, but only ~14k unique movies)
            for (int32_t mid : loc.movie_candidates) {
                uint8_t& st = movie_state[mid];
                if (st == 0) { st = 3; distinct_movie_ids_vec.push_back(mid); }
            }
        }
        TRACE_COUNT("q9a_ci_rows_visited",  ci_rows_visited);
        TRACE_COUNT("q9a_ci_rows_role_pass", ci_rows_role_pass);
        TRACE_COUNT("q9a_ci_rows_kind_pass", ci_rows_kind_pass);
        TRACE_COUNT("q9a_ci_qualifying",    (int64_t)qualifying_ci.size());
        TRACE_COUNT("q9a_distinct_movies",  (int64_t)distinct_movie_ids_vec.size());
    }

    if (qualifying_ci.empty()) {
        TRACE_COUNT("q9a_query_output_rows", 0);
        return {{"info", "info_1", "count_star()"}};
    }

    // -----------------------------------------------------------------------
    // Step 3: Build movie_info lookup — PARALLELIZED.
    // -----------------------------------------------------------------------
    std::vector<std::string> mi1_info_pool;
    mi1_info_pool.reserve(256);
    std::unordered_map<std::string, int32_t> mi1_info_intern;
    mi1_info_intern.reserve(256);
    std::unordered_map<int32_t, std::vector<int32_t>> movie_to_mi1_infos;
    movie_to_mi1_infos.reserve(distinct_movie_ids_vec.size() * 2);

    {
        PROFILE_SCOPE("q9a_mi1_build");
        const auto& mi = db->movie_info;
        const int32_t* __restrict__ mi_movie_id     = mi.movie_id.data();
        const int32_t* __restrict__ mi_info_type_id = mi.info_type_id.data();
        const auto& mi_csr = mi.movie_id_csr;

        int64_t csr_total = 0;
        for (int32_t mid : distinct_movie_ids_vec) {
            auto [b, e] = mi_csr.range(mid);
            csr_total += (e - b);
        }
        int64_t part_total = 0;
        for (int32_t it1 : it1_ids) {
            if (it1 >= 0 && it1 < (int32_t)mi.type_part_start.size())
                part_total += (mi.type_part_end[it1] - mi.type_part_start[it1]);
        }
        TRACE_COUNT("q9a_mi1_csr_total",  csr_total);
        TRACE_COUNT("q9a_mi1_part_total", part_total);

        int n_threads = pool.num_threads;

        struct LocalMI {
            std::unordered_map<int32_t, std::vector<int32_t>> movie_infos;
            std::unordered_map<std::string, int32_t> intern;
            std::vector<std::string> pool_vec;
            int64_t rows_scanned = 0, rows_emitted = 0;
        };
        std::vector<LocalMI> mi_locals(n_threads);

        if (part_total <= csr_total) {
            // movie_state==3 means kind-valid+seen: reuse directly.
            struct Range { int32_t beg, end; };
            std::vector<Range> mi_ranges;
            for (int32_t it1 : it1_ids) {
                if (it1 < 0 || it1 >= (int32_t)mi.type_part_start.size()) continue;
                int32_t b = mi.type_part_start[it1];
                int32_t e = mi.type_part_end[it1];
                if (b < e) mi_ranges.push_back({b, e});
            }
            int64_t mi_total = 0;
            for (auto& rng : mi_ranges) mi_total += (rng.end - rng.beg);

            pool.parallel_for([&](int tid, int n) {
                LocalMI& loc = mi_locals[tid];
                int64_t chunk_beg = (mi_total * tid) / n;
                int64_t chunk_end = (mi_total * (tid + 1)) / n;
                int64_t offset = 0;
                for (const auto& rng : mi_ranges) {
                    int64_t rlen = rng.end - rng.beg;
                    int64_t r_beg_off = std::max((int64_t)0, chunk_beg - offset);
                    int64_t r_end_off = std::min(rlen, chunk_end - offset);
                    if (r_beg_off < r_end_off) {
                        int32_t row_beg = rng.beg + (int32_t)r_beg_off;
                        int32_t row_end = rng.beg + (int32_t)r_end_off;
                        for (int32_t r = row_beg; r < row_end; ++r) {
                            ++loc.rows_scanned;
                            int32_t mid = mi_movie_id[r];
                            if ((uint32_t)mid > (uint32_t)max_title_id || movie_state[mid] != 3) continue;
                            const std::string& inf = mi.info_str[r];
                            if (!ilike1.match(inf)) continue;
                            auto [iit, ins] = loc.intern.emplace(inf, (int32_t)loc.pool_vec.size());
                            if (ins) loc.pool_vec.push_back(inf);
                            loc.movie_infos[mid].push_back(iit->second);
                            ++loc.rows_emitted;
                        }
                    }
                    offset += rlen;
                    if (offset >= chunk_end) break;
                }
            });
        } else {
            int32_t nm = (int32_t)distinct_movie_ids_vec.size();
            pool.parallel_for([&](int tid, int n) {
                LocalMI& loc = mi_locals[tid];
                int32_t beg = (nm * tid) / n;
                int32_t end = (nm * (tid + 1)) / n;
                for (int32_t i = beg; i < end; ++i) {
                    int32_t mid = distinct_movie_ids_vec[i];
                    auto [csr_beg, csr_end] = mi_csr.range(mid);
                    for (int32_t mi_pos = csr_beg; mi_pos < csr_end; ++mi_pos) {
                        int32_t mi_row = mi_csr.values[mi_pos];
                        ++loc.rows_scanned;
                        int32_t itid = mi_info_type_id[mi_row];
                        if (itid > max_it1 || !it1_valid[itid]) continue;
                        const std::string& inf = mi.info_str[mi_row];
                        if (!ilike1.match(inf)) continue;
                        auto [iit, ins] = loc.intern.emplace(inf, (int32_t)loc.pool_vec.size());
                        if (ins) loc.pool_vec.push_back(inf);
                        loc.movie_infos[mid].push_back(iit->second);
                        ++loc.rows_emitted;
                    }
                }
            });
        }

        int64_t rows_scanned = 0, rows_emitted = 0;
        for (int t = 0; t < n_threads; ++t) {
            LocalMI& loc = mi_locals[t];
            rows_scanned += loc.rows_scanned;
            rows_emitted += loc.rows_emitted;
            std::vector<int32_t> remap(loc.pool_vec.size());
            for (int32_t i = 0; i < (int32_t)loc.pool_vec.size(); ++i) {
                auto [iit, ins] = mi1_info_intern.emplace(loc.pool_vec[i], (int32_t)mi1_info_pool.size());
                if (ins) mi1_info_pool.push_back(loc.pool_vec[i]);
                remap[i] = iit->second;
            }
            for (auto& [mid, idxs] : loc.movie_infos) {
                auto& dst = movie_to_mi1_infos[mid];
                for (int32_t idx : idxs)
                    dst.push_back(remap[idx]);
            }
        }
        TRACE_COUNT("q9a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q9a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q9a_mi1_movie_groups", (int64_t)movie_to_mi1_infos.size());
    }

    // -----------------------------------------------------------------------
    // Step 4: Final aggregation — PARALLELIZED.
    // -----------------------------------------------------------------------
    struct IntPairHash {
        size_t operator()(const std::pair<int32_t,int32_t>& p) const noexcept {
            return (size_t)(uint32_t)p.first * 1000003ULL ^ (size_t)(uint32_t)p.second;
        }
    };
    std::unordered_map<std::pair<int32_t,int32_t>, int64_t, IntPairHash> counts;
    counts.reserve(1024);

    {
        PROFILE_SCOPE("q9a_join");
        int n_threads = pool.num_threads;
        int32_t nci = (int32_t)qualifying_ci.size();

        struct LocalJoin {
            std::unordered_map<std::pair<int32_t,int32_t>, int64_t, IntPairHash> counts;
            int64_t ci_emit = 0;
        };
        std::vector<LocalJoin> join_locals(n_threads);
        for (auto& lj : join_locals) lj.counts.reserve(512);

        pool.parallel_for([&](int tid, int n) {
            LocalJoin& loc = join_locals[tid];
            int32_t beg = (nci * tid) / n;
            int32_t end_idx = (nci * (tid + 1)) / n;
            for (int32_t i = beg; i < end_idx; ++i) {
                const auto& [mid, pi_infos_ptr] = qualifying_ci[i];
                auto mi1_it = movie_to_mi1_infos.find(mid);
                if (mi1_it == movie_to_mi1_infos.end()) continue;
                const std::vector<int32_t>& mi1_infos = mi1_it->second;
                const std::vector<int32_t>& pi_infos   = *pi_infos_ptr;
                for (int32_t mi1_idx : mi1_infos) {
                    for (int32_t pi_idx : pi_infos) {
                        loc.counts[{mi1_idx, pi_idx}]++;
                        ++loc.ci_emit;
                    }
                }
            }
        });

        int64_t ci_emit = 0;
        for (int t = 0; t < n_threads; ++t) {
            LocalJoin& loc = join_locals[t];
            ci_emit += loc.ci_emit;
            for (auto& [key, cnt] : loc.counts)
                counts[key] += cnt;
        }
        TRACE_COUNT("q9a_ci_emit",       ci_emit);
        TRACE_COUNT("q9a_output_groups", (int64_t)counts.size());
    }

    TRACE_COUNT("q9a_query_output_rows", (int64_t)counts.size());

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"info", "info_1", "count_star()"});
    rows.reserve(counts.size() + 1);
    for (const auto& [key, cnt] : counts) {
        rows.push_back({mi1_info_pool[key.first], pi_info_pool[key.second], std::to_string(cnt)});
    }
    return rows;
}
