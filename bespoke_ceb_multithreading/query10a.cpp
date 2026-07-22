#include "query10a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
// v2: person_id_csr optimization
#include <immintrin.h>

static ThreadPool& pool = get_query_pool();

#include <atomic>
#include <strings.h>    // strcasestr (POSIX/GNU)
#include <mutex>
#include <numeric>
#include <array>
#include <algorithm>
#include <cstdint>
#include <cctype>
#include <cstring>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
/**
 * SELECT n.name, mi1.info, MIN(t.production_year), MAX(t.production_year)
 * FROM title as t, kind_type as kt, movie_info as mi1, info_type as it1,
 *      cast_info as ci, role_type as rt, name as n
 * WHERE t.id = ci.movie_id AND t.id = mi1.movie_id
 *   AND mi1.info_type_id = it1.id AND t.kind_id = kt.id
 *   AND ci.person_id = n.id AND ci.movie_id = mi1.movie_id
 *   AND ci.role_id = rt.id
 *   AND (it1.id IN ID) AND (mi1.info IN INFO)
 *   AND (n.name ILIKE NAME) AND (kt.kind IN KIND) AND (rt.role IN ROLE)
 * GROUP BY mi1.info, n.name
 */

std::vector<std::vector<std::string>> run_q10a(Database* db, const Q10aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q10a: db is null");
    }

    PROFILE_SCOPE("q10a_total");

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------
    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };

    // Case-insensitive ILIKE match with '%' wildcards only.
    auto ilike_lc = [](const char* tp, size_t tl,
                       const char* pp, size_t pl) -> bool {
        bool ap = true;
        for (size_t i = 0; i < pl; ++i)
            if (pp[i] != '%') { ap = false; break; }
        if (ap) return true;

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
    };

    // Strip surrounding single-quotes from ILIKE pattern string.
    auto strip_sq = [](const std::string& s) -> std::string {
        if (s.size() >= 2 && s.front() == '\'' && s.back() == '\'')
            return s.substr(1, s.size() - 2);
        return s;
    };

    // Build lowercased pattern for n.name ILIKE NAME
    const std::string raw_pat = strip_sq(args.NAME);
    std::string lc_pat;
    lc_pat.reserve(raw_pat.size());
    for (unsigned char c : raw_pat) lc_pat += (char)std::tolower(c);
    const char* pat_ptr = lc_pat.c_str();
    const size_t pat_len = lc_pat.size();

    // Fast path: simple contains pattern "%needle%" with no inner '%'
    bool simple_contains = false;
    std::string needle;
    if (pat_len >= 2 && lc_pat.front() == '%' && lc_pat.back() == '%') {
        std::string mid_s(lc_pat.begin() + 1, lc_pat.end() - 1);
        if (mid_s.find('%') == std::string::npos) {
            simple_contains = true;
            needle = std::move(mid_s);
        }
    }
    const char* ndl_ptr = simple_contains ? needle.c_str() : nullptr;
    const size_t ndl_len = simple_contains ? needle.size() : 0;

    auto name_matches = [&](const std::string& nm_str) -> bool {
        if (simple_contains) {
            if (nm_str.size() < ndl_len) return false;
            return strcasestr(nm_str.c_str(), ndl_ptr) != nullptr;
        }
        std::string lc_nm;
        lc_nm.reserve(nm_str.size());
        for (unsigned char c : nm_str) lc_nm += (char)std::tolower(c);
        return ilike_lc(lc_nm.c_str(), lc_nm.size(), pat_ptr, pat_len);
    };

    // -----------------------------------------------------------------------
    // 1. Resolve valid kind_ids from KIND in-list
    // -----------------------------------------------------------------------
    std::vector<bool> kind_id_ok;
    int32_t kind_id_ok_max = 0;
    {
        std::unordered_set<std::string> kind_set;
        bool null_ok = false;
        for (const auto& s : args.KIND) {
            if (is_null(s)) null_ok = true;
            else kind_set.insert(s);
        }
        for (size_t i = 0; i < db->kind_type.id.size(); ++i)
            if (db->kind_type.id[i] > kind_id_ok_max) kind_id_ok_max = db->kind_type.id[i];
        kind_id_ok.assign(kind_id_ok_max + 2, false);
        int64_t cnt = 0;
        for (size_t i = 0; i < db->kind_type.id.size(); ++i) {
            if (kind_set.count(db->kind_type.kind[i])) {
                kind_id_ok[db->kind_type.id[i]] = true;
                ++cnt;
            }
        }
        TRACE_COUNT("q10a_valid_kind_ids", cnt);
    }

    // -----------------------------------------------------------------------
    // 2. Resolve valid role_ids from ROLE in-list
    // -----------------------------------------------------------------------
    std::vector<bool> role_id_ok;
    int32_t role_id_ok_max = 0;
    {
        std::unordered_set<std::string> role_set;
        for (const auto& s : args.ROLE)
            if (!is_null(s)) role_set.insert(s);
        for (size_t i = 0; i < db->role_type.id.size(); ++i)
            if (db->role_type.id[i] > role_id_ok_max) role_id_ok_max = db->role_type.id[i];
        role_id_ok.assign(role_id_ok_max + 2, false);
        int64_t cnt = 0;
        for (size_t i = 0; i < db->role_type.id.size(); ++i) {
            if (role_set.count(db->role_type.role[i])) {
                role_id_ok[db->role_type.id[i]] = true;
                ++cnt;
            }
        }
        TRACE_COUNT("q10a_valid_role_ids", cnt);
    }

    // -----------------------------------------------------------------------
    // 3. Resolve valid info_type ids from ID in-list (it1.id IN ID)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_it_ids;
    for (const auto& s : args.ID)
        if (!is_null(s)) valid_it_ids.insert(std::stoi(s));
    TRACE_COUNT("q10a_valid_it_ids", (int64_t)valid_it_ids.size());

    // -----------------------------------------------------------------------
    // 4. Intern the allowed mi1.info strings from INFO in-list.
    // -----------------------------------------------------------------------
    std::unordered_map<std::string, int32_t> info_intern;
    std::vector<std::string> info_strs;
    {
        int32_t n = 0;
        for (const auto& s : args.INFO) {
            if (!is_null(s) && !info_intern.count(s)) {
                info_intern[s] = n++;
                info_strs.push_back(s);
            }
        }
    }
    TRACE_COUNT("q10a_info_intern_size", (int64_t)info_intern.size());
    if (info_intern.empty()) {
        return {{"name", "info", "min(t.production_year)", "max(t.production_year)"}};
    }

    // -----------------------------------------------------------------------
    // 5. Build movie_ok and movie_year using kind_part_start/end partitions.
    //    This avoids scanning all 5M title rows: only scan rows for valid kinds.
    //    kind_part_start[k]/kind_part_end[k] give row range for kind_id == k.
    // -----------------------------------------------------------------------
    const int32_t max_title_id = (int32_t)db->title.id_to_row.size() - 1;
    // Use uint8_t (not vector<bool>) for movie_ok: avoids bit-packing overhead
    // in random-access reads during mi1_build (each access needs shift+mask for bool).
    std::vector<uint8_t>  movie_ok(max_title_id + 2, 0);
    {
        PROFILE_SCOPE("q10a_title_scan");
        int64_t rows_scanned=0, rows_emitted=0;
        const auto& t = db->title;
        const int32_t* __restrict__ tid_ptr = t.id.data();
        // Only build movie_ok (not movie_year) — avoids 46MB random-write array.
        // movie_year is populated later for only the 482K valid movies.

        const bool has_kind_parts = !t.kind_part_start.empty();
        if (has_kind_parts) {
            const int32_t kps_size = (int32_t)t.kind_part_start.size();
            std::vector<std::pair<int32_t,int32_t>> ranges;
            for (int32_t k = 1; k <= kind_id_ok_max && k < kps_size; ++k) {
                if (!kind_id_ok[k]) continue;
                ranges.emplace_back(t.kind_part_start[k], t.kind_part_end[k]);
            }
            std::vector<int32_t> flat_beg, flat_end_arr;
            for (auto& [b,e] : ranges) { flat_beg.push_back(b); flat_end_arr.push_back(e); }
            const int32_t n_ranges = (int32_t)flat_beg.size();

            const int n_threads = pool.num_threads;
            std::vector<int64_t> thr_sc(n_threads,0), thr_em(n_threads,0);
            pool.parallel_for([&](int tid, int n_thr) {
                int64_t sc = 0, em = 0;
                uint8_t* __restrict__ mok = movie_ok.data();
                for (int32_t ri = tid; ri < n_ranges; ri += n_thr) {
                    int32_t beg = flat_beg[ri], end = flat_end_arr[ri];
                    for (int32_t r = beg; r < end; ++r) {
                        ++sc;
                        int32_t mid = tid_ptr[r];
                        if ((uint32_t)mid > (uint32_t)max_title_id) continue;
                        mok[mid] = 1;
                        ++em;
                    }
                }
                thr_sc[tid] = sc; thr_em[tid] = em;
            });
            for (int t2 = 0; t2 < n_threads; ++t2) {
                rows_scanned += thr_sc[t2];
                rows_emitted += thr_em[t2];
            }
        } else {
            const int32_t* __restrict__ kid_ptr = t.kind_id.data();
            const int32_t n_rows = (int32_t)t.id.size();
            const int n_threads = pool.num_threads;
            std::vector<int64_t> thr_sc(n_threads,0), thr_em(n_threads,0);
            pool.parallel_for([&](int tid, int n_thr) {
                int32_t chunk = (n_rows + n_thr - 1) / n_thr;
                int32_t beg   = tid * chunk;
                int32_t end   = std::min(beg + chunk, n_rows);
                int64_t sc = 0, em = 0;
                for (int32_t r = beg; r < end; ++r) {
                    ++sc;
                    int32_t mid = tid_ptr[r];
                    if ((uint32_t)mid > (uint32_t)max_title_id) continue;
                    int32_t kid = kid_ptr[r];
                    if (kid < 0 || kid > kind_id_ok_max || !kind_id_ok[kid]) continue;
                    movie_ok[mid] = 1;
                    ++em;
                }
                thr_sc[tid] = sc; thr_em[tid] = em;
            });
            for (int t2 = 0; t2 < n_threads; ++t2) {
                rows_scanned += thr_sc[t2];
                rows_emitted += thr_em[t2];
            }
        }
        TRACE_COUNT("q10a_title_rows_scanned", rows_scanned);
        TRACE_COUNT("q10a_title_rows_emitted", rows_emitted);
        TRACE_COUNT("q10a_valid_movie_ids",     rows_emitted);
    }
    // -----------------------------------------------------------------------
    // 6. Scan movie_info via type_part index.
    //    Build: movie_iid_mask[movie_id] -> bitmask of info_intern_ids present.
    // -----------------------------------------------------------------------
    const int32_t mask_size = max_title_id + 2;
    std::vector<uint8_t>  movie_iid_mask(mask_size, 0);  // uint8_t: supports up to 8 INFO values
    std::vector<int32_t>  valid_movie_list;
    valid_movie_list.reserve(250000);

    const int32_t n_dict = (int32_t)db->movie_info.info_dict_vec.size();
    // Pre-compute dict_to_bit: maps dict_id -> bitmask (0 if not in INFO, 1<<iid otherwise).
    // Avoids two separate operations (lookup + shift) in the hot mi1_build loop.
    std::vector<uint8_t>  dict_to_bit(n_dict, 0);
    {
        for (int32_t d = 0; d < n_dict; ++d) {
            auto it = info_intern.find(db->movie_info.info_dict_vec[d]);
            if (it != info_intern.end()) dict_to_bit[d] = (uint8_t)(1u << (uint32_t)it->second);  // up to 8 bits
        }
    }
    const uint8_t*  __restrict__ dtb = dict_to_bit.data();

    {
        PROFILE_SCOPE("q10a_mi1_build");
        int64_t rows_scanned=0, rows_emitted=0;
        const auto& mi  = db->movie_info;
        const auto& tps = mi.type_part_start;
        const auto& tpe = mi.type_part_end;
        const int32_t max_type = (int32_t)tps.size() - 1;
        const int32_t* __restrict__ mi_mid = mi.movie_id.data();
        const int32_t* __restrict__ mi_iid = mi.info_id.data();
        uint8_t*       __restrict__ mok    = movie_ok.data();
        // Use atomic operations on mmask for parallel OR-updates.
        // movie_iid_mask is uint32_t — use __atomic_fetch_or for lock-free OR.
        uint8_t*       __restrict__ mmask  = movie_iid_mask.data();

        // Collect all (movie_id) that get at least one bit set, per thread.
        // Dedup via per-thread 'first seen' tracking using mmask state.
        // Since multiple threads can update the same mmask[mid] concurrently,
        // we use two-phase approach:
        //   Phase 1 (parallel): update mmask[] using atomic-OR
        //   Phase 2 (parallel): scan mmask[] to build valid_movie_list

        const int n_threads = pool.num_threads;

        // Collect all valid info_type ranges
        struct ItRange { int32_t beg, end; };
        std::vector<ItRange> it_ranges;
        for (int32_t it_id : valid_it_ids) {
            if (it_id < 0 || it_id > max_type) continue;
            it_ranges.push_back({tps[it_id], tpe[it_id]});
        }

        // Phase 1: parallel update of mmask[] using atomic OR.
        //   Split each info_type range across threads.
        //   Concurrent OR to same mmask[mid] is safe with __atomic_fetch_or.
        std::vector<int64_t> thr_sc(n_threads,0), thr_em(n_threads,0);
        {
            // Flatten all rows across all info_types into a single virtual range
            // and distribute evenly across threads.
            int64_t total_rows = 0;
            std::vector<int64_t> range_start_global; // global row index of start of each range
            for (auto& r : it_ranges) {
                range_start_global.push_back(total_rows);
                total_rows += r.end - r.beg;
            }

            pool.parallel_for([&](int tid, int n_thr) {
                int64_t chunk = (total_rows + n_thr - 1) / n_thr;
                int64_t g_beg = (int64_t)tid * chunk;
                int64_t g_end = std::min(g_beg + chunk, total_rows);
                int64_t sc = 0, em = 0;

                // Find which ranges overlap with [g_beg, g_end)
                for (int ri = 0; ri < (int)it_ranges.size(); ++ri) {
                    int64_t rg_beg = range_start_global[ri];
                    int64_t rg_end = rg_beg + (it_ranges[ri].end - it_ranges[ri].beg);
                    if (rg_end <= g_beg || rg_beg >= g_end) continue;

                    int64_t local_beg = std::max(g_beg, rg_beg) - rg_beg;
                    int64_t local_end = std::min(g_end, rg_end) - rg_beg;
                    int32_t row_beg = it_ranges[ri].beg + (int32_t)local_beg;
                    int32_t row_end = it_ranges[ri].beg + (int32_t)local_end;

                    for (int32_t r = row_beg; r < row_end; ++r) {
                        ++sc;
                        int32_t mid = mi_mid[r];
                        if ((uint32_t)mid > (uint32_t)max_title_id || !mok[mid]) continue;
                        int32_t dict_id = mi_iid[r];
                        if ((uint32_t)dict_id >= (uint32_t)n_dict) continue;
                        uint8_t  bit = dtb[dict_id];
                        if (!bit) continue;
                        __atomic_fetch_or((uint8_t*)&mmask[mid], (uint8_t)bit, __ATOMIC_RELAXED);
                        ++em;
                    }
                }
                thr_sc[tid] = sc;
                thr_em[tid] = em;
            });
        }
        for (int t = 0; t < n_threads; ++t) {
            rows_scanned += thr_sc[t];
            rows_emitted += thr_em[t];
        }

        // Phase 2: build valid_movie_list by scanning mmask[] (only for movie_ok entries).
        //   Parallelize by movie_id range; each thread appends to local list; merge.
        {
            std::vector<std::vector<int32_t>> thr_vml(n_threads);
            pool.parallel_for([&](int tid, int n_thr) {
                int32_t chunk = (max_title_id + 1 + n_thr - 1) / n_thr;
                int32_t beg   = tid * chunk;
                int32_t end   = std::min(beg + chunk, max_title_id + 1);
                auto& lv = thr_vml[tid];
                for (int32_t mid = beg; mid < end; ++mid) {
                    if (mok[mid] && mmask[mid])
                        lv.push_back(mid);
                }
            });
            for (int t = 0; t < n_threads; ++t) {
                for (int32_t mid : thr_vml[t])
                    valid_movie_list.push_back(mid);
            }
        }

        TRACE_COUNT("q10a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q10a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q10a_mi1_movie_groups", (int64_t)valid_movie_list.size());
    }
    // Sort valid_movie_list by movie_id to improve cast_info scan locality.
    // Use 2-pass 12-bit radix sort: faster than std::sort for 212K integers.
    // Movie IDs fit in ~22 bits at SF=2 (max ~5M); 2 × 12-bit passes suffice.
    // 2 × 212K = 424K scatter ops vs std::sort's ~3.75M comparisons.
    {
        PROFILE_SCOPE("q10a_vml_sort");
        const int32_t vml_n = (int32_t)valid_movie_list.size();
        if (vml_n > 1) {
            constexpr int    R  = 12;
            constexpr int    SZ = 1 << R;  // 4096
            constexpr uint32_t M = SZ - 1;

            std::vector<int32_t> tmp_vml(vml_n);
            int32_t h0[SZ] = {}, h1[SZ] = {};

            for (int32_t i = 0; i < vml_n; ++i) {
                uint32_t v = (uint32_t)valid_movie_list[i];
                h0[v & M]++;
                h1[(v >> R) & M]++;
            }
            {
                int32_t s0 = 0, s1 = 0;
                for (int k = 0; k < SZ; ++k) {
                    int32_t c0 = h0[k]; h0[k] = s0; s0 += c0;
                    int32_t c1 = h1[k]; h1[k] = s1; s1 += c1;
                }
            }
            for (int32_t i = 0; i < vml_n; ++i) {
                uint32_t key = (uint32_t)valid_movie_list[i] & M;
                tmp_vml[h0[key]++] = valid_movie_list[i];
            }
            for (int32_t i = 0; i < vml_n; ++i) {
                uint32_t key = ((uint32_t)tmp_vml[i] >> R) & M;
                valid_movie_list[h1[key]++] = tmp_vml[i];
            }
        }
    }

    // movie_year lookup done on-demand in Phase 3 (only ~17 persons × 22 entries).

    // -----------------------------------------------------------------------
    // 7. Main join — optimized Phase A with movie_iid_mask pre-filter.
    //
    //    Original Phase A collected ALL 3.9M role-passing pairs, then sorted
    //    by person_id, then Phase C processed 3.9M pairs doing ILIKE at 761K
    //    group boundaries.
    //
    //    KEY OPTIMIZATION: In Phase A, add `iid_mask[mid] != 0` filter.
    //    Only 26K pairs pass both role AND movie_iid_mask → sort 26K (trivial).
    //    ILIKE runs for only ~5K unique persons (vs 761K) in Phase C.
    //
    //    The additional `iid_mask[mid]` read per role-passing row adds ~58M cy
    //    (3.9M reads into 20MB array), but saves:
    //      - Phase B sort: 29M cy (sorting 3.9M → 26K pairs)
    //      - Phase C: 70M cy (processing 3.9M → 26K pairs, 761K → 5K ILIKE)
    //    Net gain: ~(29+70-58) = ~41M cy savings.
    // -----------------------------------------------------------------------
    struct Agg {
        int32_t min_yr = std::numeric_limits<int32_t>::max();
        int32_t max_yr = std::numeric_limits<int32_t>::min();
    };

    struct GroupKey {
        int32_t     iid;
        std::string name;
        bool operator==(const GroupKey& o) const {
            return iid == o.iid && name == o.name;
        }
    };
    struct GroupKeyHash {
        size_t operator()(const GroupKey& k) const {
            size_t h1 = std::hash<int32_t>{}(k.iid);
            size_t h2 = std::hash<std::string>{}(k.name);
            return h1 ^ (h2 * 2654435761ULL);
        }
    };
    std::unordered_map<GroupKey, Agg, GroupKeyHash> agg_map;
    agg_map.reserve(16384);

    [[maybe_unused]] const int32_t max_person_id = (int32_t)db->name.id_to_row.size() - 1;

    {
        PROFILE_SCOPE("q10a_cast_info_join");
        int64_t ci_rows_scanned=0, ci_rows_movie_visited=0, ci_rows_role_pass=0;
        int64_t ci_rows_movie_pass=0, ci_rows_person_pass=0, ci_persons_first_seen=0;
        const auto& ci  = db->cast_info;
        const auto& nm  = db->name;
        const auto& csr = ci.movie_id_csr;

        const int n_threads = pool.num_threads;
        const int32_t  n_valid   = (int32_t)valid_movie_list.size();
        const int32_t* __restrict__ ci_pid   = ci.person_id.data();
        const int32_t* __restrict__ ci_rid   = ci.role_id.data();
        const int32_t* __restrict__ csr_offs = csr.offsets.data();
        const int32_t* __restrict__ vml      = valid_movie_list.data();
        const int32_t* __restrict__ nm_id2row_ptr = nm.id_to_row.data();
        const int32_t  nm_id2row_size             = (int32_t)nm.id_to_row.size();

        // Per-thread chunk boundaries over valid_movie_list
        // Partition by cast_info row count (not movie count) for load balance.
        // Compute prefix sums of CSR row counts for valid movies.
        std::vector<int64_t> movie_ci_prefix(n_valid + 1, 0);
        for (int32_t vi = 0; vi < n_valid; ++vi) {
            int32_t mid = vml[vi];
            movie_ci_prefix[vi + 1] = movie_ci_prefix[vi] + (csr_offs[mid + 1] - csr_offs[mid]);
        }
        const int64_t total_ci = movie_ci_prefix[n_valid];

        std::vector<int32_t> chunk_beg(n_threads + 1);
        chunk_beg[0] = 0;
        for (int t = 1; t < n_threads; ++t) {
            int64_t target = (int64_t)t * total_ci / n_threads;
            // Binary search for the first vi where movie_ci_prefix[vi] >= target
            int32_t lo = chunk_beg[t-1], hi = n_valid;
            while (lo < hi) {
                int32_t mid_vi = (lo + hi) / 2;
                if (movie_ci_prefix[mid_vi] < target) lo = mid_vi + 1;
                else hi = mid_vi;
            }
            chunk_beg[t] = lo;
        }
        chunk_beg[n_threads] = n_valid;

        // -------------------------------------------------------------------
        // 3-phase approach:
        //   Phase 1: CSR scan (by movie) — build person_seen + per-thread pid lists
        //   Phase 2: ILIKE check for seen pids (parallel, per-thread)
        //   Phase 3: For each ILIKE-matching person, use person_id_csr to aggregate
        //            (eliminates Phase AC which re-scanned all 51.8M cast_info rows)
        //   At SF=5 only ~17 persons pass ILIKE → Phase 3 is O(17 × 22) = trivial.
        // -------------------------------------------------------------------

        const int32_t max_pid = (int32_t)nm.id_to_row.size();
        // Use uint64_t bitset (2.6MB vs 20.8MB byte array) to reduce cold-cache
        // page-fault overhead. Each cache line covers 512 person_ids (vs 64).
        // 8x fewer cache lines needed → 8x fewer L3 misses on first access.
        const size_t ps_words = ((size_t)(max_pid + 1) + 63) / 64;
        std::vector<uint64_t> person_seen(ps_words, 0);
        uint64_t* __restrict__ ps_base = person_seen.data();
        std::vector<std::vector<int32_t>> thr_seen_pids(n_threads);

        // Bitset helper inlines
        auto ps_test = [&](int32_t pid) -> bool {
            return (ps_base[(uint32_t)pid >> 6] >> ((uint32_t)pid & 63)) & 1;
        };
        auto ps_set = [&](int32_t pid) {
            // Non-atomic OR (benign race: monotone 0→1 writes)
            ps_base[(uint32_t)pid >> 6] |= (1ULL << ((uint32_t)pid & 63));
        };

        {
            PROFILE_SCOPE("q10a_phase_a_pass1");
            std::vector<int64_t> thr_sc(n_threads,0), thr_rp(n_threads,0), thr_vi(n_threads,0);
            pool.parallel_for([&](int tid, int /*n_thr*/) {
                int32_t beg = chunk_beg[tid];
                int32_t end = chunk_beg[tid + 1];
                int64_t lsc = 0, lrp = 0, lvi = 0;
                auto& lpids = thr_seen_pids[tid];
                lpids.reserve(65536);
                int32_t prev_pid_seen = -1; // cache last-added pid to avoid re-check
                constexpr int32_t PFDIST = 16; // prefetch distance
                for (int32_t vi = beg; vi < end; ++vi) {
                    int32_t mid       = vml[vi];
                    int32_t csr_begin = csr_offs[mid];
                    int32_t csr_end   = csr_offs[mid + 1];
                    ++lvi;
                    for (int32_t r = csr_begin; r < csr_end; ++r) {
                        ++lsc;
                        int32_t rid = ci_rid[r];
                        // Prefetch person_seen bitset word for upcoming pid
                        if (r + PFDIST < csr_end) {
                            uint32_t pid_ahead = (uint32_t)ci_pid[r + PFDIST];
                            if (pid_ahead < (uint32_t)max_pid)
                                __builtin_prefetch(ps_base + (pid_ahead >> 6), 0, 1);
                        }
                        if ((uint32_t)rid > (uint32_t)role_id_ok_max || !role_id_ok[rid]) continue;
                        ++lrp;
                        int32_t pid = ci_pid[r];
                        if ((uint32_t)pid < (uint32_t)max_pid && pid != prev_pid_seen) {
                            if (!ps_test(pid)) {
                                ps_set(pid);
                                lpids.push_back(pid);
                            }
                            prev_pid_seen = pid;
                        }
                    }
                }
                thr_sc[tid] = lsc; thr_rp[tid] = lrp; thr_vi[tid] = lvi;
            });
            for (int t = 0; t < n_threads; ++t) {
                ci_rows_scanned += thr_sc[t]; ci_rows_role_pass += thr_rp[t]; ci_rows_movie_visited += thr_vi[t];
            }
        }

        // Phase 2: ILIKE check for seen pids (each thread checks its own list).
        // Collect matching (pid, nm_row) pairs for Phase 3 aggregation.
        struct MatchPerson { int32_t pid; int32_t nm_row; };
        std::vector<std::vector<MatchPerson>> thr_match(n_threads);
        int64_t n_prescan_pids = 0;
        {
            PROFILE_SCOPE("q10a_name_prescan");
            const int32_t nm_rows_sz = (int32_t)nm.name_str.size();
            std::vector<int64_t> thr_cnt(n_threads, 0);
            pool.parallel_for([&](int tid, int n_thr) {
                const auto& lpids = thr_seen_pids[tid];
                const int32_t n_lpids = (int32_t)lpids.size();
                const int32_t* lp = lpids.data();
                constexpr int32_t PFDIST = 8;
                int64_t cnt = 0;
                for (int32_t i = 0; i < n_lpids; ++i) {
                    // Prefetch nm_id2row_ptr for upcoming pid
                    if (i + PFDIST < n_lpids) {
                        int32_t ahead_pid = lp[i + PFDIST];
                        if ((uint32_t)ahead_pid < (uint32_t)max_pid)
                            __builtin_prefetch(nm_id2row_ptr + ahead_pid, 0, 1);
                    }
                    int32_t pid = lp[i];
                    int32_t nr = nm_id2row_ptr[pid];
                    ++cnt;
                    if (nr < 0 || nr >= nm_rows_sz) continue;
                    if (name_matches(nm.name_str[nr]))
                        thr_match[tid].push_back({pid, nr});
                }
                thr_cnt[tid] = cnt;
            });
            for (int t = 0; t < n_threads; ++t) n_prescan_pids += thr_cnt[t];
            TRACE_COUNT("q10a_name_prescan_pids", n_prescan_pids);
        }

        // Phase 3: for each matching person, use person_id_csr to find their cast entries.
        // Check role_ok AND movie_ok AND iid_mask → aggregate (iid, name, year).
        // With only ~17 persons at SF=5, this is O(17 × 22) = trivial.
        using LocalMap = std::unordered_map<GroupKey, Agg, GroupKeyHash>;
        std::vector<LocalMap> local_maps(n_threads);
        for (auto& lm : local_maps) lm.reserve(256);

        std::vector<int64_t> thr_person_pass(n_threads, 0);
        std::vector<int64_t> thr_movie_pass(n_threads, 0);
        std::vector<int64_t> thr_persons_seen2(n_threads, 0);

        {
            PROFILE_SCOPE("q10a_phase_ac");
            const auto& pid_csr = ci.person_id_csr;
            const int32_t* __restrict__ ci_mid       = ci.movie_id.data();
            const int32_t* __restrict__ pid_csr_offs = pid_csr.offsets.data();
            const int32_t* __restrict__ pid_csr_vals = pid_csr.values.data();
            const int32_t  pid_csr_size              = (int32_t)pid_csr.offsets.size();
            const uint8_t*  __restrict__ iid_mask    = movie_iid_mask.data();
            const uint8_t*  __restrict__ mok_arr     = movie_ok.data();
            // On-demand year lookup via title.id_to_row + production_year
            const int32_t* __restrict__ id2row_ptr = db->title.id_to_row.data();
            const int32_t  id2row_sz               = (int32_t)db->title.id_to_row.size();
            const int32_t* __restrict__ py_ptr     = db->title.production_year.data();

            pool.parallel_for([&](int tid, int /*n_thr*/) {
                LocalMap& lagg = local_maps[tid];
                int64_t lperson_pass = 0, lmovie_pass = 0, lpersons_seen = 0;

                for (auto& mp : thr_match[tid]) {
                    int32_t pid    = mp.pid;
                    int32_t nm_row = mp.nm_row;
                    ++lpersons_seen;

                    if ((uint32_t)pid + 1 >= (uint32_t)pid_csr_size) continue;
                    int32_t ci_beg = pid_csr_offs[pid];
                    int32_t ci_end = pid_csr_offs[pid + 1];
                    const std::string& nm_str = nm.name_str[nm_row];

                    // pid_csr.values[vi] is the actual cast_info row index
                    for (int32_t vi = ci_beg; vi < ci_end; ++vi) {
                        int32_t r = pid_csr_vals[vi];
                        int32_t rid = ci_rid[r];
                        if ((uint32_t)rid > (uint32_t)role_id_ok_max || !role_id_ok[rid]) continue;

                        int32_t mid = ci_mid[r];
                        if ((uint32_t)mid > (uint32_t)max_title_id) continue;
                        if (!mok_arr[mid]) continue;
                        uint8_t  mask = iid_mask[mid];
                        if (!mask) continue;

                        ++lperson_pass;
                        ++lmovie_pass;

                        // On-demand year lookup (trivial: ~374 lookups total)
                        int32_t yr = 0;
                        if ((uint32_t)mid < (uint32_t)id2row_sz) {
                            int32_t row = id2row_ptr[mid];
                            if (row >= 0) yr = py_ptr[row];
                        }
                        uint32_t m = mask;
                        while (m) {
                            int32_t iid = __builtin_ctz(m);
                            m &= m - 1;
                            auto& agg = lagg[GroupKey{iid, nm_str}];
                            if (yr > 0) {
                                if (yr < agg.min_yr) agg.min_yr = yr;
                                if (yr > agg.max_yr) agg.max_yr = yr;
                            }
                        }
                    }
                }
                thr_person_pass[tid]  = lperson_pass;
                thr_movie_pass[tid]   = lmovie_pass;
                thr_persons_seen2[tid] = lpersons_seen;
            });
        }

        // Merge counts and agg_maps
        for (int t = 0; t < n_threads; ++t) {
            ci_rows_person_pass   += thr_person_pass[t];
            ci_rows_movie_pass    += thr_movie_pass[t];
            ci_persons_first_seen += thr_persons_seen2[t];

            for (auto& [key, local_agg] : local_maps[t]) {
                auto& agg = agg_map[key];
                if (local_agg.min_yr < agg.min_yr) agg.min_yr = local_agg.min_yr;
                if (local_agg.max_yr > agg.max_yr) agg.max_yr = local_agg.max_yr;
            }
        }
        TRACE_COUNT("q10a_ci_rows_scanned",     ci_rows_scanned);
        TRACE_COUNT("q10a_ci_rows_role_pass",   ci_rows_role_pass);
        TRACE_COUNT("q10a_ci_movies_visited",   ci_rows_movie_visited);
        TRACE_COUNT("q10a_rp_pairs_collected",  ci_rows_role_pass);
        TRACE_COUNT("q10a_ci_rows_movie_pass",  ci_rows_movie_pass);
        TRACE_COUNT("q10a_ci_rows_person_pass", ci_rows_person_pass);
        TRACE_COUNT("q10a_ci_persons_first_seen", ci_persons_first_seen);
        TRACE_COUNT("q10a_output_groups",        (int64_t)agg_map.size());
    }

    TRACE_COUNT("q10a_query_output_rows", (int64_t)agg_map.size());

    // -----------------------------------------------------------------------
    // 8. Assemble output rows
    // -----------------------------------------------------------------------
    std::vector<std::vector<std::string>> rows;
    rows.push_back({"name", "info",
                    "min(t.production_year)", "max(t.production_year)"});

    for (const auto& [key, agg] : agg_map) {
        const std::string& info_str = info_strs[key.iid];

        std::string min_s = (agg.min_yr == std::numeric_limits<int32_t>::max())
                            ? "" : std::to_string(agg.min_yr);
        std::string max_s = (agg.max_yr == std::numeric_limits<int32_t>::min())
                            ? "" : std::to_string(agg.max_yr);

        rows.push_back({key.name, info_str, min_s, max_s});
    }

    return rows;
}
