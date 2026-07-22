#include "query7a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <cstdint>
#include <cmath>
#include <limits>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
/** SELECT COUNT(*)
FROM title as t,
movie_info as mi1,
kind_type as kt,
info_type as it1,
info_type as it3,
info_type as it4,
movie_info_idx as mii1,
movie_info_idx as mii2,
movie_keyword as mk,
keyword as k,
aka_name as an,
name as n,
info_type as it5,
person_info as pi1,
cast_info as ci,
role_type as rt
WHERE
t.id = mi1.movie_id
AND t.id = ci.movie_id
AND t.id = mii1.movie_id
AND t.id = mii2.movie_id
AND t.id = mk.movie_id
AND mk.keyword_id = k.id
AND mi1.info_type_id = it1.id
AND mii1.info_type_id = it3.id
AND mii2.info_type_id = it4.id
AND t.kind_id = kt.id
AND (kt.kind IN KIND)
AND (t.production_year <= YEAR1)
AND (t.production_year >= YEAR2)
AND (mi1.info IN INFO1)
AND (it1.id IN ID1)
AND it3.id = ID2
AND it4.id = ID3
AND (mii2.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND mii2.info::float <= INFO2)
AND (mii2.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND INFO3 <= mii2.info::float)
AND (mii1.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND INFO4 <= mii1.info::float)
AND (mii1.info ~ '^(?:[1-9]\d*|0)?(?:\.\d+)?$' AND mii1.info::float <= INFO5)
AND n.id = ci.person_id
AND ci.person_id = pi1.person_id
AND it5.id = pi1.info_type_id
AND n.id = pi1.person_id
AND n.id = an.person_id
AND rt.id = ci.role_id
AND (n.gender IN GENDER)
AND (n.name_pcode_nf IN NAME_PCODE_NF)
AND (ci.note IN NOTE)
AND (rt.role IN ROLE)
AND (it5.id IN ID4) */

std::vector<std::vector<std::string>> run_q7a(Database* db, const Q7aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q7a: db is null");
    }
    PROFILE_SCOPE("q7a_total");

    static constexpr int MAX_WIN = 8;

    // -----------------------------------------------------------------------
    // Null-sentinel helper
    // -----------------------------------------------------------------------
    auto is_null = [](const std::string& s) {
        return s == "<<NULL>>" || s == "NULL";
    };

    // -----------------------------------------------------------------------
    // Parse scalar parameters
    // -----------------------------------------------------------------------
    int year1 = -1, year2 = -1;
    if (!args.YEAR1.empty() && !is_null(args.YEAR1)) year1 = std::stoi(args.YEAR1);
    if (!args.YEAR2.empty() && !is_null(args.YEAR2)) year2 = std::stoi(args.YEAR2);

    int32_t id2 = -1, id3 = -1;
    if (!args.ID2.empty() && !is_null(args.ID2)) id2 = std::stoi(args.ID2);
    if (!args.ID3.empty() && !is_null(args.ID3)) id3 = std::stoi(args.ID3);

    float info2 =  std::numeric_limits<float>::max();  // mii2 upper bound
    float info3 = -std::numeric_limits<float>::max();  // mii2 lower bound
    float info4 = -std::numeric_limits<float>::max();  // mii1 lower bound
    float info5 =  std::numeric_limits<float>::max();  // mii1 upper bound
    if (!args.INFO2.empty() && !is_null(args.INFO2)) info2 = std::stof(args.INFO2);
    if (!args.INFO3.empty() && !is_null(args.INFO3)) info3 = std::stof(args.INFO3);
    if (!args.INFO4.empty() && !is_null(args.INFO4)) info4 = std::stof(args.INFO4);
    if (!args.INFO5.empty() && !is_null(args.INFO5)) info5 = std::stof(args.INFO5);

    // -----------------------------------------------------------------------
    // Resolve ID1 set: valid info_type_ids for mi1  (it1.id IN ID1)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> id1_set;
    for (const auto& s : args.ID1)
        if (!is_null(s)) id1_set.insert(std::stoi(s));

    // -----------------------------------------------------------------------
    // Resolve ID4 set: valid info_type_ids for pi1  (it5.id IN ID4)
    // -----------------------------------------------------------------------
    std::vector<int32_t> id4_vec;
    for (const auto& s : args.ID4)
        if (!is_null(s)) id4_vec.push_back(std::stoi(s));
    std::sort(id4_vec.begin(), id4_vec.end());
    id4_vec.erase(std::unique(id4_vec.begin(), id4_vec.end()), id4_vec.end());

    // -----------------------------------------------------------------------
    // Resolve valid kind_ids from kind_type  (kt.kind IN KIND)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_kind_ids;
    {
        std::unordered_set<std::string> kind_str_set;
        bool kind_null_ok = false;
        for (const auto& s : args.KIND) {
            if (is_null(s)) kind_null_ok = true;
            else            kind_str_set.insert(s);
        }
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_str_set.count(kt.kind[i]))
                valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
        TRACE_COUNT("q7a_valid_kind_ids", (int64_t)valid_kind_ids.size());
    }

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
        TRACE_COUNT("q7a_valid_role_ids", (int64_t)valid_role_ids.size());
    }

    // -----------------------------------------------------------------------
    // Build note set for cast_info  (ci.note IN NOTE)
    // NULL in the IN-list follows SQL semantics: x = NULL is always UNKNOWN,
    // so NULLs in the list never match any row.  Simply ignore them.
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> note_set;
    for (const auto& s : args.NOTE)
        if (!is_null(s)) note_set.insert(s);

    // Early exit: if NOTE set is empty, ci.note IN NOTE is always false =>
    // the entire join produces 0 rows.
    if (note_set.empty()) {
        TRACE_COUNT("q7a_early_exit_empty_note", 1);
        std::vector<std::vector<std::string>> rows;
        rows.push_back({"count_star()"});
        rows.push_back({"0"});
        return rows;
    }

    TRACE_COUNT("q7a_note_set_size", (int64_t)note_set.size());

    // -----------------------------------------------------------------------
    // Gender filter  (n.gender IN GENDER)
    // NULL in the IN-list does not match anything (SQL semantics).
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> gender_set;
    for (const auto& s : args.GENDER)
        if (!is_null(s)) gender_set.insert(s);
    TRACE_COUNT("q7a_gender_set_size", (int64_t)gender_set.size());

    // -----------------------------------------------------------------------
    // name_pcode_nf filter  (n.name_pcode_nf IN NAME_PCODE_NF)
    // NULL in the IN-list does not match anything (SQL semantics).
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> pcode_set;
    for (const auto& s : args.NAME_PCODE_NF)
        if (!is_null(s)) pcode_set.insert(s);
    TRACE_COUNT("q7a_pcode_set_size", (int64_t)pcode_set.size());

    // -----------------------------------------------------------------------
    // INFO1 string set for mi1  (mi1.info IN INFO1)
    // Uses comma-join reconstruction to reassemble tokens split by the parser.
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> info1_set;
    bool info1_null_ok = false;
    for (const auto& s : args.INFO1) {
        if (is_null(s)) info1_null_ok = true;
        else            info1_set.insert(s);
    }
    {
        const std::vector<std::string>& tokens = args.INFO1;
        if (!tokens.empty() && !id1_set.empty()) {
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
                for (int32_t r = 0; r < (int32_t)mi.movie_id.size(); ++r) {
                    if (!id1_set.count(mi.info_type_id[r])) continue;
                    if (cands.count(mi.info_str[r])) found.insert(mi.info_str[r]);
                }
                if (!found.empty()) {
                    info1_set.clear();
                    int32_t i = 0;
                    while (i < n_tok) {
                        if (is_null(tokens[i])) { ++i; continue; }
                        bool matched = false;
                        for (int32_t len = std::min((int32_t)MAX_WIN, n_tok - i);
                             len >= 2 && !matched; --len) {
                            for (const char* sep : {", ", ","}) {
                                std::string c = tokens[i];
                                for (int32_t k2 = 1; k2 < len; ++k2) {
                                    c += sep;
                                    c += tokens[i + k2];
                                }
                                if (found.count(c)) {
                                    info1_set.insert(c);
                                    i += len;
                                    matched = true;
                                    break;
                                }
                            }
                        }
                        if (!matched) {
                            info1_set.insert(tokens[i]);
                            ++i;
                        }
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Build mii1_count[movie_id]:
    //   # movie_info_idx rows with info_type_id == id2  (it3.id = ID2, alias mii1)
    //   AND info_float is valid AND INFO4 <= info_float <= INFO5
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int32_t> mii1_count;
    if (id2 >= 0) {
        PROFILE_SCOPE("q7a_mii1_build");
        const auto& mii = db->movie_info_idx;
        int32_t beg = 0, end = (int32_t)mii.movie_id.size();
        if (id2 < (int32_t)mii.type_part_start.size()) {
            beg = mii.type_part_start[id2];
            end = mii.type_part_end[id2];
        }
        int64_t rows_emitted = 0;
        for (int32_t r = beg; r < end; ++r) {
            if (mii.info_type_id[r] != id2) continue;
            float fv = mii.info_float[r];
            if (std::isnan(fv)) continue;
            if (fv < info4 || fv > info5) continue;
            ++mii1_count[mii.movie_id[r]];
            ++rows_emitted;
        }
        TRACE_COUNT("q7a_mii1_rows_scanned", (int64_t)(end - beg));
        TRACE_COUNT("q7a_mii1_rows_emitted", rows_emitted);
        TRACE_COUNT("q7a_mii1_groups",       (int64_t)mii1_count.size());
    }

    // -----------------------------------------------------------------------
    // Build mii2_count[movie_id]:
    //   # movie_info_idx rows with info_type_id == id3  (it4.id = ID3, alias mii2)
    //   AND info_float is valid AND INFO3 <= info_float <= INFO2
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int32_t> mii2_count;
    if (id3 >= 0) {
        PROFILE_SCOPE("q7a_mii2_build");
        const auto& mii = db->movie_info_idx;
        int32_t beg = 0, end = (int32_t)mii.movie_id.size();
        if (id3 < (int32_t)mii.type_part_start.size()) {
            beg = mii.type_part_start[id3];
            end = mii.type_part_end[id3];
        }
        int64_t rows_emitted = 0;
        for (int32_t r = beg; r < end; ++r) {
            if (mii.info_type_id[r] != id3) continue;
            float fv = mii.info_float[r];
            if (std::isnan(fv)) continue;
            if (fv < info3 || fv > info2) continue;
            ++mii2_count[mii.movie_id[r]];
            ++rows_emitted;
        }
        TRACE_COUNT("q7a_mii2_rows_scanned", (int64_t)(end - beg));
        TRACE_COUNT("q7a_mii2_rows_emitted", rows_emitted);
        TRACE_COUNT("q7a_mii2_groups",       (int64_t)mii2_count.size());
    }

    // -----------------------------------------------------------------------
    // Build mi1_count[movie_id]:
    //   # movie_info rows where info_type_id IN ID1 AND info IN INFO1
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int32_t> mi1_count;
    if (!id1_set.empty()) {
        PROFILE_SCOPE("q7a_mi1_build");
        int64_t rows_scanned = 0, rows_emitted = 0;
        const auto& mi = db->movie_info;
        // Use type_part_start/end partition index to scan only rows with matching info_type_id
        for (int32_t it_id : id1_set) {
            int32_t beg = 0, end = (int32_t)mi.movie_id.size();
            if (it_id >= 0 && it_id < (int32_t)mi.type_part_start.size()) {
                beg = mi.type_part_start[it_id];
                end = mi.type_part_end[it_id];
            }
            for (int32_t r = beg; r < end; ++r) {
                ++rows_scanned;
                if (mi.info_type_id[r] != it_id) continue;
                const std::string& inf = mi.info_str[r];
                bool ok = inf.empty() ? info1_null_ok : info1_set.count(inf) > 0;
                if (!ok) continue;
                ++mi1_count[mi.movie_id[r]];
                ++rows_emitted;
            }
        }
        TRACE_COUNT("q7a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q7a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q7a_mi1_groups",       (int64_t)mi1_count.size());
    }


    // -----------------------------------------------------------------------
    // Build movie_contrib[movie_id] using smart join order:
    //   Drive from the smallest movie-side set (mii1_count), then probe
    //   mii2_count, mi1_count, title (kind+year), and mk CSR.
    //   This avoids a full scan of movie_keyword (9M rows) and title (5M rows).
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, int64_t> movie_contrib;
    {
        PROFILE_SCOPE("q7a_movie_join");
        const auto& t  = db->title;
        const auto& mk = db->movie_keyword;
        int64_t rows_emitted = 0;

        // Helper: check title for kind+year filter using id_to_row lookup
        auto title_ok = [&](int32_t mid) -> bool {
            if (mid < 0 || mid >= (int32_t)t.id_to_row.size()) return false;
            int32_t tr = t.id_to_row[mid];
            if (tr < 0) return false;
            if (!valid_kind_ids.count(t.kind_id[tr])) return false;
            int32_t py = t.production_year[tr];
            if (py == -1) return false;
            if (year1 >= 0 && py > year1) return false;
            if (year2 >= 0 && py < year2) return false;
            return true;
        };

        if (!mii1_count.empty()) {
            // Drive from mii1 (typically smallest movie set)
            TRACE_COUNT("q7a_movie_join_driver_size", (int64_t)mii1_count.size());
            for (auto& [mid, mm1_cnt] : mii1_count) {
                // Probe mii2
                auto mm2_it = mii2_count.find(mid);
                if (mm2_it == mii2_count.end()) continue;
                // Probe mi1
                auto mi1_it = mi1_count.find(mid);
                if (!mi1_count.empty() && mi1_it == mi1_count.end()) continue;
                // Check title kind+year via id_to_row
                if (!title_ok(mid)) continue;
                // Count mk rows via CSR (no full scan)
                auto [mk_beg, mk_end] = mk.movie_id_csr.range(mid);
                int32_t mk_cnt = mk_end - mk_beg;
                if (mk_cnt == 0) continue;

                int64_t contrib = (int64_t)mk_cnt
                                * (mi1_it != mi1_count.end() ? (int64_t)mi1_it->second : 1LL)
                                * (int64_t)mm1_cnt
                                * (int64_t)mm2_it->second;
                movie_contrib[mid] = contrib;
                ++rows_emitted;
            }
        } else if (!mii2_count.empty()) {
            // Drive from mii2
            TRACE_COUNT("q7a_movie_join_driver_size", (int64_t)mii2_count.size());
            for (auto& [mid, mm2_cnt] : mii2_count) {
                auto mi1_it = mi1_count.find(mid);
                if (!mi1_count.empty() && mi1_it == mi1_count.end()) continue;
                if (!title_ok(mid)) continue;
                auto [mk_beg, mk_end] = mk.movie_id_csr.range(mid);
                int32_t mk_cnt = mk_end - mk_beg;
                if (mk_cnt == 0) continue;
                int64_t contrib = (int64_t)mk_cnt
                                * (mi1_it != mi1_count.end() ? (int64_t)mi1_it->second : 1LL)
                                * 1LL
                                * (int64_t)mm2_cnt;
                movie_contrib[mid] = contrib;
                ++rows_emitted;
            }
        } else if (!mi1_count.empty()) {
            // Drive from mi1
            TRACE_COUNT("q7a_movie_join_driver_size", (int64_t)mi1_count.size());
            for (auto& [mid, mi1_cnt] : mi1_count) {
                if (!title_ok(mid)) continue;
                auto [mk_beg, mk_end] = mk.movie_id_csr.range(mid);
                int32_t mk_cnt = mk_end - mk_beg;
                if (mk_cnt == 0) continue;
                int64_t contrib = (int64_t)mk_cnt * (int64_t)mi1_cnt;
                movie_contrib[mid] = contrib;
                ++rows_emitted;
            }
        } else {
            // Fallback: full title scan
            PROFILE_SCOPE("q7a_title_scan_fallback");
            int64_t rows_scanned = 0;
            for (int32_t r = 0; r < (int32_t)t.id.size(); ++r) {
                ++rows_scanned;
                if (!valid_kind_ids.count(t.kind_id[r])) continue;
                int32_t py = t.production_year[r];
                if (py == -1) continue;
                if (year1 >= 0 && py > year1) continue;
                if (year2 >= 0 && py < year2) continue;
                int32_t mid = t.id[r];
                auto [mk_beg, mk_end] = mk.movie_id_csr.range(mid);
                int32_t mk_cnt = mk_end - mk_beg;
                if (mk_cnt == 0) continue;
                movie_contrib[mid] = (int64_t)mk_cnt;
                ++rows_emitted;
            }
            TRACE_COUNT("q7a_title_fallback_rows_scanned", rows_scanned);
        }

        TRACE_COUNT("q7a_title_rows_emitted", rows_emitted);
        TRACE_COUNT("q7a_movie_contrib_size", (int64_t)movie_contrib.size());
    }


    // -----------------------------------------------------------------------
    // Main join over name (person side):
    //   DuckDB-inspired join order: start from person_info (small after
    //   type-partition filter), then probe name (gender+pcode), aka_name,
    //   cast_info.  This avoids a full scan of the 4-8M name table.
    // -----------------------------------------------------------------------
    const auto& nm  = db->name;
    const auto& ci  = db->cast_info;
    const auto& pi1 = db->person_info;
    const auto& an  = db->aka_name;

    int64_t persons_gender_pass = 0, persons_pcode_pass = 0;
    int64_t persons_an_pass = 0, persons_pi_pass = 0;
    int64_t ci_rows_probed = 0, ci_rows_matched = 0;
    int64_t count = 0;

    {
    PROFILE_SCOPE("q7a_person_join");

    // Build per-person pi1_count using type_part_start/end, keyed by person_id.
    // Only scan the relevant partitions of person_info.
    // person_id -> pi1_count (only persons with pi1_count > 0 stored)
    std::unordered_map<int32_t, int32_t> pi1_person_count;
    {
        PROFILE_SCOPE("q7a_pi1_build");
        int64_t rows_scanned = 0;
        for (int32_t it_id : id4_vec) {
            int32_t beg = 0, end = (int32_t)pi1.person_id.size();
            if (it_id >= 0 && it_id < (int32_t)pi1.type_part_start.size()) {
                beg = pi1.type_part_start[it_id];
                end = pi1.type_part_end[it_id];
            }
            for (int32_t r = beg; r < end; ++r) {
                ++rows_scanned;
                if (pi1.info_type_id[r] != it_id) continue;
                ++pi1_person_count[pi1.person_id[r]];
            }
        }
        TRACE_COUNT("q7a_pi1_build_rows_scanned", rows_scanned);
        TRACE_COUNT("q7a_pi1_persons_found", (int64_t)pi1_person_count.size());
    }

    // Now iterate over persons with pi1 entries, filter via name table (gender+pcode),
    // check aka_name presence, then probe cast_info.
    for (auto& [pid, pi1_cnt] : pi1_person_count) {
        // Look up name row for this person
        int32_t nrow = -1;
        if (pid >= 0 && pid < (int32_t)nm.id_to_row.size())
            nrow = nm.id_to_row[pid];
        if (nrow < 0) continue;

        // n.gender IN GENDER
        if (!gender_set.count(nm.gender[nrow])) continue;
        ++persons_gender_pass;

        // n.name_pcode_nf IN NAME_PCODE_NF
        if (!pcode_set.count(nm.name_pcode_nf[nrow])) continue;
        ++persons_pcode_pass;

        // person_info count already in pi1_cnt
        int64_t pi1_count = pi1_cnt;
        ++persons_pi_pass;

        // aka_name: must have at least one row  (n.id = an.person_id)
        auto [an_beg, an_end] = an.person_id_csr.range(pid);
        int32_t an_count = an_end - an_beg;
        if (an_count == 0) continue;
        ++persons_an_pass;

        // cast_info: iterate ci rows for this person
        {
            auto [ci_beg, ci_end] = ci.person_id_csr.range(pid);
            for (int32_t ci_i = ci_beg; ci_i < ci_end; ++ci_i) {
                int32_t row = ci.person_id_csr.values[ci_i];
                ++ci_rows_probed;

                // rt.id = ci.role_id AND rt.role IN ROLE
                if (!valid_role_ids.count(ci.role_id[row])) continue;

                // ci.note IN NOTE
                const std::string& note_val = ci.note[row];
                if (!note_set.count(note_val)) continue;

                // t.id = ci.movie_id — movie must pass all movie-side filters
                auto mc_it = movie_contrib.find(ci.movie_id[row]);
                if (mc_it == movie_contrib.end()) continue;

                count += mc_it->second * pi1_count * (int64_t)an_count;
                ++ci_rows_matched;
            }
        }
    }
    } // end PROFILE_SCOPE q7a_person_join

    TRACE_COUNT("q7a_name_rows_total",     (int64_t)nm.id.size());
    TRACE_COUNT("q7a_persons_gender_pass", persons_gender_pass);
    TRACE_COUNT("q7a_persons_pcode_pass",  persons_pcode_pass);
    TRACE_COUNT("q7a_persons_an_pass",     persons_an_pass);
    TRACE_COUNT("q7a_persons_pi_pass",     persons_pi_pass);
    TRACE_COUNT("q7a_ci_rows_probed",      ci_rows_probed);
    TRACE_COUNT("q7a_ci_rows_matched",     ci_rows_matched);
    TRACE_COUNT("q7a_info1_set_size",      (int64_t)info1_set.size());
    TRACE_COUNT("q7a_query_output_rows",   1);

    // -----------------------------------------------------------------------
    // Return result
    // -----------------------------------------------------------------------
    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
