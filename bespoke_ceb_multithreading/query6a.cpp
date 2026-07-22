#include "query6a.hpp"
#include "trace.hpp"
#include "query_pool.hpp"
static ThreadPool& pool = get_query_pool();

#include <algorithm>
#include <immintrin.h>
#include <cstring>
#include <cassert>
#include <cstdint>
#include <cmath>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

// SQL:
// SELECT COUNT(*)
// FROM title t, movie_info mi1, kind_type kt,
//      info_type it1, info_type it3, info_type it4,
//      movie_info_idx mii1, movie_info_idx mii2,
//      aka_name an, name n, info_type it5,
//      person_info pi1, cast_info ci, role_type rt
// WHERE
//   t.id = mi1.movie_id AND t.id = ci.movie_id
//   AND t.id = mii1.movie_id AND t.id = mii2.movie_id
//   AND mi1.info_type_id = it1.id AND mii1.info_type_id = it3.id
//   AND mii2.info_type_id = it4.id AND t.kind_id = kt.id
//   AND (kt.kind IN KIND)
//   AND (t.production_year <= YEAR1) AND (t.production_year >= YEAR2)
//   AND (mi1.info IN INFO1) AND (it1.id IN ID1)
//   AND it3.id = ID2  AND it4.id = ID3
//   AND mii2 numeric: INFO3 <= mii2.info::float <= INFO2
//   AND mii1 numeric: INFO4 <= mii1.info::float <= INFO5
//   AND n.id = ci.person_id AND ci.person_id = pi1.person_id
//   AND it5.id = pi1.info_type_id AND n.id = an.person_id
//   AND rt.id = ci.role_id
//   AND (n.gender IN GENDER) AND (n.name_pcode_nf IN NAME_PCODE_NF)
//   AND (ci.note IN NOTE) AND (rt.role IN ROLE) AND (it5.id IN ID4)

std::vector<std::vector<std::string>> run_q6a(Database* db, const Q6aArgs& args) {
    if (!db) {
        throw std::runtime_error("run_q6a: db is null");
    }
    PROFILE_SCOPE("q6a_total");

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
    std::unordered_set<int32_t> id4_set;
    for (const auto& s : args.ID4)
        if (!is_null(s)) id4_set.insert(std::stoi(s));

    // -----------------------------------------------------------------------
    // Resolve valid kind_ids from kind_type  (kt.kind IN KIND)
    // -----------------------------------------------------------------------
    std::unordered_set<int32_t> valid_kind_ids;
    {
        std::unordered_set<std::string> kind_str_set;
        bool kind_null_ok = false;
        for (const auto& s : args.KIND)
        {
            if (is_null(s)) kind_null_ok = true;
            else            kind_str_set.insert(s);
        }
        const auto& kt = db->kind_type;
        for (size_t i = 0; i < kt.id.size(); ++i)
            if (kind_str_set.count(kt.kind[i]))
                valid_kind_ids.insert(kt.id[i]);
        if (kind_null_ok) valid_kind_ids.insert(-1);
        TRACE_COUNT("q6a_valid_kind_ids", (int64_t)valid_kind_ids.size());
    }

    // Build flat bool array for kind_id lookup (kind_ids are small integers, max ~7)
    int32_t max_kind_id = 0;
    for (int32_t k : valid_kind_ids) if (k > max_kind_id) max_kind_id = k;
    std::vector<uint8_t> kind_id_ok(max_kind_id + 2, 0);
    for (int32_t k : valid_kind_ids) if (k >= 0 && k <= max_kind_id) kind_id_ok[k] = 1;

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
        TRACE_COUNT("q6a_valid_role_ids", (int64_t)valid_role_ids.size());
    }

    // Build flat bool array for role_id lookup (role_ids are small integers, max ~12)
    int32_t max_role_id = 0;
    for (int32_t r : valid_role_ids) if (r > max_role_id) max_role_id = r;
    std::vector<uint8_t> role_id_ok(max_role_id + 2, 0);
    for (int32_t r : valid_role_ids) if (r >= 0) role_id_ok[r] = 1;

    // Build flat bool array for id4 (info_type_id for person_info)
    int32_t max_id4 = 0;
    for (int32_t x : id4_set) if (x > max_id4) max_id4 = x;
    std::vector<uint8_t> id4_ok(max_id4 + 2, 0);
    for (int32_t x : id4_set) if (x >= 0) id4_ok[x] = 1;

    // -----------------------------------------------------------------------
    // Build note set for cast_info  (ci.note IN NOTE)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> note_set;
    for (const auto& s : args.NOTE)
        if (!is_null(s)) note_set.insert(s);
    TRACE_COUNT("q6a_note_set_size", (int64_t)note_set.size());
    bool note_set_empty = note_set.empty();

    // -----------------------------------------------------------------------
    // EARLY EXIT #0: if note_set is empty, ci.note IN NOTE can never be
    // satisfied, so count=0.  Exit immediately before any expensive work.
    // This is the earliest possible check — only O(1) per-arg parsing done.
    // -----------------------------------------------------------------------
    if (note_set_empty) {
        // kind_ids and role_ids already emitted above; emit remaining counters
        TRACE_COUNT("q6a_mii1_rows_scanned",  0);
        TRACE_COUNT("q6a_mii1_rows_emitted",  0);
        TRACE_COUNT("q6a_mii1_groups",        0);
        TRACE_COUNT("q6a_mii2_rows_scanned",  0);
        TRACE_COUNT("q6a_mii2_rows_emitted",  0);
        TRACE_COUNT("q6a_mii2_groups",        0);
        TRACE_COUNT("q6a_mi1_rows_scanned",   0);
        TRACE_COUNT("q6a_mi1_rows_emitted",   0);
        TRACE_COUNT("q6a_mi1_groups",         0);
        TRACE_COUNT("q6a_title_driver",       0);
        TRACE_COUNT("q6a_driver_cands",       0);
        TRACE_COUNT("q6a_title_rows_scanned", 0);
        TRACE_COUNT("q6a_title_rows_emitted", 0);
        TRACE_COUNT("q6a_movie_contrib_size", 0);
        TRACE_COUNT("q6a_pi_candidates",      0);
        TRACE_COUNT("q6a_name_rows_total",    (int64_t)db->name.id.size());
        TRACE_COUNT("q6a_persons_gender_pass",0);
        TRACE_COUNT("q6a_persons_pcode_pass", 0);
        TRACE_COUNT("q6a_persons_an_pass",    0);
        TRACE_COUNT("q6a_persons_pi_pass",    0);
        TRACE_COUNT("q6a_ci_rows_probed",     0);
        TRACE_COUNT("q6a_ci_rows_matched",    0);
        TRACE_COUNT("q6a_info1_set_size",     0);
        TRACE_COUNT("q6a_gender_set_size",    0);
        TRACE_COUNT("q6a_pcode_set_size",     0);
        TRACE_COUNT("q6a_query_output_rows",  1);
        std::vector<std::vector<std::string>> early_rows;
        early_rows.push_back({"count_star()"});
        early_rows.push_back({"0"});
        return early_rows;
    }

    // -----------------------------------------------------------------------
    // Gender filter  (n.gender IN GENDER)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> gender_set;
    for (const auto& s : args.GENDER)
        if (!is_null(s)) gender_set.insert(s);
    TRACE_COUNT("q6a_gender_set_size", (int64_t)gender_set.size());

    // Build gender byte set for fast single-byte comparison
    uint8_t gender_byte_ok[256] = {};
    for (const auto& s : gender_set)
        if (!s.empty()) gender_byte_ok[(uint8_t)s[0]] = 1;
    bool gender_empty = gender_set.empty();

    // -----------------------------------------------------------------------
    // name_pcode_nf filter  (n.name_pcode_nf IN NAME_PCODE_NF)
    // -----------------------------------------------------------------------
    std::unordered_set<std::string> pcode_set;
    for (const auto& s : args.NAME_PCODE_NF)
        if (!is_null(s)) pcode_set.insert(s);
    TRACE_COUNT("q6a_pcode_set_size", (int64_t)pcode_set.size());

    // -----------------------------------------------------------------------
    // INFO1 string set for mi1  (mi1.info IN INFO1)
    // Uses comma-join reconstruction (same pattern as Q5a).
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
                                for (int32_t k = 1; k < len; ++k) {
                                    c += sep;
                                    c += tokens[i + k];
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

    // Common max movie_id bound (title table)
    int32_t mc_max_mid = (int32_t)db->title.id_to_row.size();

    // -----------------------------------------------------------------------
    // Pre-build name_row_pass[nrow]: sequential scan of name table to mark
    // rows passing gender + pcode filters.  This replaces per-person string
    // hash lookups in the hot person-join loop with O(1) byte array reads.
    // Sequential write once; random read only for pi_person_ids (~134K).
    // -----------------------------------------------------------------------
    const auto& nm  = db->name;
    const int32_t nm_nrows = (int32_t)nm.id.size();
    std::vector<uint8_t> name_row_pass; // name_row_pass[nrow]=1 if gender+pcode pass
    bool use_name_row_pass = false;
    if (!pcode_set.empty() && !gender_set.empty()) {
        PROFILE_SCOPE("q6a_name_row_pass_build");
        name_row_pass.assign(nm_nrows, 0);
        for (int32_t nrow = 0; nrow < nm_nrows; ++nrow) {
            uint8_t gb = nm.gender_byte[nrow];
            if (!gender_byte_ok[gb]) continue;
            if (pcode_set.count(nm.name_pcode_nf[nrow]))
                name_row_pass[nrow] = 1;
        }
        use_name_row_pass = true;
        TRACE_COUNT("q6a_name_row_pass_done", 1);
    } else if (!gender_set.empty()) {
        PROFILE_SCOPE("q6a_name_row_pass_build_gender");
        name_row_pass.assign(nm_nrows, 0);
        for (int32_t nrow = 0; nrow < nm_nrows; ++nrow) {
            uint8_t gb = nm.gender_byte[nrow];
            if (gender_byte_ok[gb]) name_row_pass[nrow] = 1;
        }
        use_name_row_pass = true;
        TRACE_COUNT("q6a_name_row_pass_done", 2);
    }

    // -----------------------------------------------------------------------
    // Build mii1_flat[movie_id]:
    //   # movie_info_idx rows with info_type_id == id2  (it3.id = ID2)
    //   AND info_float is valid AND INFO4 <= info_float <= INFO5.
    // -----------------------------------------------------------------------
    std::vector<int32_t> mii1_flat;
    bool use_mii1 = false;
    if (id2 >= 0) {
        PROFILE_SCOPE("q6a_mii1_build");
        const auto& mii = db->movie_info_idx;
        int32_t beg = 0, end = (int32_t)mii.movie_id.size();
        if (id2 < (int32_t)mii.type_part_start.size()) {
            beg = mii.type_part_start[id2];
            end = mii.type_part_end[id2];
        }
        mii1_flat.assign(mc_max_mid, 0);
        int64_t rows_emitted = 0;
        for (int32_t r = beg; r < end; ++r) {
            float fv = mii.info_float[r];
            if (std::isnan(fv)) continue;
            if (fv < info4 || fv > info5) continue;
            int32_t mid = mii.movie_id[r];
            if (mid >= 0 && mid < mc_max_mid) ++mii1_flat[mid];
            ++rows_emitted;
        }
        use_mii1 = true;
        int64_t groups = 0;
        for (int32_t i = 0; i < mc_max_mid; ++i) if (mii1_flat[i]) ++groups;
        TRACE_COUNT("q6a_mii1_rows_scanned", (int64_t)(end - beg));
        TRACE_COUNT("q6a_mii1_rows_emitted", rows_emitted);
        TRACE_COUNT("q6a_mii1_groups",       groups);
    }

    // -----------------------------------------------------------------------
    // Build mii2_flat[movie_id]:
    //   # movie_info_idx rows with info_type_id == id3  (it4.id = ID3)
    //   AND info_float is valid AND INFO3 <= info_float <= INFO2.
    // -----------------------------------------------------------------------
    std::vector<int32_t> mii2_flat;
    bool use_mii2 = false;
    if (id3 >= 0) {
        PROFILE_SCOPE("q6a_mii2_build");
        const auto& mii = db->movie_info_idx;
        int32_t beg = 0, end = (int32_t)mii.movie_id.size();
        if (id3 < (int32_t)mii.type_part_start.size()) {
            beg = mii.type_part_start[id3];
            end = mii.type_part_end[id3];
        }
        mii2_flat.assign(mc_max_mid, 0);
        int64_t rows_emitted = 0;
        for (int32_t r = beg; r < end; ++r) {
            float fv = mii.info_float[r];
            if (std::isnan(fv)) continue;
            if (fv < info3 || fv > info2) continue;
            int32_t mid = mii.movie_id[r];
            if (mid >= 0 && mid < mc_max_mid) ++mii2_flat[mid];
            ++rows_emitted;
        }
        use_mii2 = true;
        int64_t groups = 0;
        for (int32_t i = 0; i < mc_max_mid; ++i) if (mii2_flat[i]) ++groups;
        TRACE_COUNT("q6a_mii2_rows_scanned", (int64_t)(end - beg));
        TRACE_COUNT("q6a_mii2_rows_emitted", rows_emitted);
        TRACE_COUNT("q6a_mii2_groups",       groups);
    }

    // -----------------------------------------------------------------------
    // Build mi1_count_vec[movie_id]:
    //   # movie_info rows where info_type_id IN ID1 AND info IN INFO1
    // -----------------------------------------------------------------------
    int32_t mi1_max_mid = mc_max_mid;
    std::vector<int32_t> mi1_count_vec;
    bool use_mi1 = false;
    if (!id1_set.empty()) {
        PROFILE_SCOPE("q6a_mi1_build");
        int64_t rows_scanned = 0, rows_emitted = 0;
        const auto& mi = db->movie_info;
        mi1_count_vec.assign(mi1_max_mid, 0);

        // Convert info1_set strings to interned integer IDs for fast lookup.
        int32_t max_intern_id = (int32_t)mi.info_dict_vec.size() - 1;
        std::vector<uint8_t> info1_id_ok;
        bool use_intern = (!mi.info_dict_map.empty() && !mi.info_id.empty() && max_intern_id >= 0);
        if (use_intern) {
            info1_id_ok.assign(max_intern_id + 2, 0);
            for (const auto& s : info1_set) {
                auto it = mi.info_dict_map.find(s);
                if (it != mi.info_dict_map.end() && it->second <= max_intern_id)
                    info1_id_ok[it->second] = 1;
            }
            if (info1_null_ok) {
                auto it = mi.info_dict_map.find("");
                if (it != mi.info_dict_map.end() && it->second <= max_intern_id)
                    info1_id_ok[it->second] = 1;
                auto it2 = mi.info_dict_map.find("<<NULL>>");
                if (it2 != mi.info_dict_map.end() && it2->second <= max_intern_id)
                    info1_id_ok[it2->second] = 1;
            }
        }
        int32_t info1_id_ok_size = (int32_t)info1_id_ok.size();

        if (!mi.type_part_start.empty()) {
            int32_t max_type = (int32_t)mi.type_part_start.size() - 1;
            for (int32_t tid : id1_set) {
                if (tid < 0 || tid > max_type) continue;
                int32_t beg = mi.type_part_start[tid];
                int32_t end = mi.type_part_end[tid];
                for (int32_t r = beg; r < end; ++r) {
                    ++rows_scanned;
                    bool ok = use_intern
                        ? ((mi.info_id[r] >= 0 && mi.info_id[r] < info1_id_ok_size) && info1_id_ok[mi.info_id[r]])
                        : (mi.info_str[r].empty() ? info1_null_ok : info1_set.count(mi.info_str[r]) > 0);
                    if (!ok) continue;
                    int32_t mid = mi.movie_id[r];
                    if (mid >= 0 && mid < mi1_max_mid) ++mi1_count_vec[mid];
                    ++rows_emitted;
                }
            }
        } else {
            for (int32_t r = 0; r < (int32_t)mi.movie_id.size(); ++r) {
                ++rows_scanned;
                if (!id1_set.count(mi.info_type_id[r])) continue;
                bool ok = use_intern
                    ? ((mi.info_id[r] >= 0 && mi.info_id[r] < info1_id_ok_size) && info1_id_ok[mi.info_id[r]])
                    : (mi.info_str[r].empty() ? info1_null_ok : info1_set.count(mi.info_str[r]) > 0);
                if (!ok) continue;
                int32_t mid = mi.movie_id[r];
                if (mid >= 0 && mid < mi1_max_mid) ++mi1_count_vec[mid];
                ++rows_emitted;
            }
        }
        use_mi1 = true;
        int64_t groups = 0;
        for (int32_t i = 0; i < mi1_max_mid; ++i) if (mi1_count_vec[i]) ++groups;
        TRACE_COUNT("q6a_mi1_rows_scanned", rows_scanned);
        TRACE_COUNT("q6a_mi1_rows_emitted", rows_emitted);
        TRACE_COUNT("q6a_mi1_groups",       groups);
    }

    // -----------------------------------------------------------------------
    // Build movie_contrib_vec[movie_id]:
    //   For each title row passing kind/year filters AND appearing in all
    //   three side flat arrays, store mii2_flat * mii1_flat * mi1_count_vec.
    // Uses sparse intersection: iterate the smallest non-null flat array.
    // -----------------------------------------------------------------------
    std::vector<int64_t> movie_contrib_vec(mc_max_mid, 0);
    {
        PROFILE_SCOPE("q6a_title_scan");
        const auto& t = db->title;
        int64_t rows_scanned = 0, rows_emitted = 0;
        const int32_t kind_ok_size = (int32_t)kind_id_ok.size();

        if (use_mii2 || use_mii1 || use_mi1) {
            const int32_t* __restrict__ id_to_row = t.id_to_row.data();
            const int32_t  itr_size = (int32_t)t.id_to_row.size();
            const int32_t* __restrict__ kid_ptr  = t.kind_id.data();
            const int32_t* __restrict__ year_ptr = t.production_year.data();
            const int32_t* __restrict__ mii2_ptr = use_mii2 ? mii2_flat.data() : nullptr;
            const int32_t* __restrict__ mii1_ptr = use_mii1 ? mii1_flat.data() : nullptr;
            const int32_t* __restrict__ mi1_ptr  = use_mi1  ? mi1_count_vec.data() : nullptr;
            int64_t*       __restrict__ mc_ptr   = movie_contrib_vec.data();

            // Pick the driver flat array: smallest non-zero footprint.
            struct Candidate { int32_t mid; int32_t cnt; };
            std::vector<Candidate> driver_cands;
            const int32_t* __restrict__ driver_ptr = nullptr;

            if (use_mii1) {
                driver_ptr = mii1_ptr;
                TRACE_COUNT("q6a_title_driver", 1);
            } else if (use_mi1) {
                driver_ptr = mi1_ptr;
                TRACE_COUNT("q6a_title_driver", 2);
            } else if (use_mii2) {
                driver_ptr = mii2_ptr;
                TRACE_COUNT("q6a_title_driver", 3);
            }

            if (driver_ptr) {
                driver_cands.reserve(32768);
                for (int32_t mid = 0; mid < mc_max_mid; ++mid) {
                    if (driver_ptr[mid] > 0)
                        driver_cands.push_back({mid, driver_ptr[mid]});
                }
            }
            TRACE_COUNT("q6a_driver_cands", (int64_t)driver_cands.size());

            if (driver_ptr) {
                for (const auto& [mid, drv_cnt] : driver_cands) {
                    int32_t c_mii2 = (use_mii2 && mii2_ptr != driver_ptr) ? mii2_ptr[mid] : (driver_ptr == mii2_ptr ? drv_cnt : 1);
                    if (use_mii2 && c_mii2 == 0) continue;
                    int32_t c_mii1 = (use_mii1 && mii1_ptr != driver_ptr) ? mii1_ptr[mid] : (driver_ptr == mii1_ptr ? drv_cnt : 1);
                    if (use_mii1 && c_mii1 == 0) continue;
                    int32_t c_mi1  = (use_mi1  && mi1_ptr  != driver_ptr) ? mi1_ptr[mid]  : (driver_ptr == mi1_ptr  ? drv_cnt : 1);
                    if (use_mi1  && c_mi1  == 0) continue;

                    ++rows_scanned;
                    if (mid < 0 || mid >= itr_size) continue;
                    int32_t trow = id_to_row[mid];
                    if (trow < 0) continue;
                    int32_t kid = kid_ptr[trow];
                    bool kind_pass = (kid >= 0 && kid < kind_ok_size) ? kind_id_ok[kid] : false;
                    if (!kind_pass) continue;
                    int32_t py = year_ptr[trow];
                    if (py == -1) continue;
                    if (year1 >= 0 && py > year1) continue;
                    if (year2 >= 0 && py < year2) continue;
                    mc_ptr[mid] = (int64_t)c_mii2 * (int64_t)c_mii1 * (int64_t)c_mi1;
                    ++rows_emitted;
                }
            } else {
                // No driver — full title scan
                const int32_t  ntitle = (int32_t)t.id.size();
                const int32_t* __restrict__ tid_ptr  = t.id.data();
                for (int32_t r = 0; r < ntitle; ++r) {
                    int32_t mid = tid_ptr[r];
                    if (mid < 0 || mid >= mc_max_mid) continue;
                    ++rows_scanned;
                    int32_t kid = kid_ptr[r];
                    bool kind_pass = (kid >= 0 && kid < kind_ok_size) ? kind_id_ok[kid] : false;
                    if (!kind_pass) continue;
                    int32_t py = year_ptr[r];
                    if (py == -1) continue;
                    if (year1 >= 0 && py > year1) continue;
                    if (year2 >= 0 && py < year2) continue;
                    mc_ptr[mid] = 1;
                    ++rows_emitted;
                }
            }
        } else {
            // All side maps empty — full title scan (degenerate case).
            const int32_t  ntitle = (int32_t)t.id.size();
            const int32_t* __restrict__ tid_ptr  = t.id.data();
            const int32_t* __restrict__ kid_ptr  = t.kind_id.data();
            const int32_t* __restrict__ year_ptr = t.production_year.data();
            int64_t*       __restrict__ mc_ptr   = movie_contrib_vec.data();
            for (int32_t r = 0; r < ntitle; ++r) {
                ++rows_scanned;
                int32_t kid = kid_ptr[r];
                bool kind_pass = (kid >= 0 && kid < kind_ok_size) ? kind_id_ok[kid] : false;
                if (!kind_pass) continue;
                int32_t py = year_ptr[r];
                if (py == -1) continue;
                if (year1 >= 0 && py > year1) continue;
                if (year2 >= 0 && py < year2) continue;
                int32_t mid = tid_ptr[r];
                if (mid >= 0 && mid < mc_max_mid)
                    mc_ptr[mid] = 1;
                ++rows_emitted;
            }
        }
        TRACE_COUNT("q6a_title_rows_scanned", rows_scanned);
        TRACE_COUNT("q6a_title_rows_emitted", rows_emitted);
        int64_t mc_size = 0;
        for (int32_t i = 0; i < mc_max_mid; ++i) if (movie_contrib_vec[i]) ++mc_size;
        TRACE_COUNT("q6a_movie_contrib_size", mc_size);
    }

    // -----------------------------------------------------------------------
    // Build movie_contrib bitset: compact bit array for O(1) membership test
    // in the ci inner loop.  Avoids random accesses into int64 movie_contrib_vec.
    // -----------------------------------------------------------------------
    const int32_t mc_bitset_bytes = (mc_max_mid + 7) / 8;
    std::vector<uint8_t> mc_bitset(mc_bitset_bytes, 0);
    {
        PROFILE_SCOPE("q6a_mc_bitset_build");
        for (int32_t mid = 0; mid < mc_max_mid; ++mid) {
            if (movie_contrib_vec[mid] != 0)
                mc_bitset[mid >> 3] |= (uint8_t)(1u << (mid & 7));
        }
        TRACE_COUNT("q6a_mc_bitset_done", 1);
    }

    // -----------------------------------------------------------------------
    // Main join over name (person-side):
    //   For each qualifying person (gender + pcode_nf filters):
    //     - aka_name existence  (n.id = an.person_id)
    //     - Count qualifying person_info rows  (it5.id IN ID4)
    //     - For each qualifying cast_info row (role IN ROLE, note IN NOTE,
    //       movie in movie_contrib): accumulate count
    // NOTE: note_set is non-empty here (early exit handles empty case above).
    // -----------------------------------------------------------------------
    const auto& ci  = db->cast_info;
    const auto& pi1 = db->person_info;
    const auto& an  = db->aka_name;

    int64_t count = 0;
    int64_t persons_gender_pass = 0, persons_pcode_pass = 0;
    int64_t persons_an_pass = 0, persons_pi_pass = 0;
    int64_t ci_rows_probed = 0, ci_rows_matched = 0;

    const int32_t role_ok_sz  = (int32_t)role_id_ok.size();
    const int32_t id4_ok_sz   = (int32_t)id4_ok.size();
    const int64_t* __restrict__ mc_ptr_global = movie_contrib_vec.data();
    const int32_t  mc_max_mid_g = mc_max_mid;
    const uint8_t* __restrict__ mc_bs = mc_bitset.data();

    // -----------------------------------------------------------------------
    // Build pi_count_flat and pi_person_ids: compact list of person_ids
    // with at least one matching person_info row (pi candidates).
    // -----------------------------------------------------------------------
    bool pi_has_partitions = !pi1.type_part_start.empty();
    int32_t pi_max_pid = (int32_t)nm.id_to_row.size();
    std::vector<int32_t> pi_count_flat;
    std::vector<int32_t> pi_person_ids;
    {
        PROFILE_SCOPE("q6a_pi_candidate_build");
        if (pi_has_partitions && !id4_set.empty()) {
            int32_t pi_max_type = (int32_t)pi1.type_part_start.size() - 1;
            pi_count_flat.assign(pi_max_pid, 0);
            for (int32_t tid : id4_set) {
                if (tid < 0 || tid > pi_max_type) continue;
                int32_t beg = pi1.type_part_start[tid];
                int32_t end = pi1.type_part_end[tid];
                for (int32_t r = beg; r < end; ++r) {
                    int32_t pid = pi1.person_id[r];
                    if (pid >= 0 && pid < pi_max_pid) {
                        if (pi_count_flat[pid] == 0) pi_person_ids.push_back(pid);
                        ++pi_count_flat[pid];
                    }
                }
            }
        }
        TRACE_COUNT("q6a_pi_candidates", (int64_t)pi_person_ids.size());
    }

    bool use_pi_driven = pi_has_partitions && !pi_person_ids.empty();

    // Sort pi_person_ids by name table row for cache-friendly sequential name access.
    // Converts 134K random id_to_row+gender_byte+pcode lookups into sequential reads.
    if (use_pi_driven && !pi_person_ids.empty()) {
        PROFILE_SCOPE("q6a_pi_sort");
        const int32_t* itr = nm.id_to_row.data();
        const int32_t  itr_sz = (int32_t)nm.id_to_row.size();
        std::sort(pi_person_ids.begin(), pi_person_ids.end(),
            [&](int32_t a, int32_t b) {
                int32_t ra = (a >= 0 && a < itr_sz) ? itr[a] : -1;
                int32_t rb = (b >= 0 && b < itr_sz) ? itr[b] : -1;
                return ra < rb;
            });
        TRACE_COUNT("q6a_pi_sorted", 1);
    }

    {
    PROFILE_SCOPE("q6a_person_join");
    if (use_pi_driven) {
        int32_t nm_max_id = (int32_t)nm.id_to_row.size() - 1;
        const int32_t* __restrict__ an_off  = an.person_id_csr.offsets.data();
        const int32_t  an_off_sz = (int32_t)an.person_id_csr.offsets.size();
        const int32_t* __restrict__ ci_off  = ci.person_id_csr.offsets.data();
        const int32_t  ci_off_sz = (int32_t)ci.person_id_csr.offsets.size();
        const int32_t* __restrict__ ci_vals = ci.person_id_csr.values.data();
        const int32_t* __restrict__ ci_rid  = ci.role_id.data();
        const int32_t* __restrict__ ci_mid  = ci.movie_id.data();
        const std::string* __restrict__ ci_note_ptr = ci.note.data();

        for (int32_t pid : pi_person_ids) {
            int32_t pi1_count = pi_count_flat[pid];

            if (pid < 0 || pid > nm_max_id) continue;
            int32_t nrow = nm.id_to_row[pid];
            if (nrow < 0) continue;

            // Gender + pcode check: use prebuilt name_row_pass if available
            if (use_name_row_pass) {
                if (!name_row_pass[nrow]) continue;
            } else {
                uint8_t gb = nm.gender_byte[nrow];
                if (!gender_byte_ok[gb]) continue;
                if (!pcode_set.count(nm.name_pcode_nf[nrow])) continue;
            }
            ++persons_gender_pass;
            ++persons_pcode_pass;
            ++persons_pi_pass;

            // aka_name: must have at least one row
            int32_t an_beg = (pid < an_off_sz - 1) ? an_off[pid]     : 0;
            int32_t an_end = (pid < an_off_sz - 1) ? an_off[pid + 1] : 0;
            int32_t an_count = an_end - an_beg;
            if (an_count == 0) continue;
            ++persons_an_pass;

            // cast_info: iterate ci rows for this person
            {
                int32_t ci_beg = (pid < ci_off_sz - 1) ? ci_off[pid]     : 0;
                int32_t ci_end = (pid < ci_off_sz - 1) ? ci_off[pid + 1] : 0;
                for (int32_t ci_i = ci_beg; ci_i < ci_end; ++ci_i) {
                    int32_t row = ci_vals[ci_i];
                    ++ci_rows_probed;
                    int32_t rid = ci_rid[row];
                    if (rid < 0 || rid >= role_ok_sz || !role_id_ok[rid]) continue;
                    if (!note_set.count(ci_note_ptr[row])) continue;
                    int32_t cmid = ci_mid[row];
                    if (cmid < 0 || cmid >= mc_max_mid_g) continue;
                    // Fast bitset check before random access to movie_contrib_vec
                    if (!(mc_bs[cmid >> 3] & (1u << (cmid & 7)))) continue;
                    int64_t mc_val = mc_ptr_global[cmid];
                    if (mc_val == 0) continue;
                    count += mc_val * (int64_t)pi1_count * (int64_t)an_count;
                    ++ci_rows_matched;
                }
            }
        }
    } else {
        // Fallback: full name scan (when pi partitions unavailable).
        const int32_t* __restrict__ an_off  = an.person_id_csr.offsets.data();
        const int32_t  an_off_sz = (int32_t)an.person_id_csr.offsets.size();
        const int32_t* __restrict__ ci_off  = ci.person_id_csr.offsets.data();
        const int32_t  ci_off_sz = (int32_t)ci.person_id_csr.offsets.size();
        const int32_t* __restrict__ ci_vals = ci.person_id_csr.values.data();
        const int32_t* __restrict__ ci_rid  = ci.role_id.data();
        const int32_t* __restrict__ ci_mid  = ci.movie_id.data();
        const std::string* __restrict__ ci_note_ptr = ci.note.data();

        for (int32_t nrow = 0; nrow < (int32_t)nm.id.size(); ++nrow) {
            if (use_name_row_pass) {
                if (!name_row_pass[nrow]) continue;
            } else {
                uint8_t gb = nm.gender_byte[nrow];
                if (!gender_byte_ok[gb]) continue;
                if (!pcode_set.count(nm.name_pcode_nf[nrow])) continue;
            }
            ++persons_gender_pass;
            ++persons_pcode_pass;

            int32_t pid = nm.id[nrow];

            int32_t an_beg = (pid < an_off_sz - 1) ? an_off[pid]     : 0;
            int32_t an_end = (pid < an_off_sz - 1) ? an_off[pid + 1] : 0;
            int32_t an_count = an_end - an_beg;
            if (an_count == 0) continue;
            ++persons_an_pass;

            // person_info: count rows with it5.id IN ID4
            int64_t pi1_count = 0;
            {
                const int32_t* pi_off = pi1.person_id_csr.offsets.data();
                const int32_t  pi_off_sz = (int32_t)pi1.person_id_csr.offsets.size();
                int32_t pi_beg = (pid < pi_off_sz - 1) ? pi_off[pid]     : 0;
                int32_t pi_end = (pid < pi_off_sz - 1) ? pi_off[pid + 1] : 0;
                const int32_t* pi_vals = pi1.person_id_csr.values.data();
                const int32_t* pi_itid = pi1.info_type_id.data();
                for (int32_t pi_r = pi_beg; pi_r < pi_end; ++pi_r) {
                    int32_t row  = pi_vals[pi_r];
                    int32_t itid = pi_itid[row];
                    if (itid >= 0 && itid < id4_ok_sz && id4_ok[itid]) ++pi1_count;
                }
            }
            if (pi1_count == 0) continue;
            ++persons_pi_pass;

            {
                int32_t ci_beg = (pid < ci_off_sz - 1) ? ci_off[pid]     : 0;
                int32_t ci_end = (pid < ci_off_sz - 1) ? ci_off[pid + 1] : 0;
                for (int32_t ci_i = ci_beg; ci_i < ci_end; ++ci_i) {
                    int32_t row = ci_vals[ci_i];
                    ++ci_rows_probed;
                    int32_t rid = ci_rid[row];
                    if (rid < 0 || rid >= role_ok_sz || !role_id_ok[rid]) continue;
                    if (!note_set.count(ci_note_ptr[row])) continue;
                    int32_t cmid = ci_mid[row];
                    if (cmid < 0 || cmid >= mc_max_mid_g) continue;
                    if (!(mc_bs[cmid >> 3] & (1u << (cmid & 7)))) continue;
                    int64_t mc_val = mc_ptr_global[cmid];
                    if (mc_val == 0) continue;
                    count += mc_val * pi1_count * (int64_t)an_count;
                    ++ci_rows_matched;
                }
            }
        }
    }
    } // end PROFILE_SCOPE q6a_person_join

    TRACE_COUNT("q6a_name_rows_total",     (int64_t)nm.id.size());
    TRACE_COUNT("q6a_persons_gender_pass", persons_gender_pass);
    TRACE_COUNT("q6a_persons_pcode_pass",  persons_pcode_pass);
    TRACE_COUNT("q6a_persons_an_pass",     persons_an_pass);
    TRACE_COUNT("q6a_persons_pi_pass",     persons_pi_pass);
    TRACE_COUNT("q6a_ci_rows_probed",      ci_rows_probed);
    TRACE_COUNT("q6a_ci_rows_matched",     ci_rows_matched);
    TRACE_COUNT("q6a_info1_set_size",      (int64_t)info1_set.size());
    TRACE_COUNT("q6a_query_output_rows",   1);

    std::vector<std::vector<std::string>> rows;
    rows.push_back({"count_star()"});
    rows.push_back({std::to_string(count)});
    return rows;
}
