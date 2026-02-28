#include "query_q7a.hpp"

#include <cstdlib>
#include <string>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

namespace q7a_internal {

struct IntFilter {
    std::vector<uint8_t> flags;
    size_t count = 0;
};

struct StringFilter {
    std::vector<int32_t> values;
    std::unordered_set<int32_t> lookup;
    bool allow_null = false;
    bool use_lookup = false;
};

inline bool flag_contains(const std::vector<uint8_t>& flags, int32_t id) {
    if (id < 0) {
        return false;
    }
    const size_t idx = static_cast<size_t>(id);
    return idx < flags.size() && flags[idx] != 0;
}

inline bool set_flag(std::vector<uint8_t>& flags, int32_t id, size_t& count) {
    if (id < 0) {
        return false;
    }
    const size_t idx = static_cast<size_t>(id);
    if (idx >= flags.size()) {
        flags.resize(idx + 1, 0);
    }
    if (flags[idx] != 0) {
        return false;
    }
    flags[idx] = 1;
    ++count;
    return true;
}

IntFilter build_int_filter(const std::vector<std::string>& values) {
    IntFilter filter;
    int32_t max_id = -1;
    std::vector<int32_t> parsed;
    parsed.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        try {
            const int32_t parsed_id = static_cast<int32_t>(std::stoi(val));
            if (parsed_id < 0) {
                continue;
            }
            parsed.push_back(parsed_id);
            if (parsed_id > max_id) {
                max_id = parsed_id;
            }
        } catch (const std::exception&) {
            continue;
        }
    }
    if (max_id < 0) {
        return filter;
    }
    filter.flags.assign(static_cast<size_t>(max_id) + 1, 0);
    for (const int32_t id : parsed) {
        const size_t idx = static_cast<size_t>(id);
        if (filter.flags[idx] == 0) {
            filter.flags[idx] = 1;
            ++filter.count;
        }
    }
    return filter;
}

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const ColumnData& col,
                                            const std::vector<std::string>& values) {
    StringFilter filter;
    (void)col;
    size_t i = 0;
    while (i < values.size()) {
        if (values[i] == "<<NULL>>" || values[i] == "NULL" || values[i] == "null") {
            ++i;
            continue;
        }
        std::string merged_space = values[i];
        std::string merged_nospace = values[i];
        int32_t found_id = -1;
        size_t found_idx = i;
        for (size_t j = i; j < values.size(); ++j) {
            if (j != i) {
                if (values[j] == "<<NULL>>" || values[j] == "NULL" || values[j] == "null") {
                    break;
                }
                merged_space += ", " + values[j];
                merged_nospace += "," + values[j];
            }
            int32_t space_id = -1;
            if (pool.try_get_id(merged_space, space_id)) {
                found_id = space_id;
                found_idx = j;
            }
            int32_t nospace_id = -1;
            if (pool.try_get_id(merged_nospace, nospace_id)) {
                found_id = nospace_id;
                found_idx = j;
            }
        }
        if (found_id != -1) {
            bool already_present = false;
            for (const int32_t existing : filter.values) {
                if (existing == found_id) {
                    already_present = true;
                    break;
                }
            }
            if (!already_present) {
                filter.values.push_back(found_id);
            }
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
    if (filter.values.size() > 8) {
        filter.use_lookup = true;
        filter.lookup.reserve(filter.values.size() * 2);
        for (const int32_t id : filter.values) {
            filter.lookup.insert(id);
        }
    }
    return filter;
}

bool matches_string(const ColumnData& col, size_t idx, const StringFilter& filter) {
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    const int32_t id = col.str_ids[idx];
    if (filter.use_lookup) {
        return filter.lookup.find(id) != filter.lookup.end();
    }
    for (const int32_t candidate : filter.values) {
        if (candidate == id) {
            return true;
        }
    }
    return false;
}

bool is_digits(const std::string& value) {
    if (value.empty()) {
        return false;
    }
    for (char c : value) {
        if (c < '0' || c > '9') {
            return false;
        }
    }
    return true;
}

bool parse_numeric_value(const std::string& value, double& out) {
    if (value.empty()) {
        return false;
    }
    const size_t dot_pos = value.find('.');
    if (dot_pos != std::string::npos && value.find('.', dot_pos + 1) != std::string::npos) {
        return false;
    }
    if (dot_pos == std::string::npos) {
        if (!is_digits(value)) {
            return false;
        }
        if (value.size() > 1 && value[0] == '0') {
            return false;
        }
    } else {
        const std::string left = value.substr(0, dot_pos);
        const std::string right = value.substr(dot_pos + 1);
        if (right.empty() || !is_digits(right)) {
            return false;
        }
        if (!left.empty()) {
            if (!is_digits(left)) {
                return false;
            }
            if (left.size() > 1 && left[0] == '0') {
                return false;
            }
        }
    }
    char* end = nullptr;
    out = std::strtod(value.c_str(), &end);
    return end != value.c_str() && end == value.c_str() + value.size();
}

}  // namespace q7a_internal

#ifdef TRACE
struct Q7aTrace {
    TraceRecorder recorder;
    uint64_t name_rows_scanned = 0;
    uint64_t name_rows_emitted = 0;
    uint64_t aka_name_rows_scanned = 0;
    uint64_t aka_name_rows_emitted = 0;
    uint64_t aka_name_agg_rows_in = 0;
    uint64_t aka_name_groups_created = 0;
    uint64_t aka_name_agg_rows_emitted = 0;
    uint64_t person_info_rows_scanned = 0;
    uint64_t person_info_rows_emitted = 0;
    uint64_t person_info_agg_rows_in = 0;
    uint64_t person_info_groups_created = 0;
    uint64_t person_info_agg_rows_emitted = 0;
    uint64_t movie_info_rows_scanned = 0;
    uint64_t movie_info_rows_emitted = 0;
    uint64_t movie_info_agg_rows_in = 0;
    uint64_t movie_info_groups_created = 0;
    uint64_t movie_info_agg_rows_emitted = 0;
    uint64_t movie_info_idx_rows_scanned = 0;
    uint64_t movie_info_idx_rows_emitted_id2 = 0;
    uint64_t movie_info_idx_rows_emitted_id3 = 0;
    uint64_t movie_info_idx_agg_rows_in_id2 = 0;
    uint64_t movie_info_idx_agg_rows_in_id3 = 0;
    uint64_t movie_info_idx_groups_created_id2 = 0;
    uint64_t movie_info_idx_groups_created_id3 = 0;
    uint64_t movie_info_idx_agg_rows_emitted_id2 = 0;
    uint64_t movie_info_idx_agg_rows_emitted_id3 = 0;
    uint64_t movie_keyword_rows_scanned = 0;
    uint64_t movie_keyword_rows_emitted = 0;
    uint64_t movie_keyword_agg_rows_in = 0;
    uint64_t movie_keyword_groups_created = 0;
    uint64_t movie_keyword_agg_rows_emitted = 0;
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t cast_info_join_build_rows_in = 0;
    uint64_t cast_info_join_probe_rows_in = 0;
    uint64_t cast_info_join_rows_emitted = 0;
    uint64_t movie_cast_agg_rows_in = 0;
    uint64_t movie_cast_groups_created = 0;
    uint64_t movie_cast_agg_rows_emitted = 0;
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t title_join_build_rows_in = 0;
    uint64_t title_join_probe_rows_in = 0;
    uint64_t title_join_rows_emitted = 0;
    uint64_t query_output_rows = 0;
};
#endif

int64_t run_q7a(const Database& db, const Q7aArgs& args) {
    int64_t total = 0;
#ifdef TRACE
    Q7aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q7a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);
    const int32_t id2 = static_cast<int32_t>(std::stoi(args.ID2));
    const int32_t id3 = static_cast<int32_t>(std::stoi(args.ID3));
    const double info2 = std::stod(args.INFO2);
    const double info3 = std::stod(args.INFO3);
    const double info4 = std::stod(args.INFO4);
    const double info5 = std::stod(args.INFO5);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& kind_col = kind_type.columns.at("kind");
    const auto kind_filter =
        q7a_internal::build_string_filter_for_column(db.string_pool, kind_col, args.KIND);
    std::vector<uint8_t> allowed_kind_flags;
    size_t allowed_kind_count = 0;
    const auto& kind_id_col = kind_type.columns.at("id");
    for (int64_t i = 0; i < kind_type.row_count; ++i) {
        if (kind_id_col.is_null[i] != 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(i);
        if (q7a_internal::matches_string(kind_col, idx, kind_filter)) {
            q7a_internal::set_flag(allowed_kind_flags, kind_id_col.i32[idx], allowed_kind_count);
        }
    }
    if (allowed_kind_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& info_type = db.tables.at("info_type");
    std::vector<uint8_t> valid_info_type_flags;
    size_t valid_info_type_count = 0;
    const auto& info_type_id_col = info_type.columns.at("id");
    for (int64_t i = 0; i < info_type.row_count; ++i) {
        if (info_type_id_col.is_null[i] != 0) {
            continue;
        }
        q7a_internal::set_flag(valid_info_type_flags, info_type_id_col.i32[i],
                               valid_info_type_count);
    }
    if (!q7a_internal::flag_contains(valid_info_type_flags, id2) ||
        !q7a_internal::flag_contains(valid_info_type_flags, id3)) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto info_type_filter1 = q7a_internal::build_int_filter(args.ID1);
    if (info_type_filter1.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto info_type_filter4 = q7a_internal::build_int_filter(args.ID4);
    if (info_type_filter4.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& role_type = db.tables.at("role_type");
    const auto& role_col = role_type.columns.at("role");
    const auto role_filter =
        q7a_internal::build_string_filter_for_column(db.string_pool, role_col, args.ROLE);
    std::vector<uint8_t> allowed_role_flags;
    size_t allowed_role_count = 0;
    const auto& role_id_col = role_type.columns.at("id");
    for (int64_t i = 0; i < role_type.row_count; ++i) {
        if (role_id_col.is_null[i] != 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(i);
        if (q7a_internal::matches_string(role_col, idx, role_filter)) {
            q7a_internal::set_flag(allowed_role_flags, role_id_col.i32[idx], allowed_role_count);
        }
    }
    if (allowed_role_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& cast_info = db.tables.at("cast_info");
    const auto& note_col = cast_info.columns.at("note");
    q7a_internal::StringFilter note_filter;
#ifdef TRACE
    if (!skip_query) {
        note_filter =
            q7a_internal::build_string_filter_for_column(db.string_pool, note_col, args.NOTE);
    }
#else
    note_filter = q7a_internal::build_string_filter_for_column(db.string_pool, note_col, args.NOTE);
#endif
    if (note_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& name = db.tables.at("name");
    const auto& gender_col = name.columns.at("gender");
    const auto& pcode_col = name.columns.at("name_pcode_nf");
    const auto gender_filter =
        q7a_internal::build_string_filter_for_column(db.string_pool, gender_col, args.GENDER);
    const auto pcode_filter =
        q7a_internal::build_string_filter_for_column(db.string_pool, pcode_col,
                                                     args.NAME_PCODE_NF);
    if (gender_filter.values.empty() || pcode_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_info_col = movie_info.columns.at("info");
    const auto info_filter =
        q7a_internal::build_string_filter_for_column(db.string_pool, mi_info_col, args.INFO1);
    if (info_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    std::vector<uint8_t> allowed_movie_flags;
    std::vector<int32_t> allowed_movie_ids;
    allowed_movie_ids.reserve(static_cast<size_t>(title.row_count));
    size_t allowed_movie_count = 0;

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q7a_title_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < title.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.title_rows_scanned;
#endif
        if (t_id.is_null[idx] != 0 || t_kind_id.is_null[idx] != 0 ||
            t_year.is_null[idx] != 0) {
            continue;
        }
        const int32_t kind_id = t_kind_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_kind_flags, kind_id)) {
            continue;
        }
        const int32_t year = t_year.i32[idx];
        if (year > year1 || year < year2) {
            continue;
        }
        const int32_t movie_id = t_id.i32[idx];
        if (q7a_internal::set_flag(allowed_movie_flags, movie_id, allowed_movie_count)) {
            allowed_movie_ids.push_back(movie_id);
        }
#ifdef TRACE
        ++trace.title_rows_emitted;
        ++trace.title_join_probe_rows_in;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_movie_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<uint8_t> allowed_person_flags;
    size_t allowed_person_count = 0;
    const auto& name_id_col = name.columns.at("id");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q7a_name_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < name.row_count; ++i) {
        if (name_id_col.is_null[i] != 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (!q7a_internal::matches_string(gender_col, idx, gender_filter)) {
            continue;
        }
        if (!q7a_internal::matches_string(pcode_col, idx, pcode_filter)) {
            continue;
        }
        q7a_internal::set_flag(allowed_person_flags, name_id_col.i32[idx], allowed_person_count);
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_person_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int64_t> aka_counts(allowed_person_flags.size(), 0);
    size_t aka_nonzero = 0;
    const auto& aka_name = db.tables.at("aka_name");
    const auto& aka_person_id = aka_name.columns.at("person_id");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q7a_aka_name_scan");
#endif
    for (int64_t i = 0; i < aka_name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.aka_name_rows_scanned;
#endif
        if (aka_person_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t person_id = aka_person_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_person_flags, person_id)) {
            continue;
        }
#ifdef TRACE
        ++trace.aka_name_rows_emitted;
        ++trace.aka_name_agg_rows_in;
        if (aka_counts[static_cast<size_t>(person_id)] == 0) {
            ++trace.aka_name_groups_created;
        }
#endif
        if (aka_counts[static_cast<size_t>(person_id)]++ == 0) {
            ++aka_nonzero;
        }
    }
#ifdef TRACE
    }
#endif
    if (aka_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int64_t> person_info_counts(allowed_person_flags.size(), 0);
    size_t person_info_nonzero = 0;
    const auto& person_info = db.tables.at("person_info");
    const auto& pi_person_id = person_info.columns.at("person_id");
    const auto& pi_info_type_id = person_info.columns.at("info_type_id");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q7a_person_info_scan");
#endif
    for (int64_t i = 0; i < person_info.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.person_info_rows_scanned;
#endif
        if (pi_person_id.is_null[idx] != 0 || pi_info_type_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t person_id = pi_person_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_person_flags, person_id)) {
            continue;
        }
        const int32_t info_type_id = pi_info_type_id.i32[idx];
        if (!q7a_internal::flag_contains(info_type_filter4.flags, info_type_id)) {
            continue;
        }
#ifdef TRACE
        ++trace.person_info_rows_emitted;
        ++trace.person_info_agg_rows_in;
        if (person_info_counts[static_cast<size_t>(person_id)] == 0) {
            ++trace.person_info_groups_created;
        }
#endif
        if (person_info_counts[static_cast<size_t>(person_id)]++ == 0) {
            ++person_info_nonzero;
        }
    }
#ifdef TRACE
    }
#endif
    if (person_info_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");

    std::vector<int64_t> mi_counts(allowed_movie_flags.size(), 0);
    size_t mi_nonzero = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q7a_movie_info_scan");
#endif
    for (int64_t i = 0; i < movie_info.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if (mi_movie_id.is_null[idx] != 0 || mi_info_type_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mi_movie_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_movie_flags, movie_id)) {
            continue;
        }
        const int32_t info_type_id = mi_info_type_id.i32[idx];
        if (!q7a_internal::flag_contains(info_type_filter1.flags, info_type_id)) {
            continue;
        }
        if (!q7a_internal::matches_string(mi_info_col, idx, info_filter)) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_info_rows_emitted;
        ++trace.movie_info_agg_rows_in;
        if (mi_counts[static_cast<size_t>(movie_id)] == 0) {
            ++trace.movie_info_groups_created;
        }
#endif
        if (mi_counts[static_cast<size_t>(movie_id)]++ == 0) {
            ++mi_nonzero;
        }
    }
#ifdef TRACE
    }
#endif
    if (mi_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int64_t> mii1_counts(allowed_movie_flags.size(), 0);
    std::vector<int64_t> mii2_counts(allowed_movie_flags.size(), 0);
    size_t mii1_nonzero = 0;
    size_t mii2_nonzero = 0;
    const auto& movie_info_idx = db.tables.at("movie_info_idx");
    const auto& mii_movie_id = movie_info_idx.columns.at("movie_id");
    const auto& mii_info_type_id = movie_info_idx.columns.at("info_type_id");
    const auto& mii_info_col = movie_info_idx.columns.at("info");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q7a_movie_info_idx_scan");
#endif
    for (int64_t i = 0; i < movie_info_idx.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_info_idx_rows_scanned;
#endif
        if (mii_movie_id.is_null[idx] != 0 || mii_info_type_id.is_null[idx] != 0 ||
            mii_info_col.is_null[idx] != 0) {
            continue;
        }
        const int32_t info_type_id = mii_info_type_id.i32[idx];
        if (info_type_id != id2 && info_type_id != id3) {
            continue;
        }
        const int32_t movie_id = mii_movie_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_movie_flags, movie_id)) {
            continue;
        }
        const std::string& info_str = db.string_pool.get(mii_info_col.str_ids[idx]);
        double numeric_value = 0.0;
        if (!q7a_internal::parse_numeric_value(info_str, numeric_value)) {
            continue;
        }
        if (info_type_id == id2) {
            if (numeric_value < info4 || numeric_value > info5) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_info_idx_rows_emitted_id2;
            ++trace.movie_info_idx_agg_rows_in_id2;
            if (mii1_counts[static_cast<size_t>(movie_id)] == 0) {
                ++trace.movie_info_idx_groups_created_id2;
            }
#endif
            if (mii1_counts[static_cast<size_t>(movie_id)]++ == 0) {
                ++mii1_nonzero;
            }
        } else {
            if (numeric_value < info3 || numeric_value > info2) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_info_idx_rows_emitted_id3;
            ++trace.movie_info_idx_agg_rows_in_id3;
            if (mii2_counts[static_cast<size_t>(movie_id)] == 0) {
                ++trace.movie_info_idx_groups_created_id3;
            }
#endif
            if (mii2_counts[static_cast<size_t>(movie_id)]++ == 0) {
                ++mii2_nonzero;
            }
        }
    }
#ifdef TRACE
    }
#endif
    if (mii1_nonzero == 0 || mii2_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int64_t> keyword_counts(allowed_movie_flags.size(), 0);
    size_t keyword_nonzero = 0;
    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q7a_movie_keyword_scan");
#endif
    for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_keyword_rows_scanned;
#endif
        if (mk_movie_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mk_movie_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_movie_flags, movie_id)) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_keyword_rows_emitted;
        ++trace.movie_keyword_agg_rows_in;
        if (keyword_counts[static_cast<size_t>(movie_id)] == 0) {
            ++trace.movie_keyword_groups_created;
        }
#endif
        if (keyword_counts[static_cast<size_t>(movie_id)]++ == 0) {
            ++keyword_nonzero;
        }
    }
#ifdef TRACE
    }
#endif
    if (keyword_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int64_t> movie_cast_counts(allowed_movie_flags.size(), 0);
    size_t movie_cast_nonzero = 0;
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q7a_cast_info_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < cast_info.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.cast_info_rows_scanned;
#endif
        if (ci_movie_id.is_null[idx] != 0 || ci_person_id.is_null[idx] != 0 ||
            ci_role_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = ci_movie_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_movie_flags, movie_id)) {
            continue;
        }
        const int32_t person_id = ci_person_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_person_flags, person_id)) {
            continue;
        }
        const int32_t role_id = ci_role_id.i32[idx];
        if (!q7a_internal::flag_contains(allowed_role_flags, role_id)) {
            continue;
        }
        if (!q7a_internal::matches_string(note_col, idx, note_filter)) {
            continue;
        }
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.cast_info_join_probe_rows_in;
#endif
        const int64_t aka_count = aka_counts[static_cast<size_t>(person_id)];
        if (aka_count == 0) {
            continue;
        }
        const int64_t pi_count = person_info_counts[static_cast<size_t>(person_id)];
        if (pi_count == 0) {
            continue;
        }
        const int64_t multiplier = aka_count * pi_count;
#ifdef TRACE
        ++trace.cast_info_join_rows_emitted;
        ++trace.movie_cast_agg_rows_in;
        if (movie_cast_counts[static_cast<size_t>(movie_id)] == 0) {
            ++trace.movie_cast_groups_created;
        }
#endif
        if (movie_cast_counts[static_cast<size_t>(movie_id)] == 0) {
            ++movie_cast_nonzero;
        }
        movie_cast_counts[static_cast<size_t>(movie_id)] += multiplier;
    }
#ifdef TRACE
        }
    }
#endif
    if (movie_cast_nonzero == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    for (const int32_t movie_id : allowed_movie_ids) {
        const size_t movie_idx = static_cast<size_t>(movie_id);
        const int64_t cast_count = movie_cast_counts[movie_idx];
        if (cast_count == 0) {
            continue;
        }
        const int64_t mi_count = mi_counts[movie_idx];
        if (mi_count == 0) {
            continue;
        }
        const int64_t mii1_count = mii1_counts[movie_idx];
        if (mii1_count == 0) {
            continue;
        }
        const int64_t mii2_count = mii2_counts[movie_idx];
        if (mii2_count == 0) {
            continue;
        }
        const int64_t keyword_count = keyword_counts[movie_idx];
        if (keyword_count == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.title_join_rows_emitted;
#endif
        total += cast_count * mi_count * mii1_count * mii2_count * keyword_count;
    }
#ifdef TRACE
    trace.aka_name_agg_rows_emitted = aka_nonzero;
    trace.person_info_agg_rows_emitted = person_info_nonzero;
    trace.movie_info_agg_rows_emitted = mi_nonzero;
    trace.movie_info_idx_agg_rows_emitted_id2 = mii1_nonzero;
    trace.movie_info_idx_agg_rows_emitted_id3 = mii2_nonzero;
    trace.movie_keyword_agg_rows_emitted = keyword_nonzero;
    trace.movie_cast_agg_rows_emitted = movie_cast_nonzero;
    trace.title_join_build_rows_in = static_cast<uint64_t>(
        movie_cast_nonzero + mi_nonzero + mii1_nonzero + mii2_nonzero + keyword_nonzero);
    trace.cast_info_join_build_rows_in = static_cast<uint64_t>(
        allowed_person_count + aka_nonzero + person_info_nonzero + allowed_role_count);
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q7a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q7a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q7a_aka_name_scan_rows_scanned", trace.aka_name_rows_scanned);
    print_count("q7a_aka_name_scan_rows_emitted", trace.aka_name_rows_emitted);
    print_count("q7a_aka_name_agg_rows_in", trace.aka_name_agg_rows_in);
    print_count("q7a_aka_name_groups_created", trace.aka_name_groups_created);
    print_count("q7a_aka_name_agg_rows_emitted", trace.aka_name_agg_rows_emitted);
    print_count("q7a_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q7a_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q7a_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q7a_person_info_groups_created", trace.person_info_groups_created);
    print_count("q7a_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q7a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q7a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q7a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q7a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q7a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q7a_movie_info_idx_scan_rows_scanned", trace.movie_info_idx_rows_scanned);
    print_count("q7a_movie_info_idx_scan_rows_emitted_id2", trace.movie_info_idx_rows_emitted_id2);
    print_count("q7a_movie_info_idx_scan_rows_emitted_id3", trace.movie_info_idx_rows_emitted_id3);
    print_count("q7a_movie_info_idx_agg_rows_in_id2", trace.movie_info_idx_agg_rows_in_id2);
    print_count("q7a_movie_info_idx_agg_rows_in_id3", trace.movie_info_idx_agg_rows_in_id3);
    print_count("q7a_movie_info_idx_groups_created_id2",
                trace.movie_info_idx_groups_created_id2);
    print_count("q7a_movie_info_idx_groups_created_id3",
                trace.movie_info_idx_groups_created_id3);
    print_count("q7a_movie_info_idx_agg_rows_emitted_id2",
                trace.movie_info_idx_agg_rows_emitted_id2);
    print_count("q7a_movie_info_idx_agg_rows_emitted_id3",
                trace.movie_info_idx_agg_rows_emitted_id3);
    print_count("q7a_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q7a_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q7a_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q7a_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q7a_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q7a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q7a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q7a_cast_info_join_build_rows_in", trace.cast_info_join_build_rows_in);
    print_count("q7a_cast_info_join_probe_rows_in", trace.cast_info_join_probe_rows_in);
    print_count("q7a_cast_info_join_rows_emitted", trace.cast_info_join_rows_emitted);
    print_count("q7a_movie_cast_agg_rows_in", trace.movie_cast_agg_rows_in);
    print_count("q7a_movie_cast_groups_created", trace.movie_cast_groups_created);
    print_count("q7a_movie_cast_agg_rows_emitted", trace.movie_cast_agg_rows_emitted);
    print_count("q7a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q7a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q7a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q7a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q7a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q7a_query_output_rows", trace.query_output_rows);
#endif
    return total;
}