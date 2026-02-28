#include "query_q10a.hpp"

#include <algorithm>
#include <cctype>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "trace.hpp"

namespace q10a_internal {

struct IntFilter {
    std::unordered_set<int32_t> values;
};

struct StringFilter {
    std::unordered_set<int32_t> values;
};

struct IdMask {
    std::vector<uint8_t> mask;
    bool any = false;
};

std::string to_lower(const std::string& value) {
    std::string out;
    out.resize(value.size());
    for (size_t i = 0; i < value.size(); ++i) {
        out[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(value[i])));
    }
    return out;
}

std::string strip_single_quotes(const std::string& value) {
    if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
        return value.substr(1, value.size() - 2);
    }
    return value;
}

bool like_match(const std::string& pattern, const std::string& text) {
    size_t p = 0;
    size_t t = 0;
    size_t star = std::string::npos;
    size_t match = 0;
    while (t < text.size()) {
        if (p < pattern.size() && (pattern[p] == '_' || pattern[p] == text[t])) {
            ++p;
            ++t;
            continue;
        }
        if (p < pattern.size() && pattern[p] == '%') {
            star = p++;
            match = t;
            continue;
        }
        if (star != std::string::npos) {
            p = star + 1;
            ++match;
            t = match;
            continue;
        }
        return false;
    }
    while (p < pattern.size() && pattern[p] == '%') {
        ++p;
    }
    return p == pattern.size();
}

struct LikePattern {
    enum class Type { kMatchAll, kExact, kPrefix, kSuffix, kContains, kGeneral };
    Type type = Type::kGeneral;
    std::string token;
    std::string pattern_lower;
};

bool equals_case_insensitive(const std::string& value, const std::string& token) {
    if (value.size() != token.size()) {
        return false;
    }
    for (size_t i = 0; i < token.size(); ++i) {
        if (static_cast<char>(std::tolower(static_cast<unsigned char>(value[i]))) != token[i]) {
            return false;
        }
    }
    return true;
}

bool starts_with_case_insensitive(const std::string& value, const std::string& token) {
    if (value.size() < token.size()) {
        return false;
    }
    for (size_t i = 0; i < token.size(); ++i) {
        if (static_cast<char>(std::tolower(static_cast<unsigned char>(value[i]))) != token[i]) {
            return false;
        }
    }
    return true;
}

bool ends_with_case_insensitive(const std::string& value, const std::string& token) {
    if (value.size() < token.size()) {
        return false;
    }
    const size_t offset = value.size() - token.size();
    for (size_t i = 0; i < token.size(); ++i) {
        if (static_cast<char>(std::tolower(static_cast<unsigned char>(value[offset + i]))) !=
            token[i]) {
            return false;
        }
    }
    return true;
}

bool contains_case_insensitive(const std::string& value, const std::string& token) {
    if (token.empty()) {
        return true;
    }
    if (value.size() < token.size()) {
        return false;
    }
    const size_t limit = value.size() - token.size();
    for (size_t i = 0; i <= limit; ++i) {
        size_t j = 0;
        for (; j < token.size(); ++j) {
            if (static_cast<char>(std::tolower(static_cast<unsigned char>(value[i + j]))) !=
                token[j]) {
                break;
            }
        }
        if (j == token.size()) {
            return true;
        }
    }
    return false;
}

LikePattern build_ilike_pattern(const std::string& pattern_raw) {
    LikePattern pattern;
    pattern.pattern_lower = to_lower(strip_single_quotes(pattern_raw));
    if (pattern.pattern_lower == "%") {
        pattern.type = LikePattern::Type::kMatchAll;
        return pattern;
    }

    const bool has_underscore = pattern.pattern_lower.find('_') != std::string::npos;
    const size_t first_percent = pattern.pattern_lower.find('%');
    if (!has_underscore) {
        if (first_percent == std::string::npos) {
            pattern.type = LikePattern::Type::kExact;
            pattern.token = pattern.pattern_lower;
            return pattern;
        }
        const size_t last_percent = pattern.pattern_lower.rfind('%');
        if (first_percent == 0 && last_percent == pattern.pattern_lower.size() - 1 &&
            pattern.pattern_lower.find('%', 1) == last_percent) {
            pattern.type = LikePattern::Type::kContains;
            if (pattern.pattern_lower.size() > 2) {
                pattern.token = pattern.pattern_lower.substr(1, pattern.pattern_lower.size() - 2);
            }
            return pattern;
        }
        if (first_percent == 0 && pattern.pattern_lower.find('%', 1) == std::string::npos) {
            pattern.type = LikePattern::Type::kSuffix;
            pattern.token = pattern.pattern_lower.substr(1);
            return pattern;
        }
        if (last_percent == pattern.pattern_lower.size() - 1 &&
            pattern.pattern_lower.find('%') == last_percent) {
            pattern.type = LikePattern::Type::kPrefix;
            pattern.token = pattern.pattern_lower.substr(0, pattern.pattern_lower.size() - 1);
            return pattern;
        }
    }

    pattern.type = LikePattern::Type::kGeneral;
    return pattern;
}

bool ilike_match_lower(const LikePattern& pattern, const std::string& value_lower) {
    switch (pattern.type) {
        case LikePattern::Type::kMatchAll:
            return true;
        case LikePattern::Type::kExact:
            return value_lower == pattern.token;
        case LikePattern::Type::kPrefix:
            return value_lower.size() >= pattern.token.size() &&
                   value_lower.compare(0, pattern.token.size(), pattern.token) == 0;
        case LikePattern::Type::kSuffix:
            return value_lower.size() >= pattern.token.size() &&
                   value_lower.compare(value_lower.size() - pattern.token.size(),
                                       pattern.token.size(),
                                       pattern.token) == 0;
        case LikePattern::Type::kContains:
            if (pattern.token.empty()) {
                return true;
            }
            return value_lower.find(pattern.token) != std::string::npos;
        case LikePattern::Type::kGeneral:
            return like_match(pattern.pattern_lower, value_lower);
    }
    return false;
}

bool ilike_match(const LikePattern& pattern, const std::string& value) {
    return ilike_match_lower(pattern, to_lower(value));
}

IntFilter build_int_filter(const std::vector<std::string>& values) {
    IntFilter filter;
    filter.values.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        try {
            filter.values.insert(static_cast<int32_t>(std::stoi(val)));
        } catch (const std::exception&) {
            continue;
        }
    }
    return filter;
}

std::unordered_set<int32_t> build_column_string_values(const ColumnData& col) {
    std::unordered_set<int32_t> values;
    values.reserve(col.str_ids.size());
    for (size_t i = 0; i < col.str_ids.size(); ++i) {
        if (col.is_null[i] != 0) {
            continue;
        }
        values.insert(col.str_ids[i]);
    }
    return values;
}

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const ColumnData& col,
                                            const std::vector<std::string>& values) {
    StringFilter filter;
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
            filter.values.insert(found_id);
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
    return filter;
}

IdMask build_allowed_id_mask(const TableData& table,
                             const std::string& id_col_name,
                             const std::string& str_col_name,
                             const StringFilter& filter) {
    IdMask ids;
    ids.mask.assign(static_cast<size_t>(table.row_count) + 1, 0);
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0 || str_col.is_null[i] != 0) {
            continue;
        }
        if (filter.values.find(str_col.str_ids[static_cast<size_t>(i)]) == filter.values.end()) {
            continue;
        }
        const int32_t id = id_col.i32[static_cast<size_t>(i)];
        if (id < 0 || static_cast<size_t>(id) >= ids.mask.size()) {
            continue;
        }
        ids.mask[static_cast<size_t>(id)] = 1;
        ids.any = true;
    }
    return ids;
}

}  // namespace q10a_internal

#ifdef TRACE
struct Q10aTrace {
    TraceRecorder recorder;
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t movie_info_rows_scanned = 0;
    uint64_t movie_info_rows_emitted = 0;
    uint64_t movie_info_agg_rows_in = 0;
    uint64_t movie_info_groups_created = 0;
    uint64_t movie_info_agg_rows_emitted = 0;
    uint64_t name_rows_scanned = 0;
    uint64_t name_rows_emitted = 0;
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t result_join_build_rows_in = 0;
    uint64_t result_join_probe_rows_in = 0;
    uint64_t result_join_rows_emitted = 0;
    uint64_t result_agg_rows_in = 0;
    uint64_t result_groups_created = 0;
    uint64_t result_agg_rows_emitted = 0;
    uint64_t query_output_rows = 0;
};
#endif

std::vector<Q10aResultRow> run_q10a(const Database& db, const Q10aArgs& args) {
    std::vector<Q10aResultRow> rows;
#ifdef TRACE
    Q10aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q10a_total");
#endif
    const auto info_type_filter = q10a_internal::build_int_filter(args.ID);
    if (info_type_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& info_type = db.tables.at("info_type");
    const auto& title = db.tables.at("title");
    const auto& movie_info = db.tables.at("movie_info");
    const auto& cast_info = db.tables.at("cast_info");
    const auto& name = db.tables.at("name");

    const auto kind_filter = q10a_internal::build_string_filter_for_column(
        db.string_pool, kind_type.columns.at("kind"), args.KIND);
    const auto role_filter = q10a_internal::build_string_filter_for_column(
        db.string_pool, role_type.columns.at("role"), args.ROLE);
    const auto info_filter = q10a_internal::build_string_filter_for_column(
        db.string_pool, movie_info.columns.at("info"), args.INFO);
    const size_t string_pool_size = db.string_pool.lower_values.size();
    const auto& lower_values = db.string_pool.lower_values;
    const bool use_short_info =
        info_filter.values.size() <= static_cast<size_t>(std::numeric_limits<int16_t>::max());
    std::vector<int16_t> info_dense_by_str_id16;
    std::vector<int32_t> info_dense_by_str_id32;
    if (use_short_info) {
        info_dense_by_str_id16.assign(string_pool_size, -1);
    } else {
        info_dense_by_str_id32.assign(string_pool_size, -1);
    }
    std::vector<int32_t> info_dense_to_id;
    info_dense_to_id.reserve(info_filter.values.size());
    for (const auto info_id : info_filter.values) {
        if (info_id < 0 || static_cast<size_t>(info_id) >= string_pool_size) {
            continue;
        }
        if (use_short_info) {
            if (info_dense_by_str_id16[static_cast<size_t>(info_id)] != -1) {
                continue;
            }
            info_dense_by_str_id16[static_cast<size_t>(info_id)] =
                static_cast<int16_t>(info_dense_to_id.size());
        } else {
            if (info_dense_by_str_id32[static_cast<size_t>(info_id)] != -1) {
                continue;
            }
            info_dense_by_str_id32[static_cast<size_t>(info_id)] =
                static_cast<int32_t>(info_dense_to_id.size());
        }
        info_dense_to_id.push_back(info_id);
    }

    const auto kind_ids =
        q10a_internal::build_allowed_id_mask(kind_type, "id", "kind", kind_filter);
    if (!kind_ids.any || role_filter.values.empty() || info_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    const auto role_ids =
        q10a_internal::build_allowed_id_mask(role_type, "id", "role", role_filter);
    if (!role_ids.any) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto name_pattern = q10a_internal::build_ilike_pattern(args.NAME);
    std::vector<uint8_t> info_type_mask(static_cast<size_t>(info_type.row_count) + 1, 0);
    bool has_info_type = false;
    for (const auto id : info_type_filter.values) {
        if (id < 0 || static_cast<size_t>(id) >= info_type_mask.size()) {
            continue;
        }
        info_type_mask[static_cast<size_t>(id)] = 1;
        has_info_type = true;
    }
    if (!has_info_type) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const size_t title_row_count = static_cast<size_t>(title.row_count) + 1;
    std::vector<int32_t> title_year_value(title_row_count, 0);
    std::vector<uint8_t> title_present(title_row_count, 0);
    std::vector<uint8_t> title_has_year(title_row_count, 0);
    size_t title_year_count = 0;
    const auto& title_id_col = title.columns.at("id");
    const auto& title_kind_col = title.columns.at("kind_id");
    const auto& title_year_col = title.columns.at("production_year");
    const bool title_id_has_null = title_id_col.has_nulls;
    const bool title_kind_has_null = title_kind_col.has_nulls;
    const bool title_year_has_null = title_year_col.has_nulls;
    const auto* title_id_data = title_id_col.i32.data();
    const auto* title_kind_data = title_kind_col.i32.data();
    const auto* title_year_col_data = title_year_col.i32.data();
    const auto* title_id_null = title_id_col.is_null.data();
    const auto* title_kind_null = title_kind_col.is_null.data();
    const auto* title_year_null = title_year_col.is_null.data();
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q10a_title_scan");
        if (!skip_query) {
#endif
    const bool title_id_in_range =
        title_id_col.i32_min >= 0 &&
        title_id_col.i32_max < static_cast<int32_t>(title_row_count);
    const bool kind_id_in_range =
        title_kind_col.i32_min >= 0 &&
        title_kind_col.i32_max < static_cast<int32_t>(kind_ids.mask.size());
    for (int64_t i = 0; i < title.row_count; ++i) {
#ifdef TRACE
        ++trace.title_rows_scanned;
#endif
        if ((title_id_has_null && title_id_null[i] != 0) ||
            (title_kind_has_null && title_kind_null[i] != 0)) {
            continue;
        }
        const int32_t kind_id = title_kind_data[static_cast<size_t>(i)];
        if (!kind_id_in_range) {
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_ids.mask.size()) {
                continue;
            }
        }
        if (kind_ids.mask[static_cast<size_t>(kind_id)] == 0) {
            continue;
        }
        const int32_t title_id = title_id_data[static_cast<size_t>(i)];
        if (!title_id_in_range) {
            if (title_id < 0 || static_cast<size_t>(title_id) >= title_row_count) {
                continue;
            }
        }
        const size_t title_index = static_cast<size_t>(title_id);
        if (title_present[title_index] == 0) {
            title_present[title_index] = 1;
            ++title_year_count;
        }
        if (title_year_has_null && title_year_null[i] != 0) {
            title_has_year[title_index] = 0;
        } else {
            title_has_year[title_index] = 1;
            title_year_value[title_index] = title_year_col_data[static_cast<size_t>(i)];
        }
#ifdef TRACE
        ++trace.title_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (title_year_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> movie_info_offsets(static_cast<size_t>(title.row_count) + 1, -1);
    std::vector<int32_t> movie_info_counts(static_cast<size_t>(title.row_count) + 1, 0);
    std::vector<uint16_t> movie_info_values16;
    std::vector<int32_t> movie_info_values32;
    const size_t movie_info_reserve =
        static_cast<size_t>(movie_info.row_count / 5) + 16;
    if (use_short_info) {
        movie_info_values16.reserve(movie_info_reserve);
    } else {
        movie_info_values32.reserve(movie_info_reserve);
    }
    size_t movie_info_group_count = 0;
    const auto& mi_movie_col = movie_info.columns.at("movie_id");
    const auto& mi_type_col = movie_info.columns.at("info_type_id");
    const auto& mi_info_col = movie_info.columns.at("info");
    const bool mi_movie_has_null = mi_movie_col.has_nulls;
    const bool mi_type_has_null = mi_type_col.has_nulls;
    const bool mi_info_has_null = mi_info_col.has_nulls;
    const auto* mi_movie_data = mi_movie_col.i32.data();
    const auto* mi_type_data = mi_type_col.i32.data();
    const auto* mi_info_data = mi_info_col.str_ids.data();
    const auto* mi_movie_null = mi_movie_col.is_null.data();
    const auto* mi_type_null = mi_type_col.is_null.data();
    const auto* mi_info_null = mi_info_col.is_null.data();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q10a_movie_info_scan");
#endif
    const bool mi_movie_in_range =
        mi_movie_col.i32_min >= 0 &&
        mi_movie_col.i32_max < static_cast<int32_t>(movie_info_offsets.size());
    const bool mi_type_in_range =
        mi_type_col.i32_min >= 0 &&
        mi_type_col.i32_max < static_cast<int32_t>(info_type_mask.size());
    for (int64_t i = 0; i < movie_info.row_count; ++i) {
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if ((mi_type_has_null && mi_type_null[i] != 0) ||
            (mi_info_has_null && mi_info_null[i] != 0)) {
            continue;
        }
        const int32_t info_type_id = mi_type_data[static_cast<size_t>(i)];
        if (!mi_type_in_range) {
            if (info_type_id < 0 || static_cast<size_t>(info_type_id) >= info_type_mask.size()) {
                continue;
            }
        }
        if (info_type_mask[static_cast<size_t>(info_type_id)] == 0) {
            continue;
        }
        const int32_t info_id = mi_info_data[static_cast<size_t>(i)];
        if (info_id < 0 || static_cast<size_t>(info_id) >= string_pool_size) {
            continue;
        }
        const int32_t info_dense = use_short_info
                                       ? static_cast<int32_t>(
                                             info_dense_by_str_id16[static_cast<size_t>(info_id)])
                                       : info_dense_by_str_id32[static_cast<size_t>(info_id)];
        if (info_dense == -1) {
            continue;
        }
        if (mi_movie_has_null && mi_movie_null[i] != 0) {
            continue;
        }
        const int32_t movie_id = mi_movie_data[static_cast<size_t>(i)];
        if (!mi_movie_in_range) {
            if (movie_id < 0 || static_cast<size_t>(movie_id) >= movie_info_offsets.size()) {
                continue;
            }
        }
        if (title_present[static_cast<size_t>(movie_id)] == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_info_rows_emitted;
        ++trace.movie_info_agg_rows_in;
#else
#endif
        if (movie_info_offsets[static_cast<size_t>(movie_id)] == -1) {
#ifdef TRACE
            ++trace.movie_info_groups_created;
#endif
            movie_info_offsets[static_cast<size_t>(movie_id)] = static_cast<int32_t>(
                use_short_info ? movie_info_values16.size() : movie_info_values32.size());
            ++movie_info_group_count;
        }
        if (use_short_info) {
            movie_info_values16.push_back(static_cast<uint16_t>(info_dense));
        } else {
            movie_info_values32.push_back(info_dense);
        }
        ++movie_info_counts[static_cast<size_t>(movie_id)];
    }
#ifdef TRACE
    }
#endif
    if ((use_short_info && movie_info_values16.empty()) ||
        (!use_short_info && movie_info_values32.empty())) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> person_name_index_by_id(static_cast<size_t>(name.row_count) + 1, -1);
    std::vector<uint8_t> candidate_person_mask(person_name_index_by_id.size(), 0);
    std::vector<int32_t> filtered_movie_ids;
    std::vector<int32_t> filtered_person_ids;
    const size_t filtered_reserve =
        static_cast<size_t>(cast_info.row_count / 64) + 16;
    filtered_movie_ids.reserve(filtered_reserve);
    filtered_person_ids.reserve(filtered_reserve);

    const auto& ci_movie_col = cast_info.columns.at("movie_id");
    const auto& ci_person_col = cast_info.columns.at("person_id");
    const auto& ci_role_col = cast_info.columns.at("role_id");
    const bool ci_movie_in_range =
        ci_movie_col.i32_min >= 0 &&
        ci_movie_col.i32_max < static_cast<int32_t>(title_row_count);
    const bool ci_person_in_range =
        ci_person_col.i32_min >= 0 &&
        ci_person_col.i32_max < static_cast<int32_t>(person_name_index_by_id.size());
    const bool ci_role_in_range =
        ci_role_col.i32_min >= 0 &&
        ci_role_col.i32_max < static_cast<int32_t>(role_ids.mask.size());
    const bool ci_movie_has_null = ci_movie_col.has_nulls;
    const bool ci_person_has_null = ci_person_col.has_nulls;
    const bool ci_role_has_null = ci_role_col.has_nulls;
    const auto* ci_movie_data = ci_movie_col.i32.data();
    const auto* ci_person_data = ci_person_col.i32.data();
    const auto* ci_role_data = ci_role_col.i32.data();
    const auto* ci_movie_null = ci_movie_col.is_null.data();
    const auto* ci_person_null = ci_person_col.is_null.data();
    const auto* ci_role_null = ci_role_col.is_null.data();
    const auto* role_mask = role_ids.mask.data();
    const auto* movie_info_offset_data = movie_info_offsets.data();
    const auto* movie_info_count_data = movie_info_counts.data();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q10a_cast_info_scan");
#endif
    std::vector<int32_t> candidate_movies;
    if (cast_info.sorted_by_movie_id && !ci_movie_has_null) {
        candidate_movies.reserve(movie_info_group_count);
        for (size_t movie_id = 0; movie_id < movie_info_offsets.size(); ++movie_id) {
            if (movie_info_offset_data[movie_id] == -1) {
                continue;
            }
            if (title_present[movie_id] == 0) {
                continue;
            }
            candidate_movies.push_back(static_cast<int32_t>(movie_id));
        }
    }
    auto collect_row = [&](size_t row, int32_t movie_id) {
        if (ci_role_has_null && ci_role_null[row] != 0) {
            return;
        }
        const int32_t role_id = ci_role_data[row];
        if (!ci_role_in_range) {
            if (role_id < 0 || static_cast<size_t>(role_id) >= role_ids.mask.size()) {
                return;
            }
        }
        if (role_mask[static_cast<size_t>(role_id)] == 0) {
            return;
        }
        if (ci_person_has_null && ci_person_null[row] != 0) {
            return;
        }
        const int32_t person_id = ci_person_data[row];
        if (!ci_person_in_range) {
            if (person_id < 0 ||
                static_cast<size_t>(person_id) >= person_name_index_by_id.size()) {
                return;
            }
        }
        candidate_person_mask[static_cast<size_t>(person_id)] = 1;
        filtered_movie_ids.push_back(movie_id);
        filtered_person_ids.push_back(person_id);
    };
    if (cast_info.sorted_by_movie_id && !ci_movie_has_null) {
        size_t row = 0;
        size_t candidate_idx = 0;
        const size_t row_count = static_cast<size_t>(cast_info.row_count);
        while (row < row_count && candidate_idx < candidate_movies.size()) {
            const int32_t target = candidate_movies[candidate_idx];
            while (row < row_count && ci_movie_data[row] < target) {
                ++row;
            }
            if (row >= row_count) {
                break;
            }
            const int32_t current_movie = ci_movie_data[row];
            if (current_movie > target) {
                ++candidate_idx;
                continue;
            }
            while (row < row_count && ci_movie_data[row] == target) {
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                collect_row(row, target);
                ++row;
            }
            ++candidate_idx;
        }
    } else {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            if (ci_movie_has_null && ci_movie_null[i] != 0) {
                continue;
            }
            const int32_t movie_id = ci_movie_data[static_cast<size_t>(i)];
            if (!ci_movie_in_range) {
                if (movie_id < 0 || static_cast<size_t>(movie_id) >= title_row_count) {
                    continue;
                }
            }
            if (title_present[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            if (movie_info_offset_data[static_cast<size_t>(movie_id)] == -1) {
                continue;
            }
            collect_row(static_cast<size_t>(i), movie_id);
        }
    }
#ifdef TRACE
    }
#endif
    if (filtered_movie_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> name_dense_by_str_id(string_pool_size, -1);
    std::vector<int32_t> name_id_by_index;
    name_id_by_index.reserve(1024);
    size_t name_group_count = 0;
    const auto& name_id_col = name.columns.at("id");
    const auto& name_str_col = name.columns.at("name");
    const bool match_all_names = name_pattern.type == q10a_internal::LikePattern::Type::kMatchAll;
    const bool name_id_has_null = name_id_col.has_nulls;
    const bool name_str_has_null = name_str_col.has_nulls;
    const bool name_id_in_range =
        name_id_col.i32_min >= 0 &&
        name_id_col.i32_max < static_cast<int32_t>(person_name_index_by_id.size());
    const auto* name_id_data = name_id_col.i32.data();
    const auto* name_str_data = name_str_col.str_ids.data();
    const auto* name_id_null = name_id_col.is_null.data();
    const auto* name_str_null = name_str_col.is_null.data();
    std::vector<int8_t> name_match_cache;
    if (!match_all_names) {
        name_match_cache.assign(string_pool_size, -1);
    }
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q10a_name_scan");
#endif
    for (int64_t i = 0; i < name.row_count; ++i) {
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (name_id_has_null && name_id_null[i] != 0) {
            continue;
        }
        const int32_t person_id = name_id_data[static_cast<size_t>(i)];
        if (!name_id_in_range) {
            if (person_id < 0 ||
                static_cast<size_t>(person_id) >= person_name_index_by_id.size()) {
                continue;
            }
        }
        if (candidate_person_mask[static_cast<size_t>(person_id)] == 0) {
            continue;
        }
        if (name_str_has_null && name_str_null[i] != 0) {
            continue;
        }
        const int32_t name_str_id = name_str_data[static_cast<size_t>(i)];
        if (name_str_id < 0 || static_cast<size_t>(name_str_id) >= lower_values.size()) {
            continue;
        }
        if (!match_all_names) {
            int8_t cached = name_match_cache[static_cast<size_t>(name_str_id)];
            if (cached == -1) {
                const std::string& name_lower = lower_values[static_cast<size_t>(name_str_id)];
                cached = q10a_internal::ilike_match_lower(name_pattern, name_lower) ? 1 : 0;
                name_match_cache[static_cast<size_t>(name_str_id)] = cached;
            }
            if (cached == 0) {
                continue;
            }
        }
        int32_t name_index = name_dense_by_str_id[static_cast<size_t>(name_str_id)];
        if (name_index == -1) {
            name_index = static_cast<int32_t>(name_id_by_index.size());
            name_dense_by_str_id[static_cast<size_t>(name_str_id)] = name_index;
            name_id_by_index.push_back(name_str_id);
            ++name_group_count;
        }
        person_name_index_by_id[static_cast<size_t>(person_id)] = name_index;
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif
    if (name_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    struct Agg {
        bool has_year{};
        int32_t min_year{};
        int32_t max_year{};
    };
    constexpr size_t kAggMaxSlots = 5'000'000;
    const size_t info_count = info_dense_to_id.size();
    const size_t total_slots = info_count * name_group_count;
    const bool use_dense_agg = total_slots > 0 && total_slots <= kAggMaxSlots;
    std::vector<Agg> agg_matrix;
    std::vector<uint8_t> agg_used;
    std::vector<uint32_t> agg_used_indices;
    std::unordered_map<uint64_t, Agg> agg_map;
    if (use_dense_agg) {
        agg_matrix.resize(total_slots);
        agg_used.assign(total_slots, 0);
        agg_used_indices.reserve(std::min(total_slots, static_cast<size_t>(100000)));
    } else {
        agg_map.reserve(std::min(total_slots, kAggMaxSlots) + 16);
    }

#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q10a_result_join");
#endif
    const auto* person_name_index_data = person_name_index_by_id.data();
    const auto* movie_info_values16_data = movie_info_values16.data();
    const auto* movie_info_values32_data = movie_info_values32.data();
    auto process_row_short = [&](int32_t name_index,
                                 bool year_has,
                                 int32_t year_value,
                                 int32_t offset,
                                 int32_t count) {
        const size_t agg_base = static_cast<size_t>(name_index) * info_count;
        const int32_t end = offset + count;
        for (int32_t idx = offset; idx < end; ++idx) {
            const int32_t info_dense =
                static_cast<int32_t>(movie_info_values16_data[static_cast<size_t>(idx)]);
#ifdef TRACE
            ++trace.result_join_rows_emitted;
            ++trace.result_agg_rows_in;
            if (use_dense_agg) {
                const size_t agg_index = agg_base + static_cast<size_t>(info_dense);
                Agg& entry = agg_matrix[agg_index];
                if (agg_used[agg_index] == 0) {
                    agg_used[agg_index] = 1;
                    agg_used_indices.push_back(static_cast<uint32_t>(agg_index));
                    if (year_has) {
                        entry = Agg{true, year_value, year_value};
                    } else {
                        entry = Agg{false, 0, 0};
                    }
                    ++trace.result_groups_created;
                } else if (year_has) {
                    if (entry.has_year) {
                        entry.min_year = std::min(entry.min_year, year_value);
                        entry.max_year = std::max(entry.max_year, year_value);
                    } else {
                        entry.has_year = true;
                        entry.min_year = year_value;
                        entry.max_year = year_value;
                    }
                }
            } else {
                const uint64_t key =
                    (static_cast<uint64_t>(static_cast<uint32_t>(info_dense)) << 32) |
                    static_cast<uint32_t>(name_index);
                auto it = agg_map.find(key);
                if (it == agg_map.end()) {
                    if (year_has) {
                        agg_map.emplace(key, Agg{true, year_value, year_value});
                    } else {
                        agg_map.emplace(key, Agg{false, 0, 0});
                    }
                    ++trace.result_groups_created;
                } else if (year_has) {
                    if (it->second.has_year) {
                        it->second.min_year = std::min(it->second.min_year, year_value);
                        it->second.max_year = std::max(it->second.max_year, year_value);
                    } else {
                        it->second.has_year = true;
                        it->second.min_year = year_value;
                        it->second.max_year = year_value;
                    }
                }
            }
#else
            if (use_dense_agg) {
                const size_t agg_index = agg_base + static_cast<size_t>(info_dense);
                Agg& entry = agg_matrix[agg_index];
                if (agg_used[agg_index] == 0) {
                    agg_used[agg_index] = 1;
                    agg_used_indices.push_back(static_cast<uint32_t>(agg_index));
                    if (year_has) {
                        entry = Agg{true, year_value, year_value};
                    } else {
                        entry = Agg{false, 0, 0};
                    }
                } else if (year_has) {
                    if (entry.has_year) {
                        entry.min_year = std::min(entry.min_year, year_value);
                        entry.max_year = std::max(entry.max_year, year_value);
                    } else {
                        entry.has_year = true;
                        entry.min_year = year_value;
                        entry.max_year = year_value;
                    }
                }
            } else {
                const uint64_t key =
                    (static_cast<uint64_t>(static_cast<uint32_t>(info_dense)) << 32) |
                    static_cast<uint32_t>(name_index);
                auto it = agg_map.find(key);
                if (it == agg_map.end()) {
                    if (year_has) {
                        agg_map.emplace(key, Agg{true, year_value, year_value});
                    } else {
                        agg_map.emplace(key, Agg{false, 0, 0});
                    }
                } else if (year_has) {
                    if (it->second.has_year) {
                        it->second.min_year = std::min(it->second.min_year, year_value);
                        it->second.max_year = std::max(it->second.max_year, year_value);
                    } else {
                        it->second.has_year = true;
                        it->second.min_year = year_value;
                        it->second.max_year = year_value;
                    }
                }
            }
#endif
        }
    };
    auto process_row_long = [&](int32_t name_index,
                                bool year_has,
                                int32_t year_value,
                                int32_t offset,
                                int32_t count) {
        const size_t agg_base = static_cast<size_t>(name_index) * info_count;
        const int32_t end = offset + count;
        for (int32_t idx = offset; idx < end; ++idx) {
            const int32_t info_dense = movie_info_values32_data[static_cast<size_t>(idx)];
#ifdef TRACE
            ++trace.result_join_rows_emitted;
            ++trace.result_agg_rows_in;
            if (use_dense_agg) {
                const size_t agg_index = agg_base + static_cast<size_t>(info_dense);
                Agg& entry = agg_matrix[agg_index];
                if (agg_used[agg_index] == 0) {
                    agg_used[agg_index] = 1;
                    agg_used_indices.push_back(static_cast<uint32_t>(agg_index));
                    if (year_has) {
                        entry = Agg{true, year_value, year_value};
                    } else {
                        entry = Agg{false, 0, 0};
                    }
                    ++trace.result_groups_created;
                } else if (year_has) {
                    if (entry.has_year) {
                        entry.min_year = std::min(entry.min_year, year_value);
                        entry.max_year = std::max(entry.max_year, year_value);
                    } else {
                        entry.has_year = true;
                        entry.min_year = year_value;
                        entry.max_year = year_value;
                    }
                }
            } else {
                const uint64_t key =
                    (static_cast<uint64_t>(static_cast<uint32_t>(info_dense)) << 32) |
                    static_cast<uint32_t>(name_index);
                auto it = agg_map.find(key);
                if (it == agg_map.end()) {
                    if (year_has) {
                        agg_map.emplace(key, Agg{true, year_value, year_value});
                    } else {
                        agg_map.emplace(key, Agg{false, 0, 0});
                    }
                    ++trace.result_groups_created;
                } else if (year_has) {
                    if (it->second.has_year) {
                        it->second.min_year = std::min(it->second.min_year, year_value);
                        it->second.max_year = std::max(it->second.max_year, year_value);
                    } else {
                        it->second.has_year = true;
                        it->second.min_year = year_value;
                        it->second.max_year = year_value;
                    }
                }
            }
#else
            if (use_dense_agg) {
                const size_t agg_index = agg_base + static_cast<size_t>(info_dense);
                Agg& entry = agg_matrix[agg_index];
                if (agg_used[agg_index] == 0) {
                    agg_used[agg_index] = 1;
                    agg_used_indices.push_back(static_cast<uint32_t>(agg_index));
                    if (year_has) {
                        entry = Agg{true, year_value, year_value};
                    } else {
                        entry = Agg{false, 0, 0};
                    }
                } else if (year_has) {
                    if (entry.has_year) {
                        entry.min_year = std::min(entry.min_year, year_value);
                        entry.max_year = std::max(entry.max_year, year_value);
                    } else {
                        entry.has_year = true;
                        entry.min_year = year_value;
                        entry.max_year = year_value;
                    }
                }
            } else {
                const uint64_t key =
                    (static_cast<uint64_t>(static_cast<uint32_t>(info_dense)) << 32) |
                    static_cast<uint32_t>(name_index);
                auto it = agg_map.find(key);
                if (it == agg_map.end()) {
                    if (year_has) {
                        agg_map.emplace(key, Agg{true, year_value, year_value});
                    } else {
                        agg_map.emplace(key, Agg{false, 0, 0});
                    }
                } else if (year_has) {
                    if (it->second.has_year) {
                        it->second.min_year = std::min(it->second.min_year, year_value);
                        it->second.max_year = std::max(it->second.max_year, year_value);
                    } else {
                        it->second.has_year = true;
                        it->second.min_year = year_value;
                        it->second.max_year = year_value;
                    }
                }
            }
#endif
        }
    };

    for (size_t i = 0; i < filtered_movie_ids.size(); ++i) {
        const int32_t person_id = filtered_person_ids[i];
        const int32_t name_index =
            person_name_index_data[static_cast<size_t>(person_id)];
        if (name_index == -1) {
            continue;
        }
        const int32_t movie_id = filtered_movie_ids[i];
        const int32_t offset = movie_info_offset_data[static_cast<size_t>(movie_id)];
        const int32_t count = movie_info_count_data[static_cast<size_t>(movie_id)];
        const bool year_has = title_has_year[static_cast<size_t>(movie_id)] != 0;
        const int32_t year_value = title_year_value[static_cast<size_t>(movie_id)];
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.result_join_probe_rows_in;
#endif
        if (use_short_info) {
            process_row_short(name_index, year_has, year_value, offset, count);
        } else {
            process_row_long(name_index, year_has, year_value, offset, count);
        }
    }
#ifdef TRACE
    }
#endif

    if (use_dense_agg) {
        rows.reserve(agg_used_indices.size());
        for (const uint32_t agg_index : agg_used_indices) {
            const size_t name_index = agg_index / info_count;
            const size_t info_index = agg_index % info_count;
            const Agg& entry = agg_matrix[agg_index];
            rows.push_back(Q10aResultRow{
                name_id_by_index[name_index],
                info_dense_to_id[info_index],
                entry.has_year,
                entry.min_year,
                entry.max_year});
        }
    } else {
        rows.reserve(agg_map.size());
        for (const auto& entry : agg_map) {
            const int32_t info_index = static_cast<int32_t>(entry.first >> 32);
            const int32_t name_index = static_cast<int32_t>(entry.first & 0xffffffffu);
            rows.push_back(Q10aResultRow{
                name_id_by_index[static_cast<size_t>(name_index)],
                info_dense_to_id[static_cast<size_t>(info_index)],
                entry.second.has_year,
                entry.second.min_year,
                entry.second.max_year});
        }
    }
#ifdef TRACE
    trace.movie_info_agg_rows_emitted = movie_info_group_count;
    trace.result_agg_rows_emitted = rows.size();
    trace.result_join_build_rows_in =
        static_cast<uint64_t>(title_year_count + movie_info_group_count + name_group_count);
    trace.query_output_rows = rows.size();
#endif
#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q10a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q10a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q10a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q10a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q10a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q10a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q10a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q10a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q10a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q10a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q10a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q10a_result_join_build_rows_in", trace.result_join_build_rows_in);
    print_count("q10a_result_join_probe_rows_in", trace.result_join_probe_rows_in);
    print_count("q10a_result_join_rows_emitted", trace.result_join_rows_emitted);
    print_count("q10a_result_agg_rows_in", trace.result_agg_rows_in);
    print_count("q10a_result_groups_created", trace.result_groups_created);
    print_count("q10a_result_agg_rows_emitted", trace.result_agg_rows_emitted);
    print_count("q10a_query_output_rows", trace.query_output_rows);
#endif
    return rows;
}