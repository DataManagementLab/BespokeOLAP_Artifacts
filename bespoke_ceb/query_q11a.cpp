#include "query_q11a.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "trace.hpp"

namespace q11a_internal {

struct IntFilter {
    std::unordered_set<int32_t> values;
};

struct StringFilter {
    std::unordered_set<int32_t> values;
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
    std::string pattern;
    std::string needle;
    bool is_contains = false;
    bool is_exact = false;
};

bool contains_case_insensitive(const std::string& needle, const std::string& text) {
    if (needle.empty()) {
        return true;
    }
    if (needle.size() > text.size()) {
        return false;
    }
    const size_t limit = text.size() - needle.size();
    for (size_t i = 0; i <= limit; ++i) {
        size_t j = 0;
        for (; j < needle.size(); ++j) {
            const char lhs =
                static_cast<char>(std::tolower(static_cast<unsigned char>(text[i + j])));
            if (lhs != needle[j]) {
                break;
            }
        }
        if (j == needle.size()) {
            return true;
        }
    }
    return false;
}

bool equals_case_insensitive(const std::string& pattern, const std::string& text) {
    if (pattern.size() != text.size()) {
        return false;
    }
    for (size_t i = 0; i < pattern.size(); ++i) {
        const char lhs =
            static_cast<char>(std::tolower(static_cast<unsigned char>(text[i])));
        if (lhs != pattern[i]) {
            return false;
        }
    }
    return true;
}

LikePattern build_ilike_pattern(const std::string& raw) {
    LikePattern matcher;
    matcher.pattern = to_lower(strip_single_quotes(raw));
    if (matcher.pattern.find('%') == std::string::npos &&
        matcher.pattern.find('_') == std::string::npos) {
        matcher.is_exact = true;
        return matcher;
    }
    if (matcher.pattern.size() >= 2 && matcher.pattern.front() == '%' &&
        matcher.pattern.back() == '%') {
        bool has_inner_wildcard = false;
        for (size_t i = 1; i + 1 < matcher.pattern.size(); ++i) {
            if (matcher.pattern[i] == '%' || matcher.pattern[i] == '_') {
                has_inner_wildcard = true;
                break;
            }
        }
        if (!has_inner_wildcard) {
            matcher.is_contains = true;
            matcher.needle = matcher.pattern.substr(1, matcher.pattern.size() - 2);
            return matcher;
        }
    }
    return matcher;
}

bool ilike_match(const LikePattern& pattern, const std::string& value) {
    if (pattern.is_exact) {
        return equals_case_insensitive(pattern.pattern, value);
    }
    if (pattern.is_contains) {
        return contains_case_insensitive(pattern.needle, value);
    }
    return like_match(pattern.pattern, to_lower(value));
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
    const auto valid_ids = build_column_string_values(col);
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
            if (pool.try_get_id(merged_space, space_id) &&
                valid_ids.find(space_id) != valid_ids.end()) {
                found_id = space_id;
                found_idx = j;
            }
            int32_t nospace_id = -1;
            if (pool.try_get_id(merged_nospace, nospace_id) &&
                valid_ids.find(nospace_id) != valid_ids.end()) {
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

std::unordered_set<int32_t> build_valid_ids(const TableData& table,
                                            const std::string& id_col_name) {
    std::unordered_set<int32_t> ids;
    const auto& id_col = table.columns.at(id_col_name);
    ids.reserve(static_cast<size_t>(table.row_count));
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        ids.insert(id_col.i32[static_cast<size_t>(i)]);
    }
    return ids;
}

struct CastKey {
    int32_t gender_id{};
    int32_t role_id{};

    bool operator==(const CastKey& other) const {
        return gender_id == other.gender_id && role_id == other.role_id;
    }
};

struct CastKeyHash {
    size_t operator()(const CastKey& key) const {
        const size_t h1 = std::hash<int32_t>{}(key.gender_id);
        const size_t h2 = std::hash<int32_t>{}(key.role_id);
        return h1 ^ (h2 << 1);
    }
};

struct ResultKey {
    int32_t gender_id{};
    int32_t role_id{};
    int32_t company_name_id{};

    bool operator==(const ResultKey& other) const {
        return gender_id == other.gender_id && role_id == other.role_id &&
               company_name_id == other.company_name_id;
    }
};

struct ResultKeyHash {
    size_t operator()(const ResultKey& key) const {
        const size_t h1 = std::hash<int32_t>{}(key.gender_id);
        const size_t h2 = std::hash<int32_t>{}(key.role_id);
        const size_t h3 = std::hash<int32_t>{}(key.company_name_id);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

int32_t find_or_add_id(std::vector<int32_t>& ids, int32_t value) {
    for (size_t i = 0; i < ids.size(); ++i) {
        if (ids[i] == value) {
            return static_cast<int32_t>(i);
        }
    }
    ids.push_back(value);
    return static_cast<int32_t>(ids.size() - 1);
}

}  // namespace q11a_internal

#ifdef TRACE
struct Q11aTrace {
    TraceRecorder recorder;
    uint64_t role_type_rows_scanned = 0;
    uint64_t role_type_rows_emitted = 0;
    uint64_t kind_type_rows_scanned = 0;
    uint64_t kind_type_rows_emitted = 0;
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t movie_info_rows_scanned = 0;
    uint64_t movie_info_rows_emitted = 0;
    uint64_t movie_info_agg_rows_in = 0;
    uint64_t movie_info_groups_created = 0;
    uint64_t movie_info_agg_rows_emitted = 0;
    uint64_t company_name_rows_scanned = 0;
    uint64_t company_name_rows_emitted = 0;
    uint64_t company_type_rows_scanned = 0;
    uint64_t company_type_rows_emitted = 0;
    uint64_t movie_companies_rows_scanned = 0;
    uint64_t movie_companies_rows_emitted = 0;
    uint64_t movie_companies_agg_rows_in = 0;
    uint64_t movie_companies_groups_created = 0;
    uint64_t movie_companies_agg_rows_emitted = 0;
    uint64_t name_rows_scanned = 0;
    uint64_t name_rows_emitted = 0;
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t cast_info_agg_rows_in = 0;
    uint64_t cast_info_groups_created = 0;
    uint64_t cast_info_agg_rows_emitted = 0;
    uint64_t result_join_build_rows_in = 0;
    uint64_t result_join_probe_rows_in = 0;
    uint64_t result_join_rows_emitted = 0;
    uint64_t result_agg_rows_in = 0;
    uint64_t result_groups_created = 0;
    uint64_t result_agg_rows_emitted = 0;
    uint64_t sort_rows_in = 0;
    uint64_t sort_rows_out = 0;
    uint64_t query_output_rows = 0;
};
#endif

std::vector<Q11aResultRow> run_q11a(const Database& db, const Q11aArgs& args) {
    std::vector<Q11aResultRow> results;
#ifdef TRACE
    Q11aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto info_type_filter = q11a_internal::build_int_filter(args.ID);
    if (info_type_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& title = db.tables.at("title");
    const auto& movie_info = db.tables.at("movie_info");
    const auto& movie_companies = db.tables.at("movie_companies");
    const auto& company_name = db.tables.at("company_name");
    const auto& company_type = db.tables.at("company_type");
    const auto& cast_info = db.tables.at("cast_info");
    const auto& name = db.tables.at("name");

    const auto role_filter = q11a_internal::build_string_filter_for_column(
        db.string_pool, role_type.columns.at("role"), args.ROLE);
    if (role_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> role_id_to_idx;
    std::vector<int32_t> role_idx_to_str_id;
    role_id_to_idx.reserve(static_cast<size_t>(role_type.row_count));
    role_idx_to_str_id.reserve(static_cast<size_t>(role_filter.values.size()));
    const auto& rt_id_col = role_type.columns.at("id");
    const auto& rt_role_col = role_type.columns.at("role");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_role_type_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < role_type.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.role_type_rows_scanned;
#endif
        if (rt_id_col.is_null[idx] != 0 || rt_role_col.is_null[idx] != 0) {
            continue;
        }
        if (role_filter.values.find(rt_role_col.str_ids[idx]) == role_filter.values.end()) {
            continue;
        }
        const int32_t role_id = rt_id_col.i32[idx];
        if (role_id < 0) {
            continue;
        }
        if (static_cast<size_t>(role_id) >= role_id_to_idx.size()) {
            role_id_to_idx.resize(static_cast<size_t>(role_id) + 1, -1);
        }
        if (role_id_to_idx[static_cast<size_t>(role_id)] == -1) {
            const int32_t role_idx = static_cast<int32_t>(role_idx_to_str_id.size());
            role_id_to_idx[static_cast<size_t>(role_id)] = role_idx;
            role_idx_to_str_id.push_back(rt_role_col.str_ids[idx]);
        }
#ifdef TRACE
        ++trace.role_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (role_idx_to_str_id.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto kind_pattern = q11a_internal::build_ilike_pattern(args.KIND);
    std::vector<uint8_t> allowed_kind_mask;
    bool has_allowed_kind = false;
    const auto& kt_id_col = kind_type.columns.at("id");
    const auto& kt_kind_col = kind_type.columns.at("kind");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_kind_type_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < kind_type.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.kind_type_rows_scanned;
#endif
        if (kt_id_col.is_null[idx] != 0 || kt_kind_col.is_null[idx] != 0) {
            continue;
        }
        const auto& value = db.string_pool.get(kt_kind_col.str_ids[idx]);
        if (!q11a_internal::ilike_match(kind_pattern, value)) {
            continue;
        }
        const int32_t kind_id = kt_id_col.i32[idx];
        if (kind_id < 0) {
            continue;
        }
        if (static_cast<size_t>(kind_id) >= allowed_kind_mask.size()) {
            allowed_kind_mask.resize(static_cast<size_t>(kind_id) + 1, 0);
        }
        allowed_kind_mask[static_cast<size_t>(kind_id)] = 1;
        has_allowed_kind = true;
#ifdef TRACE
        ++trace.kind_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (!has_allowed_kind) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> allowed_movie_ids;
    allowed_movie_ids.reserve(static_cast<size_t>(title.row_count));
    int32_t max_movie_id = -1;
    const auto& title_id_col = title.columns.at("id");
    const auto& title_kind_col = title.columns.at("kind_id");
    const auto& title_year_col = title.columns.at("production_year");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_title_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < title.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.title_rows_scanned;
#endif
        if (title_id_col.is_null[idx] != 0 || title_kind_col.is_null[idx] != 0 ||
            title_year_col.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = title_id_col.i32[idx];
        const int32_t kind_id = title_kind_col.i32[idx];
        if (movie_id > max_movie_id) {
            max_movie_id = movie_id;
        }
        if (kind_id < 0 || static_cast<size_t>(kind_id) >= allowed_kind_mask.size() ||
            allowed_kind_mask[static_cast<size_t>(kind_id)] == 0) {
            continue;
        }
        const int32_t year = title_year_col.i32[idx];
        if (year > year1 || year < year2) {
            continue;
        }
        allowed_movie_ids.push_back(movie_id);
#ifdef TRACE
        ++trace.title_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_movie_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    if (max_movie_id < 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    std::vector<uint8_t> allowed_movie_mask(static_cast<size_t>(max_movie_id) + 1, 0);
    for (int32_t movie_id : allowed_movie_ids) {
        if (movie_id < 0) {
            continue;
        }
        allowed_movie_mask[static_cast<size_t>(movie_id)] = 1;
    }

    const auto info_pattern = q11a_internal::build_ilike_pattern(args.INFO);
    std::vector<int64_t> movie_info_counts(static_cast<size_t>(max_movie_id) + 1, 0);
    uint64_t movie_info_group_count = 0;
    const bool info_type_single = info_type_filter.values.size() == 1;
    int32_t info_type_value = 0;
    if (info_type_single) {
        info_type_value = *info_type_filter.values.begin();
    }
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_movie_info_scan");
#endif
    for (int64_t i = 0; i < movie_info.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if (mi_movie_id.is_null[idx] != 0 || mi_info_type_id.is_null[idx] != 0 ||
            mi_info.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mi_movie_id.i32[idx];
        if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_mask.size() ||
            allowed_movie_mask[static_cast<size_t>(movie_id)] == 0) {
            continue;
        }
        const int32_t info_type_id = mi_info_type_id.i32[idx];
        if (info_type_single) {
            if (info_type_id != info_type_value) {
                continue;
            }
        } else if (info_type_filter.values.find(info_type_id) ==
                   info_type_filter.values.end()) {
            continue;
        }
        const auto& value = db.string_pool.get(mi_info.str_ids[idx]);
        if (!q11a_internal::ilike_match(info_pattern, value)) {
            continue;
        }
        const size_t movie_index = static_cast<size_t>(movie_id);
        const bool is_new = movie_info_counts[movie_index] == 0;
#ifdef TRACE
        ++trace.movie_info_rows_emitted;
        ++trace.movie_info_agg_rows_in;
        if (is_new) {
            ++trace.movie_info_groups_created;
        }
#endif
        if (is_new) {
            ++movie_info_group_count;
        }
        ++movie_info_counts[movie_index];
    }
#ifdef TRACE
    }
#endif
    if (movie_info_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto name_pattern = q11a_internal::build_ilike_pattern(args.NAME);
    std::vector<int32_t> company_id_to_name_id;
    company_id_to_name_id.reserve(static_cast<size_t>(company_name.row_count));
    bool has_company_name = false;
    const auto& cn_id_col = company_name.columns.at("id");
    const auto& cn_name_col = company_name.columns.at("name");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_company_name_scan");
#endif
    for (int64_t i = 0; i < company_name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.company_name_rows_scanned;
#endif
        if (cn_id_col.is_null[idx] != 0 || cn_name_col.is_null[idx] != 0) {
            continue;
        }
        const auto& value = db.string_pool.get(cn_name_col.str_ids[idx]);
        if (!q11a_internal::ilike_match(name_pattern, value)) {
            continue;
        }
        const int32_t company_id = cn_id_col.i32[idx];
        if (company_id < 0) {
            continue;
        }
        if (static_cast<size_t>(company_id) >= company_id_to_name_id.size()) {
            company_id_to_name_id.resize(static_cast<size_t>(company_id) + 1, -1);
        }
        company_id_to_name_id[static_cast<size_t>(company_id)] = cn_name_col.str_ids[idx];
        has_company_name = true;
#ifdef TRACE
        ++trace.company_name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif
    if (!has_company_name) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<uint8_t> valid_company_type_mask;
    bool has_company_type = false;
    const auto& ct_id_col = company_type.columns.at("id");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_company_type_scan");
        if (!skip_query) {
#endif
    for (int64_t i = 0; i < company_type.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.company_type_rows_scanned;
#endif
        if (ct_id_col.is_null[idx] != 0) {
            continue;
        }
        const int32_t company_type_id = ct_id_col.i32[idx];
        if (company_type_id < 0) {
            continue;
        }
        if (static_cast<size_t>(company_type_id) >= valid_company_type_mask.size()) {
            valid_company_type_mask.resize(static_cast<size_t>(company_type_id) + 1, 0);
        }
        valid_company_type_mask[static_cast<size_t>(company_type_id)] = 1;
        has_company_type = true;
#ifdef TRACE
        ++trace.company_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (!has_company_type) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> movie_company_index(static_cast<size_t>(max_movie_id) + 1, -1);
    std::vector<std::vector<std::pair<int32_t, int64_t>>> movie_company_counts;
    movie_company_counts.reserve(static_cast<size_t>(movie_companies.row_count));
    const auto& mc_movie_id = movie_companies.columns.at("movie_id");
    const auto& mc_company_id = movie_companies.columns.at("company_id");
    const auto& mc_company_type_id = movie_companies.columns.at("company_type_id");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_movie_companies_scan");
#endif
    for (int64_t i = 0; i < movie_companies.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_companies_rows_scanned;
#endif
        if (mc_movie_id.is_null[idx] != 0 || mc_company_id.is_null[idx] != 0 ||
            mc_company_type_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mc_movie_id.i32[idx];
        if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_mask.size() ||
            allowed_movie_mask[static_cast<size_t>(movie_id)] == 0) {
            continue;
        }
        const int32_t company_type_id = mc_company_type_id.i32[idx];
        if (company_type_id < 0 ||
            static_cast<size_t>(company_type_id) >= valid_company_type_mask.size() ||
            valid_company_type_mask[static_cast<size_t>(company_type_id)] == 0) {
            continue;
        }
        const int32_t company_id = mc_company_id.i32[idx];
        if (company_id < 0 ||
            static_cast<size_t>(company_id) >= company_id_to_name_id.size()) {
            continue;
        }
        const int32_t company_name_id =
            company_id_to_name_id[static_cast<size_t>(company_id)];
        if (company_name_id == -1) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_companies_rows_emitted;
        ++trace.movie_companies_agg_rows_in;
        const int32_t company_index = movie_company_index[static_cast<size_t>(movie_id)];
        if (company_index == -1) {
            movie_company_index[static_cast<size_t>(movie_id)] =
                static_cast<int32_t>(movie_company_counts.size());
            movie_company_counts.emplace_back(
                std::vector<std::pair<int32_t, int64_t>>{{company_name_id, 1}});
            ++trace.movie_companies_groups_created;
        } else {
            auto& entries = movie_company_counts[static_cast<size_t>(company_index)];
            bool found = false;
            for (auto& entry : entries) {
                if (entry.first == company_name_id) {
                    ++entry.second;
                    found = true;
                    break;
                }
            }
            if (!found) {
                entries.emplace_back(company_name_id, 1);
            }
        }
#else
        const int32_t company_index = movie_company_index[static_cast<size_t>(movie_id)];
        if (company_index == -1) {
            movie_company_index[static_cast<size_t>(movie_id)] =
                static_cast<int32_t>(movie_company_counts.size());
            movie_company_counts.emplace_back(
                std::vector<std::pair<int32_t, int64_t>>{{company_name_id, 1}});
        } else {
            auto& entries = movie_company_counts[static_cast<size_t>(company_index)];
            bool found = false;
            for (auto& entry : entries) {
                if (entry.first == company_name_id) {
                    ++entry.second;
                    found = true;
                    break;
                }
            }
            if (!found) {
                entries.emplace_back(company_name_id, 1);
            }
        }
#endif
    }
#ifdef TRACE
    }
#endif
    if (movie_company_counts.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> gender_ids;
    std::vector<int8_t> person_gender_idx;
    person_gender_idx.reserve(static_cast<size_t>(name.row_count));
    bool has_person = false;
    const auto& name_id_col = name.columns.at("id");
    const auto& name_gender_col = name.columns.at("gender");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_name_scan");
#endif
    for (int64_t i = 0; i < name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (name_id_col.is_null[idx] != 0) {
            continue;
        }
        int32_t gender_id = -1;
        if (name_gender_col.is_null[idx] == 0) {
            gender_id = name_gender_col.str_ids[idx];
        }
        const int32_t person_id = name_id_col.i32[idx];
        if (person_id < 0) {
            continue;
        }
        if (static_cast<size_t>(person_id) >= person_gender_idx.size()) {
            person_gender_idx.resize(static_cast<size_t>(person_id) + 1, -1);
        }
        const int32_t gender_idx =
            q11a_internal::find_or_add_id(gender_ids, gender_id);
        person_gender_idx[static_cast<size_t>(person_id)] =
            static_cast<int8_t>(gender_idx);
        has_person = true;
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif
    if (!has_person) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const size_t role_count = role_idx_to_str_id.size();
    const size_t gender_count = gender_ids.size();
    std::vector<int32_t> movie_cast_offset(static_cast<size_t>(max_movie_id) + 1, -1);
    std::vector<uint32_t> cast_counts_flat;
    std::vector<int32_t> cast_movie_ids;
    std::vector<int32_t> cast_offsets;
    std::vector<size_t> gender_offsets;
    const size_t per_movie_counts = role_count * gender_count;
    gender_offsets.reserve(gender_count);
    for (size_t idx = 0; idx < gender_count; ++idx) {
        gender_offsets.push_back(idx * role_count);
    }
    if (per_movie_counts > 0) {
        const size_t reserve_movies =
            std::min(static_cast<size_t>(allowed_movie_ids.size()),
                     static_cast<size_t>(cast_info.row_count / 16));
        cast_counts_flat.reserve(reserve_movies * per_movie_counts);
        cast_movie_ids.reserve(reserve_movies);
        cast_offsets.reserve(reserve_movies);
    }
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const auto* ci_movie_vals = ci_movie_id.i32.data();
    const auto* ci_movie_nulls = ci_movie_id.is_null.data();
    const auto* ci_person_vals = ci_person_id.i32.data();
    const auto* ci_person_nulls = ci_person_id.is_null.data();
    const auto* ci_role_vals = ci_role_id.i32.data();
    const auto* ci_role_nulls = ci_role_id.is_null.data();
    const auto* allowed_movie_data = allowed_movie_mask.data();
    const auto* role_idx_data = role_id_to_idx.data();
    const auto* gender_idx_data = person_gender_idx.data();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_cast_info_scan");
#endif
    const bool person_has_nulls = ci_person_id.has_nulls;
    const bool role_has_nulls = ci_role_id.has_nulls;
    if (cast_info.sorted_by_movie_id && !ci_movie_id.has_nulls) {
        const size_t row_count = static_cast<size_t>(cast_info.row_count);
        size_t idx = 0;
        while (idx < row_count) {
            const int32_t movie_id = ci_movie_vals[idx];
            size_t group_end = idx + 1;
            while (group_end < row_count && ci_movie_vals[group_end] == movie_id) {
                ++group_end;
            }
            if (allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
#ifdef TRACE
                trace.cast_info_rows_scanned += (group_end - idx);
#endif
                idx = group_end;
                continue;
            }
            int32_t cast_offset = -1;
            if (!person_has_nulls && !role_has_nulls) {
                for (size_t row = idx; row < group_end; ++row) {
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    const int32_t role_id = ci_role_vals[row];
                    const int32_t role_idx =
                        role_idx_data[static_cast<size_t>(role_id)];
                    if (role_idx == -1) {
                        continue;
                    }
                    const int32_t person_id = ci_person_vals[row];
                    const int8_t gender_idx =
                        gender_idx_data[static_cast<size_t>(person_id)];
                    const size_t count_index =
                        gender_offsets[static_cast<size_t>(gender_idx)] +
                        static_cast<size_t>(role_idx);
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
                    if (cast_offset == -1) {
                        const int32_t new_offset =
                            static_cast<int32_t>(cast_counts_flat.size());
                        cast_offset = new_offset;
                        cast_movie_ids.push_back(movie_id);
                        cast_offsets.push_back(new_offset);
                        cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                                    per_movie_counts,
                                                0);
                        cast_counts_flat[static_cast<size_t>(new_offset) +
                                         count_index] = 1;
                        ++trace.cast_info_groups_created;
                    } else {
                        ++cast_counts_flat[static_cast<size_t>(cast_offset) +
                                           count_index];
                    }
#else
                    if (cast_offset == -1) {
                        const int32_t new_offset =
                            static_cast<int32_t>(cast_counts_flat.size());
                        cast_offset = new_offset;
                        cast_movie_ids.push_back(movie_id);
                        cast_offsets.push_back(new_offset);
                        cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                                    per_movie_counts,
                                                0);
                        cast_counts_flat[static_cast<size_t>(new_offset) +
                                         count_index] = 1;
                    } else {
                        ++cast_counts_flat[static_cast<size_t>(cast_offset) +
                                           count_index];
                    }
#endif
                }
            } else {
                for (size_t row = idx; row < group_end; ++row) {
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    if (person_has_nulls && ci_person_nulls[row] != 0) {
                        continue;
                    }
                    if (role_has_nulls && ci_role_nulls[row] != 0) {
                        continue;
                    }
                    const int32_t role_id = ci_role_vals[row];
                    const int32_t role_idx =
                        role_idx_data[static_cast<size_t>(role_id)];
                    if (role_idx == -1) {
                        continue;
                    }
                    const int32_t person_id = ci_person_vals[row];
                    const int8_t gender_idx =
                        gender_idx_data[static_cast<size_t>(person_id)];
                    const size_t count_index =
                        gender_offsets[static_cast<size_t>(gender_idx)] +
                        static_cast<size_t>(role_idx);
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
                    if (cast_offset == -1) {
                        const int32_t new_offset =
                            static_cast<int32_t>(cast_counts_flat.size());
                        cast_offset = new_offset;
                        cast_movie_ids.push_back(movie_id);
                        cast_offsets.push_back(new_offset);
                        cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                                    per_movie_counts,
                                                0);
                        cast_counts_flat[static_cast<size_t>(new_offset) +
                                         count_index] = 1;
                        ++trace.cast_info_groups_created;
                    } else {
                        ++cast_counts_flat[static_cast<size_t>(cast_offset) +
                                           count_index];
                    }
#else
                    if (cast_offset == -1) {
                        const int32_t new_offset =
                            static_cast<int32_t>(cast_counts_flat.size());
                        cast_offset = new_offset;
                        cast_movie_ids.push_back(movie_id);
                        cast_offsets.push_back(new_offset);
                        cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                                    per_movie_counts,
                                                0);
                        cast_counts_flat[static_cast<size_t>(new_offset) +
                                         count_index] = 1;
                    } else {
                        ++cast_counts_flat[static_cast<size_t>(cast_offset) +
                                           count_index];
                    }
#endif
                }
            }
            idx = group_end;
        }
    } else if (!ci_movie_id.has_nulls && !person_has_nulls && !role_has_nulls) {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            const int32_t movie_id = ci_movie_vals[idx];
            if (allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t role_id = ci_role_vals[idx];
            const int32_t role_idx = role_idx_data[static_cast<size_t>(role_id)];
            if (role_idx == -1) {
                continue;
            }
            const int32_t person_id = ci_person_vals[idx];
            const int8_t gender_idx = gender_idx_data[static_cast<size_t>(person_id)];
            if (gender_idx < 0) {
                continue;
            }
            const size_t count_index = gender_offsets[static_cast<size_t>(gender_idx)] +
                                       static_cast<size_t>(role_idx);
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            const int32_t cast_offset =
                movie_cast_offset[static_cast<size_t>(movie_id)];
            if (cast_offset == -1) {
                const int32_t new_offset =
                    static_cast<int32_t>(cast_counts_flat.size());
                movie_cast_offset[static_cast<size_t>(movie_id)] = new_offset;
                cast_movie_ids.push_back(movie_id);
                cast_offsets.push_back(new_offset);
                cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                            per_movie_counts,
                                        0);
                cast_counts_flat[static_cast<size_t>(new_offset) + count_index] = 1;
                ++trace.cast_info_groups_created;
            } else {
                ++cast_counts_flat[static_cast<size_t>(cast_offset) + count_index];
            }
#else
            const int32_t cast_offset =
                movie_cast_offset[static_cast<size_t>(movie_id)];
            if (cast_offset == -1) {
                const int32_t new_offset =
                    static_cast<int32_t>(cast_counts_flat.size());
                movie_cast_offset[static_cast<size_t>(movie_id)] = new_offset;
                cast_movie_ids.push_back(movie_id);
                cast_offsets.push_back(new_offset);
                cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                            per_movie_counts,
                                        0);
                cast_counts_flat[static_cast<size_t>(new_offset) + count_index] = 1;
            } else {
                ++cast_counts_flat[static_cast<size_t>(cast_offset) + count_index];
            }
#endif
        }
    } else {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            if (ci_movie_nulls[idx] != 0 || ci_person_nulls[idx] != 0 ||
                ci_role_nulls[idx] != 0) {
                continue;
            }
            const int32_t movie_id = ci_movie_vals[idx];
            if (allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t role_id = ci_role_vals[idx];
            const int32_t role_idx = role_idx_data[static_cast<size_t>(role_id)];
            if (role_idx == -1) {
                continue;
            }
            const int32_t person_id = ci_person_vals[idx];
            const int8_t gender_idx = gender_idx_data[static_cast<size_t>(person_id)];
            if (gender_idx < 0) {
                continue;
            }
            const size_t count_index =
                gender_offsets[static_cast<size_t>(gender_idx)] +
                static_cast<size_t>(role_idx);
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            const int32_t cast_offset =
                movie_cast_offset[static_cast<size_t>(movie_id)];
            if (cast_offset == -1) {
                const int32_t new_offset =
                    static_cast<int32_t>(cast_counts_flat.size());
                movie_cast_offset[static_cast<size_t>(movie_id)] = new_offset;
                cast_movie_ids.push_back(movie_id);
                cast_offsets.push_back(new_offset);
                cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                            per_movie_counts,
                                        0);
                cast_counts_flat[static_cast<size_t>(new_offset) + count_index] = 1;
                ++trace.cast_info_groups_created;
            } else {
                ++cast_counts_flat[static_cast<size_t>(cast_offset) + count_index];
            }
#else
            const int32_t cast_offset =
                movie_cast_offset[static_cast<size_t>(movie_id)];
            if (cast_offset == -1) {
                const int32_t new_offset =
                    static_cast<int32_t>(cast_counts_flat.size());
                movie_cast_offset[static_cast<size_t>(movie_id)] = new_offset;
                cast_movie_ids.push_back(movie_id);
                cast_offsets.push_back(new_offset);
                cast_counts_flat.resize(static_cast<size_t>(new_offset) +
                                            per_movie_counts,
                                        0);
                cast_counts_flat[static_cast<size_t>(new_offset) + count_index] = 1;
            } else {
                ++cast_counts_flat[static_cast<size_t>(cast_offset) + count_index];
            }
#endif
        }
    }
#ifdef TRACE
    }
#endif
    if (cast_movie_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::unordered_map<q11a_internal::ResultKey, int64_t, q11a_internal::ResultKeyHash>
        result_counts;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11a_result_join");
#endif
    for (size_t cast_idx = 0; cast_idx < cast_movie_ids.size(); ++cast_idx) {
        const int32_t movie_id = cast_movie_ids[cast_idx];
        const int32_t cast_offset = cast_offsets[cast_idx];
        const uint32_t* cast_entry = cast_counts_flat.data() +
                                     static_cast<size_t>(cast_offset);
#ifdef TRACE
        ++trace.result_join_probe_rows_in;
#endif
        const int64_t info_count = movie_info_counts[static_cast<size_t>(movie_id)];
        if (info_count == 0) {
            continue;
        }
        const int32_t company_index = movie_company_index[static_cast<size_t>(movie_id)];
        if (company_index == -1) {
            continue;
        }
        const auto& company_entries =
            movie_company_counts[static_cast<size_t>(company_index)];
        for (const auto& company_entry : company_entries) {
            const int32_t company_name_id = company_entry.first;
            const int64_t company_count = company_entry.second;
            for (size_t idx = 0; idx < per_movie_counts; ++idx) {
                const uint32_t cast_count = cast_entry[idx];
                if (cast_count == 0) {
                    continue;
                }
                const size_t gender_idx = idx / role_count;
                const size_t role_idx = idx - gender_idx * role_count;
                const int32_t gender_id = gender_ids[gender_idx];
                const int32_t role_id = role_idx_to_str_id[role_idx];
                q11a_internal::ResultKey key{gender_id, role_id, company_name_id};
#ifdef TRACE
                ++trace.result_join_rows_emitted;
                ++trace.result_agg_rows_in;
                auto it_result = result_counts.find(key);
                if (it_result == result_counts.end()) {
                    result_counts.emplace(key, info_count * company_count *
                                                  static_cast<int64_t>(cast_count));
                    ++trace.result_groups_created;
                } else {
                    it_result->second += info_count * company_count *
                                         static_cast<int64_t>(cast_count);
                }
#else
                result_counts[key] += info_count * company_count *
                                      static_cast<int64_t>(cast_count);
#endif
            }
        }
    }
#ifdef TRACE
    }
#endif

    results.reserve(result_counts.size());
    for (const auto& entry : result_counts) {
        results.push_back(Q11aResultRow{entry.first.gender_id, entry.first.role_id,
                                        entry.first.company_name_id, entry.second});
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11a_sort");
        trace.sort_rows_in = results.size();
#endif
    std::sort(results.begin(), results.end(),
              [](const Q11aResultRow& a, const Q11aResultRow& b) {
                  if (a.count != b.count) {
                      return a.count > b.count;
                  }
                  if (a.gender_id != b.gender_id) {
                      return a.gender_id < b.gender_id;
                  }
                  if (a.role_id != b.role_id) {
                      return a.role_id < b.role_id;
                  }
                  return a.company_name_id < b.company_name_id;
              });
#ifdef TRACE
        trace.sort_rows_out = results.size();
    }
    trace.movie_info_agg_rows_emitted = movie_info_group_count;
    trace.movie_companies_agg_rows_emitted = movie_company_counts.size();
    trace.cast_info_agg_rows_emitted = cast_movie_ids.size();
    trace.result_agg_rows_emitted = result_counts.size();
    trace.result_join_build_rows_in =
        static_cast<uint64_t>(movie_info_group_count + movie_company_counts.size());
    trace.query_output_rows = results.size();
#endif
#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q11a_role_type_scan_rows_scanned", trace.role_type_rows_scanned);
    print_count("q11a_role_type_scan_rows_emitted", trace.role_type_rows_emitted);
    print_count("q11a_kind_type_scan_rows_scanned", trace.kind_type_rows_scanned);
    print_count("q11a_kind_type_scan_rows_emitted", trace.kind_type_rows_emitted);
    print_count("q11a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q11a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q11a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q11a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q11a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q11a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q11a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q11a_company_name_scan_rows_scanned", trace.company_name_rows_scanned);
    print_count("q11a_company_name_scan_rows_emitted", trace.company_name_rows_emitted);
    print_count("q11a_company_type_scan_rows_scanned", trace.company_type_rows_scanned);
    print_count("q11a_company_type_scan_rows_emitted", trace.company_type_rows_emitted);
    print_count("q11a_movie_companies_scan_rows_scanned", trace.movie_companies_rows_scanned);
    print_count("q11a_movie_companies_scan_rows_emitted", trace.movie_companies_rows_emitted);
    print_count("q11a_movie_companies_agg_rows_in", trace.movie_companies_agg_rows_in);
    print_count("q11a_movie_companies_groups_created", trace.movie_companies_groups_created);
    print_count("q11a_movie_companies_agg_rows_emitted", trace.movie_companies_agg_rows_emitted);
    print_count("q11a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q11a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q11a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q11a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q11a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q11a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q11a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q11a_result_join_build_rows_in", trace.result_join_build_rows_in);
    print_count("q11a_result_join_probe_rows_in", trace.result_join_probe_rows_in);
    print_count("q11a_result_join_rows_emitted", trace.result_join_rows_emitted);
    print_count("q11a_result_agg_rows_in", trace.result_agg_rows_in);
    print_count("q11a_result_groups_created", trace.result_groups_created);
    print_count("q11a_result_agg_rows_emitted", trace.result_agg_rows_emitted);
    print_count("q11a_sort_rows_in", trace.sort_rows_in);
    print_count("q11a_sort_rows_out", trace.sort_rows_out);
    print_count("q11a_query_output_rows", trace.query_output_rows);
#endif
    return results;
}