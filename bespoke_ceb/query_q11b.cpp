#include "query_q11b.hpp"

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <limits>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

namespace q11b_internal {

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

bool contains_lower(const std::string& needle, const std::string& text_lower) {
    if (needle.empty()) {
        return true;
    }
    return text_lower.find(needle) != std::string::npos;
}

bool equals_lower(const std::string& pattern_lower, const std::string& text_lower) {
    return pattern_lower == text_lower;
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

bool ilike_match_lower(const LikePattern& pattern, const std::string& value_lower) {
    if (pattern.is_exact) {
        return equals_lower(pattern.pattern, value_lower);
    }
    if (pattern.is_contains) {
        return contains_lower(pattern.needle, value_lower);
    }
    return like_match(pattern.pattern, value_lower);
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

std::unordered_set<int32_t> build_allowed_ids_from_dim(const TableData& table,
                                                       const std::string& id_col_name,
                                                       const std::string& str_col_name,
                                                       const StringFilter& filter) {
    std::unordered_set<int32_t> ids;
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    ids.reserve(static_cast<size_t>(table.row_count));
    for (int64_t i = 0; i < table.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
        if (id_col.is_null[idx] != 0) {
            continue;
        }
        if (str_col.is_null[idx] != 0) {
            continue;
        }
        if (filter.values.find(str_col.str_ids[idx]) != filter.values.end()) {
            ids.insert(id_col.i32[idx]);
        }
    }
    return ids;
}

std::unordered_set<int32_t> build_valid_ids(const TableData& table,
                                            const std::string& id_col_name) {
    std::unordered_set<int32_t> ids;
    const auto& id_col = table.columns.at(id_col_name);
    ids.reserve(static_cast<size_t>(table.row_count));
    for (int64_t i = 0; i < table.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
        if (id_col.is_null[idx] != 0) {
            continue;
        }
        ids.insert(id_col.i32[idx]);
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

struct ResultAggMap {
    std::vector<ResultKey> keys;
    std::vector<int64_t> values;
    std::vector<uint8_t> used;
    size_t mask = 0;
    size_t size = 0;

    void init(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        keys.resize(cap);
        values.resize(cap);
        used.assign(cap, 0);
        mask = cap - 1;
        size = 0;
    }

    bool add(const ResultKey& key, int64_t delta) {
        size_t slot = ResultKeyHash{}(key) & mask;
        while (used[slot] != 0) {
            if (keys[slot] == key) {
                values[slot] += delta;
                return false;
            }
            slot = (slot + 1) & mask;
        }
        used[slot] = 1;
        keys[slot] = key;
        values[slot] = delta;
        ++size;
        return true;
    }
};

struct PersonInfoMap {
    std::vector<int32_t> keys;
    std::vector<int32_t> counts;
    std::vector<int32_t> genders;
    std::vector<uint8_t> used;
    size_t mask = 0;
    size_t size = 0;

    void init(size_t expected, int32_t missing_gender) {
        size_t cap = 1;
        while (cap < expected * 2) {
            cap <<= 1;
        }
        keys.resize(cap);
        counts.resize(cap);
        genders.assign(cap, missing_gender);
        used.assign(cap, 0);
        mask = cap - 1;
        size = 0;
    }

    void insert(int32_t key, int32_t count, int32_t gender) {
        size_t slot = std::hash<int32_t>{}(key) & mask;
        while (used[slot] != 0) {
            if (keys[slot] == key) {
                counts[slot] = count;
                genders[slot] = gender;
                return;
            }
            slot = (slot + 1) & mask;
        }
        used[slot] = 1;
        keys[slot] = key;
        counts[slot] = count;
        genders[slot] = gender;
        ++size;
    }

    bool find(int32_t key, int32_t& count, int32_t& gender) const {
        if (mask == 0) {
            return false;
        }
        size_t slot = std::hash<int32_t>{}(key) & mask;
        while (used[slot] != 0) {
            if (keys[slot] == key) {
                count = counts[slot];
                gender = genders[slot];
                return true;
            }
            slot = (slot + 1) & mask;
        }
        return false;
    }
};

}  // namespace q11b_internal

#ifdef TRACE
struct Q11bTrace {
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
    uint64_t person_info_rows_scanned = 0;
    uint64_t person_info_rows_emitted = 0;
    uint64_t person_info_agg_rows_in = 0;
    uint64_t person_info_groups_created = 0;
    uint64_t person_info_agg_rows_emitted = 0;
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

std::vector<Q11bResultRow> run_q11b(const Database& db, const Q11bArgs& args) {
    std::vector<Q11bResultRow> results;
#ifdef TRACE
    Q11bTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_total");
#endif
    int32_t year_upper = 0;
    int32_t year_lower = 0;
    try {
        year_upper = static_cast<int32_t>(std::stoi(args.YEAR1));
        year_lower = static_cast<int32_t>(std::stoi(args.YEAR2));
    } catch (const std::exception&) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto info_type_filter1 = q11b_internal::build_int_filter(args.ID1);
    const auto info_type_filter2 = q11b_internal::build_int_filter(args.ID2);
    if (info_type_filter1.values.empty() || info_type_filter2.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    int32_t info_type1_max = -1;
    for (const auto value : info_type_filter1.values) {
        info_type1_max = std::max(info_type1_max, value);
    }
    std::vector<uint8_t> info_type1_flags(
        info_type1_max < 0 ? 0 : static_cast<size_t>(info_type1_max) + 1, 0);
    for (const auto value : info_type_filter1.values) {
        if (value >= 0) {
            info_type1_flags[static_cast<size_t>(value)] = 1;
        }
    }
    std::vector<int32_t> info_type1_values;
    info_type1_values.reserve(info_type_filter1.values.size());
    for (const auto value : info_type_filter1.values) {
        if (value >= 0) {
            info_type1_values.push_back(value);
        }
    }
    std::sort(info_type1_values.begin(), info_type1_values.end());
    int32_t info_type2_max = -1;
    for (const auto value : info_type_filter2.values) {
        info_type2_max = std::max(info_type2_max, value);
    }
    std::vector<uint8_t> info_type2_flags(
        info_type2_max < 0 ? 0 : static_cast<size_t>(info_type2_max) + 1, 0);
    for (const auto value : info_type_filter2.values) {
        if (value >= 0) {
            info_type2_flags[static_cast<size_t>(value)] = 1;
        }
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
    const auto& person_info = db.tables.at("person_info");

    const auto kind_filter = q11b_internal::build_string_filter_for_column(
        db.string_pool, kind_type.columns.at("kind"), args.KIND);
    if (kind_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    const auto role_filter = q11b_internal::build_string_filter_for_column(
        db.string_pool, role_type.columns.at("role"), args.ROLE);
    if (role_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> allowed_kind_ids;
    allowed_kind_ids.reserve(static_cast<size_t>(kind_type.row_count));
    int32_t max_kind_id = -1;
    const auto& kt_id_col = kind_type.columns.at("id");
    const auto& kt_kind_col = kind_type.columns.at("kind");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_kind_type_scan");
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
        if (kind_filter.values.find(kt_kind_col.str_ids[idx]) ==
            kind_filter.values.end()) {
            continue;
        }
        const int32_t kind_id = kt_id_col.i32[idx];
        allowed_kind_ids.push_back(kind_id);
        if (kind_id > max_kind_id) {
            max_kind_id = kind_id;
        }
#ifdef TRACE
        ++trace.kind_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_kind_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    std::vector<uint8_t> allowed_kind_flags(
        max_kind_id < 0 ? 0 : static_cast<size_t>(max_kind_id) + 1, 0);
    for (const int32_t kind_id : allowed_kind_ids) {
        if (kind_id >= 0) {
            allowed_kind_flags[static_cast<size_t>(kind_id)] = 1;
        }
    }

    std::vector<int32_t> role_id_to_str(static_cast<size_t>(role_type.row_count) + 1, -1);
    std::vector<int32_t> allowed_role_ids;
    allowed_role_ids.reserve(static_cast<size_t>(role_type.row_count));
    size_t role_id_count = 0;
    const auto& rt_id_col = role_type.columns.at("id");
    const auto& rt_role_col = role_type.columns.at("role");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_role_type_scan");
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
        if (static_cast<size_t>(role_id) >= role_id_to_str.size()) {
            role_id_to_str.resize(static_cast<size_t>(role_id) + 1, -1);
        }
        if (role_id_to_str[role_id] == -1) {
            ++role_id_count;
            allowed_role_ids.push_back(role_id);
        }
        role_id_to_str[role_id] = rt_role_col.str_ids[idx];
#ifdef TRACE
        ++trace.role_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (role_id_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    std::sort(allowed_role_ids.begin(), allowed_role_ids.end());

    std::vector<uint8_t> allowed_movie_ids(static_cast<size_t>(title.row_count) + 1, 0);
    size_t allowed_movie_count = 0;
    const auto& title_id_col = title.columns.at("id");
    const auto& title_kind_col = title.columns.at("kind_id");
    const auto& title_year_col = title.columns.at("production_year");
    const bool title_has_nulls =
        title_id_col.has_nulls || title_kind_col.has_nulls || title_year_col.has_nulls;
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_title_scan");
        if (!skip_query) {
#endif
    if (title_has_nulls) {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if (title_id_col.is_null[idx] != 0 || title_kind_col.is_null[idx] != 0 ||
                title_year_col.is_null[idx] != 0) {
                continue;
            }
            const int32_t kind_id = title_kind_col.i32[idx];
            if (kind_id < 0 ||
                static_cast<size_t>(kind_id) >= allowed_kind_flags.size() ||
                allowed_kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = title_year_col.i32[idx];
            if (year > year_upper || year < year_lower) {
                continue;
            }
            const int32_t movie_id = title_id_col.i32[idx];
            if (movie_id < 0) {
                continue;
            }
            if (static_cast<size_t>(movie_id) >= allowed_movie_ids.size()) {
                allowed_movie_ids.resize(static_cast<size_t>(movie_id) + 1, 0);
            }
            if (allowed_movie_ids[movie_id] == 0) {
                allowed_movie_ids[movie_id] = 1;
                ++allowed_movie_count;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
#endif
        }
    } else {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t kind_id = title_kind_col.i32[idx];
            if (kind_id < 0 ||
                static_cast<size_t>(kind_id) >= allowed_kind_flags.size() ||
                allowed_kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = title_year_col.i32[idx];
            if (year > year_upper || year < year_lower) {
                continue;
            }
            const int32_t movie_id = title_id_col.i32[idx];
            if (movie_id < 0) {
                continue;
            }
            if (static_cast<size_t>(movie_id) >= allowed_movie_ids.size()) {
                allowed_movie_ids.resize(static_cast<size_t>(movie_id) + 1, 0);
            }
            if (allowed_movie_ids[movie_id] == 0) {
                allowed_movie_ids[movie_id] = 1;
                ++allowed_movie_count;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
#endif
        }
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_movie_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> company_name_ids(static_cast<size_t>(company_name.row_count) + 1,
                                          -1);
    const auto& cn_id_col = company_name.columns.at("id");
    const auto& cn_name_col = company_name.columns.at("name");
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_company_name_scan");
#endif
    for (int64_t i = 0; i < company_name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.company_name_rows_scanned;
#endif
        if (cn_id_col.is_null[idx] != 0 || cn_name_col.is_null[idx] != 0) {
            continue;
        }
        const int32_t company_id = cn_id_col.i32[idx];
        if (company_id < 0) {
            continue;
        }
        if (static_cast<size_t>(company_id) >= company_name_ids.size()) {
            company_name_ids.resize(static_cast<size_t>(company_id) + 1, -1);
        }
        company_name_ids[company_id] = cn_name_col.str_ids[idx];
#ifdef TRACE
        ++trace.company_name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif
    if (company_name_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<uint8_t> valid_company_type_ids(
        static_cast<size_t>(company_type.row_count) + 1, 0);
    const auto& ct_id_col = company_type.columns.at("id");
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_company_type_scan");
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
        if (static_cast<size_t>(company_type_id) >= valid_company_type_ids.size()) {
            valid_company_type_ids.resize(static_cast<size_t>(company_type_id) + 1, 0);
        }
        valid_company_type_ids[company_type_id] = 1;
#ifdef TRACE
        ++trace.company_type_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (valid_company_type_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::vector<int32_t> movie_company_offsets(allowed_movie_ids.size(), -1);
    std::vector<uint32_t> movie_company_sizes(allowed_movie_ids.size(), 0);
    std::vector<int32_t> movie_company_name_ids;
    std::vector<int32_t> movie_company_counts;
    movie_company_name_ids.reserve(static_cast<size_t>(movie_companies.row_count));
    movie_company_counts.reserve(static_cast<size_t>(movie_companies.row_count));
    std::vector<int32_t> movie_company_movie_list;
    movie_company_movie_list.reserve(static_cast<size_t>(allowed_movie_count));
    size_t movie_company_movie_count = 0;
    std::vector<int32_t> local_company_names;
    std::vector<int32_t> local_company_counts;
    local_company_names.reserve(8);
    local_company_counts.reserve(8);
    int32_t current_movie_id = -1;
    const auto& mc_movie_id = movie_companies.columns.at("movie_id");
    const auto& mc_company_id = movie_companies.columns.at("company_id");
    const auto& mc_company_type_id = movie_companies.columns.at("company_type_id");
    const bool mc_has_nulls = mc_movie_id.has_nulls || mc_company_id.has_nulls ||
                              mc_company_type_id.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_movie_companies_scan");
#endif
    auto flush_movie_companies = [&]() {
        if (current_movie_id < 0) {
            return;
        }
        if (local_company_names.empty()) {
            return;
        }
        const size_t offset = movie_company_name_ids.size();
        movie_company_offsets[current_movie_id] = static_cast<int32_t>(offset);
        movie_company_sizes[current_movie_id] =
            static_cast<uint32_t>(local_company_names.size());
        movie_company_name_ids.insert(movie_company_name_ids.end(),
                                      local_company_names.begin(),
                                      local_company_names.end());
        movie_company_counts.insert(movie_company_counts.end(),
                                    local_company_counts.begin(),
                                    local_company_counts.end());
        movie_company_movie_list.push_back(current_movie_id);
        ++movie_company_movie_count;
        local_company_names.clear();
        local_company_counts.clear();
    };
    if (mc_has_nulls) {
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
            if (movie_id < 0 ||
                static_cast<size_t>(movie_id) >= allowed_movie_ids.size() ||
                allowed_movie_ids[movie_id] == 0) {
                continue;
            }
            const int32_t company_type_id = mc_company_type_id.i32[idx];
            if (company_type_id < 0 ||
                static_cast<size_t>(company_type_id) >= valid_company_type_ids.size() ||
                valid_company_type_ids[company_type_id] == 0) {
                continue;
            }
            const int32_t company_id = mc_company_id.i32[idx];
            if (company_id < 0 ||
                static_cast<size_t>(company_id) >= company_name_ids.size()) {
                continue;
            }
            const int32_t company_name_id = company_name_ids[company_id];
            if (company_name_id < 0) {
                continue;
            }
            if (movie_id != current_movie_id) {
                flush_movie_companies();
                current_movie_id = movie_id;
            }
#ifdef TRACE
            ++trace.movie_companies_rows_emitted;
            ++trace.movie_companies_agg_rows_in;
            bool found = false;
            for (size_t local_idx = 0; local_idx < local_company_names.size();
                 ++local_idx) {
                if (local_company_names[local_idx] == company_name_id) {
                    ++local_company_counts[local_idx];
                    found = true;
                    break;
                }
            }
            if (!found) {
                local_company_names.push_back(company_name_id);
                local_company_counts.push_back(1);
                ++trace.movie_companies_groups_created;
            }
#else
            bool found = false;
            for (size_t local_idx = 0; local_idx < local_company_names.size();
                 ++local_idx) {
                if (local_company_names[local_idx] == company_name_id) {
                    ++local_company_counts[local_idx];
                    found = true;
                    break;
                }
            }
            if (!found) {
                local_company_names.push_back(company_name_id);
                local_company_counts.push_back(1);
            }
#endif
        }
    } else {
        for (int64_t i = 0; i < movie_companies.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_companies_rows_scanned;
#endif
            const int32_t movie_id = mc_movie_id.i32[idx];
            if (movie_id < 0 ||
                static_cast<size_t>(movie_id) >= allowed_movie_ids.size() ||
                allowed_movie_ids[movie_id] == 0) {
                continue;
            }
            const int32_t company_type_id = mc_company_type_id.i32[idx];
            if (company_type_id < 0 ||
                static_cast<size_t>(company_type_id) >= valid_company_type_ids.size() ||
                valid_company_type_ids[company_type_id] == 0) {
                continue;
            }
            const int32_t company_id = mc_company_id.i32[idx];
            if (company_id < 0 ||
                static_cast<size_t>(company_id) >= company_name_ids.size()) {
                continue;
            }
            const int32_t company_name_id = company_name_ids[company_id];
            if (company_name_id < 0) {
                continue;
            }
            if (movie_id != current_movie_id) {
                flush_movie_companies();
                current_movie_id = movie_id;
            }
#ifdef TRACE
            ++trace.movie_companies_rows_emitted;
            ++trace.movie_companies_agg_rows_in;
            bool found = false;
            for (size_t local_idx = 0; local_idx < local_company_names.size();
                 ++local_idx) {
                if (local_company_names[local_idx] == company_name_id) {
                    ++local_company_counts[local_idx];
                    found = true;
                    break;
                }
            }
            if (!found) {
                local_company_names.push_back(company_name_id);
                local_company_counts.push_back(1);
                ++trace.movie_companies_groups_created;
            }
#else
            bool found = false;
            for (size_t local_idx = 0; local_idx < local_company_names.size();
                 ++local_idx) {
                if (local_company_names[local_idx] == company_name_id) {
                    ++local_company_counts[local_idx];
                    found = true;
                    break;
                }
            }
            if (!found) {
                local_company_names.push_back(company_name_id);
                local_company_counts.push_back(1);
            }
#endif
        }
    }
    flush_movie_companies();
#ifdef TRACE
    }
#endif
    if (movie_company_movie_list.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto info_pattern = q11b_internal::build_ilike_pattern(args.INFO1);
    const size_t string_pool_size = db.string_pool.lower_values.size();
    std::vector<int8_t> info_match_cache(string_pool_size, -1);
    std::vector<int32_t> movie_info_counts(allowed_movie_ids.size(), 0);
    std::vector<int32_t> movie_info_movie_list;
    movie_info_movie_list.reserve(movie_company_movie_list.size());
    size_t movie_info_group_count = 0;
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const bool mi_has_nulls =
        mi_movie_id.has_nulls || mi_info_type_id.has_nulls || mi_info.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_movie_info_scan");
#endif
    const auto process_movie_info_row = [&](size_t idx) {
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if (mi_has_nulls) {
            if (mi_movie_id.is_null[idx] != 0 || mi_info_type_id.is_null[idx] != 0 ||
                mi_info.is_null[idx] != 0) {
                return;
            }
        }
        const int32_t movie_id = mi_movie_id.i32[idx];
        if (movie_id < 0 || static_cast<size_t>(movie_id) >= movie_info_counts.size()) {
            return;
        }
        if (movie_company_sizes[movie_id] == 0) {
            return;
        }
        const int32_t info_type_id = mi_info_type_id.i32[idx];
        if (info_type_id < 0 ||
            static_cast<size_t>(info_type_id) >= info_type1_flags.size() ||
            info_type1_flags[static_cast<size_t>(info_type_id)] == 0) {
            return;
        }
        const int32_t info_str_id = mi_info.str_ids[idx];
        if (info_str_id < 0 || static_cast<size_t>(info_str_id) >= info_match_cache.size()) {
            return;
        }
        int8_t matches = info_match_cache[info_str_id];
        if (matches == -1) {
            const auto& value_lower = db.string_pool.get_lower(info_str_id);
            matches =
                q11b_internal::ilike_match_lower(info_pattern, value_lower) ? 1 : 0;
            info_match_cache[info_str_id] = matches;
        }
        if (matches == 0) {
            return;
        }
#ifdef TRACE
        ++trace.movie_info_rows_emitted;
        ++trace.movie_info_agg_rows_in;
        if (movie_info_counts[movie_id] == 0) {
            ++trace.movie_info_groups_created;
            ++movie_info_group_count;
            movie_info_movie_list.push_back(movie_id);
        }
        ++movie_info_counts[movie_id];
#else
        if (movie_info_counts[movie_id] == 0) {
            ++movie_info_group_count;
            movie_info_movie_list.push_back(movie_id);
        }
        ++movie_info_counts[movie_id];
#endif
    };
    if (movie_info.sorted_by_movie_id) {
        size_t pos = 0;
        const auto& mi_ids = mi_movie_id.i32;
        for (const int32_t movie_id : movie_company_movie_list) {
            if (pos >= mi_ids.size()) {
                break;
            }
            auto it = std::lower_bound(mi_ids.begin() + pos, mi_ids.end(), movie_id);
            if (it == mi_ids.end()) {
                break;
            }
            size_t start = static_cast<size_t>(it - mi_ids.begin());
            if (mi_ids[start] != movie_id) {
                pos = start;
                continue;
            }
            const size_t movie_end = static_cast<size_t>(
                std::upper_bound(mi_ids.begin() + start, mi_ids.end(), movie_id) -
                mi_ids.begin());
            if (movie_info.sorted_by_info_type_id && !info_type1_values.empty()) {
                const auto& info_ids = mi_info_type_id.i32;
                size_t type_pos = start;
                for (const int32_t info_type_id : info_type1_values) {
                    if (type_pos >= movie_end) {
                        break;
                    }
                    auto type_it = std::lower_bound(info_ids.begin() + type_pos,
                                                    info_ids.begin() + movie_end,
                                                    info_type_id);
                    if (type_it == info_ids.begin() + movie_end) {
                        break;
                    }
                    size_t type_start = static_cast<size_t>(type_it - info_ids.begin());
                    if (info_ids[type_start] != info_type_id) {
                        type_pos = type_start;
                        continue;
                    }
                    size_t end = type_start;
                    while (end < movie_end && info_ids[end] == info_type_id) {
#ifdef TRACE
                        ++trace.movie_info_rows_scanned;
#endif
                        if (mi_has_nulls) {
                            if (mi_movie_id.is_null[end] != 0 ||
                                mi_info_type_id.is_null[end] != 0 ||
                                mi_info.is_null[end] != 0) {
                                ++end;
                                continue;
                            }
                        }
                        const int32_t info_str_id = mi_info.str_ids[end];
                        if (info_str_id < 0 ||
                            static_cast<size_t>(info_str_id) >= info_match_cache.size()) {
                            ++end;
                            continue;
                        }
                        int8_t matches = info_match_cache[info_str_id];
                        if (matches == -1) {
                            const auto& value_lower = db.string_pool.get_lower(info_str_id);
                            matches = q11b_internal::ilike_match_lower(info_pattern,
                                                                       value_lower)
                                          ? 1
                                          : 0;
                            info_match_cache[info_str_id] = matches;
                        }
                        if (matches == 0) {
                            ++end;
                            continue;
                        }
#ifdef TRACE
                        ++trace.movie_info_rows_emitted;
                        ++trace.movie_info_agg_rows_in;
                        if (movie_info_counts[movie_id] == 0) {
                            ++trace.movie_info_groups_created;
                            ++movie_info_group_count;
                            movie_info_movie_list.push_back(movie_id);
                        }
                        ++movie_info_counts[movie_id];
#else
                        if (movie_info_counts[movie_id] == 0) {
                            ++movie_info_group_count;
                            movie_info_movie_list.push_back(movie_id);
                        }
                        ++movie_info_counts[movie_id];
#endif
                        ++end;
                    }
                    type_pos = end;
                }
            } else {
                size_t end = start;
                while (end < movie_end) {
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    if (mi_has_nulls) {
                        if (mi_movie_id.is_null[end] != 0 ||
                            mi_info_type_id.is_null[end] != 0 ||
                            mi_info.is_null[end] != 0) {
                            ++end;
                            continue;
                        }
                    }
                    const int32_t info_type_id = mi_info_type_id.i32[end];
                    if (info_type_id < 0 ||
                        static_cast<size_t>(info_type_id) >= info_type1_flags.size() ||
                        info_type1_flags[static_cast<size_t>(info_type_id)] == 0) {
                        ++end;
                        continue;
                    }
                    const int32_t info_str_id = mi_info.str_ids[end];
                    if (info_str_id < 0 ||
                        static_cast<size_t>(info_str_id) >= info_match_cache.size()) {
                        ++end;
                        continue;
                    }
                    int8_t matches = info_match_cache[info_str_id];
                    if (matches == -1) {
                        const auto& value_lower = db.string_pool.get_lower(info_str_id);
                        matches =
                            q11b_internal::ilike_match_lower(info_pattern, value_lower)
                                ? 1
                                : 0;
                        info_match_cache[info_str_id] = matches;
                    }
                    if (matches == 0) {
                        ++end;
                        continue;
                    }
#ifdef TRACE
                    ++trace.movie_info_rows_emitted;
                    ++trace.movie_info_agg_rows_in;
                    if (movie_info_counts[movie_id] == 0) {
                        ++trace.movie_info_groups_created;
                        ++movie_info_group_count;
                        movie_info_movie_list.push_back(movie_id);
                    }
                    ++movie_info_counts[movie_id];
#else
                    if (movie_info_counts[movie_id] == 0) {
                        ++movie_info_group_count;
                        movie_info_movie_list.push_back(movie_id);
                    }
                    ++movie_info_counts[movie_id];
#endif
                    ++end;
                }
            }
            pos = movie_end;
        }
    } else {
        for (int64_t i = 0; i < movie_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            process_movie_info_row(idx);
        }
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

    const std::vector<int32_t>& cast_movie_list = movie_info_movie_list;

    const auto person_info_pattern = q11b_internal::build_ilike_pattern(args.INFO2);
    std::vector<int8_t> person_info_match_cache(string_pool_size, -1);
    std::vector<int32_t> person_info_counts(static_cast<size_t>(name.row_count) + 1, 0);
    std::vector<int32_t> person_info_person_list;
    person_info_person_list.reserve(32768);
    size_t person_info_group_count = 0;
    const auto& pi_person_id = person_info.columns.at("person_id");
    const auto& pi_info_type_id = person_info.columns.at("info_type_id");
    const auto& pi_info = person_info.columns.at("info");
    const bool pi_has_nulls =
        pi_person_id.has_nulls || pi_info_type_id.has_nulls || pi_info.has_nulls;
    std::vector<int32_t> info_type2_values;
    info_type2_values.reserve(info_type_filter2.values.size());
    for (const auto value : info_type_filter2.values) {
        if (value >= 0) {
            info_type2_values.push_back(value);
        }
    }
    std::sort(info_type2_values.begin(), info_type2_values.end());
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_person_info_scan");
#endif
    const auto process_person_info_row = [&](size_t idx, bool skip_info_type_check) {
#ifdef TRACE
        ++trace.person_info_rows_scanned;
#endif
        if (pi_has_nulls) {
            if (pi_person_id.is_null[idx] != 0 || pi_info.is_null[idx] != 0 ||
                pi_info_type_id.is_null[idx] != 0) {
                return;
            }
        }
        if (!skip_info_type_check) {
            const int32_t info_type_id = pi_info_type_id.i32[idx];
            if (info_type_id < 0 ||
                static_cast<size_t>(info_type_id) >= info_type2_flags.size() ||
                info_type2_flags[static_cast<size_t>(info_type_id)] == 0) {
                return;
            }
        }
        const int32_t info_str_id = pi_info.str_ids[idx];
        if (info_str_id < 0 ||
            static_cast<size_t>(info_str_id) >= person_info_match_cache.size()) {
            return;
        }
        int8_t matches = person_info_match_cache[info_str_id];
        if (matches == -1) {
            const auto& value_lower = db.string_pool.get_lower(info_str_id);
            matches = q11b_internal::ilike_match_lower(person_info_pattern, value_lower)
                          ? 1
                          : 0;
            person_info_match_cache[info_str_id] = matches;
        }
        if (matches == 0) {
            return;
        }
        const int32_t person_id = pi_person_id.i32[idx];
        if (person_id < 0) {
            return;
        }
        if (static_cast<size_t>(person_id) >= person_info_counts.size()) {
            person_info_counts.resize(static_cast<size_t>(person_id) + 1, 0);
        }
#ifdef TRACE
        ++trace.person_info_rows_emitted;
        ++trace.person_info_agg_rows_in;
        if (person_info_counts[person_id] == 0) {
            ++trace.person_info_groups_created;
            ++person_info_group_count;
            person_info_person_list.push_back(person_id);
        }
        ++person_info_counts[person_id];
#else
        if (person_info_counts[person_id] == 0) {
            ++person_info_group_count;
            person_info_person_list.push_back(person_id);
        }
        ++person_info_counts[person_id];
#endif
    };
    if (person_info.sorted_by_info_type_id && !info_type2_values.empty()) {
        size_t pos = 0;
        const auto& info_ids = pi_info_type_id.i32;
        const size_t row_count = info_ids.size();
        for (const int32_t info_type_id : info_type2_values) {
            if (pos >= row_count) {
                break;
            }
            auto it = std::lower_bound(info_ids.begin() + pos, info_ids.end(),
                                       info_type_id);
            if (it == info_ids.end()) {
                break;
            }
            size_t start = static_cast<size_t>(it - info_ids.begin());
            if (info_ids[start] != info_type_id) {
                pos = start;
                continue;
            }
            size_t end = start;
            while (end < row_count && info_ids[end] == info_type_id) {
                process_person_info_row(end, true);
                ++end;
            }
            pos = end;
        }
    } else {
        for (int64_t i = 0; i < person_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            process_person_info_row(idx, false);
        }
    }
#ifdef TRACE
    }
#endif
    if (person_info_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const int32_t missing_gender = std::numeric_limits<int32_t>::min();
    std::vector<int32_t> person_gender_ids(static_cast<size_t>(name.row_count) + 1,
                                           missing_gender);
    const auto& name_id_col = name.columns.at("id");
    const auto& name_gender_col = name.columns.at("gender");
    const bool name_id_has_nulls = name_id_col.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_name_scan");
#endif
    std::sort(person_info_person_list.begin(), person_info_person_list.end());
    const auto& name_ids = name_id_col.i32;
    size_t name_pos = 0;
    for (const int32_t person_id : person_info_person_list) {
        if (name_pos >= name_ids.size()) {
            break;
        }
        auto it = std::lower_bound(name_ids.begin() + name_pos, name_ids.end(),
                                   person_id);
        if (it == name_ids.end()) {
            break;
        }
        size_t idx = static_cast<size_t>(it - name_ids.begin());
        name_pos = idx + 1;
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (name_ids[idx] != person_id) {
            continue;
        }
        if (name_id_has_nulls && name_id_col.is_null[idx] != 0) {
            continue;
        }
        int32_t gender_id = -1;
        if (name_gender_col.is_null[idx] == 0) {
            gender_id = name_gender_col.str_ids[idx];
        }
        if (static_cast<size_t>(person_id) >= person_gender_ids.size()) {
            person_gender_ids.resize(static_cast<size_t>(person_id) + 1, missing_gender);
        }
        person_gender_ids[person_id] = gender_id;
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif
    if (person_gender_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    q11b_internal::PersonInfoMap person_info_map;
    person_info_map.init(person_info_person_list.size(), missing_gender);
    for (const int32_t person_id : person_info_person_list) {
        if (person_id < 0 || static_cast<size_t>(person_id) >= person_info_counts.size()) {
            continue;
        }
        const int32_t count = person_info_counts[person_id];
        if (count == 0) {
            continue;
        }
        const int32_t gender_id = person_gender_ids[person_id];
        person_info_map.insert(person_id, count, gender_id);
    }

    q11b_internal::ResultAggMap result_counts;
    result_counts.init(131072);
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const bool ci_has_nulls =
        ci_movie_id.has_nulls || ci_person_id.has_nulls || ci_role_id.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q11b_cast_info_scan");
#endif
    const auto process_cast_row = [&](size_t idx) {
#ifdef TRACE
        ++trace.cast_info_rows_scanned;
#endif
        if (ci_has_nulls) {
            if (ci_movie_id.is_null[idx] != 0 || ci_person_id.is_null[idx] != 0 ||
                ci_role_id.is_null[idx] != 0) {
                return;
            }
        }
        const int32_t movie_id = ci_movie_id.i32[idx];
        if (movie_id < 0 || static_cast<size_t>(movie_id) >= movie_info_counts.size()) {
            return;
        }
        const int32_t info_count = movie_info_counts[movie_id];
        if (info_count == 0) {
            return;
        }
        const uint32_t company_size = movie_company_sizes[movie_id];
        if (company_size == 0) {
            return;
        }
        const size_t company_offset = static_cast<size_t>(movie_company_offsets[movie_id]);
        const int32_t role_id = ci_role_id.i32[idx];
        if (role_id < 0 || static_cast<size_t>(role_id) >= role_id_to_str.size()) {
            return;
        }
        const int32_t role_str_id = role_id_to_str[role_id];
        if (role_str_id < 0) {
            return;
        }
        const int32_t person_id = ci_person_id.i32[idx];
        if (person_id < 0) {
            return;
        }
        int32_t person_info_count = 0;
        int32_t gender_id = missing_gender;
        if (!person_info_map.find(person_id, person_info_count, gender_id)) {
            return;
        }
        if (gender_id == missing_gender) {
            return;
        }
        q11b_internal::CastKey key{gender_id, role_str_id};
        const int64_t base_count =
            static_cast<int64_t>(info_count) * static_cast<int64_t>(person_info_count);
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.cast_info_agg_rows_in;
        ++trace.result_join_probe_rows_in;
        for (uint32_t idx = 0; idx < company_size; ++idx) {
            const size_t company_idx = company_offset + idx;
            const int32_t company_name_id = movie_company_name_ids[company_idx];
            const int32_t company_count = movie_company_counts[company_idx];
            q11b_internal::ResultKey result_key{key.gender_id, key.role_id,
                                                company_name_id};
            const int64_t delta = base_count * static_cast<int64_t>(company_count);
            ++trace.result_join_rows_emitted;
            ++trace.result_agg_rows_in;
            if (result_counts.add(result_key, delta)) {
                ++trace.result_groups_created;
            }
        }
#else
        for (uint32_t idx = 0; idx < company_size; ++idx) {
            const size_t company_idx = company_offset + idx;
            const int32_t company_name_id = movie_company_name_ids[company_idx];
            const int32_t company_count = movie_company_counts[company_idx];
            q11b_internal::ResultKey result_key{key.gender_id, key.role_id,
                                                company_name_id};
            const int64_t delta = base_count * static_cast<int64_t>(company_count);
            result_counts.add(result_key, delta);
        }
#endif
    };
    if (cast_info.sorted_by_movie_id && !cast_movie_list.empty()) {
        size_t pos = 0;
        const auto& ci_ids = ci_movie_id.i32;
        std::vector<q11b_internal::CastKey> movie_keys;
        std::vector<int64_t> movie_key_counts;
        movie_keys.reserve(32);
        movie_key_counts.reserve(32);
        for (const int32_t movie_id : cast_movie_list) {
            if (pos >= ci_ids.size()) {
                break;
            }
            auto it = std::lower_bound(ci_ids.begin() + pos, ci_ids.end(), movie_id);
            if (it == ci_ids.end()) {
                break;
            }
            size_t start = static_cast<size_t>(it - ci_ids.begin());
            if (ci_ids[start] != movie_id) {
                pos = start;
                continue;
            }
            const int32_t info_count = movie_info_counts[movie_id];
            if (info_count == 0) {
                pos = start + 1;
                continue;
            }
            const uint32_t company_size = movie_company_sizes[movie_id];
            if (company_size == 0) {
                pos = start + 1;
                continue;
            }
            const size_t company_offset = static_cast<size_t>(movie_company_offsets[movie_id]);
            movie_keys.clear();
            movie_key_counts.clear();
            const size_t movie_end = static_cast<size_t>(
                std::upper_bound(ci_ids.begin() + start, ci_ids.end(), movie_id) -
                ci_ids.begin());
            if (cast_info.sorted_by_role_id && !allowed_role_ids.empty()) {
                const auto& role_ids = ci_role_id.i32;
                size_t role_pos = start;
                for (const int32_t role_id : allowed_role_ids) {
                    if (role_pos >= movie_end) {
                        break;
                    }
                    auto role_it = std::lower_bound(role_ids.begin() + role_pos,
                                                    role_ids.begin() + movie_end, role_id);
                    if (role_it == role_ids.begin() + movie_end) {
                        break;
                    }
                    size_t role_start = static_cast<size_t>(role_it - role_ids.begin());
                    if (role_ids[role_start] != role_id) {
                        role_pos = role_start;
                        continue;
                    }
                    const int32_t role_str_id = role_id_to_str[role_id];
                    if (role_str_id < 0) {
                        role_pos = role_start + 1;
                        continue;
                    }
                    size_t end = role_start;
                    while (end < movie_end && role_ids[end] == role_id) {
#ifdef TRACE
                        ++trace.cast_info_rows_scanned;
#endif
                        if (ci_has_nulls) {
                            if (ci_movie_id.is_null[end] != 0 ||
                                ci_person_id.is_null[end] != 0 ||
                                ci_role_id.is_null[end] != 0) {
                                ++end;
                                continue;
                            }
                        }
                        const int32_t person_id = ci_person_id.i32[end];
                        if (person_id < 0) {
                            ++end;
                            continue;
                        }
                        int32_t person_info_count = 0;
                        int32_t gender_id = missing_gender;
                        if (!person_info_map.find(person_id, person_info_count,
                                                  gender_id)) {
                            ++end;
                            continue;
                        }
                        if (gender_id == missing_gender) {
                            ++end;
                            continue;
                        }
#ifdef TRACE
                        ++trace.cast_info_rows_emitted;
                        ++trace.cast_info_agg_rows_in;
                        ++trace.result_join_probe_rows_in;
#endif
                        q11b_internal::CastKey key{gender_id, role_str_id};
                        bool merged = false;
                        for (size_t key_idx = 0; key_idx < movie_keys.size(); ++key_idx) {
                            if (movie_keys[key_idx] == key) {
                                movie_key_counts[key_idx] +=
                                    static_cast<int64_t>(person_info_count);
                                merged = true;
                                break;
                            }
                        }
                        if (!merged) {
                            movie_keys.push_back(key);
                            movie_key_counts.push_back(
                                static_cast<int64_t>(person_info_count));
                        }
                        ++end;
                    }
                    role_pos = end;
                }
            } else {
                size_t end = start;
                while (end < movie_end) {
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    if (ci_has_nulls) {
                        if (ci_movie_id.is_null[end] != 0 ||
                            ci_person_id.is_null[end] != 0 ||
                            ci_role_id.is_null[end] != 0) {
                            ++end;
                            continue;
                        }
                    }
                    const int32_t role_id = ci_role_id.i32[end];
                    if (role_id < 0 ||
                        static_cast<size_t>(role_id) >= role_id_to_str.size()) {
                        ++end;
                        continue;
                    }
                    const int32_t role_str_id = role_id_to_str[role_id];
                    if (role_str_id < 0) {
                        ++end;
                        continue;
                    }
                    const int32_t person_id = ci_person_id.i32[end];
                    if (person_id < 0) {
                        ++end;
                        continue;
                    }
                    int32_t person_info_count = 0;
                    int32_t gender_id = missing_gender;
                    if (!person_info_map.find(person_id, person_info_count, gender_id)) {
                        ++end;
                        continue;
                    }
                    if (gender_id == missing_gender) {
                        ++end;
                        continue;
                    }
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
                    ++trace.result_join_probe_rows_in;
#endif
                    q11b_internal::CastKey key{gender_id, role_str_id};
                    bool merged = false;
                    for (size_t key_idx = 0; key_idx < movie_keys.size(); ++key_idx) {
                        if (movie_keys[key_idx] == key) {
                            movie_key_counts[key_idx] +=
                                static_cast<int64_t>(person_info_count);
                            merged = true;
                            break;
                        }
                    }
                    if (!merged) {
                        movie_keys.push_back(key);
                        movie_key_counts.push_back(
                            static_cast<int64_t>(person_info_count));
                    }
                    ++end;
                }
            }
            for (size_t key_idx = 0; key_idx < movie_keys.size(); ++key_idx) {
                const auto& key = movie_keys[key_idx];
                const int64_t base_count =
                    static_cast<int64_t>(info_count) * movie_key_counts[key_idx];
#ifdef TRACE
                for (uint32_t idx = 0; idx < company_size; ++idx) {
                    const size_t company_idx = company_offset + idx;
                    const int32_t company_name_id = movie_company_name_ids[company_idx];
                    const int32_t company_count = movie_company_counts[company_idx];
                    q11b_internal::ResultKey result_key{key.gender_id, key.role_id,
                                                        company_name_id};
                    const int64_t delta =
                        base_count * static_cast<int64_t>(company_count);
                    ++trace.result_join_rows_emitted;
                    ++trace.result_agg_rows_in;
                    if (result_counts.add(result_key, delta)) {
                        ++trace.result_groups_created;
                    }
                }
#else
                for (uint32_t idx = 0; idx < company_size; ++idx) {
                    const size_t company_idx = company_offset + idx;
                    const int32_t company_name_id = movie_company_name_ids[company_idx];
                    const int32_t company_count = movie_company_counts[company_idx];
                    q11b_internal::ResultKey result_key{key.gender_id, key.role_id,
                                                        company_name_id};
                    const int64_t delta =
                        base_count * static_cast<int64_t>(company_count);
                    result_counts.add(result_key, delta);
                }
#endif
            }
            pos = movie_end;
        }
    } else {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            process_cast_row(idx);
        }
    }
#ifdef TRACE
    }
#endif
    if (result_counts.size == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    results.reserve(result_counts.size);
    for (size_t i = 0; i < result_counts.used.size(); ++i) {
        if (result_counts.used[i] == 0) {
            continue;
        }
        results.push_back(Q11bResultRow{result_counts.keys[i].gender_id,
                                        result_counts.keys[i].role_id,
                                        result_counts.keys[i].company_name_id,
                                        result_counts.values[i]});
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q11b_sort");
        trace.sort_rows_in = results.size();
#endif
    std::sort(results.begin(), results.end(),
              [](const Q11bResultRow& a, const Q11bResultRow& b) {
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
    trace.movie_companies_agg_rows_emitted = movie_company_movie_count;
    trace.person_info_agg_rows_emitted = person_info_group_count;
    trace.cast_info_agg_rows_emitted = 0;
    trace.result_agg_rows_emitted = result_counts.size;
    trace.result_join_build_rows_in =
        static_cast<uint64_t>(movie_info_group_count + movie_company_movie_count);
    trace.query_output_rows = results.size();
#endif
#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q11b_role_type_scan_rows_scanned", trace.role_type_rows_scanned);
    print_count("q11b_role_type_scan_rows_emitted", trace.role_type_rows_emitted);
    print_count("q11b_kind_type_scan_rows_scanned", trace.kind_type_rows_scanned);
    print_count("q11b_kind_type_scan_rows_emitted", trace.kind_type_rows_emitted);
    print_count("q11b_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q11b_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q11b_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q11b_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q11b_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q11b_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q11b_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q11b_company_name_scan_rows_scanned", trace.company_name_rows_scanned);
    print_count("q11b_company_name_scan_rows_emitted", trace.company_name_rows_emitted);
    print_count("q11b_company_type_scan_rows_scanned", trace.company_type_rows_scanned);
    print_count("q11b_company_type_scan_rows_emitted", trace.company_type_rows_emitted);
    print_count("q11b_movie_companies_scan_rows_scanned", trace.movie_companies_rows_scanned);
    print_count("q11b_movie_companies_scan_rows_emitted", trace.movie_companies_rows_emitted);
    print_count("q11b_movie_companies_agg_rows_in", trace.movie_companies_agg_rows_in);
    print_count("q11b_movie_companies_groups_created", trace.movie_companies_groups_created);
    print_count("q11b_movie_companies_agg_rows_emitted",
                trace.movie_companies_agg_rows_emitted);
    print_count("q11b_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q11b_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q11b_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q11b_person_info_groups_created", trace.person_info_groups_created);
    print_count("q11b_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q11b_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q11b_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q11b_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q11b_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q11b_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q11b_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q11b_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q11b_result_join_build_rows_in", trace.result_join_build_rows_in);
    print_count("q11b_result_join_probe_rows_in", trace.result_join_probe_rows_in);
    print_count("q11b_result_join_rows_emitted", trace.result_join_rows_emitted);
    print_count("q11b_result_agg_rows_in", trace.result_agg_rows_in);
    print_count("q11b_result_groups_created", trace.result_groups_created);
    print_count("q11b_result_agg_rows_emitted", trace.result_agg_rows_emitted);
    print_count("q11b_sort_rows_in", trace.sort_rows_in);
    print_count("q11b_sort_rows_out", trace.sort_rows_out);
    print_count("q11b_query_output_rows", trace.query_output_rows);
#endif
    return results;
}