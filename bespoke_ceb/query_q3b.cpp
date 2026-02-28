#include "query_q3b.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

#ifdef TRACE
struct Q3bTrace {
    TraceRecorder recorder;
    uint64_t name_rows_scanned = 0;
    uint64_t name_rows_emitted = 0;
    uint64_t company_rows_scanned = 0;
    uint64_t company_rows_emitted = 0;
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t movie_keyword_rows_scanned = 0;
    uint64_t movie_keyword_rows_emitted = 0;
    uint64_t movie_keyword_agg_rows_in = 0;
    uint64_t movie_keyword_groups_created = 0;
    uint64_t movie_keyword_agg_rows_emitted = 0;
    uint64_t movie_companies_rows_scanned = 0;
    uint64_t movie_companies_rows_emitted = 0;
    uint64_t movie_companies_agg_rows_in = 0;
    uint64_t movie_companies_groups_created = 0;
    uint64_t movie_companies_agg_rows_emitted = 0;
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t cast_info_agg_rows_in = 0;
    uint64_t cast_info_groups_created = 0;
    uint64_t cast_info_agg_rows_emitted = 0;
    uint64_t join_build_rows_in = 0;
    uint64_t join_probe_rows_in = 0;
    uint64_t join_rows_emitted = 0;
    uint64_t agg_rows_in = 0;
    uint64_t agg_groups_created = 0;
    uint64_t agg_rows_emitted = 0;
    uint64_t sort_rows_in = 0;
    uint64_t sort_rows_out = 0;
    uint64_t query_output_rows = 0;
};
#endif

namespace q3b_internal {

struct StringFilter {
    std::unordered_set<int32_t> values;
};

struct LikePattern {
    enum class Type { kMatchAll, kExact, kPrefix, kSuffix, kContains, kGeneral };
    Type type = Type::kGeneral;
    std::string pattern_lower;
    std::string token;
};

struct DenseBoolSet {
    std::vector<uint8_t> values;
    size_t count = 0;
};

struct DenseIdMap {
    std::vector<int32_t> values;
    size_t count = 0;
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

bool starts_with(const std::string& value, const std::string& token) {
    return value.size() >= token.size() && value.compare(0, token.size(), token) == 0;
}

bool ends_with(const std::string& value, const std::string& token) {
    return value.size() >= token.size() &&
           value.compare(value.size() - token.size(), token.size(), token) == 0;
}

bool contains_token(const std::string& value, const std::string& token) {
    if (token.empty()) {
        return true;
    }
    return value.find(token) != std::string::npos;
}

bool ilike_match(const LikePattern& pattern, const std::string& value_lower) {
    switch (pattern.type) {
        case LikePattern::Type::kMatchAll:
            return true;
        case LikePattern::Type::kExact:
            return value_lower == pattern.token;
        case LikePattern::Type::kPrefix:
            return starts_with(value_lower, pattern.token);
        case LikePattern::Type::kSuffix:
            return ends_with(value_lower, pattern.token);
        case LikePattern::Type::kContains:
            return contains_token(value_lower, pattern.token);
        case LikePattern::Type::kGeneral:
            return like_match(pattern.pattern_lower, value_lower);
    }
    return false;
}

std::unordered_set<int32_t> build_column_string_values(const ColumnData& col) {
    std::unordered_set<int32_t> values;
    values.reserve(col.str_ids.size());
    if (!col.has_nulls) {
        for (size_t i = 0; i < col.str_ids.size(); ++i) {
            values.insert(col.str_ids[i]);
        }
        return values;
    }
    const auto* nulls = col.is_null.data();
    for (size_t i = 0; i < col.str_ids.size(); ++i) {
        if (nulls[i] != 0) {
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

DenseBoolSet build_allowed_ids_from_dim(const TableData& table, const std::string& id_col_name,
                                        const std::string& str_col_name,
                                        const StringFilter& filter) {
    DenseBoolSet ids;
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    ids.values.assign(static_cast<size_t>(table.row_count) + 1, 0);
    int32_t max_id = -1;
    const bool id_has_nulls = id_col.has_nulls;
    const bool str_has_nulls = str_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* str_nulls = str_has_nulls ? str_col.is_null.data() : nullptr;
    const auto* str_ids = str_col.str_ids.data();
    const auto* id_values = id_col.i32.data();
    for (int64_t i = 0; i < table.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
        if (id_has_nulls && id_nulls[idx] != 0) {
            continue;
        }
        if (str_has_nulls && str_nulls[idx] != 0) {
            continue;
        }
        const int32_t id = id_values[idx];
        if (id > max_id) {
            max_id = id;
        }
        if (id < 0) {
            continue;
        }
        if (filter.values.find(str_ids[idx]) == filter.values.end()) {
            continue;
        }
        const size_t id_index = static_cast<size_t>(id);
        if (id_index >= ids.values.size()) {
            ids.values.resize(id_index + 1, 0);
        }
        if (ids.values[id_index] == 0) {
            ids.values[id_index] = 1;
            ++ids.count;
        }
    }
    if (max_id >= 0 && static_cast<size_t>(max_id) >= ids.values.size()) {
        ids.values.resize(static_cast<size_t>(max_id) + 1, 0);
    }
    return ids;
}

DenseIdMap build_person_name_map(const TableData& name, const StringPool& pool,
                                 const LikePattern& pattern) {
    DenseIdMap person_name;
    person_name.values.assign(static_cast<size_t>(name.row_count) + 1, -1);
    std::vector<int8_t> match_cache;
    const auto& id_col = name.columns.at("id");
    const auto& name_col = name.columns.at("name");
    const auto& pcode_col = name.columns.at("name_pcode_nf");
    const bool id_has_nulls = id_col.has_nulls;
    const bool name_has_nulls = name_col.has_nulls;
    const bool pcode_has_nulls = pcode_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* name_nulls = name_has_nulls ? name_col.is_null.data() : nullptr;
    const auto* pcode_nulls = pcode_has_nulls ? pcode_col.is_null.data() : nullptr;
    const auto* id_values = id_col.i32.data();
    const auto* name_ids = name_col.str_ids.data();
    const auto* pcode_ids = pcode_col.str_ids.data();
    int32_t max_id = -1;
    for (int64_t i = 0; i < name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
        if ((id_has_nulls && id_nulls[idx] != 0) ||
            (name_has_nulls && name_nulls[idx] != 0) ||
            (pcode_has_nulls && pcode_nulls[idx] != 0)) {
            continue;
        }
        const int32_t person_id = id_values[idx];
        if (person_id > max_id) {
            max_id = person_id;
        }
        if (person_id < 0) {
            continue;
        }
        const int32_t pcode_id = pcode_ids[idx];
        if (pcode_id < 0) {
            continue;
        }
        const size_t pcode_index = static_cast<size_t>(pcode_id);
        if (pcode_index >= match_cache.size()) {
            match_cache.resize(pcode_index + 1, -1);
        }
        int8_t match = match_cache[pcode_index];
        if (match < 0) {
            const auto& pcode_val = pool.lower_values[pcode_index];
            match = ilike_match(pattern, pcode_val) ? 1 : 0;
            match_cache[pcode_index] = match;
        }
        if (match == 0) {
            continue;
        }
        const size_t id_index = static_cast<size_t>(person_id);
        if (id_index >= person_name.values.size()) {
            person_name.values.resize(id_index + 1, -1);
        }
        if (person_name.values[id_index] == -1) {
            ++person_name.count;
        }
        person_name.values[id_index] = name_ids[idx];
    }
    if (max_id >= 0 && static_cast<size_t>(max_id) >= person_name.values.size()) {
        person_name.values.resize(static_cast<size_t>(max_id) + 1, -1);
    }
    return person_name;
}

DenseIdMap build_person_name_map_filtered(const TableData& name, const StringPool& pool,
                                          const LikePattern& pattern,
                                          const std::vector<uint8_t>& candidate_persons) {
    DenseIdMap person_name;
    if (candidate_persons.empty()) {
        return person_name;
    }
    person_name.values.assign(candidate_persons.size(), -1);
    std::vector<int8_t> match_cache(pool.lower_values.size(), -1);
    const auto& id_col = name.columns.at("id");
    const auto& name_col = name.columns.at("name");
    const auto& pcode_col = name.columns.at("name_pcode_nf");
    const bool id_has_nulls = id_col.has_nulls;
    const bool name_has_nulls = name_col.has_nulls;
    const bool pcode_has_nulls = pcode_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* name_nulls = name_has_nulls ? name_col.is_null.data() : nullptr;
    const auto* pcode_nulls = pcode_has_nulls ? pcode_col.is_null.data() : nullptr;
    const auto* id_values = id_col.i32.data();
    const auto* name_ids = name_col.str_ids.data();
    const auto* pcode_ids = pcode_col.str_ids.data();
    const size_t candidate_size = candidate_persons.size();
    if (!id_has_nulls && !name_has_nulls && !pcode_has_nulls) {
        for (int64_t i = 0; i < name.row_count; ++i) {
            const int32_t person_id = id_values[i];
            if (person_id < 0 || static_cast<size_t>(person_id) >= candidate_size) {
                continue;
            }
            if (candidate_persons[static_cast<size_t>(person_id)] == 0) {
                continue;
            }
            const int32_t pcode_id = pcode_ids[i];
            if (pcode_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(pcode_id)];
            if (match < 0) {
                const auto& pcode_val = pool.lower_values[static_cast<size_t>(pcode_id)];
                match = ilike_match(pattern, pcode_val) ? 1 : 0;
                match_cache[static_cast<size_t>(pcode_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            if (person_name.values[static_cast<size_t>(person_id)] == -1) {
                ++person_name.count;
            }
            person_name.values[static_cast<size_t>(person_id)] = name_ids[i];
        }
    } else {
        for (int64_t i = 0; i < name.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            if ((id_has_nulls && id_nulls[idx] != 0) ||
                (name_has_nulls && name_nulls[idx] != 0) ||
                (pcode_has_nulls && pcode_nulls[idx] != 0)) {
                continue;
            }
            const int32_t person_id = id_values[idx];
            if (person_id < 0 || static_cast<size_t>(person_id) >= candidate_size) {
                continue;
            }
            if (candidate_persons[static_cast<size_t>(person_id)] == 0) {
                continue;
            }
            const int32_t pcode_id = pcode_ids[idx];
            if (pcode_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(pcode_id)];
            if (match < 0) {
                const auto& pcode_val = pool.lower_values[static_cast<size_t>(pcode_id)];
                match = ilike_match(pattern, pcode_val) ? 1 : 0;
                match_cache[static_cast<size_t>(pcode_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            if (person_name.values[static_cast<size_t>(person_id)] == -1) {
                ++person_name.count;
            }
            person_name.values[static_cast<size_t>(person_id)] = name_ids[idx];
        }
    }
    return person_name;
}

DenseIdMap build_company_name_map(const TableData& company_name, const StringPool& pool,
                                  const LikePattern& pattern) {
    DenseIdMap company_map;
    company_map.values.assign(static_cast<size_t>(company_name.row_count) + 1, -1);
    std::vector<int8_t> match_cache(pool.lower_values.size(), -1);
    const auto& id_col = company_name.columns.at("id");
    const auto& name_col = company_name.columns.at("name");
    const bool id_has_nulls = id_col.has_nulls;
    const bool name_has_nulls = name_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* name_nulls = name_has_nulls ? name_col.is_null.data() : nullptr;
    const auto* id_values = id_col.i32.data();
    const auto* name_ids = name_col.str_ids.data();
    int32_t max_id = -1;
    if (!id_has_nulls && !name_has_nulls) {
        for (int64_t i = 0; i < company_name.row_count; ++i) {
            const int32_t company_id = id_values[i];
            if (company_id > max_id) {
                max_id = company_id;
            }
            if (company_id < 0) {
                continue;
            }
            const int32_t name_id = name_ids[i];
            if (name_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(name_id)];
            if (match < 0) {
                const auto& value = pool.lower_values[static_cast<size_t>(name_id)];
                match = ilike_match(pattern, value) ? 1 : 0;
                match_cache[static_cast<size_t>(name_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            const size_t id_index = static_cast<size_t>(company_id);
            if (id_index >= company_map.values.size()) {
                company_map.values.resize(id_index + 1, -1);
            }
            if (company_map.values[id_index] == -1) {
                ++company_map.count;
            }
            company_map.values[id_index] = name_ids[i];
        }
    } else {
        for (int64_t i = 0; i < company_name.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            if ((id_has_nulls && id_nulls[idx] != 0) ||
                (name_has_nulls && name_nulls[idx] != 0)) {
                continue;
            }
            const int32_t company_id = id_values[idx];
            if (company_id > max_id) {
                max_id = company_id;
            }
            if (company_id < 0) {
                continue;
            }
            const int32_t name_id = name_ids[idx];
            if (name_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(name_id)];
            if (match < 0) {
                const auto& value = pool.lower_values[static_cast<size_t>(name_id)];
                match = ilike_match(pattern, value) ? 1 : 0;
                match_cache[static_cast<size_t>(name_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            const size_t id_index = static_cast<size_t>(company_id);
            if (id_index >= company_map.values.size()) {
                company_map.values.resize(id_index + 1, -1);
            }
            if (company_map.values[id_index] == -1) {
                ++company_map.count;
            }
            company_map.values[id_index] = name_ids[idx];
        }
    }
    if (max_id >= 0 && static_cast<size_t>(max_id) >= company_map.values.size()) {
        company_map.values.resize(static_cast<size_t>(max_id) + 1, -1);
    }
    return company_map;
}

DenseIdMap build_movie_title_map(const TableData& title, const StringPool& pool,
                                 const LikePattern& pattern, const DenseBoolSet& kind_ids) {
    DenseIdMap movie_title;
    movie_title.values.assign(static_cast<size_t>(title.row_count) + 1, -1);
    std::vector<int8_t> match_cache;
    const auto& id_col = title.columns.at("id");
    const auto& title_col = title.columns.at("title");
    const auto& kind_col = title.columns.at("kind_id");
    const bool id_has_nulls = id_col.has_nulls;
    const bool title_has_nulls = title_col.has_nulls;
    const bool kind_has_nulls = kind_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* title_nulls = title_has_nulls ? title_col.is_null.data() : nullptr;
    const auto* kind_nulls = kind_has_nulls ? kind_col.is_null.data() : nullptr;
    const auto* id_values = id_col.i32.data();
    const auto* title_ids = title_col.str_ids.data();
    const auto* kind_values = kind_col.i32.data();
    int32_t max_id = -1;
    for (int64_t i = 0; i < title.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
        if ((id_has_nulls && id_nulls[idx] != 0) ||
            (title_has_nulls && title_nulls[idx] != 0) ||
            (kind_has_nulls && kind_nulls[idx] != 0)) {
            continue;
        }
        const int32_t movie_id = id_values[idx];
        if (movie_id > max_id) {
            max_id = movie_id;
        }
        const int32_t kind_id = kind_values[idx];
        if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_ids.values.size() ||
            kind_ids.values[static_cast<size_t>(kind_id)] == 0) {
            continue;
        }
        const int32_t title_id = title_ids[idx];
        if (title_id < 0) {
            continue;
        }
        const size_t title_index = static_cast<size_t>(title_id);
        if (title_index >= match_cache.size()) {
            match_cache.resize(title_index + 1, -1);
        }
        int8_t match = match_cache[title_index];
        if (match < 0) {
            const auto& title_val = pool.lower_values[title_index];
            match = ilike_match(pattern, title_val) ? 1 : 0;
            match_cache[title_index] = match;
        }
        if (match == 0) {
            continue;
        }
        const size_t id_index = static_cast<size_t>(movie_id);
        if (id_index >= movie_title.values.size()) {
            movie_title.values.resize(id_index + 1, -1);
        }
        if (movie_title.values[id_index] == -1) {
            ++movie_title.count;
        }
        movie_title.values[id_index] = title_ids[idx];
    }
    if (max_id >= 0 && static_cast<size_t>(max_id) >= movie_title.values.size()) {
        movie_title.values.resize(static_cast<size_t>(max_id) + 1, -1);
    }
    return movie_title;
}

DenseIdMap build_movie_title_map_filtered(const TableData& title, const StringPool& pool,
                                          const LikePattern& pattern,
                                          const DenseBoolSet& kind_ids,
                                          const std::vector<uint8_t>& candidate_movies,
                                          int32_t max_movie_id) {
    DenseIdMap movie_title;
    if (max_movie_id < 0 || candidate_movies.empty()) {
        return movie_title;
    }
    movie_title.values.assign(static_cast<size_t>(max_movie_id) + 1, -1);
    std::vector<int8_t> match_cache(pool.lower_values.size(), -1);
    const auto& id_col = title.columns.at("id");
    const auto& title_col = title.columns.at("title");
    const auto& kind_col = title.columns.at("kind_id");
    const bool id_has_nulls = id_col.has_nulls;
    const bool title_has_nulls = title_col.has_nulls;
    const bool kind_has_nulls = kind_col.has_nulls;
    const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
    const auto* title_nulls = title_has_nulls ? title_col.is_null.data() : nullptr;
    const auto* kind_nulls = kind_has_nulls ? kind_col.is_null.data() : nullptr;
    const auto* id_values = id_col.i32.data();
    const auto* title_ids = title_col.str_ids.data();
    const auto* kind_values = kind_col.i32.data();
    if (!id_has_nulls && !title_has_nulls && !kind_has_nulls) {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const int32_t movie_id = id_values[i];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (candidate_movies[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t kind_id = kind_values[i];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_ids.values.size() ||
                kind_ids.values[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t title_id = title_ids[i];
            if (title_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(title_id)];
            if (match < 0) {
                const auto& title_val = pool.lower_values[static_cast<size_t>(title_id)];
                match = ilike_match(pattern, title_val) ? 1 : 0;
                match_cache[static_cast<size_t>(title_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            if (movie_title.values[static_cast<size_t>(movie_id)] == -1) {
                ++movie_title.count;
            }
            movie_title.values[static_cast<size_t>(movie_id)] = title_id;
        }
    } else {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
            if ((id_has_nulls && id_nulls[idx] != 0) ||
                (title_has_nulls && title_nulls[idx] != 0) ||
                (kind_has_nulls && kind_nulls[idx] != 0)) {
                continue;
            }
            const int32_t movie_id = id_values[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (candidate_movies[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t kind_id = kind_values[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_ids.values.size() ||
                kind_ids.values[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t title_id = title_ids[idx];
            if (title_id < 0) {
                continue;
            }
            int8_t match = match_cache[static_cast<size_t>(title_id)];
            if (match < 0) {
                const auto& title_val = pool.lower_values[static_cast<size_t>(title_id)];
                match = ilike_match(pattern, title_val) ? 1 : 0;
                match_cache[static_cast<size_t>(title_id)] = match;
            }
            if (match == 0) {
                continue;
            }
            if (movie_title.values[static_cast<size_t>(movie_id)] == -1) {
                ++movie_title.count;
            }
            movie_title.values[static_cast<size_t>(movie_id)] = title_id;
        }
    }
    return movie_title;
}

struct ResultKey {
    int32_t title_id{};
    int32_t person_name_id{};
    int32_t company_name_id{};

    bool operator==(const ResultKey& other) const {
        return title_id == other.title_id && person_name_id == other.person_name_id &&
               company_name_id == other.company_name_id;
    }
};

struct ResultKeyHash {
    size_t operator()(const ResultKey& key) const {
        const size_t h1 = std::hash<int32_t>{}(key.title_id);
        const size_t h2 = std::hash<int32_t>{}(key.person_name_id);
        const size_t h3 = std::hash<int32_t>{}(key.company_name_id);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

}  // namespace q3b_internal

std::vector<Q3bResultRow> run_q3b(const Database& db, const Q3bArgs& args) {
    std::vector<Q3bResultRow> results;
#ifdef TRACE
    Q3bTrace trace;
    {
        PROFILE_SCOPE(&trace.recorder, "q3b_total");
#endif
    const auto title_pattern = q3b_internal::build_ilike_pattern(args.TITLE);
    const auto name_pcode_pattern = q3b_internal::build_ilike_pattern(args.NAME_PCODE_NF);
    const auto company_pattern = q3b_internal::build_ilike_pattern(args.NAME);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& kind_col = kind_type.columns.at("kind");
    const auto kind_filter =
        q3b_internal::build_string_filter_for_column(db.string_pool, kind_col, args.KIND);
    const auto allowed_kind_ids =
        q3b_internal::build_allowed_ids_from_dim(kind_type, "id", "kind", kind_filter);
    bool skip_query = allowed_kind_ids.count == 0;

    const auto& role_type = db.tables.at("role_type");
    const auto& role_col = role_type.columns.at("role");
    const auto role_filter =
        q3b_internal::build_string_filter_for_column(db.string_pool, role_col, args.ROLE);
    const auto allowed_role_ids =
        q3b_internal::build_allowed_ids_from_dim(role_type, "id", "role", role_filter);
    if (allowed_role_ids.count == 0) {
        skip_query = true;
    }

    const auto& company_name = db.tables.at("company_name");
    q3b_internal::DenseIdMap company_name_map;
    if (!skip_query) {
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_company_name_scan");
#endif
        company_name_map =
            q3b_internal::build_company_name_map(company_name, db.string_pool, company_pattern);
#ifdef TRACE
        }
        trace.company_rows_scanned = static_cast<uint64_t>(company_name.row_count);
        trace.company_rows_emitted = company_name_map.count;
#endif
        if (company_name_map.count == 0) {
            skip_query = true;
        }
    }

    const auto& movie_companies = db.tables.at("movie_companies");
    const auto& mc_movie_id = movie_companies.columns.at("movie_id");
    const auto& mc_company_id = movie_companies.columns.at("company_id");
    const auto& mc_company_type_id = movie_companies.columns.at("company_type_id");
    std::unordered_map<int32_t, std::unordered_map<int32_t, int64_t>> mc_by_movie;
    const size_t mc_reserve =
        std::min(static_cast<size_t>(movie_companies.row_count),
                 static_cast<size_t>(company_name_map.count * 4 + 64));
    mc_by_movie.reserve(mc_reserve);
    int32_t max_movie_id = -1;
    if (!skip_query) {
        const bool mc_movie_has_nulls = mc_movie_id.has_nulls;
        const bool mc_company_has_nulls = mc_company_id.has_nulls;
        const bool mc_type_has_nulls = mc_company_type_id.has_nulls;
        const auto* mc_movie_nulls = mc_movie_has_nulls ? mc_movie_id.is_null.data() : nullptr;
        const auto* mc_company_nulls = mc_company_has_nulls ? mc_company_id.is_null.data() : nullptr;
        const auto* mc_type_nulls = mc_type_has_nulls ? mc_company_type_id.is_null.data() : nullptr;
        const auto* mc_movie_values = mc_movie_id.i32.data();
        const auto* mc_company_values = mc_company_id.i32.data();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_movie_companies_scan");
#endif
        for (int64_t i = 0; i < movie_companies.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_companies_rows_scanned;
#endif
            if ((mc_movie_has_nulls && mc_movie_nulls[idx] != 0) ||
                (mc_company_has_nulls && mc_company_nulls[idx] != 0) ||
                (mc_type_has_nulls && mc_type_nulls[idx] != 0)) {
                continue;
            }
            const int32_t movie_id = mc_movie_values[idx];
            if (movie_id > max_movie_id) {
                max_movie_id = movie_id;
            }
            const int32_t company_id = mc_company_values[idx];
            if (company_name_map.values[static_cast<size_t>(company_id)] == -1) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_companies_rows_emitted;
            ++trace.movie_companies_agg_rows_in;
            auto& company_map = mc_by_movie[movie_id];
            auto it_company = company_map.find(company_id);
            if (it_company == company_map.end()) {
                company_map.emplace(company_id, 1);
                ++trace.movie_companies_groups_created;
            } else {
                ++it_company->second;
            }
#else
            ++mc_by_movie[movie_id][company_id];
#endif
        }
#ifdef TRACE
        }
#endif
        if (mc_by_movie.empty()) {
            skip_query = true;
        }
    }

    std::vector<uint8_t> movie_in_mc;
    if (!skip_query) {
        if (max_movie_id < 0) {
            skip_query = true;
        } else {
            movie_in_mc.assign(static_cast<size_t>(max_movie_id) + 1, 0);
            for (const auto& entry : mc_by_movie) {
                const int32_t movie_id = entry.first;
                if (movie_id >= 0 && movie_id <= max_movie_id) {
                    movie_in_mc[static_cast<size_t>(movie_id)] = 1;
                }
            }
        }
    }

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");
    std::vector<int64_t> mk_counts;
    size_t mk_nonzero = 0;
    if (!skip_query) {
        mk_counts.assign(static_cast<size_t>(max_movie_id) + 1, 0);
        const bool mk_movie_has_nulls = mk_movie_id.has_nulls;
        const bool mk_keyword_has_nulls = mk_keyword_id.has_nulls;
        const auto* mk_movie_nulls = mk_movie_has_nulls ? mk_movie_id.is_null.data() : nullptr;
        const auto* mk_keyword_nulls = mk_keyword_has_nulls ? mk_keyword_id.is_null.data() : nullptr;
        const auto* mk_movie_values = mk_movie_id.i32.data();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_movie_keyword_scan");
#endif
        for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_keyword_rows_scanned;
#endif
            if ((mk_movie_has_nulls && mk_movie_nulls[idx] != 0) ||
                (mk_keyword_has_nulls && mk_keyword_nulls[idx] != 0)) {
                continue;
            }
            const int32_t movie_id = mk_movie_values[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (movie_in_mc[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_keyword_rows_emitted;
            ++trace.movie_keyword_agg_rows_in;
            if (mk_counts[static_cast<size_t>(movie_id)] == 0) {
                ++trace.movie_keyword_groups_created;
            }
            ++mk_counts[static_cast<size_t>(movie_id)];
#else
            if (mk_counts[static_cast<size_t>(movie_id)] == 0) {
                ++mk_nonzero;
            }
            ++mk_counts[static_cast<size_t>(movie_id)];
#endif
        }
#ifdef TRACE
        }
        for (int64_t value : mk_counts) {
            if (value != 0) {
                ++mk_nonzero;
            }
        }
        trace.movie_keyword_agg_rows_emitted = mk_nonzero;
#endif
        if (mk_nonzero == 0) {
            skip_query = true;
        }
    }

    std::vector<int32_t> candidate_movies;
    if (!skip_query) {
        candidate_movies.reserve(mc_by_movie.size());
        for (const auto& entry : mc_by_movie) {
            const int32_t movie_id = entry.first;
            if (mk_counts[static_cast<size_t>(movie_id)] != 0) {
                candidate_movies.push_back(movie_id);
            }
        }
        if (candidate_movies.empty()) {
            skip_query = true;
        } else {
            std::sort(candidate_movies.begin(), candidate_movies.end());
        }
    }

    const auto& title = db.tables.at("title");
    q3b_internal::DenseIdMap movie_title_map;
    std::vector<uint8_t> movie_in_candidates;
    if (!skip_query) {
        movie_title_map.values.assign(static_cast<size_t>(max_movie_id) + 1, -1);
        movie_in_candidates.assign(static_cast<size_t>(max_movie_id) + 1, 0);
        for (int32_t movie_id : candidate_movies) {
            if (movie_id >= 0 && movie_id <= max_movie_id) {
                movie_in_candidates[static_cast<size_t>(movie_id)] = 1;
            }
        }
        const auto& id_col = title.columns.at("id");
        const auto& title_col = title.columns.at("title");
        const auto& kind_col = title.columns.at("kind_id");
        const bool id_has_nulls = id_col.has_nulls;
        const bool title_has_nulls = title_col.has_nulls;
        const bool kind_has_nulls = kind_col.has_nulls;
        const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
        const auto* title_nulls = title_has_nulls ? title_col.is_null.data() : nullptr;
        const auto* kind_nulls = kind_has_nulls ? kind_col.is_null.data() : nullptr;
        const auto* id_values = id_col.i32.data();
        const auto* title_ids = title_col.str_ids.data();
        const auto* kind_values = kind_col.i32.data();
        std::vector<int8_t> match_cache(db.string_pool.lower_values.size(), -1);
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_title_scan");
#endif
        if (title.sorted_by_movie_id && !id_has_nulls) {
            const size_t row_count = static_cast<size_t>(title.row_count);
            for (int32_t movie_id : candidate_movies) {
                const int32_t* ptr =
                    std::lower_bound(id_values, id_values + row_count, movie_id);
                if (ptr == id_values + row_count || *ptr != movie_id) {
                    continue;
                }
                const size_t idx = static_cast<size_t>(ptr - id_values);
                if ((title_has_nulls && title_nulls[idx] != 0) ||
                    (kind_has_nulls && kind_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t kind_id = kind_values[idx];
                if (kind_id < 0 || static_cast<size_t>(kind_id) >= allowed_kind_ids.values.size() ||
                    allowed_kind_ids.values[static_cast<size_t>(kind_id)] == 0) {
                    continue;
                }
                const int32_t title_id = title_ids[idx];
                if (title_id < 0) {
                    continue;
                }
                int8_t match = match_cache[static_cast<size_t>(title_id)];
                if (match < 0) {
                    const auto& title_val = db.string_pool.lower_values[static_cast<size_t>(title_id)];
                    match = q3b_internal::ilike_match(title_pattern, title_val) ? 1 : 0;
                    match_cache[static_cast<size_t>(title_id)] = match;
                }
                if (match == 0) {
                    continue;
                }
                if (movie_title_map.values[static_cast<size_t>(movie_id)] == -1) {
                    ++movie_title_map.count;
                }
                movie_title_map.values[static_cast<size_t>(movie_id)] = title_id;
            }
        } else {
            movie_title_map =
                q3b_internal::build_movie_title_map_filtered(title, db.string_pool, title_pattern,
                                                             allowed_kind_ids, movie_in_candidates,
                                                             max_movie_id);
        }
#ifdef TRACE
        }
        trace.title_rows_scanned = static_cast<uint64_t>(candidate_movies.size());
        trace.title_rows_emitted = movie_title_map.count;
#endif
        if (movie_title_map.count == 0) {
            skip_query = true;
        }
    }

    std::vector<int32_t> final_movies;
    if (!skip_query) {
        final_movies.reserve(candidate_movies.size());
        for (int32_t movie_id : candidate_movies) {
            if (movie_title_map.values[static_cast<size_t>(movie_id)] != -1) {
                final_movies.push_back(movie_id);
            }
        }
        if (final_movies.empty()) {
            skip_query = true;
        }
    }

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    std::unordered_map<int32_t, std::unordered_map<int32_t, int64_t>> ci_by_movie;
    const size_t ci_reserve = std::min(static_cast<size_t>(cast_info.row_count),
                                       static_cast<size_t>(final_movies.size()));
    ci_by_movie.reserve(ci_reserve);
    std::vector<uint8_t> person_in_cast;
    std::vector<int32_t> candidate_person_ids;
    if (!skip_query) {
        const bool ci_movie_has_nulls = ci_movie_id.has_nulls;
        const bool ci_person_has_nulls = ci_person_id.has_nulls;
        const bool ci_role_has_nulls = ci_role_id.has_nulls;
        const auto* ci_movie_nulls = ci_movie_has_nulls ? ci_movie_id.is_null.data() : nullptr;
        const auto* ci_person_nulls = ci_person_has_nulls ? ci_person_id.is_null.data() : nullptr;
        const auto* ci_role_nulls = ci_role_has_nulls ? ci_role_id.is_null.data() : nullptr;
        const auto* ci_movie_values = ci_movie_id.i32.data();
        const auto* ci_person_values = ci_person_id.i32.data();
        const auto* ci_role_values = ci_role_id.i32.data();
        const auto* role_allowed = allowed_role_ids.values.data();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_cast_info_scan");
#endif
        if (cast_info.sorted_by_movie_id && !ci_movie_has_nulls &&
            !final_movies.empty()) {
            const size_t row_count = static_cast<size_t>(cast_info.row_count);
            size_t search_pos = 0;
            if (!ci_person_has_nulls && !ci_role_has_nulls) {
                for (int32_t movie_id : final_movies) {
                    if (search_pos >= row_count) {
                        break;
                    }
                    const int32_t* start_ptr =
                        std::lower_bound(ci_movie_values + search_pos,
                                         ci_movie_values + row_count, movie_id);
                    if (start_ptr == ci_movie_values + row_count) {
                        break;
                    }
                    if (*start_ptr != movie_id) {
                        search_pos = static_cast<size_t>(start_ptr - ci_movie_values);
                        continue;
                    }
                    size_t idx = static_cast<size_t>(start_ptr - ci_movie_values);
                    size_t end = idx + 1;
                    while (end < row_count && ci_movie_values[end] == movie_id) {
                        ++end;
                    }
                    for (size_t row = idx; row < end; ++row) {
#ifdef TRACE
                        ++trace.cast_info_rows_scanned;
#endif
                        const int32_t person_id = ci_person_values[row];
                        const int32_t role_id = ci_role_values[row];
                        if (role_allowed[static_cast<size_t>(role_id)] == 0) {
                            continue;
                        }
#ifdef TRACE
                        ++trace.cast_info_rows_emitted;
                        ++trace.cast_info_agg_rows_in;
                        auto& person_map = ci_by_movie[movie_id];
                        auto it_person = person_map.find(person_id);
                        if (it_person == person_map.end()) {
                            person_map.emplace(person_id, 1);
                            ++trace.cast_info_groups_created;
                        } else {
                            ++it_person->second;
                        }
#else
                        ++ci_by_movie[movie_id][person_id];
#endif
                        if (person_id >= 0) {
                            const size_t person_index = static_cast<size_t>(person_id);
                            if (person_index >= person_in_cast.size()) {
                                person_in_cast.resize(person_index + 1, 0);
                            }
                            if (person_in_cast[person_index] == 0) {
                                person_in_cast[person_index] = 1;
                                candidate_person_ids.push_back(person_id);
                            }
                        }
                    }
                    search_pos = end;
                }
            } else {
                for (int32_t movie_id : final_movies) {
                    if (search_pos >= row_count) {
                        break;
                    }
                    const int32_t* start_ptr =
                        std::lower_bound(ci_movie_values + search_pos,
                                         ci_movie_values + row_count, movie_id);
                    if (start_ptr == ci_movie_values + row_count) {
                        break;
                    }
                    if (*start_ptr != movie_id) {
                        search_pos = static_cast<size_t>(start_ptr - ci_movie_values);
                        continue;
                    }
                    size_t idx = static_cast<size_t>(start_ptr - ci_movie_values);
                    size_t end = idx + 1;
                    while (end < row_count && ci_movie_values[end] == movie_id) {
                        ++end;
                    }
                    for (size_t row = idx; row < end; ++row) {
#ifdef TRACE
                        ++trace.cast_info_rows_scanned;
#endif
                        if ((ci_person_has_nulls && ci_person_nulls[row] != 0) ||
                            (ci_role_has_nulls && ci_role_nulls[row] != 0)) {
                            continue;
                        }
                        const int32_t person_id = ci_person_values[row];
                        const int32_t role_id = ci_role_values[row];
                        if (role_allowed[static_cast<size_t>(role_id)] == 0) {
                            continue;
                        }
#ifdef TRACE
                        ++trace.cast_info_rows_emitted;
                        ++trace.cast_info_agg_rows_in;
                        auto& person_map = ci_by_movie[movie_id];
                        auto it_person = person_map.find(person_id);
                        if (it_person == person_map.end()) {
                            person_map.emplace(person_id, 1);
                            ++trace.cast_info_groups_created;
                        } else {
                            ++it_person->second;
                        }
#else
                        ++ci_by_movie[movie_id][person_id];
#endif
                        if (person_id >= 0) {
                            const size_t person_index = static_cast<size_t>(person_id);
                            if (person_index >= person_in_cast.size()) {
                                person_in_cast.resize(person_index + 1, 0);
                            }
                            if (person_in_cast[person_index] == 0) {
                                person_in_cast[person_index] = 1;
                                candidate_person_ids.push_back(person_id);
                            }
                        }
                    }
                    search_pos = end;
                }
            }
        } else {
            for (int64_t i = 0; i < cast_info.row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                if ((ci_movie_has_nulls && ci_movie_nulls[idx] != 0) ||
                    (ci_person_has_nulls && ci_person_nulls[idx] != 0) ||
                    (ci_role_has_nulls && ci_role_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t movie_id = ci_movie_values[idx];
                if (movie_id < 0 || movie_id > max_movie_id ||
                    movie_title_map.values[static_cast<size_t>(movie_id)] == -1) {
                    continue;
                }
                const int32_t person_id = ci_person_values[idx];
                const int32_t role_id = ci_role_values[idx];
                if (role_allowed[static_cast<size_t>(role_id)] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.cast_info_rows_emitted;
                ++trace.cast_info_agg_rows_in;
                auto& person_map = ci_by_movie[movie_id];
                auto it_person = person_map.find(person_id);
                if (it_person == person_map.end()) {
                    person_map.emplace(person_id, 1);
                    ++trace.cast_info_groups_created;
                } else {
                    ++it_person->second;
                }
#else
                ++ci_by_movie[movie_id][person_id];
#endif
                if (person_id >= 0) {
                    const size_t person_index = static_cast<size_t>(person_id);
                    if (person_index >= person_in_cast.size()) {
                        person_in_cast.resize(person_index + 1, 0);
                    }
                    if (person_in_cast[person_index] == 0) {
                        person_in_cast[person_index] = 1;
                        candidate_person_ids.push_back(person_id);
                    }
                }
            }
        }
#ifdef TRACE
        }
#endif
        if (ci_by_movie.empty()) {
            skip_query = true;
        }
    }

    const auto& name = db.tables.at("name");
    q3b_internal::DenseIdMap person_name_map;
    if (!skip_query) {
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_name_scan");
#endif
        const auto& id_col = name.columns.at("id");
        const auto& name_col = name.columns.at("name");
        const auto& pcode_col = name.columns.at("name_pcode_nf");
        const bool id_has_nulls = id_col.has_nulls;
        const bool name_has_nulls = name_col.has_nulls;
        const bool pcode_has_nulls = pcode_col.has_nulls;
        const auto* id_nulls = id_has_nulls ? id_col.is_null.data() : nullptr;
        const auto* name_nulls = name_has_nulls ? name_col.is_null.data() : nullptr;
        const auto* pcode_nulls = pcode_has_nulls ? pcode_col.is_null.data() : nullptr;
        const auto* id_values = id_col.i32.data();
        const auto* name_ids = name_col.str_ids.data();
        const auto* pcode_ids = pcode_col.str_ids.data();
        if (name.sorted_by_movie_id && !id_has_nulls) {
            person_name_map.values.assign(person_in_cast.size(), -1);
            std::vector<int8_t> match_cache(db.string_pool.lower_values.size(), -1);
            const size_t row_count = static_cast<size_t>(name.row_count);
            for (int32_t person_id : candidate_person_ids) {
                if (person_id < 0 || static_cast<size_t>(person_id) >= person_in_cast.size()) {
                    continue;
                }
                const int32_t* ptr =
                    std::lower_bound(id_values, id_values + row_count, person_id);
                if (ptr == id_values + row_count || *ptr != person_id) {
                    continue;
                }
                const size_t idx = static_cast<size_t>(ptr - id_values);
                if ((name_has_nulls && name_nulls[idx] != 0) ||
                    (pcode_has_nulls && pcode_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t pcode_id = pcode_ids[idx];
                if (pcode_id < 0) {
                    continue;
                }
                int8_t match = match_cache[static_cast<size_t>(pcode_id)];
                if (match < 0) {
                    const auto& pcode_val =
                        db.string_pool.lower_values[static_cast<size_t>(pcode_id)];
                    match = q3b_internal::ilike_match(name_pcode_pattern, pcode_val) ? 1 : 0;
                    match_cache[static_cast<size_t>(pcode_id)] = match;
                }
                if (match == 0) {
                    continue;
                }
                if (person_name_map.values[static_cast<size_t>(person_id)] == -1) {
                    ++person_name_map.count;
                }
                person_name_map.values[static_cast<size_t>(person_id)] = name_ids[idx];
            }
        } else {
            person_name_map = q3b_internal::build_person_name_map_filtered(
                name, db.string_pool, name_pcode_pattern, person_in_cast);
        }
#ifdef TRACE
        }
        trace.name_rows_scanned = static_cast<uint64_t>(name.row_count);
        trace.name_rows_emitted = person_name_map.count;
#endif
        if (person_name_map.count == 0) {
            skip_query = true;
        }
    }

    std::unordered_map<q3b_internal::ResultKey, int64_t, q3b_internal::ResultKeyHash> result_counts;
    if (!skip_query) {
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3b_join_aggregate");
#endif
        for (const auto& movie_entry : ci_by_movie) {
#ifdef TRACE
            ++trace.join_probe_rows_in;
#endif
            const int32_t movie_id = movie_entry.first;
            const int64_t mk_count = mk_counts[static_cast<size_t>(movie_id)];
            if (mk_count == 0) {
                continue;
            }
            const auto mc_it = mc_by_movie.find(movie_id);
            if (mc_it == mc_by_movie.end()) {
                continue;
            }
            const int32_t title_id = movie_title_map.values[static_cast<size_t>(movie_id)];
            if (title_id < 0) {
                continue;
            }
            const auto& person_map = movie_entry.second;
            const auto& company_map = mc_it->second;
            for (const auto& person_entry : person_map) {
                const int32_t person_id = person_entry.first;
                const int32_t person_name_id =
                    person_name_map.values[static_cast<size_t>(person_id)];
                if (person_name_id < 0) {
                    continue;
                }
                for (const auto& company_entry : company_map) {
                    const int32_t company_id = company_entry.first;
                    const int32_t company_name_id =
                        company_name_map.values[static_cast<size_t>(company_id)];
                    if (company_name_id < 0) {
                        continue;
                    }
                    const int64_t count =
                        mk_count * person_entry.second * company_entry.second;
                    q3b_internal::ResultKey key{title_id, person_name_id, company_name_id};
#ifdef TRACE
                    ++trace.join_rows_emitted;
                    ++trace.agg_rows_in;
                    auto it_result = result_counts.find(key);
                    if (it_result == result_counts.end()) {
                        result_counts.emplace(key, count);
                        ++trace.agg_groups_created;
                    } else {
                        it_result->second += count;
                    }
#else
                    result_counts[key] += count;
#endif
                }
            }
        }
#ifdef TRACE
        }
#endif
    }

    results.reserve(result_counts.size());
    for (const auto& entry : result_counts) {
        results.push_back(Q3bResultRow{entry.first.title_id, entry.first.person_name_id,
                                       entry.first.company_name_id, entry.second});
    }

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q3b_sort");
        trace.sort_rows_in = results.size();
#endif
    std::sort(results.begin(), results.end(),
              [&db](const Q3bResultRow& a, const Q3bResultRow& b) {
                  if (a.count != b.count) {
                      return a.count > b.count;
                  }
                  const auto& at = db.string_pool.get(a.title_id);
                  const auto& bt = db.string_pool.get(b.title_id);
                  if (at != bt) {
                      return at < bt;
                  }
                  const auto& an = db.string_pool.get(a.person_name_id);
                  const auto& bn = db.string_pool.get(b.person_name_id);
                  if (an != bn) {
                      return an < bn;
                  }
                  const auto& ac = db.string_pool.get(a.company_name_id);
                  const auto& bc = db.string_pool.get(b.company_name_id);
                  return ac < bc;
              });
#ifdef TRACE
        trace.sort_rows_out = results.size();
    }
    uint64_t mc_group_count = 0;
    for (const auto& entry : mc_by_movie) {
        mc_group_count += entry.second.size();
    }
    uint64_t ci_group_count = 0;
    for (const auto& entry : ci_by_movie) {
        ci_group_count += entry.second.size();
    }
    trace.movie_companies_agg_rows_emitted = mc_group_count;
    trace.cast_info_agg_rows_emitted = ci_group_count;
    trace.join_build_rows_in = static_cast<uint64_t>(mk_nonzero + mc_group_count +
                                                     ci_group_count + movie_title_map.count +
                                                     person_name_map.count +
                                                     company_name_map.count);
    trace.agg_rows_emitted = result_counts.size();
    trace.query_output_rows = results.size();
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q3b_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q3b_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q3b_company_name_scan_rows_scanned", trace.company_rows_scanned);
    print_count("q3b_company_name_scan_rows_emitted", trace.company_rows_emitted);
    print_count("q3b_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q3b_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q3b_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q3b_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q3b_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q3b_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q3b_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q3b_movie_companies_scan_rows_scanned", trace.movie_companies_rows_scanned);
    print_count("q3b_movie_companies_scan_rows_emitted", trace.movie_companies_rows_emitted);
    print_count("q3b_movie_companies_agg_rows_in", trace.movie_companies_agg_rows_in);
    print_count("q3b_movie_companies_groups_created", trace.movie_companies_groups_created);
    print_count("q3b_movie_companies_agg_rows_emitted", trace.movie_companies_agg_rows_emitted);
    print_count("q3b_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q3b_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q3b_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q3b_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q3b_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q3b_join_build_rows_in", trace.join_build_rows_in);
    print_count("q3b_join_probe_rows_in", trace.join_probe_rows_in);
    print_count("q3b_join_rows_emitted", trace.join_rows_emitted);
    print_count("q3b_agg_rows_in", trace.agg_rows_in);
    print_count("q3b_agg_groups_created", trace.agg_groups_created);
    print_count("q3b_agg_rows_emitted", trace.agg_rows_emitted);
    print_count("q3b_sort_rows_in", trace.sort_rows_in);
    print_count("q3b_sort_rows_out", trace.sort_rows_out);
    print_count("q3b_query_output_rows", trace.query_output_rows);
#endif
    return results;
}