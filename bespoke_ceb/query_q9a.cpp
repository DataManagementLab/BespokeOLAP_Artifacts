#include "query_q9a.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

namespace q9a_internal {

struct IntFilter {
    std::vector<int32_t> values;
    bool single = false;
    int32_t single_value = 0;
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

bool like_match_lower(const std::string& pattern, const std::string& text) {
    size_t p = 0;
    size_t t = 0;
    size_t star = std::string::npos;
    size_t match = 0;
    while (t < text.size()) {
        const char text_char = text[t];
        if (p < pattern.size() && (pattern[p] == '_' || pattern[p] == text_char)) {
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

bool contains_lower(const std::string& haystack_lower, const std::string& needle_lower) {
    return haystack_lower.find(needle_lower) != std::string::npos;
}

struct CompiledLikePattern {
    std::string pattern;
    std::string needle;
    enum class Mode { Like, Contains, Prefix, Suffix, Equals };
    Mode mode = Mode::Like;
};

CompiledLikePattern compile_ilike_pattern(const std::string& raw_pattern) {
    CompiledLikePattern compiled;
    compiled.pattern = to_lower(strip_single_quotes(raw_pattern));
    const bool has_underscore = compiled.pattern.find('_') != std::string::npos;
    if (!has_underscore) {
        const size_t first_percent = compiled.pattern.find('%');
        if (first_percent == std::string::npos) {
            compiled.needle = compiled.pattern;
            compiled.mode = CompiledLikePattern::Mode::Equals;
            return compiled;
        }
        const size_t last_percent = compiled.pattern.rfind('%');
        if (first_percent == last_percent) {
            if (first_percent == 0) {
                compiled.needle = compiled.pattern.substr(1);
                compiled.mode = CompiledLikePattern::Mode::Suffix;
                return compiled;
            }
            if (first_percent == compiled.pattern.size() - 1) {
                compiled.needle = compiled.pattern.substr(0, compiled.pattern.size() - 1);
                compiled.mode = CompiledLikePattern::Mode::Prefix;
                return compiled;
            }
        }
        if (first_percent == 0 && last_percent == compiled.pattern.size() - 1) {
            const size_t inner_pos = compiled.pattern.find('%', 1);
            if (inner_pos == compiled.pattern.size() - 1) {
                compiled.needle = compiled.pattern.substr(1, compiled.pattern.size() - 2);
                compiled.mode = CompiledLikePattern::Mode::Contains;
                return compiled;
            }
        }
    }
    return compiled;
}

bool ilike_match(const CompiledLikePattern& compiled, const std::string& value_lower) {
    switch (compiled.mode) {
        case CompiledLikePattern::Mode::Contains:
            return contains_lower(value_lower, compiled.needle);
        case CompiledLikePattern::Mode::Prefix:
            return value_lower.size() >= compiled.needle.size() &&
                   value_lower.compare(0, compiled.needle.size(), compiled.needle) == 0;
        case CompiledLikePattern::Mode::Suffix:
            return value_lower.size() >= compiled.needle.size() &&
                   value_lower.compare(value_lower.size() - compiled.needle.size(),
                                       compiled.needle.size(), compiled.needle) == 0;
        case CompiledLikePattern::Mode::Equals:
            return value_lower == compiled.needle;
        case CompiledLikePattern::Mode::Like:
            break;
    }
    return like_match_lower(compiled.pattern, value_lower);
}

IntFilter build_int_filter(const std::vector<std::string>& values) {
    IntFilter filter;
    filter.values.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        try {
            filter.values.push_back(static_cast<int32_t>(std::stoi(val)));
        } catch (const std::exception&) {
            continue;
        }
    }
    if (filter.values.size() == 1) {
        filter.single = true;
        filter.single_value = filter.values.front();
    }
    return filter;
}

bool matches_int_filter(const IntFilter& filter, int32_t value) {
    if (filter.values.empty()) {
        return false;
    }
    if (filter.single) {
        return value == filter.single_value;
    }
    return std::find(filter.values.begin(), filter.values.end(), value) != filter.values.end();
}

std::vector<uint8_t> build_int_filter_flags_from_dim(const TableData& table,
                                                     const std::string& id_col_name,
                                                     const IntFilter& filter) {
    if (filter.values.empty()) {
        return {};
    }
    const auto& id_col = table.columns.at(id_col_name);
    const int32_t* id_data = id_col.i32.data();
    const uint8_t* id_nulls = id_col.is_null.data();
    const bool has_nulls = id_col.has_nulls;
    int32_t max_id = 0;
    const size_t row_count = static_cast<size_t>(table.row_count);
    if (!has_nulls) {
        for (size_t i = 0; i < row_count; ++i) {
            max_id = std::max(max_id, id_data[i]);
        }
    } else {
        for (size_t i = 0; i < row_count; ++i) {
            if (id_nulls[i] != 0) {
                continue;
            }
            max_id = std::max(max_id, id_data[i]);
        }
    }
    std::vector<uint8_t> flags(static_cast<size_t>(max_id) + 1, 0);
    for (int32_t value : filter.values) {
        if (value < 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(value);
        if (idx < flags.size()) {
            flags[idx] = 1;
        }
    }
    return flags;
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

std::vector<uint8_t> build_allowed_id_flags_from_dim(const TableData& table,
                                                     const std::string& id_col_name,
                                                     const std::string& str_col_name,
                                                     const StringFilter& filter) {
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    int32_t max_id = 0;
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        max_id = std::max(max_id, id_col.i32[static_cast<size_t>(i)]);
    }
    std::vector<uint8_t> flags(static_cast<size_t>(max_id) + 1, 0);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0 || str_col.is_null[i] != 0) {
            continue;
        }
        if (filter.values.find(str_col.str_ids[static_cast<size_t>(i)]) != filter.values.end()) {
            const int32_t id = id_col.i32[static_cast<size_t>(i)];
            if (id >= 0) {
                flags[static_cast<size_t>(id)] = 1;
            }
        }
    }
    return flags;
}

bool any_flag_set(const std::vector<uint8_t>& flags) {
    for (const auto value : flags) {
        if (value != 0) {
            return true;
        }
    }
    return false;
}

struct InfoCount {
    int32_t info_id{};
    int32_t count{};
};

void add_info_count(std::vector<InfoCount>& entries, int32_t info_id) {
    for (auto& entry : entries) {
        if (entry.info_id == info_id) {
            ++entry.count;
            return;
        }
    }
    entries.push_back({info_id, 1});
}

struct ResultKey {
    int32_t info1_id{};
    int32_t info2_id{};

    bool operator==(const ResultKey& other) const {
        return info1_id == other.info1_id && info2_id == other.info2_id;
    }
};

struct ResultKeyHash {
    size_t operator()(const ResultKey& key) const {
        const size_t h1 = std::hash<int32_t>{}(key.info1_id);
        const size_t h2 = std::hash<int32_t>{}(key.info2_id);
        return h1 ^ (h2 << 1);
    }
};

}  // namespace q9a_internal

#ifdef TRACE
struct Q9aTrace {
    TraceRecorder recorder;
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t movie_info_rows_scanned = 0;
    uint64_t movie_info_rows_emitted = 0;
    uint64_t movie_info_agg_rows_in = 0;
    uint64_t movie_info_groups_created = 0;
    uint64_t movie_info_agg_rows_emitted = 0;
    uint64_t person_info_rows_scanned = 0;
    uint64_t person_info_rows_emitted = 0;
    uint64_t person_info_agg_rows_in = 0;
    uint64_t person_info_groups_created = 0;
    uint64_t person_info_agg_rows_emitted = 0;
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

std::vector<Q9aResultRow> run_q9a(const Database& db, const Q9aArgs& args) {
    std::vector<Q9aResultRow> results;
#ifdef TRACE
    Q9aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q9a_total");
#endif
    const auto info_type_filter1 = q9a_internal::build_int_filter(args.ID1);
    const auto info_type_filter2 = q9a_internal::build_int_filter(args.ID2);
    if (info_type_filter1.values.empty() || info_type_filter2.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& kind_type = db.tables.at("kind_type");
    const auto& info_type = db.tables.at("info_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& title = db.tables.at("title");
    const auto& movie_info = db.tables.at("movie_info");
    const auto& person_info = db.tables.at("person_info");
    const auto& cast_info = db.tables.at("cast_info");

    const auto kind_filter = q9a_internal::build_string_filter_for_column(
        db.string_pool, kind_type.columns.at("kind"), args.KIND);
    const auto role_filter = q9a_internal::build_string_filter_for_column(
        db.string_pool, role_type.columns.at("role"), args.ROLE);

    const auto kind_flags =
        q9a_internal::build_allowed_id_flags_from_dim(kind_type, "id", "kind", kind_filter);
    if (!q9a_internal::any_flag_set(kind_flags) || role_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    const auto role_flags =
        q9a_internal::build_allowed_id_flags_from_dim(role_type, "id", "role", role_filter);
    if (!q9a_internal::any_flag_set(role_flags)) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto info_type_flags1 =
        q9a_internal::build_int_filter_flags_from_dim(info_type, "id", info_type_filter1);
    const auto info_type_flags2 =
        q9a_internal::build_int_filter_flags_from_dim(info_type, "id", info_type_filter2);
    if (!q9a_internal::any_flag_set(info_type_flags1) ||
        !q9a_internal::any_flag_set(info_type_flags2)) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& title_id_col = title.columns.at("id");
    const auto& title_kind_col = title.columns.at("kind_id");
    const int32_t title_max_id = title_id_col.i32_max;
    const int32_t cast_info_movie_max = cast_info.columns.at("movie_id").i32_max;
    const int32_t movie_id_max = std::max(title_max_id, cast_info_movie_max);
    std::vector<uint8_t> allowed_movie_flags(
        movie_id_max >= 0 ? static_cast<size_t>(movie_id_max) + 1 : 1, 0);
    const int32_t max_movie_id = movie_id_max;
    const bool title_has_nulls = title_id_col.has_nulls || title_kind_col.has_nulls;
    const int32_t* title_id_data = title_id_col.i32.data();
    const int32_t* title_kind_data = title_kind_col.i32.data();
    const uint8_t* title_id_nulls = title_id_col.is_null.data();
    const uint8_t* title_kind_nulls = title_kind_col.is_null.data();
    const size_t title_row_count = static_cast<size_t>(title.row_count);
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q9a_title_scan");
        if (!skip_query) {
#endif
    if (!title_has_nulls) {
        for (size_t idx = 0; idx < title_row_count; ++idx) {
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t movie_id = title_id_data[idx];
            const int32_t kind_id = title_kind_data[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_flags.size() ||
                kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            allowed_movie_flags[static_cast<size_t>(movie_id)] = 1;
#ifdef TRACE
            ++trace.title_rows_emitted;
#endif
        }
    } else {
        for (size_t idx = 0; idx < title_row_count; ++idx) {
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if (title_id_nulls[idx] != 0 || title_kind_nulls[idx] != 0) {
                continue;
            }
            const int32_t movie_id = title_id_data[idx];
            const int32_t kind_id = title_kind_data[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_flags.size() ||
                kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            allowed_movie_flags[static_cast<size_t>(movie_id)] = 1;
#ifdef TRACE
            ++trace.title_rows_emitted;
#endif
        }
    }
#ifdef TRACE
        }
    }
#endif
    if (!q9a_internal::any_flag_set(allowed_movie_flags)) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto info1_pattern = q9a_internal::compile_ilike_pattern(args.INFO1);
    const auto info2_pattern = q9a_internal::compile_ilike_pattern(args.INFO2);

    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const bool mi_has_nulls =
        mi_movie_id.has_nulls || mi_info_type_id.has_nulls || mi_info.has_nulls;
    const int32_t* mi_movie_data = mi_movie_id.i32.data();
    const int32_t* mi_info_type_data = mi_info_type_id.i32.data();
    const int32_t* mi_info_data = mi_info.str_ids.data();
    const uint8_t* mi_movie_nulls = mi_movie_id.is_null.data();
    const uint8_t* mi_info_type_nulls = mi_info_type_id.is_null.data();
    const uint8_t* mi_info_nulls = mi_info.is_null.data();
    const uint8_t* info_type_flags1_data = info_type_flags1.data();
    const size_t movie_info_row_count = static_cast<size_t>(movie_info.row_count);

    std::vector<std::vector<q9a_internal::InfoCount>> movie_info_counts;
    movie_info_counts.resize(static_cast<size_t>(max_movie_id) + 1);
    std::vector<uint8_t> movie_info_has_entries(movie_info_counts.size(), 0);
    uint64_t movie_info_nonempty = 0;
    const size_t pool_size = db.string_pool.values.size();
    std::vector<int8_t> info1_match_cache(pool_size, -1);
    std::vector<int8_t> info2_match_cache(pool_size, -1);
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9a_movie_info_scan");
#endif
    if (!mi_has_nulls) {
        for (size_t idx = 0; idx < movie_info_row_count; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            const int32_t info_type_id = mi_info_type_data[idx];
            if (__builtin_expect(
                    info_type_flags1_data[static_cast<size_t>(info_type_id)] == 0, 1)) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            int8_t cached_match = info1_match_cache[static_cast<size_t>(info_id)];
            if (cached_match == -1) {
                const auto& info_value_lower = db.string_pool.get_lower(info_id);
                cached_match = q9a_internal::ilike_match(info1_pattern, info_value_lower) ? 1 : 0;
                info1_match_cache[static_cast<size_t>(info_id)] = cached_match;
            }
            if (__builtin_expect(cached_match == 0, 1)) {
                continue;
            }
            const int32_t movie_id = mi_movie_data[idx];
            if (__builtin_expect(
                    allowed_movie_flags[static_cast<size_t>(movie_id)] == 0, 1)) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
            auto& entries = movie_info_counts[static_cast<size_t>(movie_id)];
            if (entries.empty()) {
                movie_info_has_entries[static_cast<size_t>(movie_id)] = 1;
                ++trace.movie_info_groups_created;
                ++movie_info_nonempty;
            }
            q9a_internal::add_info_count(entries, info_id);
#else
            auto& entries = movie_info_counts[static_cast<size_t>(movie_id)];
            if (entries.empty()) {
                movie_info_has_entries[static_cast<size_t>(movie_id)] = 1;
                ++movie_info_nonempty;
            }
            q9a_internal::add_info_count(entries, info_id);
#endif
        }
    } else {
        for (size_t idx = 0; idx < movie_info_row_count; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if (mi_movie_nulls[idx] != 0 || mi_info_type_nulls[idx] != 0 ||
                mi_info_nulls[idx] != 0) {
                continue;
            }
            const int32_t movie_id = mi_movie_data[idx];
            const int32_t info_type_id = mi_info_type_data[idx];
            if (__builtin_expect(
                    info_type_flags1_data[static_cast<size_t>(info_type_id)] == 0, 1)) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            int8_t cached_match = info1_match_cache[static_cast<size_t>(info_id)];
            if (cached_match == -1) {
                const auto& info_value_lower = db.string_pool.get_lower(info_id);
                cached_match = q9a_internal::ilike_match(info1_pattern, info_value_lower) ? 1 : 0;
                info1_match_cache[static_cast<size_t>(info_id)] = cached_match;
            }
            if (__builtin_expect(cached_match == 0, 1)) {
                continue;
            }
            if (__builtin_expect(
                    allowed_movie_flags[static_cast<size_t>(movie_id)] == 0, 1)) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
            auto& entries = movie_info_counts[static_cast<size_t>(movie_id)];
            if (entries.empty()) {
                movie_info_has_entries[static_cast<size_t>(movie_id)] = 1;
                ++trace.movie_info_groups_created;
                ++movie_info_nonempty;
            }
            q9a_internal::add_info_count(entries, info_id);
#else
            auto& entries = movie_info_counts[static_cast<size_t>(movie_id)];
            if (entries.empty()) {
                movie_info_has_entries[static_cast<size_t>(movie_id)] = 1;
                ++movie_info_nonempty;
            }
            q9a_internal::add_info_count(entries, info_id);
#endif
        }
    }
#ifdef TRACE
    }
#endif
    if (movie_info_nonempty == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& pi_person_id = person_info.columns.at("person_id");
    const auto& pi_info_type_id = person_info.columns.at("info_type_id");
    const auto& pi_info = person_info.columns.at("info");
    const bool pi_has_nulls =
        pi_person_id.has_nulls || pi_info_type_id.has_nulls || pi_info.has_nulls;
    const int32_t* pi_person_data = pi_person_id.i32.data();
    const int32_t* pi_info_type_data = pi_info_type_id.i32.data();
    const int32_t* pi_info_data = pi_info.str_ids.data();
    const uint8_t* pi_person_nulls = pi_person_id.is_null.data();
    const uint8_t* pi_info_type_nulls = pi_info_type_id.is_null.data();
    const uint8_t* pi_info_nulls = pi_info.is_null.data();
    const uint8_t* info_type_flags2_data = info_type_flags2.data();
    const size_t person_info_row_count = static_cast<size_t>(person_info.row_count);

    const int32_t person_info_max_id = pi_person_id.i32_max;
    const int32_t cast_info_person_max_id = cast_info.columns.at("person_id").i32_max;
    const int32_t person_id_max = std::max(person_info_max_id, cast_info_person_max_id);
    const size_t person_id_size = person_id_max >= 0 ? static_cast<size_t>(person_id_max) + 1 : 1;
    std::vector<std::vector<q9a_internal::InfoCount>> person_info_counts(person_id_size);
    std::vector<uint8_t> person_info_has_entries(person_id_size, 0);
    uint64_t person_info_nonempty = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9a_person_info_scan");
#endif
    if (!pi_has_nulls) {
        for (size_t idx = 0; idx < person_info_row_count; ++idx) {
#ifdef TRACE
            ++trace.person_info_rows_scanned;
#endif
            const int32_t info_type_id = pi_info_type_data[idx];
            if (__builtin_expect(
                    info_type_flags2_data[static_cast<size_t>(info_type_id)] == 0, 1)) {
                continue;
            }
            const int32_t info_id = pi_info_data[idx];
            int8_t cached_match = info2_match_cache[static_cast<size_t>(info_id)];
            if (cached_match == -1) {
                const auto& info_value_lower = db.string_pool.get_lower(info_id);
                cached_match = q9a_internal::ilike_match(info2_pattern, info_value_lower) ? 1 : 0;
                info2_match_cache[static_cast<size_t>(info_id)] = cached_match;
            }
            if (__builtin_expect(cached_match == 0, 1)) {
                continue;
            }
#ifdef TRACE
            ++trace.person_info_rows_emitted;
            ++trace.person_info_agg_rows_in;
            const int32_t person_id = pi_person_data[idx];
            if (person_id >= 0) {
                auto& entries = person_info_counts[static_cast<size_t>(person_id)];
                if (entries.empty()) {
                    person_info_has_entries[static_cast<size_t>(person_id)] = 1;
                    ++trace.person_info_groups_created;
                    ++person_info_nonempty;
                }
                q9a_internal::add_info_count(entries, info_id);
            }
#else
            const int32_t person_id = pi_person_data[idx];
            if (person_id >= 0) {
                auto& entries = person_info_counts[static_cast<size_t>(person_id)];
                if (entries.empty()) {
                    person_info_has_entries[static_cast<size_t>(person_id)] = 1;
                    ++person_info_nonempty;
                }
                q9a_internal::add_info_count(entries, info_id);
            }
#endif
        }
    } else {
        for (size_t idx = 0; idx < person_info_row_count; ++idx) {
#ifdef TRACE
            ++trace.person_info_rows_scanned;
#endif
            if (pi_person_nulls[idx] != 0 || pi_info_type_nulls[idx] != 0 ||
                pi_info_nulls[idx] != 0) {
                continue;
            }
            const int32_t info_type_id = pi_info_type_data[idx];
            if (__builtin_expect(
                    info_type_flags2_data[static_cast<size_t>(info_type_id)] == 0, 1)) {
                continue;
            }
            const int32_t info_id = pi_info_data[idx];
            int8_t cached_match = info2_match_cache[static_cast<size_t>(info_id)];
            if (cached_match == -1) {
                const auto& info_value_lower = db.string_pool.get_lower(info_id);
                cached_match = q9a_internal::ilike_match(info2_pattern, info_value_lower) ? 1 : 0;
                info2_match_cache[static_cast<size_t>(info_id)] = cached_match;
            }
            if (__builtin_expect(cached_match == 0, 1)) {
                continue;
            }
#ifdef TRACE
            ++trace.person_info_rows_emitted;
            ++trace.person_info_agg_rows_in;
            const int32_t person_id = pi_person_data[idx];
            if (person_id >= 0) {
                auto& entries = person_info_counts[static_cast<size_t>(person_id)];
                if (entries.empty()) {
                    person_info_has_entries[static_cast<size_t>(person_id)] = 1;
                    ++trace.person_info_groups_created;
                    ++person_info_nonempty;
                }
                q9a_internal::add_info_count(entries, info_id);
            }
#else
            const int32_t person_id = pi_person_data[idx];
            if (person_id >= 0) {
                auto& entries = person_info_counts[static_cast<size_t>(person_id)];
                if (entries.empty()) {
                    person_info_has_entries[static_cast<size_t>(person_id)] = 1;
                    ++person_info_nonempty;
                }
                q9a_internal::add_info_count(entries, info_id);
            }
#endif
        }
    }
#ifdef TRACE
    }
#endif
    if (person_info_nonempty == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const bool ci_has_nulls =
        ci_movie_id.has_nulls || ci_person_id.has_nulls || ci_role_id.has_nulls;
    const int32_t* ci_movie_data = ci_movie_id.i32.data();
    const int32_t* ci_person_data = ci_person_id.i32.data();
    const int32_t* ci_role_data = ci_role_id.i32.data();
    const uint8_t* ci_movie_nulls = ci_movie_id.is_null.data();
    const uint8_t* ci_person_nulls = ci_person_id.is_null.data();
    const uint8_t* ci_role_nulls = ci_role_id.is_null.data();
    const uint8_t* role_flags_data = role_flags.data();
    const size_t cast_info_row_count = static_cast<size_t>(cast_info.row_count);

    std::vector<uint8_t> movie_cast_flags(movie_info_has_entries.size(), 0);
    const size_t movie_cast_size = movie_cast_flags.size();
    for (size_t i = 0; i < movie_cast_size; ++i) {
        movie_cast_flags[i] =
            static_cast<uint8_t>(movie_info_has_entries[i] & allowed_movie_flags[i]);
    }

    std::unordered_map<q9a_internal::ResultKey, int64_t, q9a_internal::ResultKeyHash>
        result_counts;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9a_cast_info_scan");
#endif
    if (!ci_has_nulls) {
#ifndef TRACE
        for (size_t idx = 0; idx < cast_info_row_count; ++idx) {
            const int32_t movie_id = ci_movie_data[idx];
            if (__builtin_expect(
                    movie_cast_flags[static_cast<size_t>(movie_id)] == 0, 1)) {
                continue;
            }
            const int32_t role_id = ci_role_data[idx];
            if (__builtin_expect(
                    role_flags_data[static_cast<size_t>(role_id)] == 0, 1)) {
                continue;
            }
            const int32_t person_id = ci_person_data[idx];
            if (__builtin_expect(
                    person_info_has_entries[static_cast<size_t>(person_id)] == 0, 1)) {
                continue;
            }
            const auto& movie_entries = movie_info_counts[static_cast<size_t>(movie_id)];
            const auto& person_entries = person_info_counts[static_cast<size_t>(person_id)];
            for (const auto& movie_entry : movie_entries) {
                const int32_t info1_id = movie_entry.info_id;
                const int32_t info1_count = movie_entry.count;
                for (const auto& person_entry : person_entries) {
                    const int32_t info2_id = person_entry.info_id;
                    const int32_t info2_count = person_entry.count;
                    q9a_internal::ResultKey result_key{info1_id, info2_id};
                    result_counts[result_key] +=
                        static_cast<int64_t>(info1_count) * info2_count;
                }
            }
        }
#else
        for (size_t idx = 0; idx < cast_info_row_count; ++idx) {
            ++trace.cast_info_rows_scanned;
            const int32_t role_id = ci_role_data[idx];
            const int32_t person_id = ci_person_data[idx];
            const int32_t movie_id = ci_movie_data[idx];
            const uint8_t match =
                static_cast<uint8_t>(role_flags_data[static_cast<size_t>(role_id)] &
                                     person_info_has_entries[static_cast<size_t>(person_id)] &
                                     movie_cast_flags[static_cast<size_t>(movie_id)]);
            if (__builtin_expect(match == 0, 1)) {
                continue;
            }
            const auto& movie_entries = movie_info_counts[static_cast<size_t>(movie_id)];
            const auto& person_entries = person_info_counts[static_cast<size_t>(person_id)];
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            ++trace.result_join_probe_rows_in;
            for (const auto& movie_entry : movie_entries) {
                const int32_t info1_id = movie_entry.info_id;
                const int32_t info1_count = movie_entry.count;
                for (const auto& person_entry : person_entries) {
                    const int32_t info2_id = person_entry.info_id;
                    const int32_t info2_count = person_entry.count;
                    q9a_internal::ResultKey result_key{info1_id, info2_id};
                    ++trace.result_join_rows_emitted;
                    ++trace.result_agg_rows_in;
                    auto it_result = result_counts.find(result_key);
                    if (it_result == result_counts.end()) {
                        result_counts.emplace(result_key,
                                              static_cast<int64_t>(info1_count) * info2_count);
                        ++trace.result_groups_created;
                    } else {
                        it_result->second += static_cast<int64_t>(info1_count) * info2_count;
                    }
                }
            }
        }
#endif
    } else {
        for (size_t idx = 0; idx < cast_info_row_count; ++idx) {
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            if (ci_movie_nulls[idx] != 0 || ci_person_nulls[idx] != 0 ||
                ci_role_nulls[idx] != 0) {
                continue;
            }
            const int32_t role_id = ci_role_data[idx];
            const int32_t person_id = ci_person_data[idx];
            const int32_t movie_id = ci_movie_data[idx];
            const uint8_t match =
                static_cast<uint8_t>(role_flags_data[static_cast<size_t>(role_id)] &
                                     person_info_has_entries[static_cast<size_t>(person_id)] &
                                     movie_cast_flags[static_cast<size_t>(movie_id)]);
            if (__builtin_expect(match == 0, 1)) {
                continue;
            }
            const auto& movie_entries = movie_info_counts[static_cast<size_t>(movie_id)];
            const auto& person_entries = person_info_counts[static_cast<size_t>(person_id)];
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            ++trace.result_join_probe_rows_in;
#endif
            for (const auto& movie_entry : movie_entries) {
                const int32_t info1_id = movie_entry.info_id;
                const int32_t info1_count = movie_entry.count;
                for (const auto& person_entry : person_entries) {
                    const int32_t info2_id = person_entry.info_id;
                    const int32_t info2_count = person_entry.count;
                    q9a_internal::ResultKey result_key{info1_id, info2_id};
#ifdef TRACE
                    ++trace.result_join_rows_emitted;
                    ++trace.result_agg_rows_in;
                    auto it_result = result_counts.find(result_key);
                    if (it_result == result_counts.end()) {
                        result_counts.emplace(result_key,
                                              static_cast<int64_t>(info1_count) * info2_count);
                        ++trace.result_groups_created;
                    } else {
                        it_result->second += static_cast<int64_t>(info1_count) * info2_count;
                    }
#else
                    result_counts[result_key] +=
                        static_cast<int64_t>(info1_count) * info2_count;
#endif
                }
            }
        }
    }
#ifdef TRACE
    }
#endif

    results.reserve(result_counts.size());
    for (const auto& entry : result_counts) {
        results.push_back(Q9aResultRow{entry.first.info1_id, entry.first.info2_id, entry.second});
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q9a_sort");
        trace.sort_rows_in = results.size();
#endif
    std::sort(results.begin(), results.end(),
              [](const Q9aResultRow& a, const Q9aResultRow& b) {
                  if (a.movie_info_id != b.movie_info_id) {
                      return a.movie_info_id < b.movie_info_id;
                  }
                  return a.person_info_id < b.person_info_id;
              });
#ifdef TRACE
        trace.sort_rows_out = results.size();
    }
    trace.movie_info_agg_rows_emitted = movie_info_nonempty;
    trace.person_info_agg_rows_emitted = person_info_nonempty;
    trace.cast_info_agg_rows_emitted = trace.cast_info_agg_rows_in;
    trace.result_agg_rows_emitted = result_counts.size();
    trace.result_join_build_rows_in =
        static_cast<uint64_t>(movie_info_nonempty + person_info_nonempty);
    trace.query_output_rows = results.size();
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q9a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q9a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q9a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q9a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q9a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q9a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q9a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q9a_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q9a_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q9a_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q9a_person_info_groups_created", trace.person_info_groups_created);
    print_count("q9a_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q9a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q9a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q9a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q9a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q9a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q9a_result_join_build_rows_in", trace.result_join_build_rows_in);
    print_count("q9a_result_join_probe_rows_in", trace.result_join_probe_rows_in);
    print_count("q9a_result_join_rows_emitted", trace.result_join_rows_emitted);
    print_count("q9a_result_agg_rows_in", trace.result_agg_rows_in);
    print_count("q9a_result_groups_created", trace.result_groups_created);
    print_count("q9a_result_agg_rows_emitted", trace.result_agg_rows_emitted);
    print_count("q9a_sort_rows_in", trace.sort_rows_in);
    print_count("q9a_sort_rows_out", trace.sort_rows_out);
    print_count("q9a_query_output_rows", trace.query_output_rows);
#endif
    return results;
}