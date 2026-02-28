#include "query_q9b.hpp"

#include <algorithm>
#include <cctype>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "trace.hpp"

namespace q9b_internal {

struct IntFilter {
    std::vector<int32_t> values;
    bool single = false;
    int32_t single_value = 0;
};

struct StringFilter {
    std::unordered_set<int32_t> values;
    bool single = false;
    int32_t single_value = 0;
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

bool contains_in_lower(const std::string& haystack, const std::string& needle) {
    if (needle.empty()) {
        return true;
    }
    return haystack.find(needle) != std::string::npos;
}

struct CompiledLikePattern {
    std::string pattern;
    std::string needle;
    bool simple_contains = false;
};

CompiledLikePattern compile_ilike_pattern(const std::string& raw_pattern) {
    CompiledLikePattern compiled;
    compiled.pattern = to_lower(strip_single_quotes(raw_pattern));
    const bool has_underscore = compiled.pattern.find('_') != std::string::npos;
    if (!has_underscore && compiled.pattern.size() >= 2 &&
        compiled.pattern.front() == '%' && compiled.pattern.back() == '%') {
        const size_t inner_pos = compiled.pattern.find('%', 1);
        if (inner_pos == compiled.pattern.size() - 1) {
            compiled.needle = compiled.pattern.substr(1, compiled.pattern.size() - 2);
            compiled.simple_contains = true;
        }
    }
    return compiled;
}

bool ilike_match(const CompiledLikePattern& compiled, const std::string& value) {
    if (compiled.simple_contains) {
        return contains_in_lower(value, compiled.needle);
    }
    return like_match_lower(compiled.pattern, value);
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
    if (filter.values.size() == 1) {
        filter.single = true;
        filter.single_value = *filter.values.begin();
    }
    return filter;
}

bool matches_string_filter(const StringFilter& filter, int32_t value) {
    if (filter.values.empty()) {
        return false;
    }
    if (filter.single) {
        return value == filter.single_value;
    }
    return filter.values.find(value) != filter.values.end();
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
    int32_t info_id{};
    int32_t name_id{};

    bool operator==(const ResultKey& other) const {
        return info_id == other.info_id && name_id == other.name_id;
    }
};

struct ResultKeyHash {
    size_t operator()(const ResultKey& key) const {
        const size_t h1 = std::hash<int32_t>{}(key.info_id);
        const size_t h2 = std::hash<int32_t>{}(key.name_id);
        return h1 ^ (h2 << 1);
    }
};

}  // namespace q9b_internal

#ifdef TRACE
struct Q9bTrace {
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

std::vector<Q9bResultRow> run_q9b(const Database& db, const Q9bArgs& args) {
    std::vector<Q9bResultRow> results;
#ifdef TRACE
    Q9bTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q9b_total");
#endif
    const auto info_type_filter1 = q9b_internal::build_int_filter(args.ID1);
    const auto info_type_filter2 = q9b_internal::build_int_filter(args.ID2);
    if (info_type_filter1.values.empty() || info_type_filter2.values.empty()) {
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
    const auto& person_info = db.tables.at("person_info");
    const auto& cast_info = db.tables.at("cast_info");
    const auto& name = db.tables.at("name");

    const auto kind_filter = q9b_internal::build_string_filter_for_column(
        db.string_pool, kind_type.columns.at("kind"), args.KIND);
    const auto role_filter = q9b_internal::build_string_filter_for_column(
        db.string_pool, role_type.columns.at("role"), args.ROLE);
    const auto info_filter = q9b_internal::build_string_filter_for_column(
        db.string_pool, movie_info.columns.at("info"), args.INFO);
    std::vector<int32_t> info_filter_ids;
    info_filter_ids.reserve(info_filter.values.size());
    for (const auto value : info_filter.values) {
        info_filter_ids.push_back(value);
    }

    const auto kind_flags =
        q9b_internal::build_allowed_id_flags_from_dim(kind_type, "id", "kind", kind_filter);
    if (!q9b_internal::any_flag_set(kind_flags) || role_filter.values.empty() ||
        info_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    const auto role_flags =
        q9b_internal::build_allowed_id_flags_from_dim(role_type, "id", "role", role_filter);
    if (!q9b_internal::any_flag_set(role_flags)) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

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

    std::vector<uint8_t> allowed_movie_flags(1, 0);
    std::vector<int32_t> allowed_movie_ids;
    int32_t max_movie_id = 0;
    const auto& title_id_col = title.columns.at("id");
    const auto& title_kind_col = title.columns.at("kind_id");
    const auto& title_year_col = title.columns.at("production_year");
    const bool title_has_nulls =
        title_id_col.has_nulls || title_kind_col.has_nulls || title_year_col.has_nulls;
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q9b_title_scan");
        if (!skip_query) {
#endif
    if (!title_has_nulls) {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t movie_id = title_id_col.i32[idx];
            if (movie_id > max_movie_id) {
                max_movie_id = movie_id;
                if (static_cast<size_t>(max_movie_id) >= allowed_movie_flags.size()) {
                    allowed_movie_flags.resize(static_cast<size_t>(max_movie_id) + 1, 0);
                }
            }
            const int32_t kind_id = title_kind_col.i32[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_flags.size() ||
                kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = title_year_col.i32[idx];
            if (year > year_upper || year < year_lower) {
                continue;
            }
            if (allowed_movie_flags[static_cast<size_t>(movie_id)] == 0) {
                allowed_movie_flags[static_cast<size_t>(movie_id)] = 1;
                allowed_movie_ids.push_back(movie_id);
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
            if (title_id_col.is_null[idx] != 0 || title_kind_col.is_null[idx] != 0 ||
                title_year_col.is_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = title_id_col.i32[idx];
            if (movie_id > max_movie_id) {
                max_movie_id = movie_id;
                if (static_cast<size_t>(max_movie_id) >= allowed_movie_flags.size()) {
                    allowed_movie_flags.resize(static_cast<size_t>(max_movie_id) + 1, 0);
                }
            }
            const int32_t kind_id = title_kind_col.i32[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= kind_flags.size() ||
                kind_flags[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = title_year_col.i32[idx];
            if (year > year_upper || year < year_lower) {
                continue;
            }
            if (allowed_movie_flags[static_cast<size_t>(movie_id)] == 0) {
                allowed_movie_flags[static_cast<size_t>(movie_id)] = 1;
                allowed_movie_ids.push_back(movie_id);
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
    if (allowed_movie_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }
    std::sort(allowed_movie_ids.begin(), allowed_movie_ids.end());

    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const bool movie_info_has_nulls =
        mi_movie_id.has_nulls || mi_info_type_id.has_nulls || mi_info.has_nulls;
    const bool info_type1_single = info_type_filter1.single;
    const int32_t info_type1_value = info_type_filter1.single_value;
    const bool info_filter_single = info_filter.single;
    const int32_t info_filter_value = info_filter.single_value;
    const size_t info_filter_count = info_filter_ids.size();
    const bool info_filter_small = info_filter_count <= 4;
    int32_t info_id0 = 0;
    int32_t info_id1 = 0;
    int32_t info_id2 = 0;
    int32_t info_id3 = 0;
    if (!info_filter_single && info_filter_small && info_filter_count > 0) {
        info_id0 = info_filter_ids[0];
        if (info_filter_count > 1) {
            info_id1 = info_filter_ids[1];
        }
        if (info_filter_count > 2) {
            info_id2 = info_filter_ids[2];
        }
        if (info_filter_count > 3) {
            info_id3 = info_filter_ids[3];
        }
    }
    std::unordered_map<int32_t, uint8_t> info_id_to_index;
    if (!info_filter_single && !info_filter_small) {
        info_id_to_index.reserve(info_filter_count);
        for (size_t i = 0; i < info_filter_count; ++i) {
            info_id_to_index.emplace(info_filter_ids[i], static_cast<uint8_t>(i));
        }
    }

    std::vector<int32_t> movie_info_counts_single;
    std::vector<int32_t> movie_info_counts_multi;
    std::vector<int32_t> movie_info_offsets;
    uint64_t movie_info_nonempty = 0;
    const size_t movie_info_rows = static_cast<size_t>(movie_info.row_count);
    const bool use_movie_info_seek =
        movie_info.sorted_by_movie_id &&
        (allowed_movie_ids.size() * 8u < movie_info_rows);
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9b_movie_info_scan");
#endif
    if (info_filter_single) {
        movie_info_counts_single.assign(allowed_movie_flags.size(), 0);
        const auto* movie_id_ptr = mi_movie_id.i32.data();
        const auto* info_type_ptr = mi_info_type_id.i32.data();
        const auto* info_ptr = mi_info.str_ids.data();
        const auto* movie_nulls = mi_movie_id.is_null.data();
        const auto* type_nulls = mi_info_type_id.is_null.data();
        const auto* info_nulls = mi_info.is_null.data();
        const bool check_nulls = movie_info_has_nulls;

        auto process_row = [&](size_t idx, int32_t movie_id) {
            if (check_nulls) {
                if ((movie_nulls[idx] | type_nulls[idx] | info_nulls[idx]) != 0) {
                    return;
                }
            }
            if (info_type1_single) {
                if (info_type_ptr[idx] != info_type1_value) {
                    return;
                }
            } else if (!q9b_internal::matches_int_filter(info_type_filter1, info_type_ptr[idx])) {
                return;
            }
            if (info_ptr[idx] != info_filter_value) {
                return;
            }
            auto& count = movie_info_counts_single[static_cast<size_t>(movie_id)];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
            if (count == 0) {
                ++trace.movie_info_groups_created;
                ++movie_info_nonempty;
            }
#else
            if (count == 0) {
                ++movie_info_nonempty;
            }
#endif
            ++count;
        };

        if (use_movie_info_seek) {
            const auto& movie_ids = mi_movie_id.i32;
            size_t pos = 0;
            for (const int32_t allowed_id : allowed_movie_ids) {
                auto it = std::lower_bound(movie_ids.begin() + static_cast<int64_t>(pos),
                                           movie_ids.end(), allowed_id);
                pos = static_cast<size_t>(it - movie_ids.begin());
                while (pos < movie_ids.size() && movie_ids[pos] == allowed_id) {
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    process_row(pos, allowed_id);
                    ++pos;
                }
            }
        } else {
            for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
                ++trace.movie_info_rows_scanned;
#endif
                const int32_t movie_id = movie_id_ptr[idx];
                if (movie_id < 0 ||
                    static_cast<size_t>(movie_id) >= allowed_movie_flags.size() ||
                    allowed_movie_flags[static_cast<size_t>(movie_id)] == 0) {
                    continue;
                }
                process_row(idx, movie_id);
            }
        }
    } else {
        movie_info_offsets.assign(allowed_movie_flags.size(), -1);
        movie_info_counts_multi.reserve(allowed_movie_ids.size() * info_filter_count);
        const auto* movie_id_ptr = mi_movie_id.i32.data();
        const auto* info_type_ptr = mi_info_type_id.i32.data();
        const auto* info_ptr = mi_info.str_ids.data();
        const auto* movie_nulls = mi_movie_id.is_null.data();
        const auto* type_nulls = mi_info_type_id.is_null.data();
        const auto* info_nulls = mi_info.is_null.data();
        const bool check_nulls = movie_info_has_nulls;

        auto process_row = [&](size_t idx, int32_t movie_id) {
            if (check_nulls) {
                if ((movie_nulls[idx] | type_nulls[idx] | info_nulls[idx]) != 0) {
                    return;
                }
            }
            if (info_type1_single) {
                if (info_type_ptr[idx] != info_type1_value) {
                    return;
                }
            } else if (!q9b_internal::matches_int_filter(info_type_filter1, info_type_ptr[idx])) {
                return;
            }
            const int32_t info_id = info_ptr[idx];
            int32_t info_index = -1;
            if (info_filter_small) {
                if (info_id == info_id0) {
                    info_index = 0;
                } else if (info_filter_count > 1 && info_id == info_id1) {
                    info_index = 1;
                } else if (info_filter_count > 2 && info_id == info_id2) {
                    info_index = 2;
                } else if (info_filter_count > 3 && info_id == info_id3) {
                    info_index = 3;
                }
            } else {
                auto it = info_id_to_index.find(info_id);
                if (it != info_id_to_index.end()) {
                    info_index = static_cast<int32_t>(it->second);
                }
            }
            if (info_index < 0) {
                return;
            }
            auto& offset = movie_info_offsets[static_cast<size_t>(movie_id)];
            if (offset < 0) {
                offset = static_cast<int32_t>(movie_info_counts_multi.size());
                movie_info_counts_multi.resize(static_cast<size_t>(offset) + info_filter_count, 0);
#ifdef TRACE
                ++trace.movie_info_groups_created;
#endif
                ++movie_info_nonempty;
            }
            auto& count =
                movie_info_counts_multi[static_cast<size_t>(offset) +
                                        static_cast<size_t>(info_index)];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
#endif
            ++count;
        };

        if (use_movie_info_seek) {
            const auto& movie_ids = mi_movie_id.i32;
            size_t pos = 0;
            for (const int32_t allowed_id : allowed_movie_ids) {
                auto it = std::lower_bound(movie_ids.begin() + static_cast<int64_t>(pos),
                                           movie_ids.end(), allowed_id);
                pos = static_cast<size_t>(it - movie_ids.begin());
                while (pos < movie_ids.size() && movie_ids[pos] == allowed_id) {
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    process_row(pos, allowed_id);
                    ++pos;
                }
            }
        } else {
            for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
                ++trace.movie_info_rows_scanned;
#endif
                const int32_t movie_id = movie_id_ptr[idx];
                if (movie_id < 0 ||
                    static_cast<size_t>(movie_id) >= allowed_movie_flags.size() ||
                    allowed_movie_flags[static_cast<size_t>(movie_id)] == 0) {
                    continue;
                }
                process_row(idx, movie_id);
            }
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
    const bool person_info_has_nulls = pi_person_id.has_nulls || pi_info_type_id.has_nulls;
    const bool info_type2_single = info_type_filter2.single;
    const int32_t info_type2_value = info_type_filter2.single_value;

    std::vector<int32_t> person_info_counts(1, 0);
    uint64_t person_info_nonempty = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9b_person_info_scan");
#endif
    if (!person_info_has_nulls) {
        for (int64_t i = 0; i < person_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.person_info_rows_scanned;
#endif
            if (info_type2_single) {
                if (pi_info_type_id.i32[idx] != info_type2_value) {
                    continue;
                }
            } else if (!q9b_internal::matches_int_filter(info_type_filter2,
                                                         pi_info_type_id.i32[idx])) {
                continue;
            }
#ifdef TRACE
            ++trace.person_info_rows_emitted;
            ++trace.person_info_agg_rows_in;
            const int32_t person_id = pi_person_id.i32[idx];
            if (person_id >= 0 && static_cast<size_t>(person_id) >= person_info_counts.size()) {
                person_info_counts.resize(static_cast<size_t>(person_id) + 1, 0);
            }
            if (person_id >= 0) {
                auto& count = person_info_counts[static_cast<size_t>(person_id)];
                if (count == 0) {
                    ++person_info_nonempty;
                    ++trace.person_info_groups_created;
                }
                ++count;
            }
#else
            const int32_t person_id = pi_person_id.i32[idx];
            if (person_id >= 0 && static_cast<size_t>(person_id) >= person_info_counts.size()) {
                person_info_counts.resize(static_cast<size_t>(person_id) + 1, 0);
            }
            if (person_id >= 0) {
                auto& count = person_info_counts[static_cast<size_t>(person_id)];
                if (count == 0) {
                    ++person_info_nonempty;
                }
                ++count;
            }
#endif
        }
    } else {
        for (int64_t i = 0; i < person_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.person_info_rows_scanned;
#endif
            if (pi_person_id.is_null[idx] != 0 || pi_info_type_id.is_null[idx] != 0) {
                continue;
            }
            if (info_type2_single) {
                if (pi_info_type_id.i32[idx] != info_type2_value) {
                    continue;
                }
            } else if (!q9b_internal::matches_int_filter(info_type_filter2,
                                                         pi_info_type_id.i32[idx])) {
                continue;
            }
#ifdef TRACE
            ++trace.person_info_rows_emitted;
            ++trace.person_info_agg_rows_in;
            const int32_t person_id = pi_person_id.i32[idx];
            if (person_id >= 0 && static_cast<size_t>(person_id) >= person_info_counts.size()) {
                person_info_counts.resize(static_cast<size_t>(person_id) + 1, 0);
            }
            if (person_id >= 0) {
                auto& count = person_info_counts[static_cast<size_t>(person_id)];
                if (count == 0) {
                    ++person_info_nonempty;
                    ++trace.person_info_groups_created;
                }
                ++count;
            }
#else
            const int32_t person_id = pi_person_id.i32[idx];
            if (person_id >= 0 && static_cast<size_t>(person_id) >= person_info_counts.size()) {
                person_info_counts.resize(static_cast<size_t>(person_id) + 1, 0);
            }
            if (person_id >= 0) {
                auto& count = person_info_counts[static_cast<size_t>(person_id)];
                if (count == 0) {
                    ++person_info_nonempty;
                }
                ++count;
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
    const bool cast_info_has_nulls =
        ci_movie_id.has_nulls || ci_person_id.has_nulls || ci_role_id.has_nulls;

    struct CandidateRow {
        int32_t movie_id{};
        int32_t person_id{};
    };

    std::vector<CandidateRow> candidate_rows;
    candidate_rows.reserve(256);
    std::vector<uint8_t> person_needed_flags(person_info_counts.size(), 0);
    size_t person_needed_count = 0;
    const bool use_cast_seek =
        cast_info.sorted_by_movie_id &&
        (allowed_movie_ids.size() * 8u < static_cast<size_t>(cast_info.row_count));
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9b_cast_info_scan");
#endif
    auto handle_cast_candidate = [&](size_t idx, int32_t movie_id) {
        if (cast_info_has_nulls) {
            if (ci_movie_id.is_null[idx] != 0 || ci_person_id.is_null[idx] != 0 ||
                ci_role_id.is_null[idx] != 0) {
                return;
            }
        }
        const int32_t role_id = ci_role_id.i32[idx];
        if (role_id < 0 || static_cast<size_t>(role_id) >= role_flags.size() ||
            role_flags[static_cast<size_t>(role_id)] == 0) {
            return;
        }
        const int32_t person_id = ci_person_id.i32[idx];
        if (person_id < 0 || static_cast<size_t>(person_id) >= person_info_counts.size()) {
            return;
        }
        if (person_info_counts[static_cast<size_t>(person_id)] == 0) {
            return;
        }
        if (info_filter_single) {
            if (static_cast<size_t>(movie_id) >= movie_info_counts_single.size()) {
                return;
            }
            if (movie_info_counts_single[static_cast<size_t>(movie_id)] == 0) {
                return;
            }
        } else {
            if (static_cast<size_t>(movie_id) >= movie_info_offsets.size()) {
                return;
            }
            if (movie_info_offsets[static_cast<size_t>(movie_id)] < 0) {
                return;
            }
        }
        candidate_rows.push_back(CandidateRow{movie_id, person_id});
        auto& needed = person_needed_flags[static_cast<size_t>(person_id)];
        if (needed == 0) {
            needed = 1;
            ++person_needed_count;
        }
    };

    if (use_cast_seek) {
        const auto& movie_ids = ci_movie_id.i32;
        size_t pos = 0;
        for (const int32_t allowed_id : allowed_movie_ids) {
            auto it = std::lower_bound(movie_ids.begin() + static_cast<int64_t>(pos),
                                       movie_ids.end(), allowed_id);
            pos = static_cast<size_t>(it - movie_ids.begin());
            while (pos < movie_ids.size() && movie_ids[pos] == allowed_id) {
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                handle_cast_candidate(pos, allowed_id);
                ++pos;
            }
        }
    } else {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            const int32_t movie_id = ci_movie_id.i32[idx];
            if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_flags.size() ||
                allowed_movie_flags[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            handle_cast_candidate(idx, movie_id);
        }
    }
#ifdef TRACE
    }
#endif
    if (candidate_rows.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    const auto name_pattern = q9b_internal::compile_ilike_pattern(args.NAME);
    std::vector<int8_t> name_match_cache;
    const auto& name_id_col = name.columns.at("id");
    const auto& name_name_col = name.columns.at("name");
    const bool name_has_nulls = name_id_col.has_nulls || name_name_col.has_nulls;
    std::vector<int32_t> person_name_ids(person_info_counts.size(), -1);
    uint64_t person_name_nonempty = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q9b_name_scan");
#endif
    size_t remaining_persons = person_needed_count;
    if (!name_has_nulls) {
        for (int64_t i = 0; i < name.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.name_rows_scanned;
#endif
            const int32_t person_id = name_id_col.i32[idx];
            if (person_id < 0 ||
                static_cast<size_t>(person_id) >= person_needed_flags.size()) {
                continue;
            }
            auto& needed = person_needed_flags[static_cast<size_t>(person_id)];
            if (needed == 0) {
                continue;
            }
            needed = 0;
            --remaining_persons;
            const int32_t name_str_id = name_name_col.str_ids[idx];
            if (name_str_id < 0) {
                if (remaining_persons == 0) {
                    break;
                }
                continue;
            }
            if (static_cast<size_t>(name_str_id) >= name_match_cache.size()) {
                name_match_cache.resize(static_cast<size_t>(name_str_id) + 1, -1);
            }
            int8_t match = name_match_cache[static_cast<size_t>(name_str_id)];
            if (match < 0) {
                match = static_cast<int8_t>(
                    q9b_internal::ilike_match(
                        name_pattern, db.string_pool.lower_values[static_cast<size_t>(name_str_id)])
                        ? 1
                        : 0);
                name_match_cache[static_cast<size_t>(name_str_id)] = match;
            }
            if (match == 0) {
                if (remaining_persons == 0) {
                    break;
                }
                continue;
            }
            if (person_id >= 0) {
                auto& name_id = person_name_ids[static_cast<size_t>(person_id)];
                if (name_id == -1) {
                    ++person_name_nonempty;
                }
                name_id = name_str_id;
            }
#ifdef TRACE
            ++trace.name_rows_emitted;
#endif
            if (remaining_persons == 0) {
                break;
            }
        }
    } else {
        for (int64_t i = 0; i < name.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.name_rows_scanned;
#endif
            if (name_id_col.is_null[idx] != 0 || name_name_col.is_null[idx] != 0) {
                continue;
            }
            const int32_t person_id = name_id_col.i32[idx];
            if (person_id < 0 ||
                static_cast<size_t>(person_id) >= person_needed_flags.size()) {
                continue;
            }
            auto& needed = person_needed_flags[static_cast<size_t>(person_id)];
            if (needed == 0) {
                continue;
            }
            needed = 0;
            --remaining_persons;
            const int32_t name_str_id = name_name_col.str_ids[idx];
            if (name_str_id < 0) {
                if (remaining_persons == 0) {
                    break;
                }
                continue;
            }
            if (static_cast<size_t>(name_str_id) >= name_match_cache.size()) {
                name_match_cache.resize(static_cast<size_t>(name_str_id) + 1, -1);
            }
            int8_t match = name_match_cache[static_cast<size_t>(name_str_id)];
            if (match < 0) {
                match = static_cast<int8_t>(
                    q9b_internal::ilike_match(
                        name_pattern, db.string_pool.lower_values[static_cast<size_t>(name_str_id)])
                        ? 1
                        : 0);
                name_match_cache[static_cast<size_t>(name_str_id)] = match;
            }
            if (match == 0) {
                if (remaining_persons == 0) {
                    break;
                }
                continue;
            }
            if (person_id >= 0) {
                auto& name_id = person_name_ids[static_cast<size_t>(person_id)];
                if (name_id == -1) {
                    ++person_name_nonempty;
                }
                name_id = name_str_id;
            }
#ifdef TRACE
            ++trace.name_rows_emitted;
#endif
            if (remaining_persons == 0) {
                break;
            }
        }
    }
#ifdef TRACE
    }
#endif
    if (person_name_nonempty == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return {};
#endif
    }

    std::unordered_map<q9b_internal::ResultKey, int64_t, q9b_internal::ResultKeyHash>
        result_counts;
    std::unordered_map<int32_t, int64_t> result_counts_single;
    if (info_filter_single) {
        result_counts_single.reserve(person_name_nonempty);
    } else {
        result_counts.reserve(person_name_nonempty * info_filter_count);
    }
    for (const auto& row : candidate_rows) {
        const int32_t person_id = row.person_id;
        const int32_t name_id = person_name_ids[static_cast<size_t>(person_id)];
        if (name_id == -1) {
            continue;
        }
        const int32_t person_multiplier = person_info_counts[static_cast<size_t>(person_id)];
        const int32_t movie_id = row.movie_id;
        if (info_filter_single) {
            const int32_t info_count =
                movie_info_counts_single[static_cast<size_t>(movie_id)];
            if (info_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            ++trace.result_join_probe_rows_in;
#endif
            const int64_t contribution =
                static_cast<int64_t>(info_count) * static_cast<int64_t>(person_multiplier);
#ifdef TRACE
            ++trace.result_join_rows_emitted;
            ++trace.result_agg_rows_in;
            auto it_result = result_counts_single.find(name_id);
            if (it_result == result_counts_single.end()) {
                result_counts_single.emplace(name_id, contribution);
                ++trace.result_groups_created;
            } else {
                it_result->second += contribution;
            }
#else
            result_counts_single[name_id] += contribution;
#endif
        } else {
            const int32_t offset = movie_info_offsets[static_cast<size_t>(movie_id)];
            if (offset < 0) {
                continue;
            }
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            ++trace.result_join_probe_rows_in;
#endif
            const int64_t base_multiplier = person_multiplier;
            const size_t base_offset = static_cast<size_t>(offset);
            for (size_t i = 0; i < info_filter_count; ++i) {
                const int32_t info_count =
                    movie_info_counts_multi[base_offset + i];
                if (info_count == 0) {
                    continue;
                }
                const int32_t info_id = info_filter_ids[i];
                q9b_internal::ResultKey result_key{info_id, name_id};
                const int64_t contribution = static_cast<int64_t>(info_count) * base_multiplier;
#ifdef TRACE
                ++trace.result_join_rows_emitted;
                ++trace.result_agg_rows_in;
                auto it_result = result_counts.find(result_key);
                if (it_result == result_counts.end()) {
                    result_counts.emplace(result_key, contribution);
                    ++trace.result_groups_created;
                } else {
                    it_result->second += contribution;
                }
#else
                result_counts[result_key] += contribution;
#endif
            }
        }
    }

    if (info_filter_single) {
        results.reserve(result_counts_single.size());
        for (const auto& entry : result_counts_single) {
            results.push_back(Q9bResultRow{info_filter_value, entry.first, entry.second});
        }
    } else {
        results.reserve(result_counts.size());
        for (const auto& entry : result_counts) {
            results.push_back(
                Q9bResultRow{entry.first.info_id, entry.first.name_id, entry.second});
        }
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q9b_sort");
        trace.sort_rows_in = results.size();
#endif
    std::sort(results.begin(), results.end(),
              [](const Q9bResultRow& a, const Q9bResultRow& b) {
                  if (a.movie_info_id != b.movie_info_id) {
                      return a.movie_info_id < b.movie_info_id;
                  }
                  return a.person_name_id < b.person_name_id;
              });
#ifdef TRACE
        trace.sort_rows_out = results.size();
    }
    trace.movie_info_agg_rows_emitted = movie_info_nonempty;
    trace.person_info_agg_rows_emitted = person_info_nonempty;
    trace.cast_info_agg_rows_emitted = trace.cast_info_agg_rows_in;
    trace.result_agg_rows_emitted =
        info_filter_single ? result_counts_single.size() : result_counts.size();
    trace.result_join_build_rows_in = static_cast<uint64_t>(
        movie_info_nonempty + person_info_nonempty + person_name_nonempty);
    trace.query_output_rows = results.size();
#endif
#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q9b_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q9b_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q9b_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q9b_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q9b_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q9b_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q9b_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q9b_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q9b_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q9b_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q9b_person_info_groups_created", trace.person_info_groups_created);
    print_count("q9b_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q9b_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q9b_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q9b_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q9b_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q9b_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q9b_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q9b_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q9b_result_join_build_rows_in", trace.result_join_build_rows_in);
    print_count("q9b_result_join_probe_rows_in", trace.result_join_probe_rows_in);
    print_count("q9b_result_join_rows_emitted", trace.result_join_rows_emitted);
    print_count("q9b_result_agg_rows_in", trace.result_agg_rows_in);
    print_count("q9b_result_groups_created", trace.result_groups_created);
    print_count("q9b_result_agg_rows_emitted", trace.result_agg_rows_emitted);
    print_count("q9b_sort_rows_in", trace.sort_rows_in);
    print_count("q9b_sort_rows_out", trace.sort_rows_out);
    print_count("q9b_query_output_rows", trace.query_output_rows);
#endif
    return results;
}