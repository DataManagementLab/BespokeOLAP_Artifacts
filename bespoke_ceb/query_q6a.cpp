#include "query_q6a.hpp"

#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

#include "trace.hpp"

namespace q6a_internal {

struct IntFilter {
    std::vector<int32_t> values;

    bool contains(int32_t value) const {
        for (const auto candidate : values) {
            if (candidate == value) {
                return true;
            }
        }
        return false;
    }
};

struct StringFilter {
    std::vector<int32_t> values;
    bool allow_null = false;

    bool contains(int32_t value) const {
        const size_t size = values.size();
        if (size == 0) {
            return false;
        }
        if (size <= 4) {
            for (size_t i = 0; i < size; ++i) {
                if (values[i] == value) {
                    return true;
                }
            }
            return false;
        }
        return std::binary_search(values.begin(), values.end(), value);
    }
};

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
    std::sort(filter.values.begin(), filter.values.end());
    filter.values.erase(std::unique(filter.values.begin(), filter.values.end()), filter.values.end());
    return filter;
}

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const ColumnData& /*col*/,
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
            filter.values.push_back(found_id);
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
    std::sort(filter.values.begin(), filter.values.end());
    filter.values.erase(std::unique(filter.values.begin(), filter.values.end()), filter.values.end());
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
    return filter.contains(id);
}

std::vector<uint8_t> build_id_mask_from_filter(const IntFilter& filter, size_t size_hint) {
    std::vector<uint8_t> mask;
    if (filter.values.empty()) {
        return mask;
    }
    mask.assign(size_hint, 0);
    for (const auto id : filter.values) {
        if (id < 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(id);
        if (idx < mask.size()) {
            mask[idx] = 1;
        }
    }
    return mask;
}

std::vector<uint8_t> build_allowed_id_mask(const TableData& table,
                                           const std::string& id_col_name,
                                           const std::string& str_col_name,
                                           const StringFilter& filter) {
    std::vector<uint8_t> ids;
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        if (matches_string(str_col, static_cast<size_t>(i), filter)) {
            const int32_t id = id_col.i32[static_cast<size_t>(i)];
            if (id < 0) {
                continue;
            }
            const size_t idx = static_cast<size_t>(id);
            if (idx >= ids.size()) {
                ids.resize(idx + 1, 0);
            }
            ids[idx] = 1;
        }
    }
    return ids;
}

std::vector<uint8_t> build_valid_id_mask(const TableData& table, const std::string& id_col_name) {
    std::vector<uint8_t> ids;
    const auto& id_col = table.columns.at(id_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        const int32_t id = id_col.i32[static_cast<size_t>(i)];
        if (id < 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(id);
        if (idx >= ids.size()) {
            ids.resize(idx + 1, 0);
        }
        ids[idx] = 1;
    }
    return ids;
}

bool mask_contains(const std::vector<uint8_t>& mask, int32_t id) {
    if (id < 0) {
        return false;
    }
    const size_t idx = static_cast<size_t>(id);
    return idx < mask.size() && mask[idx] != 0;
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

bool parse_numeric_value(const char* data, size_t len, double& out) {
    if (len == 0) {
        return false;
    }
    size_t pos = 0;
    bool saw_digit = false;
    bool saw_dot = false;
    double integer_part = 0.0;
    double fractional_part = 0.0;
    double frac_scale = 1.0;
    if (data[0] == '.') {
        saw_dot = true;
        pos = 1;
    }
    if (!saw_dot && len > 1 && data[0] == '0' && data[1] != '.') {
        return false;
    }
    for (; pos < len; ++pos) {
        const char c = data[pos];
        if (c == '.') {
            if (saw_dot) {
                return false;
            }
            saw_dot = true;
            continue;
        }
        if (c < '0' || c > '9') {
            return false;
        }
        saw_digit = true;
        const int digit = c - '0';
        if (!saw_dot) {
            integer_part = integer_part * 10.0 + static_cast<double>(digit);
        } else {
            frac_scale *= 0.1;
            fractional_part += static_cast<double>(digit) * frac_scale;
        }
    }
    if (!saw_digit) {
        return false;
    }
    if (saw_dot && data[len - 1] == '.') {
        return false;
    }
    out = integer_part + fractional_part;
    return true;
}

}  // namespace q6a_internal

#ifdef TRACE
struct Q6aTrace {
    TraceRecorder recorder;
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
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t title_join_build_rows_in = 0;
    uint64_t title_join_probe_rows_in = 0;
    uint64_t title_join_rows_emitted = 0;
    uint64_t movie_multiplier_agg_rows_in = 0;
    uint64_t movie_multiplier_groups_created = 0;
    uint64_t movie_multiplier_agg_rows_emitted = 0;
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
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t cast_info_join_build_rows_in = 0;
    uint64_t cast_info_join_probe_rows_in = 0;
    uint64_t cast_info_join_rows_emitted = 0;
    uint64_t query_output_rows = 0;
};
#endif

int64_t run_q6a(const Database& db, const Q6aArgs& args) {
    int64_t result = 0;
#ifdef TRACE
    Q6aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q6a_total");
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
        q6a_internal::build_string_filter_for_column(db.string_pool, kind_col, args.KIND);
    const auto allowed_kind_ids =
        q6a_internal::build_allowed_id_mask(kind_type, "id", "kind", kind_filter);
    if (allowed_kind_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& info_type = db.tables.at("info_type");
    const auto valid_info_type_ids = q6a_internal::build_valid_id_mask(info_type, "id");
    if (!q6a_internal::mask_contains(valid_info_type_ids, id2) ||
        !q6a_internal::mask_contains(valid_info_type_ids, id3)) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto info_type_filter1 = q6a_internal::build_int_filter(args.ID1);
    if (info_type_filter1.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto info_type_filter4 = q6a_internal::build_int_filter(args.ID4);
    if (info_type_filter4.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto info_type_filter1_mask =
        q6a_internal::build_id_mask_from_filter(info_type_filter1, valid_info_type_ids.size());
    if (info_type_filter1_mask.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto info_type_filter4_mask =
        q6a_internal::build_id_mask_from_filter(info_type_filter4, valid_info_type_ids.size());
    if (info_type_filter4_mask.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& role_type = db.tables.at("role_type");
    const auto& role_col = role_type.columns.at("role");
    const auto role_filter =
        q6a_internal::build_string_filter_for_column(db.string_pool, role_col, args.ROLE);
    std::vector<uint8_t> allowed_role_ids;
#ifdef TRACE
    if (!skip_query) {
        allowed_role_ids =
            q6a_internal::build_allowed_id_mask(role_type, "id", "role", role_filter);
    }
#else
    allowed_role_ids = q6a_internal::build_allowed_id_mask(role_type, "id", "role", role_filter);
#endif
    if (allowed_role_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const size_t allowed_role_size = allowed_role_ids.size();

    const auto& cast_info = db.tables.at("cast_info");
    const auto& note_col = cast_info.columns.at("note");
    q6a_internal::StringFilter note_filter;
#ifdef TRACE
    if (!skip_query) {
        note_filter =
            q6a_internal::build_string_filter_for_column(db.string_pool, note_col, args.NOTE);
    }
#else
    note_filter = q6a_internal::build_string_filter_for_column(db.string_pool, note_col, args.NOTE);
#endif
    if (note_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const bool note_single = note_filter.values.size() == 1 && !note_filter.allow_null;
    const int32_t note_single_id = note_single ? note_filter.values[0] : -1;

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const auto info1_filter =
        q6a_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO1);
    if (info1_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const bool info1_single = info1_filter.values.size() == 1 && !info1_filter.allow_null;
    const int32_t info1_single_id = info1_single ? info1_filter.values[0] : -1;
    const size_t string_pool_size = db.string_pool.values.size();
    std::vector<uint8_t> info1_mask;
    bool info1_use_mask = false;
    const uint8_t* info1_mask_data = nullptr;
    if (!info1_single && info1_filter.values.size() > 4 && string_pool_size > 0) {
        info1_mask.assign(string_pool_size, 0);
        for (const int32_t id : info1_filter.values) {
            if (id < 0) {
                continue;
            }
            const size_t idx = static_cast<size_t>(id);
            if (idx < string_pool_size) {
                info1_mask[idx] = 1;
            }
        }
        info1_use_mask = true;
        info1_mask_data = info1_mask.data();
    }

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    std::vector<uint8_t> allowed_movie;
    std::vector<int32_t> title_candidates;
    title_candidates.reserve(static_cast<size_t>(title.row_count));
    if (title.row_count > 0) {
        allowed_movie.assign(static_cast<size_t>(title.row_count), 0);
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q6a_title_scan");
        if (!skip_query) {
#endif
    const size_t allowed_kind_size = allowed_kind_ids.size();
    const size_t title_rows = static_cast<size_t>(title.row_count);
    const int32_t* t_id_data = t_id.i32.data();
    const int32_t* t_kind_id_data = t_kind_id.i32.data();
    const int32_t* t_year_data = t_year.i32.data();
    const uint8_t* t_id_null = t_id.is_null.data();
    const uint8_t* t_kind_null = t_kind_id.is_null.data();
    const uint8_t* t_year_null = t_year.is_null.data();
    const uint8_t* allowed_kind_data = allowed_kind_ids.data();
    const bool title_has_nulls = t_id.has_nulls || t_kind_id.has_nulls || t_year.has_nulls;
    if (title_has_nulls) {
        for (size_t idx = 0; idx < title_rows; ++idx) {
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if ((t_id_null[idx] | t_kind_null[idx] | t_year_null[idx]) != 0) {
                continue;
            }
            const int32_t kind_id = t_kind_id_data[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= allowed_kind_size ||
                allowed_kind_data[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = t_year_data[idx];
            if (year < year2 || year > year1) {
                continue;
            }
            const int32_t movie_id = t_id_data[idx];
            if (movie_id < 0) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (movie_idx >= allowed_movie.size()) {
                allowed_movie.resize(movie_idx + 1, 0);
            }
            if (allowed_movie[movie_idx] == 0) {
                allowed_movie[movie_idx] = 1;
                title_candidates.push_back(movie_id);
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
#endif
        }
    } else {
        for (size_t idx = 0; idx < title_rows; ++idx) {
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t kind_id = t_kind_id_data[idx];
            if (kind_id < 0 || static_cast<size_t>(kind_id) >= allowed_kind_size ||
                allowed_kind_data[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t year = t_year_data[idx];
            if (year < year2 || year > year1) {
                continue;
            }
            const int32_t movie_id = t_id_data[idx];
            if (movie_id < 0) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (movie_idx >= allowed_movie.size()) {
                allowed_movie.resize(movie_idx + 1, 0);
            }
            if (allowed_movie[movie_idx] == 0) {
                allowed_movie[movie_idx] = 1;
                title_candidates.push_back(movie_id);
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
    if (allowed_movie.empty() || title_candidates.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<int32_t> info_counts(allowed_movie.size(), 0);
    int64_t info_count_groups = 0;
    const size_t allowed_movie_size = allowed_movie.size();
    const size_t info_type_filter1_size = info_type_filter1_mask.size();
    const uint8_t* allowed_movie_data = allowed_movie.data();
    const uint8_t* info_type_filter1_data = info_type_filter1_mask.data();
    const size_t movie_info_rows = static_cast<size_t>(movie_info.row_count);
    const int32_t* mi_movie_id_data = mi_movie_id.i32.data();
    const int32_t* mi_info_type_id_data = mi_info_type_id.i32.data();
    const int32_t* mi_info_data = mi_info.str_ids.data();
    const uint8_t* mi_movie_null = mi_movie_id.is_null.data();
    const uint8_t* mi_info_type_null = mi_info_type_id.is_null.data();
    const uint8_t* mi_info_null = mi_info.is_null.data();
    int32_t* info_counts_data = info_counts.data();
    const bool mi_has_nulls =
        mi_movie_id.has_nulls || mi_info_type_id.has_nulls || mi_info.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q6a_movie_info_scan");
#endif
    constexpr size_t kMovieInfoPrefetch = 16;
    if (!mi_has_nulls && info1_single) {
        for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if (idx + kMovieInfoPrefetch < movie_info_rows) {
                const int32_t prefetch_id = mi_movie_id_data[idx + kMovieInfoPrefetch];
                if (prefetch_id >= 0) {
                    const size_t prefetch_idx = static_cast<size_t>(prefetch_id);
                    if (prefetch_idx < allowed_movie_size) {
                        __builtin_prefetch(allowed_movie_data + prefetch_idx, 0, 1);
                        __builtin_prefetch(info_counts_data + prefetch_idx, 1, 1);
                    }
                }
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (static_cast<uint32_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter1_size ||
                info_type_filter1_data[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info_id != info1_single_id) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_counts_data[movie_idx] == 0) {
                ++info_count_groups;
#ifdef TRACE
                ++trace.movie_info_groups_created;
#endif
            }
            ++info_counts_data[movie_idx];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
#endif
        }
    } else if (mi_has_nulls && info1_single) {
        for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if ((mi_movie_null[idx] | mi_info_type_null[idx]) != 0) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (static_cast<uint32_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter1_size ||
                info_type_filter1_data[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            if (mi_info_null[idx] != 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info_id != info1_single_id) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_counts_data[movie_idx] == 0) {
                ++info_count_groups;
#ifdef TRACE
                ++trace.movie_info_groups_created;
#endif
            }
            ++info_counts_data[movie_idx];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
#endif
        }
    } else if (!mi_has_nulls) {
        for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if (idx + kMovieInfoPrefetch < movie_info_rows) {
                const int32_t prefetch_id = mi_movie_id_data[idx + kMovieInfoPrefetch];
                if (prefetch_id >= 0) {
                    const size_t prefetch_idx = static_cast<size_t>(prefetch_id);
                    if (prefetch_idx < allowed_movie_size) {
                        __builtin_prefetch(allowed_movie_data + prefetch_idx, 0, 1);
                        __builtin_prefetch(info_counts_data + prefetch_idx, 1, 1);
                    }
                }
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (static_cast<uint32_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter1_size ||
                info_type_filter1_data[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info1_use_mask) {
                if (static_cast<uint32_t>(info_id) >= string_pool_size ||
                    info1_mask_data[static_cast<size_t>(info_id)] == 0) {
                    continue;
                }
            } else if (!info1_filter.contains(info_id)) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_counts_data[movie_idx] == 0) {
                ++info_count_groups;
#ifdef TRACE
                ++trace.movie_info_groups_created;
#endif
            }
            ++info_counts_data[movie_idx];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
#endif
        }
    } else {
        for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if ((mi_movie_null[idx] | mi_info_type_null[idx]) != 0) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (static_cast<uint32_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter1_size ||
                info_type_filter1_data[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            if (mi_info_null[idx] != 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info1_use_mask) {
                if (static_cast<uint32_t>(info_id) >= string_pool_size ||
                    info1_mask_data[static_cast<size_t>(info_id)] == 0) {
                    continue;
                }
            } else if (!info1_filter.contains(info_id)) {
                continue;
            }
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_counts_data[movie_idx] == 0) {
                ++info_count_groups;
#ifdef TRACE
                ++trace.movie_info_groups_created;
#endif
            }
            ++info_counts_data[movie_idx];
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
#endif
        }
    }
#ifdef TRACE
    }
#endif

    const auto& movie_info_idx = db.tables.at("movie_info_idx");
    const auto& mii_movie_id = movie_info_idx.columns.at("movie_id");
    const auto& mii_info_type_id = movie_info_idx.columns.at("info_type_id");
    const auto& mii_info = movie_info_idx.columns.at("info");

    std::vector<int32_t> idx_counts1(allowed_movie.size(), 0);
    std::vector<int32_t> idx_counts2(allowed_movie.size(), 0);
    int64_t idx_count1_groups = 0;
    int64_t idx_count2_groups = 0;
    std::vector<int8_t> numeric_state(string_pool_size, 0);
    std::vector<double> numeric_cache(string_pool_size, 0.0);
    const std::string* const* string_values = db.string_pool.values.data();
    int8_t* numeric_state_data = numeric_state.data();
    double* numeric_cache_data = numeric_cache.data();
    const size_t movie_info_idx_rows = static_cast<size_t>(movie_info_idx.row_count);
    const int32_t* mii_movie_id_data = mii_movie_id.i32.data();
    const int32_t* mii_info_type_id_data = mii_info_type_id.i32.data();
    const int32_t* mii_info_data = mii_info.str_ids.data();
    const uint8_t* mii_movie_null = mii_movie_id.is_null.data();
    const uint8_t* mii_info_type_null = mii_info_type_id.is_null.data();
    const uint8_t* mii_info_null = mii_info.is_null.data();
    int32_t* idx_counts1_data = idx_counts1.data();
    int32_t* idx_counts2_data = idx_counts2.data();
    const bool mii_has_nulls =
        mii_movie_id.has_nulls || mii_info_type_id.has_nulls || mii_info.has_nulls;

#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q6a_movie_info_idx_scan");
#endif
    if (mii_has_nulls) {
        for (size_t idx = 0; idx < movie_info_idx_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_idx_rows_scanned;
#endif
            if ((mii_movie_null[idx] | mii_info_type_null[idx] | mii_info_null[idx]) != 0) {
                continue;
            }
            const int32_t movie_id = mii_movie_id_data[idx];
            if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mii_info_type_id_data[idx];
            if (info_type_id != id2 && info_type_id != id3) {
                continue;
            }
            const int32_t info_str_id = mii_info_data[idx];
            if (info_str_id < 0 || static_cast<size_t>(info_str_id) >= string_pool_size) {
                continue;
            }
            const size_t info_str_idx = static_cast<size_t>(info_str_id);
            int8_t state = numeric_state_data[info_str_idx];
            double value = 0.0;
            if (state == 0) {
                const std::string* str_value = string_values[info_str_idx];
                if (q6a_internal::parse_numeric_value(str_value->data(), str_value->size(), value)) {
                    numeric_cache_data[info_str_idx] = value;
                    numeric_state_data[info_str_idx] = 1;
                } else {
                    numeric_state_data[info_str_idx] = -1;
                    continue;
                }
            } else if (state < 0) {
                continue;
            } else {
                value = numeric_cache_data[info_str_idx];
            }
            if (info_type_id == id2) {
                if (value < info4 || value > info5) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (idx_counts1_data[movie_idx] == 0) {
                    ++idx_count1_groups;
#ifdef TRACE
                    ++trace.movie_info_idx_groups_created_id2;
#endif
                }
                ++idx_counts1_data[movie_idx];
#ifdef TRACE
                ++trace.movie_info_idx_rows_emitted_id2;
                ++trace.movie_info_idx_agg_rows_in_id2;
#endif
            } else {
                if (value < info3 || value > info2) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_info_idx_rows_emitted_id3;
                ++trace.movie_info_idx_agg_rows_in_id3;
#endif
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (idx_counts2_data[movie_idx] == 0) {
                    ++idx_count2_groups;
#ifdef TRACE
                    ++trace.movie_info_idx_groups_created_id3;
#endif
                }
                ++idx_counts2_data[movie_idx];
            }
        }
    } else {
        for (size_t idx = 0; idx < movie_info_idx_rows; ++idx) {
#ifdef TRACE
            ++trace.movie_info_idx_rows_scanned;
#endif
            const int32_t movie_id = mii_movie_id_data[idx];
            if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_size ||
                allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t info_type_id = mii_info_type_id_data[idx];
            if (info_type_id != id2 && info_type_id != id3) {
                continue;
            }
            const int32_t info_str_id = mii_info_data[idx];
            if (info_str_id < 0 || static_cast<size_t>(info_str_id) >= string_pool_size) {
                continue;
            }
            const size_t info_str_idx = static_cast<size_t>(info_str_id);
            int8_t state = numeric_state_data[info_str_idx];
            double value = 0.0;
            if (state == 0) {
                const std::string* str_value = string_values[info_str_idx];
                if (q6a_internal::parse_numeric_value(str_value->data(), str_value->size(), value)) {
                    numeric_cache_data[info_str_idx] = value;
                    numeric_state_data[info_str_idx] = 1;
                } else {
                    numeric_state_data[info_str_idx] = -1;
                    continue;
                }
            } else if (state < 0) {
                continue;
            } else {
                value = numeric_cache_data[info_str_idx];
            }
            if (info_type_id == id2) {
                if (value < info4 || value > info5) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (idx_counts1_data[movie_idx] == 0) {
                    ++idx_count1_groups;
#ifdef TRACE
                    ++trace.movie_info_idx_groups_created_id2;
#endif
                }
                ++idx_counts1_data[movie_idx];
#ifdef TRACE
                ++trace.movie_info_idx_rows_emitted_id2;
                ++trace.movie_info_idx_agg_rows_in_id2;
#endif
            } else {
                if (value < info3 || value > info2) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_info_idx_rows_emitted_id3;
                ++trace.movie_info_idx_agg_rows_in_id3;
#endif
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (idx_counts2_data[movie_idx] == 0) {
                    ++idx_count2_groups;
#ifdef TRACE
                    ++trace.movie_info_idx_groups_created_id3;
#endif
                }
                ++idx_counts2_data[movie_idx];
            }
        }
    }
#ifdef TRACE
    }
#endif

    std::vector<int64_t> movie_multiplier(allowed_movie.size(), 0);
    int64_t movie_multiplier_groups = 0;
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q6a_title_join");
        if (!skip_query) {
#endif
    for (const auto movie_id : title_candidates) {
        const size_t movie_idx = static_cast<size_t>(movie_id);
#ifdef TRACE
        ++trace.title_join_probe_rows_in;
#endif
        const int32_t info_count = info_counts[movie_idx];
        if (info_count == 0) {
            continue;
        }
        const int32_t idx1_count = idx_counts1[movie_idx];
        if (idx1_count == 0) {
            continue;
        }
        const int32_t idx2_count = idx_counts2[movie_idx];
        if (idx2_count == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.title_join_rows_emitted;
        ++trace.movie_multiplier_agg_rows_in;
#endif
        const int64_t multiplier =
            static_cast<int64_t>(info_count) * idx1_count * idx2_count;
        if (multiplier > 0) {
            movie_multiplier[movie_idx] = multiplier;
            ++movie_multiplier_groups;
#ifdef TRACE
            ++trace.movie_multiplier_groups_created;
#endif
        }
    }
#ifdef TRACE
        }
    }
#endif
    if (movie_multiplier_groups == 0) {
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
        q6a_internal::build_string_filter_for_column(db.string_pool, gender_col, args.GENDER);
    const auto pcode_filter =
        q6a_internal::build_string_filter_for_column(db.string_pool, pcode_col, args.NAME_PCODE_NF);
    const bool gender_single = gender_filter.values.size() == 1 && !gender_filter.allow_null;
    const int32_t gender_single_id = gender_single ? gender_filter.values[0] : -1;
    const bool pcode_single = pcode_filter.values.size() == 1 && !pcode_filter.allow_null;
    const int32_t pcode_single_id = pcode_single ? pcode_filter.values[0] : -1;
    const auto& name_id_col = name.columns.at("id");
    std::vector<uint8_t> allowed_person_ids;
    int64_t allowed_person_count = 0;
    if (name.row_count > 0) {
        allowed_person_ids.assign(static_cast<size_t>(name.row_count), 0);
    }
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q6a_name_scan");
        if (!skip_query) {
#endif
    const size_t name_rows = static_cast<size_t>(name.row_count);
    const int32_t* name_id_data = name_id_col.i32.data();
    const int32_t* gender_data = gender_col.str_ids.data();
    const int32_t* pcode_data = pcode_col.str_ids.data();
    const uint8_t* name_id_null = name_id_col.is_null.data();
    const uint8_t* gender_null = gender_col.is_null.data();
    const uint8_t* pcode_null = pcode_col.is_null.data();
    const bool gender_allow_null = gender_filter.allow_null;
    const bool pcode_allow_null = pcode_filter.allow_null;
    uint8_t* allowed_person_data = allowed_person_ids.data();
    if (gender_single && pcode_single && !gender_allow_null && !pcode_allow_null) {
        for (size_t idx = 0; idx < name_rows; ++idx) {
#ifdef TRACE
            ++trace.name_rows_scanned;
#endif
            if (name_id_null[idx] != 0 || gender_null[idx] != 0 || pcode_null[idx] != 0) {
                continue;
            }
            if (gender_data[idx] != gender_single_id || pcode_data[idx] != pcode_single_id) {
                continue;
            }
            const int32_t person_id = name_id_data[idx];
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= allowed_person_ids.size()) {
                allowed_person_ids.resize(person_idx + 1, 0);
                allowed_person_data = allowed_person_ids.data();
            }
            if (allowed_person_data[person_idx] == 0) {
                allowed_person_data[person_idx] = 1;
                ++allowed_person_count;
            }
#ifdef TRACE
            ++trace.name_rows_emitted;
#endif
        }
    } else {
        for (size_t idx = 0; idx < name_rows; ++idx) {
#ifdef TRACE
            ++trace.name_rows_scanned;
#endif
            if (name_id_null[idx] != 0) {
                continue;
            }
            if (gender_null[idx] != 0) {
                if (!gender_allow_null) {
                    continue;
                }
            } else {
                const int32_t gender_id = gender_data[idx];
                if (gender_single) {
                    if (gender_id != gender_single_id) {
                        continue;
                    }
                } else if (!gender_filter.contains(gender_id)) {
                    continue;
                }
            }
            if (pcode_null[idx] != 0) {
                if (!pcode_allow_null) {
                    continue;
                }
            } else {
                const int32_t pcode_id = pcode_data[idx];
                if (pcode_single) {
                    if (pcode_id != pcode_single_id) {
                        continue;
                    }
                } else if (!pcode_filter.contains(pcode_id)) {
                    continue;
                }
            }
            const int32_t person_id = name_id_data[idx];
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= allowed_person_ids.size()) {
                allowed_person_ids.resize(person_idx + 1, 0);
                allowed_person_data = allowed_person_ids.data();
            }
            if (allowed_person_data[person_idx] == 0) {
                allowed_person_data[person_idx] = 1;
                ++allowed_person_count;
            }
#ifdef TRACE
            ++trace.name_rows_emitted;
#endif
        }
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

    std::vector<int32_t> aka_counts(allowed_person_ids.size(), 0);
    int64_t aka_count_groups = 0;
    const auto& aka_name = db.tables.at("aka_name");
    const auto& aka_person_id = aka_name.columns.at("person_id");
    const size_t allowed_person_size = allowed_person_ids.size();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q6a_aka_name_scan");
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
        if (person_id < 0 || static_cast<size_t>(person_id) >= allowed_person_size ||
            allowed_person_ids[static_cast<size_t>(person_id)] == 0) {
            continue;
        }
        const size_t person_idx = static_cast<size_t>(person_id);
#ifdef TRACE
        ++trace.aka_name_rows_emitted;
        ++trace.aka_name_agg_rows_in;
        if (aka_counts[person_idx] == 0) {
            ++trace.aka_name_groups_created;
        }
#endif
        if (aka_counts[person_idx] == 0) {
            ++aka_count_groups;
        }
        ++aka_counts[person_idx];
    }
#ifdef TRACE
    }
#endif

    std::vector<int32_t> person_info_counts(allowed_person_ids.size(), 0);
    int64_t person_info_count_groups = 0;
    const auto& person_info = db.tables.at("person_info");
    const auto& pi_person_id = person_info.columns.at("person_id");
    const auto& pi_info_type_id = person_info.columns.at("info_type_id");
    const size_t info_type_filter4_size = info_type_filter4_mask.size();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q6a_person_info_scan");
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
        if (person_id < 0 || static_cast<size_t>(person_id) >= allowed_person_size ||
            allowed_person_ids[static_cast<size_t>(person_id)] == 0) {
            continue;
        }
        const int32_t info_type_id = pi_info_type_id.i32[idx];
        if (info_type_id < 0 || static_cast<size_t>(info_type_id) >= info_type_filter4_size ||
            info_type_filter4_mask[static_cast<size_t>(info_type_id)] == 0) {
            continue;
        }
        const size_t person_idx = static_cast<size_t>(person_id);
#ifdef TRACE
        ++trace.person_info_rows_emitted;
        ++trace.person_info_agg_rows_in;
        if (person_info_counts[person_idx] == 0) {
            ++trace.person_info_groups_created;
        }
#endif
        if (person_info_counts[person_idx] == 0) {
            ++person_info_count_groups;
        }
        ++person_info_counts[person_idx];
    }
#ifdef TRACE
    }
#endif
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const size_t cast_rows = static_cast<size_t>(cast_info.row_count);
    const int32_t* ci_person_data = ci_person_id.i32.data();
    const int32_t* ci_movie_data = ci_movie_id.i32.data();
    const int32_t* ci_role_data = ci_role_id.i32.data();
    const int32_t* note_data = note_col.str_ids.data();
    const uint8_t* ci_person_null = ci_person_id.is_null.data();
    const uint8_t* ci_movie_null = ci_movie_id.is_null.data();
    const uint8_t* ci_role_null = ci_role_id.is_null.data();
    const uint8_t* note_null = note_col.is_null.data();
    const uint8_t* allowed_role_data = allowed_role_ids.data();
    const uint8_t* allowed_person_data_const = allowed_person_ids.data();
    const int32_t* aka_counts_data = aka_counts.data();
    const int32_t* person_info_counts_data = person_info_counts.data();
    const int64_t* movie_multiplier_data = movie_multiplier.data();

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q6a_cast_info_scan");
        if (!skip_query) {
#endif
    for (size_t idx = 0; idx < cast_rows; ++idx) {
#ifdef TRACE
        ++trace.cast_info_rows_scanned;
#endif
        if ((ci_person_null[idx] | ci_movie_null[idx] | ci_role_null[idx]) != 0) {
            continue;
        }
        if (note_null[idx] != 0) {
            if (!note_filter.allow_null) {
                continue;
            }
        } else {
            const int32_t note_id = note_data[idx];
            if (note_single) {
                if (note_id != note_single_id) {
                    continue;
                }
            } else if (!note_filter.contains(note_id)) {
                continue;
            }
        }
        const int32_t role_id = ci_role_data[idx];
        if (role_id < 0 || static_cast<size_t>(role_id) >= allowed_role_size ||
            allowed_role_data[static_cast<size_t>(role_id)] == 0) {
            continue;
        }
        const int32_t person_id = ci_person_data[idx];
        if (person_id < 0 || static_cast<size_t>(person_id) >= allowed_person_size ||
            allowed_person_data_const[static_cast<size_t>(person_id)] == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.cast_info_join_probe_rows_in;
#endif
        const size_t person_idx = static_cast<size_t>(person_id);
        const int32_t aka_count = aka_counts_data[person_idx];
        if (aka_count == 0) {
            continue;
        }
        const int32_t pi_count = person_info_counts_data[person_idx];
        if (pi_count == 0) {
            continue;
        }
        const int32_t movie_id = ci_movie_data[idx];
        if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_size ||
            allowed_movie_data[static_cast<size_t>(movie_id)] == 0) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(movie_id);
        const int64_t movie_mult = movie_multiplier_data[movie_idx];
        if (movie_mult == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.cast_info_join_rows_emitted;
#endif
        result += movie_mult * aka_count * pi_count;
    }
#ifdef TRACE
        }
    }
    trace.movie_info_agg_rows_emitted = static_cast<uint64_t>(info_count_groups);
    trace.movie_info_idx_agg_rows_emitted_id2 = static_cast<uint64_t>(idx_count1_groups);
    trace.movie_info_idx_agg_rows_emitted_id3 = static_cast<uint64_t>(idx_count2_groups);
    trace.movie_multiplier_agg_rows_emitted = static_cast<uint64_t>(movie_multiplier_groups);
    trace.title_join_build_rows_in =
        static_cast<uint64_t>(info_count_groups + idx_count1_groups + idx_count2_groups);
    trace.cast_info_join_build_rows_in = static_cast<uint64_t>(
        movie_multiplier_groups + aka_count_groups + person_info_count_groups);
    trace.aka_name_agg_rows_emitted = static_cast<uint64_t>(aka_count_groups);
    trace.person_info_agg_rows_emitted = static_cast<uint64_t>(person_info_count_groups);
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q6a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q6a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q6a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q6a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q6a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q6a_movie_info_idx_scan_rows_scanned", trace.movie_info_idx_rows_scanned);
    print_count("q6a_movie_info_idx_scan_rows_emitted_id2", trace.movie_info_idx_rows_emitted_id2);
    print_count("q6a_movie_info_idx_scan_rows_emitted_id3", trace.movie_info_idx_rows_emitted_id3);
    print_count("q6a_movie_info_idx_agg_rows_in_id2", trace.movie_info_idx_agg_rows_in_id2);
    print_count("q6a_movie_info_idx_agg_rows_in_id3", trace.movie_info_idx_agg_rows_in_id3);
    print_count("q6a_movie_info_idx_groups_created_id2",
                trace.movie_info_idx_groups_created_id2);
    print_count("q6a_movie_info_idx_groups_created_id3",
                trace.movie_info_idx_groups_created_id3);
    print_count("q6a_movie_info_idx_agg_rows_emitted_id2",
                trace.movie_info_idx_agg_rows_emitted_id2);
    print_count("q6a_movie_info_idx_agg_rows_emitted_id3",
                trace.movie_info_idx_agg_rows_emitted_id3);
    print_count("q6a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q6a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q6a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q6a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q6a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q6a_movie_multiplier_agg_rows_in", trace.movie_multiplier_agg_rows_in);
    print_count("q6a_movie_multiplier_groups_created", trace.movie_multiplier_groups_created);
    print_count("q6a_movie_multiplier_agg_rows_emitted", trace.movie_multiplier_agg_rows_emitted);
    print_count("q6a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q6a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q6a_aka_name_scan_rows_scanned", trace.aka_name_rows_scanned);
    print_count("q6a_aka_name_scan_rows_emitted", trace.aka_name_rows_emitted);
    print_count("q6a_aka_name_agg_rows_in", trace.aka_name_agg_rows_in);
    print_count("q6a_aka_name_groups_created", trace.aka_name_groups_created);
    print_count("q6a_aka_name_agg_rows_emitted", trace.aka_name_agg_rows_emitted);
    print_count("q6a_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q6a_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q6a_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q6a_person_info_groups_created", trace.person_info_groups_created);
    print_count("q6a_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q6a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q6a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q6a_cast_info_join_build_rows_in", trace.cast_info_join_build_rows_in);
    print_count("q6a_cast_info_join_probe_rows_in", trace.cast_info_join_probe_rows_in);
    print_count("q6a_cast_info_join_rows_emitted", trace.cast_info_join_rows_emitted);
    print_count("q6a_query_output_rows", trace.query_output_rows);
#endif
    return result;
}