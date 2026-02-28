#include "query_q2c.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

namespace q2c_internal {

struct IntFilter {
    std::vector<int32_t> values;
    std::unordered_set<int32_t> hash_values;
    bool use_hash = false;
    bool allow_null = false;

    bool matches(int32_t value) const {
        if (use_hash) {
            return hash_values.find(value) != hash_values.end();
        }
        for (int32_t entry : values) {
            if (entry == value) {
                return true;
            }
        }
        return false;
    }
};

struct StringFilter {
    std::vector<int32_t> values;
    std::unordered_set<int32_t> hash_values;
    bool use_hash = false;
    bool allow_null = false;

    bool matches(int32_t value) const {
        if (use_hash) {
            return hash_values.find(value) != hash_values.end();
        }
        for (int32_t entry : values) {
            if (entry == value) {
                return true;
            }
        }
        return false;
    }
};

struct IdBitmap {
    std::vector<uint8_t> allowed;
    size_t count = 0;

    bool contains(int32_t id) const {
        if (id < 0) {
            return false;
        }
        const size_t idx = static_cast<size_t>(id);
        return idx < allowed.size() && allowed[idx] != 0;
    }
};

IntFilter build_int_filter(const std::vector<std::string>& values, const IdBitmap* allowed) {
    IntFilter filter;
    filter.values.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        try {
            const int32_t id = static_cast<int32_t>(std::stoi(val));
            if (allowed && !allowed->contains(id)) {
                continue;
            }
            filter.values.push_back(id);
        } catch (const std::exception&) {
            continue;
        }
    }
    if (filter.values.size() > 8) {
        filter.use_hash = true;
        filter.hash_values.reserve(filter.values.size());
        for (int32_t entry : filter.values) {
            filter.hash_values.insert(entry);
        }
    }
    return filter;
}

StringFilter build_string_filter(const StringPool& pool, const std::vector<std::string>& values) {
    StringFilter filter;
    filter.values.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        int32_t id = -1;
        if (pool.try_get_id(val, id)) {
            filter.values.push_back(id);
        }
    }
    if (filter.values.size() > 8) {
        filter.use_hash = true;
        filter.hash_values.reserve(filter.values.size());
        for (int32_t entry : filter.values) {
            filter.hash_values.insert(entry);
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
            filter.values.push_back(found_id);
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
    if (filter.values.size() > 8) {
        filter.use_hash = true;
        filter.hash_values.reserve(filter.values.size());
        for (int32_t entry : filter.values) {
            filter.hash_values.insert(entry);
        }
    }
    return filter;
}

bool matches_int(const ColumnData& col, size_t idx, const IntFilter& filter) {
    if (col.has_nulls && col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    if (filter.values.empty()) {
        return false;
    }
    return filter.matches(col.i32[idx]);
}

bool matches_string(const ColumnData& col, size_t idx, const StringFilter& filter) {
    if (col.has_nulls && col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    if (filter.values.empty()) {
        return false;
    }
    return filter.matches(col.str_ids[idx]);
}

inline bool matches_int_value(const IntFilter& filter, int32_t value) {
    if (filter.values.empty()) {
        return false;
    }
    return filter.matches(value);
}

IdBitmap build_allowed_ids_from_dim(const TableData& table,
                                    const std::string& id_col_name,
                                    const std::string& str_col_name,
                                    const StringFilter& filter) {
    IdBitmap ids;
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        if (!matches_string(str_col, static_cast<size_t>(i), filter)) {
            continue;
        }
        const int32_t id = id_col.i32[static_cast<size_t>(i)];
        if (id < 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(id);
        if (idx >= ids.allowed.size()) {
            ids.allowed.resize(idx + 1, 0);
        }
        if (ids.allowed[idx] == 0) {
            ids.allowed[idx] = 1;
            ++ids.count;
        }
    }
    return ids;
}

IdBitmap build_id_bitmap(const TableData& table, const std::string& id_col_name) {
    IdBitmap ids;
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
        if (idx >= ids.allowed.size()) {
            ids.allowed.resize(idx + 1, 0);
        }
        if (ids.allowed[idx] == 0) {
            ids.allowed[idx] = 1;
            ++ids.count;
        }
    }
    return ids;
}

}  // namespace q2c_internal

int64_t run_q2c(const Database& db, const Q2cArgs& args) {
    int64_t total_count = 0;
#ifdef TRACE
    struct Q2cTrace {
        TraceRecorder recorder;
        uint64_t movie_info_rows_scanned = 0;
        uint64_t movie_info_rows_emitted_info1 = 0;
        uint64_t movie_info_rows_emitted_info2 = 0;
        uint64_t movie_info_agg_rows_in_info1 = 0;
        uint64_t movie_info_agg_rows_in_info2 = 0;
        uint64_t movie_info_groups_created_info1 = 0;
        uint64_t movie_info_groups_created_info2 = 0;
        uint64_t movie_info_agg_rows_emitted_info1 = 0;
        uint64_t movie_info_agg_rows_emitted_info2 = 0;
        uint64_t cast_info_rows_scanned = 0;
        uint64_t cast_info_rows_emitted = 0;
        uint64_t cast_info_agg_rows_in = 0;
        uint64_t cast_info_groups_created = 0;
        uint64_t cast_info_agg_rows_emitted = 0;
        uint64_t title_rows_scanned = 0;
        uint64_t title_rows_emitted = 0;
        uint64_t title_join_build_rows_in = 0;
        uint64_t title_join_probe_rows_in = 0;
        uint64_t title_join_rows_emitted = 0;
        uint64_t query_output_rows = 0;
    } trace;
    {
        PROFILE_SCOPE(&trace.recorder, "q2c_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto kind_filter = q2c_internal::build_string_filter(db.string_pool, args.KIND);
    const auto role_filter = q2c_internal::build_string_filter(db.string_pool, args.ROLE);
    const auto gender_filter = q2c_internal::build_string_filter(db.string_pool, args.GENDER);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& name = db.tables.at("name");
    const auto& info_type = db.tables.at("info_type");
    const auto info_type_ids = q2c_internal::build_id_bitmap(info_type, "id");
    const auto info_type_filter1 = q2c_internal::build_int_filter(args.ID1, &info_type_ids);
    const auto info_type_filter2 = q2c_internal::build_int_filter(args.ID2, &info_type_ids);

    const auto allowed_kind_ids =
        q2c_internal::build_allowed_ids_from_dim(kind_type, "id", "kind", kind_filter);
    const auto allowed_role_ids =
        q2c_internal::build_allowed_ids_from_dim(role_type, "id", "role", role_filter);
    const auto allowed_person_ids =
        q2c_internal::build_allowed_ids_from_dim(name, "id", "gender", gender_filter);

    bool skip_query = allowed_kind_ids.count == 0 || allowed_role_ids.count == 0 ||
                      allowed_person_ids.count == 0 || info_type_filter1.values.empty() ||
                      info_type_filter2.values.empty();

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const auto info_filter1 =
        q2c_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO1);
    const auto info_filter2 =
        q2c_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO2);

    if (info_filter1.values.empty() || info_filter2.values.empty()) {
        skip_query = true;
    }

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    const auto& t_title = title.columns.at("title");
    const auto title_filter =
        q2c_internal::build_string_filter_for_column(db.string_pool, t_title, args.TITLE);

    if (title_filter.values.empty()) {
        skip_query = true;
    }

    q2c_internal::IdBitmap allowed_movie_ids;
    std::vector<int32_t> title_movie_ids;

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2c_title_scan");
#endif
    if (!skip_query) {
        title_movie_ids.reserve(static_cast<size_t>(title.row_count / 8));
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if (t_id.is_null[idx] != 0 || t_year.is_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = t_id.i32[idx];
            const int32_t year = t_year.i32[idx];
            if (year > year1 || year < year2) {
                continue;
            }
            if (t_kind_id.is_null[idx] != 0) {
                continue;
            }
            if (!allowed_kind_ids.contains(t_kind_id.i32[idx])) {
                continue;
            }
            if (!q2c_internal::matches_string(t_title, idx, title_filter)) {
                continue;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
#endif
            if (movie_id >= 0) {
                const size_t midx = static_cast<size_t>(movie_id);
                if (midx >= allowed_movie_ids.allowed.size()) {
                    allowed_movie_ids.allowed.resize(midx + 1, 0);
                }
                if (allowed_movie_ids.allowed[midx] == 0) {
                    allowed_movie_ids.allowed[midx] = 1;
                    ++allowed_movie_ids.count;
                    title_movie_ids.push_back(movie_id);
                }
            }
        }
    }
#ifdef TRACE
    }
#endif

    const bool run_scans = !skip_query && allowed_movie_ids.count != 0;
    std::vector<int64_t> info_count1;
    std::vector<int64_t> info_count2;
    std::vector<int64_t> cast_counts;

    if (run_scans) {
        info_count1.assign(allowed_movie_ids.allowed.size(), 0);
        info_count2.assign(allowed_movie_ids.allowed.size(), 0);
        cast_counts.assign(allowed_movie_ids.allowed.size(), 0);
        const bool use_range_scan =
            movie_info.sorted_by_movie_id && cast_info.sorted_by_movie_id;

#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q2c_movie_info_scan");
#endif
        const bool mi_movie_has_nulls = mi_movie_id.has_nulls;
        const bool mi_info_type_has_nulls = mi_info_type_id.has_nulls;
        const auto* mi_movie_nulls = mi_movie_id.is_null.data();
        const auto* mi_info_type_nulls = mi_info_type_id.is_null.data();
        const auto* mi_movie_vals = mi_movie_id.i32.data();
        const auto* mi_info_type_vals = mi_info_type_id.i32.data();
        const int64_t mi_row_count = movie_info.row_count;
        const int32_t* mi_begin = mi_movie_vals;
        const int32_t* mi_end = mi_movie_vals + mi_row_count;
        if (use_range_scan) {
            for (int32_t movie_id : title_movie_ids) {
                const int32_t* lower = std::lower_bound(mi_begin, mi_end, movie_id);
                if (lower == mi_end || *lower != movie_id) {
                    continue;
                }
                const int32_t* upper = std::upper_bound(lower, mi_end, movie_id);
                for (const int32_t* ptr = lower; ptr != upper; ++ptr) {
                    const size_t idx = static_cast<size_t>(ptr - mi_begin);
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    if (mi_movie_has_nulls && mi_movie_nulls[idx] != 0) {
                        continue;
                    }
                    if (mi_info_type_has_nulls && mi_info_type_nulls[idx] != 0) {
                        continue;
                    }
                    const int32_t info_type_id = mi_info_type_vals[idx];
                    const bool matches_info_type1 =
                        q2c_internal::matches_int_value(info_type_filter1, info_type_id);
                    if (matches_info_type1 &&
                        q2c_internal::matches_string(mi_info, idx, info_filter1)) {
#ifdef TRACE
                        ++trace.movie_info_rows_emitted_info1;
                        ++trace.movie_info_agg_rows_in_info1;
#endif
                        ++info_count1[static_cast<size_t>(movie_id)];
#ifdef TRACE
                        if (info_count1[static_cast<size_t>(movie_id)] == 1) {
                            ++trace.movie_info_groups_created_info1;
                        }
#endif
                    }
                    const bool matches_info_type2 =
                        q2c_internal::matches_int_value(info_type_filter2, info_type_id);
                    if (matches_info_type2 &&
                        q2c_internal::matches_string(mi_info, idx, info_filter2)) {
#ifdef TRACE
                        ++trace.movie_info_rows_emitted_info2;
                        ++trace.movie_info_agg_rows_in_info2;
#endif
                        ++info_count2[static_cast<size_t>(movie_id)];
#ifdef TRACE
                        if (info_count2[static_cast<size_t>(movie_id)] == 1) {
                            ++trace.movie_info_groups_created_info2;
                        }
#endif
                    }
                }
            }
        } else {
            const auto* allowed_movie_bitmap = allowed_movie_ids.allowed.data();
            const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
            for (int64_t i = 0; i < mi_row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.movie_info_rows_scanned;
#endif
                if (mi_movie_has_nulls && mi_movie_nulls[idx] != 0) {
                    continue;
                }
                const int32_t movie_id = mi_movie_vals[idx];
                if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_size ||
                    allowed_movie_bitmap[static_cast<size_t>(movie_id)] == 0) {
                    continue;
                }
                if (mi_info_type_has_nulls && mi_info_type_nulls[idx] != 0) {
                    continue;
                }
                const int32_t info_type_id = mi_info_type_vals[idx];
                const bool matches_info_type1 =
                    q2c_internal::matches_int_value(info_type_filter1, info_type_id);
                if (matches_info_type1 &&
                    q2c_internal::matches_string(mi_info, idx, info_filter1)) {
#ifdef TRACE
                    ++trace.movie_info_rows_emitted_info1;
                    ++trace.movie_info_agg_rows_in_info1;
#endif
                    ++info_count1[static_cast<size_t>(movie_id)];
#ifdef TRACE
                    if (info_count1[static_cast<size_t>(movie_id)] == 1) {
                        ++trace.movie_info_groups_created_info1;
                    }
#endif
                }
                const bool matches_info_type2 =
                    q2c_internal::matches_int_value(info_type_filter2, info_type_id);
                if (matches_info_type2 &&
                    q2c_internal::matches_string(mi_info, idx, info_filter2)) {
#ifdef TRACE
                    ++trace.movie_info_rows_emitted_info2;
                    ++trace.movie_info_agg_rows_in_info2;
#endif
                    ++info_count2[static_cast<size_t>(movie_id)];
#ifdef TRACE
                    if (info_count2[static_cast<size_t>(movie_id)] == 1) {
                        ++trace.movie_info_groups_created_info2;
                    }
#endif
                }
            }
        }
#ifdef TRACE
        }
#endif

#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q2c_cast_info_scan");
#endif
        const bool ci_movie_has_nulls = ci_movie_id.has_nulls;
        const bool ci_person_has_nulls = ci_person_id.has_nulls;
        const bool ci_role_has_nulls = ci_role_id.has_nulls;
        const auto* ci_movie_nulls = ci_movie_id.is_null.data();
        const auto* ci_person_nulls = ci_person_id.is_null.data();
        const auto* ci_role_nulls = ci_role_id.is_null.data();
        const auto* ci_movie_vals = ci_movie_id.i32.data();
        const auto* ci_person_vals = ci_person_id.i32.data();
        const auto* ci_role_vals = ci_role_id.i32.data();
        const auto* role_allowed = allowed_role_ids.allowed.data();
        const size_t role_allowed_size = allowed_role_ids.allowed.size();
        const auto* person_allowed = allowed_person_ids.allowed.data();
        const size_t person_allowed_size = allowed_person_ids.allowed.size();
        const int64_t ci_row_count = cast_info.row_count;
        const int32_t* ci_begin = ci_movie_vals;
        const int32_t* ci_end = ci_movie_vals + ci_row_count;
        if (use_range_scan) {
            for (int32_t movie_id : title_movie_ids) {
                const int32_t* lower = std::lower_bound(ci_begin, ci_end, movie_id);
                if (lower == ci_end || *lower != movie_id) {
                    continue;
                }
                const int32_t* upper = std::upper_bound(lower, ci_end, movie_id);
                for (const int32_t* ptr = lower; ptr != upper; ++ptr) {
                    const size_t idx = static_cast<size_t>(ptr - ci_begin);
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    if ((ci_movie_has_nulls && ci_movie_nulls[idx] != 0) ||
                        (ci_person_has_nulls && ci_person_nulls[idx] != 0) ||
                        (ci_role_has_nulls && ci_role_nulls[idx] != 0)) {
                        continue;
                    }
                    const int32_t person_id = ci_person_vals[idx];
                    const int32_t role_id = ci_role_vals[idx];
                    if (role_id < 0 || static_cast<size_t>(role_id) >= role_allowed_size ||
                        role_allowed[static_cast<size_t>(role_id)] == 0) {
                        continue;
                    }
                    if (person_id < 0 || static_cast<size_t>(person_id) >= person_allowed_size ||
                        person_allowed[static_cast<size_t>(person_id)] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
#endif
                    ++cast_counts[static_cast<size_t>(movie_id)];
#ifdef TRACE
                    if (cast_counts[static_cast<size_t>(movie_id)] == 1) {
                        ++trace.cast_info_groups_created;
                    }
#endif
                }
            }
        } else {
            const auto* allowed_movie_bitmap = allowed_movie_ids.allowed.data();
            const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
            for (int64_t i = 0; i < ci_row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                if ((ci_movie_has_nulls && ci_movie_nulls[idx] != 0) ||
                    (ci_person_has_nulls && ci_person_nulls[idx] != 0) ||
                    (ci_role_has_nulls && ci_role_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t movie_id = ci_movie_vals[idx];
                if (movie_id < 0 || static_cast<size_t>(movie_id) >= allowed_movie_size ||
                    allowed_movie_bitmap[static_cast<size_t>(movie_id)] == 0) {
                    continue;
                }
                const int32_t person_id = ci_person_vals[idx];
                const int32_t role_id = ci_role_vals[idx];
                if (role_id < 0 || static_cast<size_t>(role_id) >= role_allowed_size ||
                    role_allowed[static_cast<size_t>(role_id)] == 0) {
                    continue;
                }
                if (person_id < 0 || static_cast<size_t>(person_id) >= person_allowed_size ||
                    person_allowed[static_cast<size_t>(person_id)] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.cast_info_rows_emitted;
                ++trace.cast_info_agg_rows_in;
#endif
                ++cast_counts[static_cast<size_t>(movie_id)];
#ifdef TRACE
                if (cast_counts[static_cast<size_t>(movie_id)] == 1) {
                    ++trace.cast_info_groups_created;
                }
#endif
            }
        }
#ifdef TRACE
        }
#endif

        for (int32_t movie_id : title_movie_ids) {
            const size_t midx = static_cast<size_t>(movie_id);
            const int64_t cast_count = cast_counts[midx];
            if (cast_count == 0) {
                continue;
            }
            const int64_t info1_count = info_count1[midx];
            if (info1_count == 0) {
                continue;
            }
            const int64_t info2_count = info_count2[midx];
            if (info2_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_join_rows_emitted;
#endif
            total_count += cast_count * info1_count * info2_count;
        }
#ifdef TRACE
        trace.movie_info_agg_rows_emitted_info1 = 0;
        trace.movie_info_agg_rows_emitted_info2 = 0;
        trace.cast_info_agg_rows_emitted = 0;
        for (int32_t movie_id : title_movie_ids) {
            const size_t midx = static_cast<size_t>(movie_id);
            if (info_count1[midx] != 0) {
                ++trace.movie_info_agg_rows_emitted_info1;
            }
            if (info_count2[midx] != 0) {
                ++trace.movie_info_agg_rows_emitted_info2;
            }
            if (cast_counts[midx] != 0) {
                ++trace.cast_info_agg_rows_emitted;
            }
        }
        trace.title_join_build_rows_in =
            static_cast<uint64_t>(trace.movie_info_agg_rows_emitted_info1 +
                                  trace.movie_info_agg_rows_emitted_info2 +
                                  trace.cast_info_agg_rows_emitted);
        trace.query_output_rows = 1;
#endif
    }
#ifdef TRACE
    if (!run_scans) {
        trace.query_output_rows = 1;
    }
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q2c_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q2c_movie_info_scan_rows_emitted_info1", trace.movie_info_rows_emitted_info1);
    print_count("q2c_movie_info_scan_rows_emitted_info2", trace.movie_info_rows_emitted_info2);
    print_count("q2c_movie_info_info1_agg_rows_in", trace.movie_info_agg_rows_in_info1);
    print_count("q2c_movie_info_info1_groups_created", trace.movie_info_groups_created_info1);
    print_count("q2c_movie_info_info1_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info1);
    print_count("q2c_movie_info_info2_agg_rows_in", trace.movie_info_agg_rows_in_info2);
    print_count("q2c_movie_info_info2_groups_created", trace.movie_info_groups_created_info2);
    print_count("q2c_movie_info_info2_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info2);
    print_count("q2c_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q2c_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q2c_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q2c_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q2c_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q2c_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q2c_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q2c_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q2c_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q2c_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q2c_query_output_rows", trace.query_output_rows);
#endif
    return total_count;
}