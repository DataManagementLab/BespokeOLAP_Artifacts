#include "query_q2a.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "trace.hpp"

namespace q2a_internal {

struct IntFilter {
    std::vector<int32_t> values;
    bool allow_null = false;
};

struct StringFilter {
    std::vector<int32_t> values;
    bool allow_null = false;
};

bool contains_value(const std::vector<int32_t>& values, int32_t value) {
    for (const auto& entry : values) {
        if (entry == value) {
            return true;
        }
    }
    return false;
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
            if (!contains_value(filter.values, found_id)) {
                filter.values.push_back(found_id);
            }
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
    return filter;
}

bool matches_int(const ColumnData& col, size_t idx, const IntFilter& filter) {
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    if (filter.values.empty()) {
        return false;
    }
    return contains_value(filter.values, col.i32[idx]);
}

bool matches_string(const ColumnData& col, size_t idx, const StringFilter& filter) {
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    if (filter.values.empty()) {
        return false;
    }
    const int32_t id = col.str_ids[idx];
    return contains_value(filter.values, id);
}

std::vector<uint8_t> build_allowed_ids_from_dim(const TableData& table,
                                                const std::string& id_col_name,
                                                const std::string& str_col_name,
                                                const StringFilter& filter) {
    if (filter.values.empty() && !filter.allow_null) {
        return {};
    }
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
            if (static_cast<size_t>(id) >= ids.size()) {
                ids.resize(static_cast<size_t>(id) + 1U, 0);
            }
            ids[static_cast<size_t>(id)] = 1U;
        }
    }
    return ids;
}

std::vector<uint8_t> build_valid_ids(const TableData& table, const std::string& id_col_name) {
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
        if (static_cast<size_t>(id) >= ids.size()) {
            ids.resize(static_cast<size_t>(id) + 1U, 0);
        }
        ids[static_cast<size_t>(id)] = 1U;
    }
    return ids;
}

bool id_allowed(const std::vector<uint8_t>& ids, int32_t id) {
    return id >= 0 && static_cast<size_t>(id) < ids.size() && ids[static_cast<size_t>(id)] != 0;
}

}  // namespace q2a_internal

int64_t run_q2a(const Database& db, const Q2aArgs& args) {
    int64_t total_count = 0;
#ifdef TRACE
    struct Q2aTrace {
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
        uint64_t movie_keyword_rows_scanned = 0;
        uint64_t movie_keyword_rows_emitted = 0;
        uint64_t movie_keyword_agg_rows_in = 0;
        uint64_t movie_keyword_groups_created = 0;
        uint64_t movie_keyword_agg_rows_emitted = 0;
        uint64_t title_rows_scanned = 0;
        uint64_t title_rows_emitted = 0;
        uint64_t title_join_build_rows_in = 0;
        uint64_t title_join_probe_rows_in = 0;
        uint64_t title_join_rows_emitted = 0;
        uint64_t query_output_rows = 0;
    } trace;
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto info_type_filter1 = q2a_internal::build_int_filter(args.ID1);
    const auto info_type_filter2 = q2a_internal::build_int_filter(args.ID2);
    const auto kind_filter = q2a_internal::build_string_filter(db.string_pool, args.KIND);
    const auto role_filter = q2a_internal::build_string_filter(db.string_pool, args.ROLE);
    const auto gender_filter = q2a_internal::build_string_filter(db.string_pool, args.GENDER);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& name = db.tables.at("name");
    const auto& info_type = db.tables.at("info_type");
    const auto& keyword = db.tables.at("keyword");

    const auto allowed_kind_ids =
        q2a_internal::build_allowed_ids_from_dim(kind_type, "id", "kind", kind_filter);
    const auto allowed_role_ids =
        q2a_internal::build_allowed_ids_from_dim(role_type, "id", "role", role_filter);
    const auto allowed_person_ids =
        q2a_internal::build_allowed_ids_from_dim(name, "id", "gender", gender_filter);
    const auto valid_info_type_ids = q2a_internal::build_valid_ids(info_type, "id");
    const auto valid_keyword_ids = q2a_internal::build_valid_ids(keyword, "id");

    if (allowed_kind_ids.empty() || allowed_role_ids.empty() || allowed_person_ids.empty()) {
        return 0;
    }

    if (info_type_filter1.values.empty() || info_type_filter2.values.empty()) {
        return 0;
    }

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");

    std::vector<int32_t> title_ids;
    title_ids.reserve(static_cast<size_t>(title.row_count / 4));
    int32_t max_movie_id = -1;

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_title_scan");
#endif
    for (int64_t i = 0; i < title.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.title_rows_scanned;
#endif
        if (t_id.is_null[idx] != 0 || t_year.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = t_id.i32[idx];
        if (movie_id < 0) {
            continue;
        }
        const int32_t year = t_year.i32[idx];
        if (year > year1 || year < year2) {
            continue;
        }
        if (t_kind_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t kind_id = t_kind_id.i32[idx];
        if (!q2a_internal::id_allowed(allowed_kind_ids, kind_id)) {
            continue;
        }

#ifdef TRACE
        ++trace.title_rows_emitted;
#endif
        title_ids.push_back(movie_id);
        if (movie_id > max_movie_id) {
            max_movie_id = movie_id;
        }
    }
#ifdef TRACE
    }
#endif

    if (title_ids.empty()) {
        return 0;
    }

    std::vector<uint8_t> title_match(static_cast<size_t>(max_movie_id) + 1U, 0);
    for (const auto movie_id : title_ids) {
        title_match[static_cast<size_t>(movie_id)] = 1U;
    }

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const auto info_filter1 =
        q2a_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO1);
    const auto info_filter2 =
        q2a_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO2);

    if (info_filter1.values.empty() || info_filter2.values.empty()) {
        return 0;
    }

    std::vector<int32_t> info_count1(static_cast<size_t>(max_movie_id) + 1U, 0);
    std::vector<int32_t> info_count2(static_cast<size_t>(max_movie_id) + 1U, 0);

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_movie_info_scan");
#endif
    for (int64_t i = 0; i < movie_info.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if (mi_movie_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mi_movie_id.i32[idx];
        if (!q2a_internal::id_allowed(title_match, movie_id)) {
            continue;
        }
        const int32_t info_type_id = mi_info_type_id.i32[idx];
        if (!q2a_internal::id_allowed(valid_info_type_ids, info_type_id)) {
            continue;
        }
        if (q2a_internal::matches_int(mi_info_type_id, idx, info_type_filter1) &&
            q2a_internal::matches_string(mi_info, idx, info_filter1)) {
#ifdef TRACE
            ++trace.movie_info_rows_emitted_info1;
            ++trace.movie_info_agg_rows_in_info1;
            if (info_count1[static_cast<size_t>(movie_id)] == 0) {
                ++trace.movie_info_groups_created_info1;
            }
#endif
            ++info_count1[static_cast<size_t>(movie_id)];
        }
        if (q2a_internal::matches_int(mi_info_type_id, idx, info_type_filter2) &&
            q2a_internal::matches_string(mi_info, idx, info_filter2)) {
#ifdef TRACE
            ++trace.movie_info_rows_emitted_info2;
            ++trace.movie_info_agg_rows_in_info2;
            if (info_count2[static_cast<size_t>(movie_id)] == 0) {
                ++trace.movie_info_groups_created_info2;
            }
#endif
            ++info_count2[static_cast<size_t>(movie_id)];
        }
    }
#ifdef TRACE
    }
#endif

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");

    std::vector<int32_t> cast_counts(static_cast<size_t>(max_movie_id) + 1U, 0);

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_cast_info_scan");
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
        if (!q2a_internal::id_allowed(title_match, movie_id)) {
            continue;
        }
        const int32_t person_id = ci_person_id.i32[idx];
        const int32_t role_id = ci_role_id.i32[idx];

        if (!q2a_internal::id_allowed(allowed_role_ids, role_id)) {
            continue;
        }
        if (!q2a_internal::id_allowed(allowed_person_ids, person_id)) {
            continue;
        }
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.cast_info_agg_rows_in;
        if (cast_counts[static_cast<size_t>(movie_id)] == 0) {
            ++trace.cast_info_groups_created;
        }
#endif
        ++cast_counts[static_cast<size_t>(movie_id)];
    }
#ifdef TRACE
    }
#endif

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");

    std::vector<int32_t> keyword_counts(static_cast<size_t>(max_movie_id) + 1U, 0);
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_movie_keyword_scan");
#endif
    for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.movie_keyword_rows_scanned;
#endif
        if (mk_movie_id.is_null[idx] != 0 || mk_keyword_id.is_null[idx] != 0) {
            continue;
        }
        const int32_t movie_id = mk_movie_id.i32[idx];
        if (!q2a_internal::id_allowed(title_match, movie_id)) {
            continue;
        }
        const int32_t keyword_id = mk_keyword_id.i32[idx];
        if (!q2a_internal::id_allowed(valid_keyword_ids, keyword_id)) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_keyword_rows_emitted;
        ++trace.movie_keyword_agg_rows_in;
        if (keyword_counts[static_cast<size_t>(movie_id)] == 0) {
            ++trace.movie_keyword_groups_created;
        }
#endif
        ++keyword_counts[static_cast<size_t>(movie_id)];
    }
#ifdef TRACE
    }
#endif

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2a_title_join");
#endif
    for (const auto movie_id : title_ids) {
#ifdef TRACE
        ++trace.title_join_probe_rows_in;
#endif
        const int32_t cast_count = cast_counts[static_cast<size_t>(movie_id)];
        if (cast_count == 0) {
            continue;
        }
        const int32_t info1_count = info_count1[static_cast<size_t>(movie_id)];
        if (info1_count == 0) {
            continue;
        }
        const int32_t info2_count = info_count2[static_cast<size_t>(movie_id)];
        if (info2_count == 0) {
            continue;
        }
        const int32_t keyword_count = keyword_counts[static_cast<size_t>(movie_id)];
        if (keyword_count == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.title_join_rows_emitted;
#endif
        total_count += static_cast<int64_t>(cast_count) * info1_count * info2_count *
                       keyword_count;
    }
#ifdef TRACE
    }
    trace.movie_info_agg_rows_emitted_info1 =
        static_cast<uint64_t>(std::count_if(info_count1.begin(), info_count1.end(),
                                            [](int32_t value) { return value != 0; }));
    trace.movie_info_agg_rows_emitted_info2 =
        static_cast<uint64_t>(std::count_if(info_count2.begin(), info_count2.end(),
                                            [](int32_t value) { return value != 0; }));
    trace.cast_info_agg_rows_emitted =
        static_cast<uint64_t>(std::count_if(cast_counts.begin(), cast_counts.end(),
                                            [](int32_t value) { return value != 0; }));
    trace.movie_keyword_agg_rows_emitted =
        static_cast<uint64_t>(std::count_if(keyword_counts.begin(), keyword_counts.end(),
                                            [](int32_t value) { return value != 0; }));
    trace.title_join_build_rows_in =
        trace.movie_info_agg_rows_emitted_info1 + trace.movie_info_agg_rows_emitted_info2 +
        trace.cast_info_agg_rows_emitted + trace.movie_keyword_agg_rows_emitted;
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q2a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q2a_movie_info_scan_rows_emitted_info1", trace.movie_info_rows_emitted_info1);
    print_count("q2a_movie_info_scan_rows_emitted_info2", trace.movie_info_rows_emitted_info2);
    print_count("q2a_movie_info_info1_agg_rows_in", trace.movie_info_agg_rows_in_info1);
    print_count("q2a_movie_info_info1_groups_created", trace.movie_info_groups_created_info1);
    print_count("q2a_movie_info_info1_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info1);
    print_count("q2a_movie_info_info2_agg_rows_in", trace.movie_info_agg_rows_in_info2);
    print_count("q2a_movie_info_info2_groups_created", trace.movie_info_groups_created_info2);
    print_count("q2a_movie_info_info2_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info2);
    print_count("q2a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q2a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q2a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q2a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q2a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q2a_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q2a_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q2a_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q2a_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q2a_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q2a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q2a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q2a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q2a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q2a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q2a_query_output_rows", trace.query_output_rows);
#endif
    return total_count;
}