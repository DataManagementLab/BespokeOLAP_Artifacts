#include "query_q2b.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "trace.hpp"

namespace q2b_internal {

inline bool contains_linear(const int32_t* values, size_t size, int32_t value) {
    for (size_t i = 0; i < size; ++i) {
        if (values[i] == value) {
            return true;
        }
    }
    return false;
}

inline size_t lower_bound_i32(const int32_t* data, size_t start, size_t end, int32_t value) {
    while (start < end) {
        const size_t mid = start + ((end - start) >> 1);
        if (data[mid] < value) {
            start = mid + 1;
        } else {
            end = mid;
        }
    }
    return start;
}

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
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    if (filter.values.empty()) {
        return false;
    }
    return filter.matches(col.i32[idx]);
}

bool matches_string(const ColumnData& col, size_t idx, const StringFilter& filter) {
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return filter.allow_null;
    }
    const int32_t id = col.str_ids[idx];
    if (filter.values.empty()) {
        return false;
    }
    return filter.matches(id);
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

}  // namespace q2b_internal

int64_t run_q2b(const Database& db, const Q2bArgs& args) {
    int64_t total_count = 0;
#ifdef TRACE
    struct Q2bTrace {
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
        PROFILE_SCOPE(&trace.recorder, "q2b_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto info_type_filter1 = q2b_internal::build_int_filter(args.ID1);
    const auto info_type_filter2 = q2b_internal::build_int_filter(args.ID2);
    const auto kind_filter = q2b_internal::build_string_filter(db.string_pool, args.KIND);
    const auto role_filter = q2b_internal::build_string_filter(db.string_pool, args.ROLE);
    const auto gender_filter = q2b_internal::build_string_filter(db.string_pool, args.GENDER);
    const auto keyword_filter = q2b_internal::build_string_filter(db.string_pool, args.KEYWORD);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& name = db.tables.at("name");
    const auto& keyword = db.tables.at("keyword");

    const auto allowed_kind_ids =
        q2b_internal::build_allowed_ids_from_dim(kind_type, "id", "kind", kind_filter);
    const auto allowed_role_ids =
        q2b_internal::build_allowed_ids_from_dim(role_type, "id", "role", role_filter);
    const auto allowed_person_ids =
        q2b_internal::build_allowed_ids_from_dim(name, "id", "gender", gender_filter);
    const auto allowed_keyword_ids =
        q2b_internal::build_allowed_ids_from_dim(keyword, "id", "keyword", keyword_filter);
    const bool skip_keyword = allowed_keyword_ids.count == 0;

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const auto info_filter1 =
        q2b_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO1);
    const auto info_filter2 =
        q2b_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO2);
    const bool check_info1 =
        !info_type_filter1.values.empty() && !info_filter1.values.empty();
    const bool check_info2 =
        !info_type_filter2.values.empty() && !info_filter2.values.empty();

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");

    q2b_internal::IdBitmap allowed_movie_ids;
    std::vector<int32_t> title_movie_ids;
    std::vector<uint32_t> keyword_counts;
    uint64_t keyword_movie_match_count = 0;
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2b_movie_keyword_scan");
#endif
    if (!skip_keyword) {
        const size_t keyword_size = static_cast<size_t>(title.row_count) + 1;
        keyword_counts.assign(keyword_size, 0);
        const int32_t* mk_movie_ids = mk_movie_id.i32.data();
        const int32_t* mk_keyword_ids = mk_keyword_id.i32.data();
        const uint8_t* allowed_keyword_flags = allowed_keyword_ids.allowed.data();
        const size_t allowed_keyword_size = allowed_keyword_ids.allowed.size();
        const bool mk_has_nulls = mk_movie_id.has_nulls || mk_keyword_id.has_nulls;
        if (mk_has_nulls) {
            const uint8_t* mk_movie_nulls = mk_movie_id.is_null.data();
            const uint8_t* mk_keyword_nulls = mk_keyword_id.is_null.data();
            for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.movie_keyword_rows_scanned;
#endif
                if ((mk_movie_nulls[idx] | mk_keyword_nulls[idx]) != 0) {
                    continue;
                }
                const int32_t keyword_id = mk_keyword_ids[idx];
                const uint32_t kidx = static_cast<uint32_t>(keyword_id);
                if (kidx >= allowed_keyword_size || allowed_keyword_flags[kidx] == 0) {
                    continue;
                }
                const int32_t movie_id = mk_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t midx = static_cast<size_t>(movie_id);
                if (midx >= keyword_counts.size()) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_keyword_rows_emitted;
                ++trace.movie_keyword_agg_rows_in;
#endif
                ++keyword_counts[midx];
#ifdef TRACE
                if (keyword_counts[midx] == 1) {
                    ++trace.movie_keyword_groups_created;
                }
#endif
            }
        } else {
            for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.movie_keyword_rows_scanned;
#endif
                const int32_t keyword_id = mk_keyword_ids[idx];
                const uint32_t kidx = static_cast<uint32_t>(keyword_id);
                if (kidx >= allowed_keyword_size || allowed_keyword_flags[kidx] == 0) {
                    continue;
                }
                const int32_t movie_id = mk_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t midx = static_cast<size_t>(movie_id);
                if (midx >= keyword_counts.size()) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_keyword_rows_emitted;
                ++trace.movie_keyword_agg_rows_in;
#endif
                ++keyword_counts[midx];
#ifdef TRACE
                if (keyword_counts[midx] == 1) {
                    ++trace.movie_keyword_groups_created;
                }
#endif
            }
        }
        for (uint32_t count : keyword_counts) {
            if (count != 0) {
                ++keyword_movie_match_count;
            }
        }
    }
#ifdef TRACE
    }
#endif
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q2b_title_scan");
#endif
    if (!skip_keyword && keyword_movie_match_count != 0) {
        if (allowed_movie_ids.allowed.empty()) {
            allowed_movie_ids.allowed.resize(static_cast<size_t>(title.row_count) + 1, 0);
        }
        title_movie_ids.reserve(static_cast<size_t>(title.row_count / 8));
        const int32_t* t_id_data = t_id.i32.data();
        const int32_t* t_year_data = t_year.i32.data();
        const int32_t* t_kind_data = t_kind_id.i32.data();
        const uint8_t* allowed_kind_flags = allowed_kind_ids.allowed.data();
        const size_t allowed_kind_size = allowed_kind_ids.allowed.size();
        const bool title_has_nulls =
            t_id.has_nulls || t_year.has_nulls || t_kind_id.has_nulls;
        if (title_has_nulls) {
            const uint8_t* t_id_nulls = t_id.is_null.data();
            const uint8_t* t_year_nulls = t_year.is_null.data();
            const uint8_t* t_kind_nulls = t_kind_id.is_null.data();
            for (int64_t i = 0; i < title.row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.title_rows_scanned;
#endif
                if ((t_id_nulls[idx] | t_year_nulls[idx]) != 0) {
                    continue;
                }
                const int32_t movie_id = t_id_data[idx];
                const int32_t year = t_year_data[idx];
                if (year > year1 || year < year2) {
                    continue;
                }
                if (t_kind_nulls[idx] != 0) {
                    continue;
                }
                const int32_t kind_id = t_kind_data[idx];
                const uint32_t kidx = static_cast<uint32_t>(kind_id);
                if (kidx >= allowed_kind_size || allowed_kind_flags[kidx] == 0) {
                    continue;
                }
                if (movie_id >= 0) {
                    const size_t midx = static_cast<size_t>(movie_id);
                    if (midx >= keyword_counts.size() || keyword_counts[midx] == 0) {
                        continue;
                    }
                    if (midx >= allowed_movie_ids.allowed.size()) {
                        allowed_movie_ids.allowed.resize(midx + 1, 0);
                    }
                    if (allowed_movie_ids.allowed[midx] == 0) {
                        allowed_movie_ids.allowed[midx] = 1;
                        ++allowed_movie_ids.count;
                        title_movie_ids.push_back(movie_id);
                    }
                }
#ifdef TRACE
                ++trace.title_rows_emitted;
                ++trace.title_join_probe_rows_in;
#endif
            }
        } else {
            for (int64_t i = 0; i < title.row_count; ++i) {
                const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                ++trace.title_rows_scanned;
#endif
                const int32_t movie_id = t_id_data[idx];
                const int32_t year = t_year_data[idx];
                if (year > year1 || year < year2) {
                    continue;
                }
                const int32_t kind_id = t_kind_data[idx];
                const uint32_t kidx = static_cast<uint32_t>(kind_id);
                if (kidx >= allowed_kind_size || allowed_kind_flags[kidx] == 0) {
                    continue;
                }
                if (movie_id >= 0) {
                    const size_t midx = static_cast<size_t>(movie_id);
                    if (midx >= keyword_counts.size() || keyword_counts[midx] == 0) {
                        continue;
                    }
                    if (midx >= allowed_movie_ids.allowed.size()) {
                        allowed_movie_ids.allowed.resize(midx + 1, 0);
                    }
                    if (allowed_movie_ids.allowed[midx] == 0) {
                        allowed_movie_ids.allowed[midx] = 1;
                        ++allowed_movie_ids.count;
                        title_movie_ids.push_back(movie_id);
                    }
                }
#ifdef TRACE
                ++trace.title_rows_emitted;
                ++trace.title_join_probe_rows_in;
#endif
            }
        }
    }
#ifdef TRACE
    }
#endif

    const bool run_scans = !skip_keyword && keyword_movie_match_count != 0 &&
                           allowed_movie_ids.count != 0 &&
                           allowed_role_ids.count != 0 && allowed_person_ids.count != 0 &&
                           check_info1 && check_info2;
    std::vector<uint32_t> info_count1;
    std::vector<uint32_t> info_count2;
    std::vector<uint32_t> cast_counts;
    if (run_scans) {
        info_count1.assign(allowed_movie_ids.allowed.size(), 0);
        info_count2.assign(allowed_movie_ids.allowed.size(), 0);
        cast_counts.assign(allowed_movie_ids.allowed.size(), 0);

        if (!title_movie_ids.empty()) {
            std::sort(title_movie_ids.begin(), title_movie_ids.end());
        }

        {
#ifdef TRACE
            PROFILE_SCOPE(&trace.recorder, "q2b_movie_info_scan");
#endif
            const int32_t* mi_movie_ids = mi_movie_id.i32.data();
            const int32_t* mi_info_type_ids = mi_info_type_id.i32.data();
            const int32_t* mi_info_ids = mi_info.str_ids.data();
            const int32_t* info_type_values1 = info_type_filter1.values.data();
            const size_t info_type_size1 = info_type_filter1.values.size();
            const int32_t* info_type_values2 = info_type_filter2.values.data();
            const size_t info_type_size2 = info_type_filter2.values.size();
            const int32_t* info_values1 = info_filter1.values.data();
            const size_t info_size1 = info_filter1.values.size();
            const int32_t* info_values2 = info_filter2.values.data();
            const size_t info_size2 = info_filter2.values.size();
            const bool info1_single = info_type_size1 == 1 && info_size1 == 1 &&
                                      !info_type_filter1.use_hash && !info_filter1.use_hash;
            const bool info2_single = info_type_size2 == 1 && info_size2 == 1 &&
                                      !info_type_filter2.use_hash && !info_filter2.use_hash;
            const bool mi_has_nulls =
                mi_movie_id.has_nulls || mi_info_type_id.has_nulls || mi_info.has_nulls;
            const size_t mi_row_count = static_cast<size_t>(movie_info.row_count);
            const bool use_sorted_scan = movie_info.sorted_by_movie_id && !title_movie_ids.empty();
            if (use_sorted_scan) {
                const uint8_t* mi_movie_nulls = mi_movie_id.is_null.data();
                const uint8_t* mi_info_type_nulls = mi_info_type_id.is_null.data();
                const uint8_t* mi_info_nulls = mi_info.is_null.data();
                size_t search_pos = 0;
                for (int32_t movie_id : title_movie_ids) {
                    search_pos =
                        q2b_internal::lower_bound_i32(mi_movie_ids, search_pos, mi_row_count,
                                                      movie_id);
                    if (search_pos >= mi_row_count || mi_movie_ids[search_pos] != movie_id) {
                        continue;
                    }
                    const size_t midx = static_cast<size_t>(movie_id);
                    size_t idx = search_pos;
                    while (idx < mi_row_count && mi_movie_ids[idx] == movie_id) {
#ifdef TRACE
                        ++trace.movie_info_rows_scanned;
#endif
                        if (mi_has_nulls) {
                            if (mi_movie_nulls[idx] != 0 ||
                                (mi_info_type_nulls[idx] | mi_info_nulls[idx]) != 0) {
                                ++idx;
                                continue;
                            }
                        }
                        const int32_t info_type_id = mi_info_type_ids[idx];
                        const int32_t info_id = mi_info_ids[idx];
                        if (info1_single) {
                            if (info_type_id == info_type_values1[0] &&
                                info_id == info_values1[0]) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info1;
                                ++trace.movie_info_agg_rows_in_info1;
#endif
                                ++info_count1[midx];
#ifdef TRACE
                                if (info_count1[midx] == 1) {
                                    ++trace.movie_info_groups_created_info1;
                                }
#endif
                            }
                        } else {
                            const bool type_match =
                                info_type_filter1.use_hash
                                    ? (info_type_filter1.hash_values.find(info_type_id) !=
                                       info_type_filter1.hash_values.end())
                                    : q2b_internal::contains_linear(info_type_values1,
                                                                    info_type_size1,
                                                                    info_type_id);
                            if (type_match) {
                                const bool info_match =
                                    info_filter1.use_hash
                                        ? (info_filter1.hash_values.find(info_id) !=
                                           info_filter1.hash_values.end())
                                        : q2b_internal::contains_linear(info_values1, info_size1,
                                                                        info_id);
                                if (info_match) {
#ifdef TRACE
                                    ++trace.movie_info_rows_emitted_info1;
                                    ++trace.movie_info_agg_rows_in_info1;
#endif
                                    ++info_count1[midx];
#ifdef TRACE
                                    if (info_count1[midx] == 1) {
                                        ++trace.movie_info_groups_created_info1;
                                    }
#endif
                                }
                            }
                        }
                        if (info2_single) {
                            if (info_type_id == info_type_values2[0] &&
                                info_id == info_values2[0]) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info2;
                                ++trace.movie_info_agg_rows_in_info2;
#endif
                                ++info_count2[midx];
#ifdef TRACE
                                if (info_count2[midx] == 1) {
                                    ++trace.movie_info_groups_created_info2;
                                }
#endif
                            }
                        } else {
                            const bool type_match =
                                info_type_filter2.use_hash
                                    ? (info_type_filter2.hash_values.find(info_type_id) !=
                                       info_type_filter2.hash_values.end())
                                    : q2b_internal::contains_linear(info_type_values2,
                                                                    info_type_size2,
                                                                    info_type_id);
                            if (type_match) {
                                const bool info_match =
                                    info_filter2.use_hash
                                        ? (info_filter2.hash_values.find(info_id) !=
                                           info_filter2.hash_values.end())
                                        : q2b_internal::contains_linear(info_values2, info_size2,
                                                                        info_id);
                                if (info_match) {
#ifdef TRACE
                                    ++trace.movie_info_rows_emitted_info2;
                                    ++trace.movie_info_agg_rows_in_info2;
#endif
                                    ++info_count2[midx];
#ifdef TRACE
                                    if (info_count2[midx] == 1) {
                                        ++trace.movie_info_groups_created_info2;
                                    }
#endif
                                }
                            }
                        }
                        ++idx;
                    }
                    search_pos = idx;
                }
            } else if (mi_has_nulls) {
                const uint8_t* mi_movie_nulls = mi_movie_id.is_null.data();
                const uint8_t* mi_info_type_nulls = mi_info_type_id.is_null.data();
                const uint8_t* mi_info_nulls = mi_info.is_null.data();
                const uint8_t* allowed_movie_flags = allowed_movie_ids.allowed.data();
                const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
                for (int64_t i = 0; i < movie_info.row_count; ++i) {
                    const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    if (mi_movie_nulls[idx] != 0) {
                        continue;
                    }
                    if ((mi_info_type_nulls[idx] | mi_info_nulls[idx]) != 0) {
                        continue;
                    }
                    const int32_t movie_id = mi_movie_ids[idx];
                    const uint32_t midx = static_cast<uint32_t>(movie_id);
                    if (midx >= allowed_movie_size || allowed_movie_flags[midx] == 0) {
                        continue;
                    }
                    const int32_t info_type_id = mi_info_type_ids[idx];
                    const int32_t info_id = mi_info_ids[idx];
                    if (info1_single) {
                        if (info_type_id == info_type_values1[0] &&
                            info_id == info_values1[0]) {
#ifdef TRACE
                            ++trace.movie_info_rows_emitted_info1;
                            ++trace.movie_info_agg_rows_in_info1;
#endif
                            ++info_count1[midx];
#ifdef TRACE
                            if (info_count1[midx] == 1) {
                                ++trace.movie_info_groups_created_info1;
                            }
#endif
                        }
                    } else {
                        const bool type_match =
                            info_type_filter1.use_hash
                                ? (info_type_filter1.hash_values.find(info_type_id) !=
                                   info_type_filter1.hash_values.end())
                                : q2b_internal::contains_linear(info_type_values1,
                                                                info_type_size1, info_type_id);
                        if (type_match) {
                            const bool info_match =
                                info_filter1.use_hash
                                    ? (info_filter1.hash_values.find(info_id) !=
                                       info_filter1.hash_values.end())
                                    : q2b_internal::contains_linear(info_values1, info_size1,
                                                                    info_id);
                            if (info_match) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info1;
                                ++trace.movie_info_agg_rows_in_info1;
#endif
                                ++info_count1[midx];
#ifdef TRACE
                                if (info_count1[midx] == 1) {
                                    ++trace.movie_info_groups_created_info1;
                                }
#endif
                            }
                        }
                    }
                    if (info2_single) {
                        if (info_type_id == info_type_values2[0] &&
                            info_id == info_values2[0]) {
#ifdef TRACE
                            ++trace.movie_info_rows_emitted_info2;
                            ++trace.movie_info_agg_rows_in_info2;
#endif
                            ++info_count2[midx];
#ifdef TRACE
                            if (info_count2[midx] == 1) {
                                ++trace.movie_info_groups_created_info2;
                            }
#endif
                        }
                    } else {
                        const bool type_match =
                            info_type_filter2.use_hash
                                ? (info_type_filter2.hash_values.find(info_type_id) !=
                                   info_type_filter2.hash_values.end())
                                : q2b_internal::contains_linear(info_type_values2,
                                                                info_type_size2, info_type_id);
                        if (type_match) {
                            const bool info_match =
                                info_filter2.use_hash
                                    ? (info_filter2.hash_values.find(info_id) !=
                                       info_filter2.hash_values.end())
                                    : q2b_internal::contains_linear(info_values2, info_size2,
                                                                    info_id);
                            if (info_match) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info2;
                                ++trace.movie_info_agg_rows_in_info2;
#endif
                                ++info_count2[midx];
#ifdef TRACE
                                if (info_count2[midx] == 1) {
                                    ++trace.movie_info_groups_created_info2;
                                }
#endif
                            }
                        }
                    }
                }
            } else {
                const uint8_t* allowed_movie_flags = allowed_movie_ids.allowed.data();
                const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
                for (int64_t i = 0; i < movie_info.row_count; ++i) {
                    const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                    ++trace.movie_info_rows_scanned;
#endif
                    const int32_t movie_id = mi_movie_ids[idx];
                    const uint32_t midx = static_cast<uint32_t>(movie_id);
                    if (midx >= allowed_movie_size || allowed_movie_flags[midx] == 0) {
                        continue;
                    }
                    const int32_t info_type_id = mi_info_type_ids[idx];
                    const int32_t info_id = mi_info_ids[idx];
                    if (info1_single) {
                        if (info_type_id == info_type_values1[0] &&
                            info_id == info_values1[0]) {
#ifdef TRACE
                            ++trace.movie_info_rows_emitted_info1;
                            ++trace.movie_info_agg_rows_in_info1;
#endif
                            ++info_count1[midx];
#ifdef TRACE
                            if (info_count1[midx] == 1) {
                                ++trace.movie_info_groups_created_info1;
                            }
#endif
                        }
                    } else {
                        const bool type_match =
                            info_type_filter1.use_hash
                                ? (info_type_filter1.hash_values.find(info_type_id) !=
                                   info_type_filter1.hash_values.end())
                                : q2b_internal::contains_linear(info_type_values1,
                                                                info_type_size1, info_type_id);
                        if (type_match) {
                            const bool info_match =
                                info_filter1.use_hash
                                    ? (info_filter1.hash_values.find(info_id) !=
                                       info_filter1.hash_values.end())
                                    : q2b_internal::contains_linear(info_values1, info_size1,
                                                                    info_id);
                            if (info_match) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info1;
                                ++trace.movie_info_agg_rows_in_info1;
#endif
                                ++info_count1[midx];
#ifdef TRACE
                                if (info_count1[midx] == 1) {
                                    ++trace.movie_info_groups_created_info1;
                                }
#endif
                            }
                        }
                    }
                    if (info2_single) {
                        if (info_type_id == info_type_values2[0] &&
                            info_id == info_values2[0]) {
#ifdef TRACE
                            ++trace.movie_info_rows_emitted_info2;
                            ++trace.movie_info_agg_rows_in_info2;
#endif
                            ++info_count2[midx];
#ifdef TRACE
                            if (info_count2[midx] == 1) {
                                ++trace.movie_info_groups_created_info2;
                            }
#endif
                        }
                    } else {
                        const bool type_match =
                            info_type_filter2.use_hash
                                ? (info_type_filter2.hash_values.find(info_type_id) !=
                                   info_type_filter2.hash_values.end())
                                : q2b_internal::contains_linear(info_type_values2,
                                                                info_type_size2, info_type_id);
                        if (type_match) {
                            const bool info_match =
                                info_filter2.use_hash
                                    ? (info_filter2.hash_values.find(info_id) !=
                                       info_filter2.hash_values.end())
                                    : q2b_internal::contains_linear(info_values2, info_size2,
                                                                    info_id);
                            if (info_match) {
#ifdef TRACE
                                ++trace.movie_info_rows_emitted_info2;
                                ++trace.movie_info_agg_rows_in_info2;
#endif
                                ++info_count2[midx];
#ifdef TRACE
                                if (info_count2[midx] == 1) {
                                    ++trace.movie_info_groups_created_info2;
                                }
#endif
                            }
                        }
                    }
                }
            }
        }

        {
#ifdef TRACE
            PROFILE_SCOPE(&trace.recorder, "q2b_cast_info_scan");
#endif
            const int32_t* ci_movie_ids = ci_movie_id.i32.data();
            const int32_t* ci_person_ids = ci_person_id.i32.data();
            const int32_t* ci_role_ids = ci_role_id.i32.data();
            const uint8_t* allowed_role_flags = allowed_role_ids.allowed.data();
            const size_t allowed_role_size = allowed_role_ids.allowed.size();
            const uint8_t* allowed_person_flags = allowed_person_ids.allowed.data();
            const size_t allowed_person_size = allowed_person_ids.allowed.size();
            const bool ci_has_nulls =
                ci_movie_id.has_nulls || ci_person_id.has_nulls || ci_role_id.has_nulls;
            const size_t ci_row_count = static_cast<size_t>(cast_info.row_count);
            const bool use_sorted_scan = cast_info.sorted_by_movie_id && !title_movie_ids.empty();
            if (use_sorted_scan) {
                const uint8_t* ci_movie_nulls = ci_movie_id.is_null.data();
                const uint8_t* ci_person_nulls = ci_person_id.is_null.data();
                const uint8_t* ci_role_nulls = ci_role_id.is_null.data();
                size_t search_pos = 0;
                for (int32_t movie_id : title_movie_ids) {
                    search_pos =
                        q2b_internal::lower_bound_i32(ci_movie_ids, search_pos, ci_row_count,
                                                      movie_id);
                    if (search_pos >= ci_row_count || ci_movie_ids[search_pos] != movie_id) {
                        continue;
                    }
                    const size_t midx = static_cast<size_t>(movie_id);
                    size_t idx = search_pos;
                    while (idx < ci_row_count && ci_movie_ids[idx] == movie_id) {
#ifdef TRACE
                        ++trace.cast_info_rows_scanned;
#endif
                        if (ci_has_nulls) {
                            if ((ci_movie_nulls[idx] | ci_person_nulls[idx] |
                                 ci_role_nulls[idx]) != 0) {
                                ++idx;
                                continue;
                            }
                        }
                        const int32_t role_id = ci_role_ids[idx];
                        const uint32_t ridx = static_cast<uint32_t>(role_id);
                        if (ridx >= allowed_role_size || allowed_role_flags[ridx] == 0) {
                            ++idx;
                            continue;
                        }
                        const int32_t person_id = ci_person_ids[idx];
                        const uint32_t pidx = static_cast<uint32_t>(person_id);
                        if (pidx >= allowed_person_size || allowed_person_flags[pidx] == 0) {
                            ++idx;
                            continue;
                        }
#ifdef TRACE
                        ++trace.cast_info_rows_emitted;
                        ++trace.cast_info_agg_rows_in;
#endif
                        ++cast_counts[midx];
#ifdef TRACE
                        if (cast_counts[midx] == 1) {
                            ++trace.cast_info_groups_created;
                        }
#endif
                        ++idx;
                    }
                    search_pos = idx;
                }
            } else if (ci_has_nulls) {
                const uint8_t* ci_movie_nulls = ci_movie_id.is_null.data();
                const uint8_t* ci_person_nulls = ci_person_id.is_null.data();
                const uint8_t* ci_role_nulls = ci_role_id.is_null.data();
                const uint8_t* allowed_movie_flags = allowed_movie_ids.allowed.data();
                const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
                for (int64_t i = 0; i < cast_info.row_count; ++i) {
                    const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    if ((ci_movie_nulls[idx] | ci_person_nulls[idx] | ci_role_nulls[idx]) != 0) {
                        continue;
                    }
                    const int32_t movie_id = ci_movie_ids[idx];
                    const uint32_t midx = static_cast<uint32_t>(movie_id);
                    if (midx >= allowed_movie_size || allowed_movie_flags[midx] == 0) {
                        continue;
                    }
                    const int32_t role_id = ci_role_ids[idx];
                    const uint32_t ridx = static_cast<uint32_t>(role_id);
                    if (ridx >= allowed_role_size || allowed_role_flags[ridx] == 0) {
                        continue;
                    }
                    const int32_t person_id = ci_person_ids[idx];
                    const uint32_t pidx = static_cast<uint32_t>(person_id);
                    if (pidx >= allowed_person_size || allowed_person_flags[pidx] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
#endif
                    ++cast_counts[midx];
#ifdef TRACE
                    if (cast_counts[midx] == 1) {
                        ++trace.cast_info_groups_created;
                    }
#endif
                }
            } else {
                const uint8_t* allowed_movie_flags = allowed_movie_ids.allowed.data();
                const size_t allowed_movie_size = allowed_movie_ids.allowed.size();
                for (int64_t i = 0; i < cast_info.row_count; ++i) {
                    const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    const int32_t movie_id = ci_movie_ids[idx];
                    const uint32_t midx = static_cast<uint32_t>(movie_id);
                    if (midx >= allowed_movie_size || allowed_movie_flags[midx] == 0) {
                        continue;
                    }
                    const int32_t role_id = ci_role_ids[idx];
                    const uint32_t ridx = static_cast<uint32_t>(role_id);
                    if (ridx >= allowed_role_size || allowed_role_flags[ridx] == 0) {
                        continue;
                    }
                    const int32_t person_id = ci_person_ids[idx];
                    const uint32_t pidx = static_cast<uint32_t>(person_id);
                    if (pidx >= allowed_person_size || allowed_person_flags[pidx] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
#endif
                    ++cast_counts[midx];
#ifdef TRACE
                    if (cast_counts[midx] == 1) {
                        ++trace.cast_info_groups_created;
                    }
#endif
                }
            }
        }

        for (int32_t movie_id : title_movie_ids) {
            const size_t midx = static_cast<size_t>(movie_id);
            const uint32_t cast_count = cast_counts[midx];
            if (cast_count == 0) {
                continue;
            }
            const uint32_t info1_count = info_count1[midx];
            if (info1_count == 0) {
                continue;
            }
            const uint32_t info2_count = info_count2[midx];
            if (info2_count == 0) {
                continue;
            }
            const uint32_t keyword_count = keyword_counts[midx];
            if (keyword_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_join_rows_emitted;
#endif
            total_count += static_cast<int64_t>(cast_count) *
                           static_cast<int64_t>(info1_count) *
                           static_cast<int64_t>(info2_count) *
                           static_cast<int64_t>(keyword_count);
        }
#ifdef TRACE
        trace.movie_info_agg_rows_emitted_info1 = 0;
        trace.movie_info_agg_rows_emitted_info2 = 0;
        trace.cast_info_agg_rows_emitted = 0;
        trace.movie_keyword_agg_rows_emitted = 0;
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
            if (keyword_counts[midx] != 0) {
                ++trace.movie_keyword_agg_rows_emitted;
            }
        }
        trace.title_join_build_rows_in = static_cast<uint64_t>(
            trace.movie_info_agg_rows_emitted_info1 + trace.movie_info_agg_rows_emitted_info2 +
            trace.cast_info_agg_rows_emitted + trace.movie_keyword_agg_rows_emitted);
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
    print_count("q2b_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q2b_movie_info_scan_rows_emitted_info1", trace.movie_info_rows_emitted_info1);
    print_count("q2b_movie_info_scan_rows_emitted_info2", trace.movie_info_rows_emitted_info2);
    print_count("q2b_movie_info_info1_agg_rows_in", trace.movie_info_agg_rows_in_info1);
    print_count("q2b_movie_info_info1_groups_created", trace.movie_info_groups_created_info1);
    print_count("q2b_movie_info_info1_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info1);
    print_count("q2b_movie_info_info2_agg_rows_in", trace.movie_info_agg_rows_in_info2);
    print_count("q2b_movie_info_info2_groups_created", trace.movie_info_groups_created_info2);
    print_count("q2b_movie_info_info2_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info2);
    print_count("q2b_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q2b_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q2b_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q2b_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q2b_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q2b_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q2b_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q2b_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q2b_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q2b_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q2b_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q2b_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q2b_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q2b_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q2b_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q2b_query_output_rows", trace.query_output_rows);
#endif
    return total_count;
}