#include "query_q1a.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "trace.hpp"

namespace {

std::vector<int32_t> build_int_id_list(const std::vector<std::string>& values) {
    std::vector<int32_t> ids;
    ids.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        try {
            ids.push_back(static_cast<int32_t>(std::stoi(val)));
        } catch (const std::exception&) {
            continue;
        }
    }
    return ids;
}

std::vector<int32_t> build_string_id_list(const StringPool& pool,
                                          const std::vector<std::string>& values) {
    std::vector<int32_t> ids;
    ids.reserve(values.size());
    for (const auto& val : values) {
        if (val == "<<NULL>>" || val == "NULL" || val == "null") {
            continue;
        }
        int32_t id = -1;
        if (pool.try_get_id(val, id)) {
            ids.push_back(id);
        }
    }
    return ids;
}

void sort_unique(std::vector<int32_t>& ids) {
    std::sort(ids.begin(), ids.end());
    ids.erase(std::unique(ids.begin(), ids.end()), ids.end());
}

bool contains_id(const std::vector<int32_t>& ids, int32_t id) {
    return std::binary_search(ids.begin(), ids.end(), id);
}

bool matches_value_id(const std::vector<int32_t>& ids, int32_t id) {
    switch (ids.size()) {
        case 0:
            return false;
        case 1:
            return ids[0] == id;
        case 2:
            return ids[0] == id || ids[1] == id;
        case 3:
            return ids[0] == id || ids[1] == id || ids[2] == id;
        default:
            return contains_id(ids, id);
    }
}

std::vector<uint8_t> build_value_mask(const std::vector<int32_t>& ids) {
    if (ids.empty()) {
        return {};
    }
    int32_t max_id = -1;
    for (const auto id : ids) {
        if (id > max_id) {
            max_id = id;
        }
    }
    if (max_id < 0) {
        return {};
    }
    std::vector<uint8_t> mask(static_cast<size_t>(max_id) + 1, 0);
    for (const auto id : ids) {
        if (id >= 0) {
            mask[static_cast<size_t>(id)] = 1;
        }
    }
    return mask;
}

std::vector<uint8_t> build_id_mask_from_dim(const TableData& table,
                                            const std::string& id_col_name,
                                            const std::string& str_col_name,
                                            const std::vector<int32_t>& allowed_str_ids) {
    std::vector<uint8_t> mask(static_cast<size_t>(table.row_count) + 1, 0);
    if (allowed_str_ids.empty()) {
        return mask;
    }
    const auto& id_col = table.columns.at(id_col_name);
    const auto& str_col = table.columns.at(str_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0 || str_col.is_null[i] != 0) {
            continue;
        }
        const int32_t id = id_col.i32[static_cast<size_t>(i)];
        if (id < 0) {
            continue;
        }
        const size_t id_idx = static_cast<size_t>(id);
        if (id_idx >= mask.size()) {
            mask.resize(id_idx + 1, 0);
        }
        if (contains_id(allowed_str_ids, str_col.str_ids[static_cast<size_t>(i)])) {
            mask[id_idx] = 1;
        }
    }
    return mask;
}

std::vector<uint8_t> build_id_mask(const TableData& table, const std::string& id_col_name) {
    std::vector<uint8_t> mask(static_cast<size_t>(table.row_count) + 1, 0);
    const auto& id_col = table.columns.at(id_col_name);
    for (int64_t i = 0; i < table.row_count; ++i) {
        if (id_col.is_null[i] != 0) {
            continue;
        }
        const int32_t id = id_col.i32[static_cast<size_t>(i)];
        if (id < 0) {
            continue;
        }
        const size_t id_idx = static_cast<size_t>(id);
        if (id_idx >= mask.size()) {
            mask.resize(id_idx + 1, 0);
        }
        mask[id_idx] = 1;
    }
    return mask;
}

std::vector<uint8_t> build_int_mask(const std::vector<int32_t>& ids, size_t size) {
    std::vector<uint8_t> mask(size, 0);
    for (const auto id : ids) {
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

}  // namespace

int64_t run_q1a(const Database& db, const Q1aArgs& args) {
    int64_t total_count = 0;
#ifdef TRACE
    struct Q1aTrace {
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
        PROFILE_SCOPE(&trace.recorder, "q1a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    auto info_type_ids1 = build_int_id_list(args.ID1);
    auto info_type_ids2 = build_int_id_list(args.ID2);
    auto kind_ids = build_string_id_list(db.string_pool, args.KIND);
    auto role_ids = build_string_id_list(db.string_pool, args.ROLE);
    auto gender_ids = build_string_id_list(db.string_pool, args.GENDER);
    auto info_value_ids1 = build_string_id_list(db.string_pool, args.INFO1);
    auto info_value_ids2 = build_string_id_list(db.string_pool, args.INFO2);
    sort_unique(info_type_ids1);
    sort_unique(info_type_ids2);
    sort_unique(kind_ids);
    sort_unique(role_ids);
    sort_unique(gender_ids);
    sort_unique(info_value_ids1);
    sort_unique(info_value_ids2);
    const auto info_value_mask1 = build_value_mask(info_value_ids1);
    const auto info_value_mask2 = build_value_mask(info_value_ids2);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& role_type = db.tables.at("role_type");
    const auto& name = db.tables.at("name");
    const auto& info_type = db.tables.at("info_type");
    const auto& title = db.tables.at("title");

    const auto allowed_kind_ids = build_id_mask_from_dim(kind_type, "id", "kind", kind_ids);
    const auto allowed_role_ids = build_id_mask_from_dim(role_type, "id", "role", role_ids);
    const auto allowed_person_ids = build_id_mask_from_dim(name, "id", "gender", gender_ids);
    const auto info_type_exists = build_id_mask(info_type, "id");
    const auto info_type_filter1 = build_int_mask(info_type_ids1, info_type_exists.size());
    const auto info_type_filter2 = build_int_mask(info_type_ids2, info_type_exists.size());
    // info_type_filter1 and info_type_filter2 are used directly in the scan.

    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    const bool t_id_has_nulls = t_id.has_nulls;
    const bool t_kind_has_nulls = t_kind_id.has_nulls;
    const bool t_year_has_nulls = t_year.has_nulls;
    const auto* t_id_null = t_id_has_nulls ? t_id.is_null.data() : nullptr;
    const auto* t_kind_id_null = t_kind_has_nulls ? t_kind_id.is_null.data() : nullptr;
    const auto* t_year_null = t_year_has_nulls ? t_year.is_null.data() : nullptr;
    const auto* t_id_data = t_id.i32.data();
    const auto* t_kind_id_data = t_kind_id.i32.data();
    const auto* t_year_data = t_year.i32.data();
    const auto* allowed_kind_mask = allowed_kind_ids.data();
    const size_t allowed_kind_mask_size = allowed_kind_ids.size();
    std::vector<int32_t> title_movie_ids;
    title_movie_ids.reserve(static_cast<size_t>(title.row_count));
    int32_t max_movie_id = -1;

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q1a_title_scan");
#endif
    if (!t_id_has_nulls && !t_kind_has_nulls && !t_year_has_nulls) {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t movie_id = t_id_data[idx];
            const int32_t year = t_year_data[idx];
            if (movie_id < 0 || year > year1 || year < year2) {
                continue;
            }
            const int32_t kind_id = t_kind_id_data[idx];
            if (kind_id < 0) {
                continue;
            }
            const size_t kind_idx = static_cast<size_t>(kind_id);
            if (kind_idx >= allowed_kind_mask_size || allowed_kind_mask[kind_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
#endif
            title_movie_ids.push_back(movie_id);
            if (movie_id > max_movie_id) {
                max_movie_id = movie_id;
            }
        }
    } else {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if (t_id_has_nulls && t_id_null[idx] != 0) {
                continue;
            }
            if (t_year_has_nulls && t_year_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = t_id_data[idx];
            const int32_t year = t_year_data[idx];
            if (movie_id < 0 || year > year1 || year < year2) {
                continue;
            }
            if (t_kind_has_nulls && t_kind_id_null[idx] != 0) {
                continue;
            }
            const int32_t kind_id = t_kind_id_data[idx];
            if (kind_id < 0) {
                continue;
            }
            const size_t kind_idx = static_cast<size_t>(kind_id);
            if (kind_idx >= allowed_kind_mask_size || allowed_kind_mask[kind_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
#endif
            title_movie_ids.push_back(movie_id);
            if (movie_id > max_movie_id) {
                max_movie_id = movie_id;
            }
        }
    }
#ifdef TRACE
    }
#endif
    if (title_movie_ids.empty()) {
        return 0;
    }

    const size_t movie_id_capacity = max_movie_id >= 0
        ? static_cast<size_t>(max_movie_id) + 1
        : 0;

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const bool mi_movie_has_nulls = mi_movie_id.has_nulls;
    const bool mi_info_type_has_nulls = mi_info_type_id.has_nulls;
    const bool mi_info_has_nulls = mi_info.has_nulls;
    const auto* mi_movie_id_null = mi_movie_has_nulls ? mi_movie_id.is_null.data() : nullptr;
    const auto* mi_info_type_null = mi_info_type_has_nulls ? mi_info_type_id.is_null.data()
                                                           : nullptr;
    const auto* mi_info_null = mi_info_has_nulls ? mi_info.is_null.data() : nullptr;
    const auto* mi_movie_id_data = mi_movie_id.i32.data();
    const auto* mi_info_type_data = mi_info_type_id.i32.data();
    const auto* mi_info_data = mi_info.str_ids.data();
    const auto* info_type_filter1_data = info_type_filter1.data();
    const auto* info_type_filter2_data = info_type_filter2.data();
    const size_t info_type_filter_size = info_type_filter1.size();
    const auto* info_value_mask1_data = info_value_mask1.data();
    const auto* info_value_mask2_data = info_value_mask2.data();
    const size_t info_value_mask1_size = info_value_mask1.size();
    const size_t info_value_mask2_size = info_value_mask2.size();
    std::vector<uint32_t> info_count1(movie_id_capacity, 0);
    std::vector<uint32_t> info_count2(movie_id_capacity, 0);

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q1a_movie_info_scan");
#endif
    if (!mi_movie_has_nulls && !mi_info_type_has_nulls && !mi_info_has_nulls) {
        for (int64_t i = 0; i < movie_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            const int32_t info_type_id = mi_info_type_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter_size) {
                continue;
            }
            const uint8_t filter1 = info_type_filter1_data[info_type_id];
            const uint8_t filter2 = info_type_filter2_data[info_type_id];
            if (!filter1 && !filter2) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            const bool match1 = filter1 && info_id >= 0 &&
                static_cast<size_t>(info_id) < info_value_mask1_size &&
                info_value_mask1_data[info_id];
            const bool match2 = filter2 && info_id >= 0 &&
                static_cast<size_t>(info_id) < info_value_mask2_size &&
                info_value_mask2_data[info_id];
            if (!match1 && !match2) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (match1) {
#ifdef TRACE
                ++trace.movie_info_rows_emitted_info1;
                ++trace.movie_info_agg_rows_in_info1;
                auto& count = info_count1[static_cast<size_t>(movie_id)];
                if (count == 0) {
                    ++trace.movie_info_groups_created_info1;
                }
                ++count;
#else
                ++info_count1[static_cast<size_t>(movie_id)];
#endif
            }
            if (match2) {
#ifdef TRACE
                ++trace.movie_info_rows_emitted_info2;
                ++trace.movie_info_agg_rows_in_info2;
                auto& count = info_count2[static_cast<size_t>(movie_id)];
                if (count == 0) {
                    ++trace.movie_info_groups_created_info2;
                }
                ++count;
#else
                ++info_count2[static_cast<size_t>(movie_id)];
#endif
            }
        }
    } else {
        for (int64_t i = 0; i < movie_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if (mi_info_type_has_nulls && mi_info_type_null[idx] != 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_data[idx];
            if (static_cast<uint32_t>(info_type_id) >= info_type_filter_size) {
                continue;
            }
            const uint8_t filter1 = info_type_filter1_data[info_type_id];
            const uint8_t filter2 = info_type_filter2_data[info_type_id];
            if (!filter1 && !filter2) {
                continue;
            }
            if (mi_info_has_nulls && mi_info_null[idx] != 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            const bool match1 = filter1 && info_id >= 0 &&
                static_cast<size_t>(info_id) < info_value_mask1_size &&
                info_value_mask1_data[info_id];
            const bool match2 = filter2 && info_id >= 0 &&
                static_cast<size_t>(info_id) < info_value_mask2_size &&
                info_value_mask2_data[info_id];
            if (!match1 && !match2) {
                continue;
            }
            if (mi_movie_has_nulls && mi_movie_id_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (match1) {
#ifdef TRACE
                ++trace.movie_info_rows_emitted_info1;
                ++trace.movie_info_agg_rows_in_info1;
                auto& count = info_count1[static_cast<size_t>(movie_id)];
                if (count == 0) {
                    ++trace.movie_info_groups_created_info1;
                }
                ++count;
#else
                ++info_count1[static_cast<size_t>(movie_id)];
#endif
            }
            if (match2) {
#ifdef TRACE
                ++trace.movie_info_rows_emitted_info2;
                ++trace.movie_info_agg_rows_in_info2;
                auto& count = info_count2[static_cast<size_t>(movie_id)];
                if (count == 0) {
                    ++trace.movie_info_groups_created_info2;
                }
                ++count;
#else
                ++info_count2[static_cast<size_t>(movie_id)];
#endif
            }
        }
    }
#ifdef TRACE
    }
#endif

    std::vector<uint8_t> info_both_mask(movie_id_capacity, 0);
    for (const int32_t movie_id : title_movie_ids) {
        const size_t idx = static_cast<size_t>(movie_id);
        if (info_count1[idx] != 0 && info_count2[idx] != 0) {
            info_both_mask[idx] = 1;
        }
    }
    const auto* info_both_mask_data = info_both_mask.data();

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const bool ci_movie_has_nulls = ci_movie_id.has_nulls;
    const bool ci_person_has_nulls = ci_person_id.has_nulls;
    const bool ci_role_has_nulls = ci_role_id.has_nulls;
    const auto* ci_movie_id_null = ci_movie_has_nulls ? ci_movie_id.is_null.data() : nullptr;
    const auto* ci_person_id_null = ci_person_has_nulls ? ci_person_id.is_null.data() : nullptr;
    const auto* ci_role_id_null = ci_role_has_nulls ? ci_role_id.is_null.data() : nullptr;
    const auto* ci_movie_id_data = ci_movie_id.i32.data();
    const auto* ci_person_id_data = ci_person_id.i32.data();
    const auto* ci_role_id_data = ci_role_id.i32.data();
    const auto* allowed_role_mask = allowed_role_ids.data();
    const auto* allowed_person_mask = allowed_person_ids.data();
    const size_t allowed_role_mask_size = allowed_role_ids.size();
    const size_t allowed_person_mask_size = allowed_person_ids.size();

    std::vector<uint32_t> cast_counts(movie_id_capacity, 0);

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q1a_cast_info_scan");
#endif
    if (!ci_movie_has_nulls && !ci_person_has_nulls && !ci_role_has_nulls) {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            const int32_t movie_id = ci_movie_id_data[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (info_both_mask_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            const int32_t role_id = ci_role_id_data[idx];
            if (role_id < 0) {
                continue;
            }
            const size_t role_idx = static_cast<size_t>(role_id);
            if (role_idx >= allowed_role_mask_size || allowed_role_mask[role_idx] == 0) {
                continue;
            }
            const int32_t person_id = ci_person_id_data[idx];
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= allowed_person_mask_size || allowed_person_mask[person_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            auto& count = cast_counts[static_cast<size_t>(movie_id)];
            if (count == 0) {
                ++trace.cast_info_groups_created;
            }
            ++count;
#else
            ++cast_counts[static_cast<size_t>(movie_id)];
#endif
        }
    } else {
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            if (ci_movie_has_nulls && ci_movie_id_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = ci_movie_id_data[idx];
            if (movie_id < 0 || movie_id > max_movie_id) {
                continue;
            }
            if (info_both_mask_data[static_cast<size_t>(movie_id)] == 0) {
                continue;
            }
            if (ci_role_has_nulls && ci_role_id_null[idx] != 0) {
                continue;
            }
            const int32_t role_id = ci_role_id_data[idx];
            if (role_id < 0) {
                continue;
            }
            const size_t role_idx = static_cast<size_t>(role_id);
            if (role_idx >= allowed_role_mask_size || allowed_role_mask[role_idx] == 0) {
                continue;
            }
            if (ci_person_has_nulls && ci_person_id_null[idx] != 0) {
                continue;
            }
            const int32_t person_id = ci_person_id_data[idx];
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= allowed_person_mask_size || allowed_person_mask[person_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            auto& count = cast_counts[static_cast<size_t>(movie_id)];
            if (count == 0) {
                ++trace.cast_info_groups_created;
            }
            ++count;
#else
            ++cast_counts[static_cast<size_t>(movie_id)];
#endif
        }
    }
#ifdef TRACE
    }
#endif


    for (int32_t movie_id : title_movie_ids) {
        const size_t movie_idx = static_cast<size_t>(movie_id);
        const auto cast_count = cast_counts[movie_idx];
        if (cast_count == 0) {
            continue;
        }
        const auto info_count_val1 = info_count1[movie_idx];
        if (info_count_val1 == 0) {
            continue;
        }
        const auto info_count_val2 = info_count2[movie_idx];
        if (info_count_val2 == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.title_join_rows_emitted;
#endif
        total_count += static_cast<int64_t>(cast_count) *
            static_cast<int64_t>(info_count_val1) * static_cast<int64_t>(info_count_val2);
    }
#ifdef TRACE
    trace.movie_info_agg_rows_emitted_info1 = trace.movie_info_groups_created_info1;
    trace.movie_info_agg_rows_emitted_info2 = trace.movie_info_groups_created_info2;
    trace.cast_info_agg_rows_emitted = trace.cast_info_groups_created;
    trace.title_join_build_rows_in = trace.movie_info_groups_created_info1 +
        trace.movie_info_groups_created_info2 + trace.cast_info_groups_created;
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q1a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q1a_movie_info_scan_rows_emitted_info1", trace.movie_info_rows_emitted_info1);
    print_count("q1a_movie_info_scan_rows_emitted_info2", trace.movie_info_rows_emitted_info2);
    print_count("q1a_movie_info_info1_agg_rows_in", trace.movie_info_agg_rows_in_info1);
    print_count("q1a_movie_info_info1_groups_created", trace.movie_info_groups_created_info1);
    print_count("q1a_movie_info_info1_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info1);
    print_count("q1a_movie_info_info2_agg_rows_in", trace.movie_info_agg_rows_in_info2);
    print_count("q1a_movie_info_info2_groups_created", trace.movie_info_groups_created_info2);
    print_count("q1a_movie_info_info2_agg_rows_emitted", trace.movie_info_agg_rows_emitted_info2);
    print_count("q1a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q1a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q1a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q1a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q1a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q1a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q1a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q1a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q1a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q1a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q1a_query_output_rows", trace.query_output_rows);
#endif
    return total_count;
}