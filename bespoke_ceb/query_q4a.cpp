#include "query_q4a.hpp"

#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

#include "trace.hpp"

namespace q4a_internal {

struct StringFilter {
    bool allow_null = false;
    std::vector<int32_t> values;
};

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const ColumnData&,
                                            const std::vector<std::string>& values) {
    StringFilter filter;
    filter.values.reserve(values.size());
    for (const auto& value : values) {
        if (value == "<<NULL>>" || value == "NULL" || value == "null") {
            continue;
        }
        int32_t id = -1;
        if (pool.try_get_id(value, id)) {
            filter.values.push_back(id);
        }
    }
    std::sort(filter.values.begin(), filter.values.end());
    filter.values.erase(std::unique(filter.values.begin(), filter.values.end()),
                        filter.values.end());
    return filter;
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
    return std::binary_search(filter.values.begin(), filter.values.end(), id);
}

struct IntFilter {
    std::vector<int32_t> values;

    bool empty() const { return values.empty(); }

    bool contains(int32_t value) const {
        if (values.empty()) {
            return false;
        }
        if (values.size() == 1) {
            return values[0] == value;
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
    filter.values.erase(std::unique(filter.values.begin(), filter.values.end()),
                        filter.values.end());
    return filter;
}

}  // namespace q4a_internal

#ifdef TRACE
struct Q4aTrace {
    TraceRecorder recorder;
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
    uint64_t cast_info_agg_rows_in = 0;
    uint64_t cast_info_groups_created = 0;
    uint64_t cast_info_agg_rows_emitted = 0;
    uint64_t person_join_build_rows_in = 0;
    uint64_t person_join_probe_rows_in = 0;
    uint64_t person_join_rows_emitted = 0;
    uint64_t query_output_rows = 0;
};
#endif

int64_t run_q4a(const Database& db, const Q4aArgs& args) {
    int64_t result = 0;
#ifdef TRACE
    Q4aTrace trace;
    {
        PROFILE_SCOPE(&trace.recorder, "q4a_total");
#endif
    const auto& name = db.tables.at("name");
    const auto& gender_col = name.columns.at("gender");
    const auto& pcode_col = name.columns.at("name_pcode_nf");
    const auto gender_filter =
        q4a_internal::build_string_filter_for_column(db.string_pool, gender_col, args.GENDER);
    const auto pcode_filter =
        q4a_internal::build_string_filter_for_column(db.string_pool, pcode_col, args.NAME_PCODE_NF);
    const auto allowed_info_type_ids = q4a_internal::build_int_filter(args.ID);

    const auto& role_type = db.tables.at("role_type");
    const auto& role_col = role_type.columns.at("role");
    const auto role_filter =
        q4a_internal::build_string_filter_for_column(db.string_pool, role_col, args.ROLE);

    const auto& cast_info = db.tables.at("cast_info");
    const auto& note_col = cast_info.columns.at("note");
    const auto note_filter =
        q4a_internal::build_string_filter_for_column(db.string_pool, note_col, args.NOTE);

#ifdef TRACE
    bool skip_query = false;
    if (gender_filter.values.empty() || pcode_filter.values.empty() ||
        allowed_info_type_ids.empty() || role_filter.values.empty() ||
        note_filter.values.empty()) {
        skip_query = true;
    }
#else
    if (gender_filter.values.empty() || pcode_filter.values.empty() ||
        allowed_info_type_ids.empty() || role_filter.values.empty() ||
        note_filter.values.empty()) {
        return 0;
    }
#endif

    const auto& name_id_col = name.columns.at("id");
    const size_t person_slots = static_cast<size_t>(name.row_count) + 1;
    std::vector<uint8_t> allowed_person_mask(person_slots, 0);
    uint64_t allowed_person_count = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q4a_name_scan");
#endif
    for (int64_t i = 0; i < name.row_count; ++i) {
        const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (name_id_col.is_null[i] != 0) {
            continue;
        }
        if (!q4a_internal::matches_string(gender_col, idx, gender_filter)) {
            continue;
        }
        if (!q4a_internal::matches_string(pcode_col, idx, pcode_filter)) {
            continue;
        }
        const int32_t person_id = name_id_col.i32[idx];
        if (person_id < 0) {
            continue;
        }
        const size_t person_idx = static_cast<size_t>(person_id);
        if (person_idx >= person_slots) {
            continue;
        }
        allowed_person_mask[person_idx] = 1;
        ++allowed_person_count;
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
    }
#ifdef TRACE
    }
#endif

    if (allowed_person_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& role_id_col = role_type.columns.at("id");
    std::vector<uint8_t> allowed_role_mask;
    bool has_allowed_role = false;
#ifdef TRACE
    if (!skip_query) {
        for (int64_t i = 0; i < role_type.row_count; ++i) {
            if (role_id_col.is_null[i] != 0) {
                continue;
            }
            const size_t idx = static_cast<size_t>(i);
            if (!q4a_internal::matches_string(role_col, idx, role_filter)) {
                continue;
            }
            const int32_t role_id = role_id_col.i32[idx];
            if (role_id < 0) {
                continue;
            }
            const size_t role_idx = static_cast<size_t>(role_id);
            if (role_idx >= allowed_role_mask.size()) {
                allowed_role_mask.resize(role_idx + 1, 0);
            }
            allowed_role_mask[role_idx] = 1;
            has_allowed_role = true;
        }
    }
#else
    for (int64_t i = 0; i < role_type.row_count; ++i) {
        if (role_id_col.is_null[i] != 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(i);
        if (!q4a_internal::matches_string(role_col, idx, role_filter)) {
            continue;
        }
        const int32_t role_id = role_id_col.i32[idx];
        if (role_id < 0) {
            continue;
        }
        const size_t role_idx = static_cast<size_t>(role_id);
        if (role_idx >= allowed_role_mask.size()) {
            allowed_role_mask.resize(role_idx + 1, 0);
        }
        allowed_role_mask[role_idx] = 1;
        has_allowed_role = true;
    }
#endif
    if (!has_allowed_role) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& aka_name = db.tables.at("aka_name");
    const auto& aka_person_id = aka_name.columns.at("person_id");
    std::vector<int32_t> aka_counts(person_slots, 0);
    std::vector<int32_t> aka_person_ids;
    aka_person_ids.reserve(static_cast<size_t>(aka_name.row_count / 10 + 1));
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q4a_aka_name_scan");
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
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= person_slots || allowed_person_mask[person_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.aka_name_rows_emitted;
            ++trace.aka_name_agg_rows_in;
            if (aka_counts[person_idx] == 0) {
                aka_person_ids.push_back(person_id);
                ++trace.aka_name_groups_created;
            }
            ++aka_counts[person_idx];
#else
            if (aka_counts[person_idx] == 0) {
                aka_person_ids.push_back(person_id);
            }
            ++aka_counts[person_idx];
#endif
        }
#ifdef TRACE
    }
#endif

    const auto& person_info = db.tables.at("person_info");
    const auto& pi_person_id = person_info.columns.at("person_id");
    const auto& pi_info_type_id = person_info.columns.at("info_type_id");
    std::vector<int32_t> person_info_counts(person_slots, 0);
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q4a_person_info_scan");
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
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= person_slots || allowed_person_mask[person_idx] == 0) {
                continue;
            }
            const int32_t info_type_id = pi_info_type_id.i32[idx];
            if (!allowed_info_type_ids.contains(info_type_id)) {
                continue;
            }
#ifdef TRACE
            ++trace.person_info_rows_emitted;
            ++trace.person_info_agg_rows_in;
            if (person_info_counts[person_idx] == 0) {
                ++trace.person_info_groups_created;
            }
            ++person_info_counts[person_idx];
#else
            ++person_info_counts[person_idx];
#endif
        }
#ifdef TRACE
    }
#endif

    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    std::vector<int32_t> cast_info_counts(person_slots, 0);
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q4a_cast_info_scan");
#endif
        for (int64_t i = 0; i < cast_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.cast_info_rows_scanned;
#endif
            if (ci_person_id.is_null[idx] != 0 || ci_role_id.is_null[idx] != 0) {
                continue;
            }
            if (!q4a_internal::matches_string(note_col, idx, note_filter)) {
                continue;
            }
            const int32_t role_id = ci_role_id.i32[idx];
            if (role_id < 0) {
                continue;
            }
            const size_t role_idx = static_cast<size_t>(role_id);
            if (role_idx >= allowed_role_mask.size() || allowed_role_mask[role_idx] == 0) {
                continue;
            }
            const int32_t person_id = ci_person_id.i32[idx];
            if (person_id < 0) {
                continue;
            }
            const size_t person_idx = static_cast<size_t>(person_id);
            if (person_idx >= person_slots || allowed_person_mask[person_idx] == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.cast_info_rows_emitted;
            ++trace.cast_info_agg_rows_in;
            if (cast_info_counts[person_idx] == 0) {
                ++trace.cast_info_groups_created;
            }
            ++cast_info_counts[person_idx];
#else
            ++cast_info_counts[person_idx];
#endif
        }
#ifdef TRACE
    }
#endif

#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q4a_person_join");
#endif
        for (const int32_t person_id : aka_person_ids) {
            const size_t person_idx = static_cast<size_t>(person_id);
#ifdef TRACE
            ++trace.person_join_probe_rows_in;
#endif
            const int32_t pi_count = person_info_counts[person_idx];
            if (pi_count == 0) {
                continue;
            }
            const int32_t ci_count = cast_info_counts[person_idx];
            if (ci_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.person_join_rows_emitted;
#endif
            result += static_cast<int64_t>(aka_counts[person_idx]) *
                      static_cast<int64_t>(pi_count) * static_cast<int64_t>(ci_count);
        }
#ifdef TRACE
    }
#endif

#ifdef TRACE
    trace.aka_name_agg_rows_emitted = aka_person_ids.size();
    trace.person_info_agg_rows_emitted = trace.person_info_groups_created;
    trace.cast_info_agg_rows_emitted = trace.cast_info_groups_created;
    trace.person_join_build_rows_in =
        static_cast<uint64_t>(trace.person_info_groups_created + trace.cast_info_groups_created);
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q4a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q4a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q4a_aka_name_scan_rows_scanned", trace.aka_name_rows_scanned);
    print_count("q4a_aka_name_scan_rows_emitted", trace.aka_name_rows_emitted);
    print_count("q4a_aka_name_agg_rows_in", trace.aka_name_agg_rows_in);
    print_count("q4a_aka_name_groups_created", trace.aka_name_groups_created);
    print_count("q4a_aka_name_agg_rows_emitted", trace.aka_name_agg_rows_emitted);
    print_count("q4a_person_info_scan_rows_scanned", trace.person_info_rows_scanned);
    print_count("q4a_person_info_scan_rows_emitted", trace.person_info_rows_emitted);
    print_count("q4a_person_info_agg_rows_in", trace.person_info_agg_rows_in);
    print_count("q4a_person_info_groups_created", trace.person_info_groups_created);
    print_count("q4a_person_info_agg_rows_emitted", trace.person_info_agg_rows_emitted);
    print_count("q4a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q4a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q4a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q4a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q4a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q4a_person_join_build_rows_in", trace.person_join_build_rows_in);
    print_count("q4a_person_join_probe_rows_in", trace.person_join_probe_rows_in);
    print_count("q4a_person_join_rows_emitted", trace.person_join_rows_emitted);
    print_count("q4a_query_output_rows", trace.query_output_rows);
#endif
    return result;
}