#include "query_q8a.hpp"

#include <string>

#include "trace.hpp"

namespace q8a_internal {

struct IntFilter {
    std::vector<int32_t> values;
};

struct IdMask {
    std::vector<uint8_t> allowed;
    size_t count = 0;
};

struct StringFilter {
    std::vector<uint8_t> allowed;
    bool allow_null = false;
    size_t count = 0;
};

bool matches_string(const ColumnData& col, size_t idx, const StringFilter& filter);

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

inline bool id_allowed(const std::vector<uint8_t>& mask, int32_t id) {
    if (id < 0) {
        return false;
    }
    const size_t idx = static_cast<size_t>(id);
    return idx < mask.size() && mask[idx] != 0;
}

IdMask build_id_mask(const TableData& table, const std::string& id_col_name) {
    IdMask mask;
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
        if (idx >= mask.allowed.size()) {
            mask.allowed.resize(idx + 1, 0);
        }
        if (mask.allowed[idx] == 0) {
            mask.allowed[idx] = 1;
            ++mask.count;
        }
    }
    return mask;
}

IdMask build_allowed_id_mask(const TableData& table, const std::string& id_col_name,
                             const std::string& str_col_name, const StringFilter& filter) {
    IdMask mask;
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
        if (idx >= mask.allowed.size()) {
            mask.allowed.resize(idx + 1, 0);
        }
        if (mask.allowed[idx] == 0) {
            mask.allowed[idx] = 1;
            ++mask.count;
        }
    }
    return mask;
}

IdMask build_int_filter_mask(const IntFilter& filter, const std::vector<uint8_t>& valid_mask) {
    IdMask mask;
    mask.allowed.assign(valid_mask.size(), 0);
    for (const auto id : filter.values) {
        if (!id_allowed(valid_mask, id)) {
            continue;
        }
        const size_t idx = static_cast<size_t>(id);
        if (mask.allowed[idx] == 0) {
            mask.allowed[idx] = 1;
            ++mask.count;
        }
    }
    return mask;
}

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const ColumnData& col,
                                            const std::vector<std::string>& values) {
    StringFilter filter;
    (void)col;
    filter.allowed.assign(pool.values.size(), 0);
    size_t i = 0;
    while (i < values.size()) {
        if (values[i] == "<<NULL>>" || values[i] == "NULL" || values[i] == "null") {
            filter.allow_null = true;
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
        if (found_id != -1 && static_cast<size_t>(found_id) < filter.allowed.size()) {
            const size_t idx = static_cast<size_t>(found_id);
            if (filter.allowed[idx] == 0) {
                filter.allowed[idx] = 1;
                ++filter.count;
            }
            i = found_idx + 1;
        } else {
            ++i;
        }
    }
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
    return id >= 0 && static_cast<size_t>(id) < filter.allowed.size() &&
           filter.allowed[static_cast<size_t>(id)] != 0;
}

bool matches_int(const ColumnData& col, size_t idx, const IntFilter& filter) {
    if (idx >= col.is_null.size()) {
        return false;
    }
    if (col.is_null[idx] != 0) {
        return false;
    }
    for (const auto value : filter.values) {
        if (col.i32[idx] == value) {
            return true;
        }
    }
    return false;
}

}  // namespace q8a_internal

#ifdef TRACE
struct Q8aTrace {
    TraceRecorder recorder;
    uint64_t movie_info_rows_scanned = 0;
    uint64_t movie_info_rows_emitted = 0;
    uint64_t movie_info_agg_rows_in = 0;
    uint64_t movie_info_groups_created = 0;
    uint64_t movie_info_agg_rows_emitted = 0;
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
    uint64_t title_rows_scanned = 0;
    uint64_t title_rows_emitted = 0;
    uint64_t movie_multiplier_agg_rows_in = 0;
    uint64_t movie_multiplier_groups_created = 0;
    uint64_t movie_multiplier_agg_rows_emitted = 0;
    uint64_t name_rows_scanned = 0;
    uint64_t name_rows_emitted = 0;
    uint64_t cast_info_rows_scanned = 0;
    uint64_t cast_info_rows_emitted = 0;
    uint64_t cast_info_join_build_rows_in = 0;
    uint64_t cast_info_join_probe_rows_in = 0;
    uint64_t cast_info_join_rows_emitted = 0;
    uint64_t query_output_rows = 0;
};
#endif

int64_t run_q8a(const Database& db, const Q8aArgs& args) {
    int64_t result = 0;
#ifdef TRACE
    Q8aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q8a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto& kind_type = db.tables.at("kind_type");
    const auto& kind_col = kind_type.columns.at("kind");
    const auto kind_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, kind_col, args.KIND1);
    if (kind_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto allowed_kind_ids =
        q8a_internal::build_allowed_id_mask(kind_type, "id", "kind", kind_filter);
    if (allowed_kind_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    const bool t_id_has_nulls = t_id.has_nulls;
    const bool t_kind_has_nulls = t_kind_id.has_nulls;
    const bool t_year_has_nulls = t_year.has_nulls;
    const size_t title_rows = static_cast<size_t>(title.row_count);
    std::vector<uint8_t> allowed_title_ids(title_rows + 1, 0);
    uint8_t* __restrict__ allowed_title_data = allowed_title_ids.data();
    const size_t allowed_title_size = allowed_title_ids.size();
    size_t allowed_title_count = 0;
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q8a_title_scan");
        if (!skip_query) {
#endif
    const int32_t* __restrict__ t_id_data = t_id.i32.data();
    const int32_t* __restrict__ t_kind_data = t_kind_id.i32.data();
    const int32_t* __restrict__ t_year_data = t_year.i32.data();
    const uint8_t* t_id_nulls = t_id_has_nulls ? t_id.is_null.data() : nullptr;
    const uint8_t* t_kind_nulls = t_kind_has_nulls ? t_kind_id.is_null.data() : nullptr;
    const uint8_t* t_year_nulls = t_year_has_nulls ? t_year.is_null.data() : nullptr;
    for (size_t idx = 0; idx < title_rows; ++idx) {
#ifdef TRACE
        ++trace.title_rows_scanned;
#endif
        if ((t_id_nulls && t_id_nulls[idx] != 0) || (t_kind_nulls && t_kind_nulls[idx] != 0) ||
            (t_year_nulls && t_year_nulls[idx] != 0)) {
            continue;
        }
        const int32_t kind_id = t_kind_data[idx];
        if (!q8a_internal::id_allowed(allowed_kind_ids.allowed, kind_id)) {
            continue;
        }
        const int32_t year = t_year_data[idx];
        if (year > year1 || year < year2) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(t_id_data[idx]);
        if (allowed_title_data[movie_idx] == 0) {
            allowed_title_data[movie_idx] = 1;
            ++allowed_title_count;
        }
#ifdef TRACE
        ++trace.title_rows_emitted;
#endif
    }
#ifdef TRACE
        }
    }
#endif
    if (allowed_title_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const uint8_t* __restrict__ allowed_title_read = allowed_title_data;

    const auto info_type_filter = q8a_internal::build_int_filter(args.ID);
    if (info_type_filter.values.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& info_type = db.tables.at("info_type");
    const auto valid_info_type_ids = q8a_internal::build_id_mask(info_type, "id");
    if (valid_info_type_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto info_type_filter_mask =
        q8a_internal::build_int_filter_mask(info_type_filter, valid_info_type_ids.allowed);
    if (info_type_filter_mask.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_info = db.tables.at("movie_info");
    const auto& mi_movie_id = movie_info.columns.at("movie_id");
    const auto& mi_info_type_id = movie_info.columns.at("info_type_id");
    const auto& mi_info = movie_info.columns.at("info");
    const bool mi_movie_has_nulls = mi_movie_id.has_nulls;
    const bool mi_info_type_has_nulls = mi_info_type_id.has_nulls;
    const bool mi_info_has_nulls = mi_info.has_nulls;
    const size_t movie_info_rows = static_cast<size_t>(movie_info.row_count);
    const auto info_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, mi_info, args.INFO);
    if (info_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<uint16_t> info_counts(allowed_title_size, 0);
    size_t info_group_count = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q8a_movie_info_scan");
#endif
    const auto* info_type_allowed = info_type_filter_mask.allowed.data();
    const auto* info_allowed = info_filter.allowed.data();
    const int32_t* __restrict__ mi_movie_data = mi_movie_id.i32.data();
    const int32_t* __restrict__ mi_info_type_data = mi_info_type_id.i32.data();
    const int32_t* __restrict__ mi_info_ids = mi_info.str_ids.data();
    const uint8_t* mi_movie_nulls = mi_movie_has_nulls ? mi_movie_id.is_null.data() : nullptr;
    const uint8_t* mi_info_type_nulls =
        mi_info_type_has_nulls ? mi_info_type_id.is_null.data() : nullptr;
    const uint8_t* mi_info_nulls = mi_info_has_nulls ? mi_info.is_null.data() : nullptr;
    uint16_t* __restrict__ info_counts_data_local = info_counts.data();
    for (size_t idx = 0; idx < movie_info_rows; ++idx) {
#ifdef TRACE
        ++trace.movie_info_rows_scanned;
#endif
        if (mi_info_nulls && mi_info_nulls[idx] != 0) {
            continue;
        }
        const size_t info_idx = static_cast<size_t>(mi_info_ids[idx]);
        if (info_allowed[info_idx] == 0) {
            continue;
        }
        if (mi_info_type_nulls && mi_info_type_nulls[idx] != 0) {
            continue;
        }
        const size_t info_type_idx = static_cast<size_t>(mi_info_type_data[idx]);
        if (info_type_allowed[info_type_idx] == 0) {
            continue;
        }
        if (mi_movie_nulls && mi_movie_nulls[idx] != 0) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(mi_movie_data[idx]);
        if (allowed_title_read[movie_idx] == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_info_rows_emitted;
        ++trace.movie_info_agg_rows_in;
#endif
        auto& count = info_counts_data_local[movie_idx];
        if (count == 0) {
            ++info_group_count;
#ifdef TRACE
            ++trace.movie_info_groups_created;
#endif
        }
        ++count;
    }
#ifdef TRACE
    }
#endif
    if (info_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& keyword = db.tables.at("keyword");
    const auto valid_keyword_ids = q8a_internal::build_id_mask(keyword, "id");
    if (valid_keyword_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");
    const bool mk_movie_has_nulls = mk_movie_id.has_nulls;
    const bool mk_keyword_has_nulls = mk_keyword_id.has_nulls;
    const size_t movie_keyword_rows = static_cast<size_t>(movie_keyword.row_count);
    std::vector<uint16_t> keyword_counts(allowed_title_size, 0);
    size_t keyword_group_count = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q8a_movie_keyword_scan");
#endif
    const auto* keyword_allowed = valid_keyword_ids.allowed.data();
    const bool keyword_all_allowed = valid_keyword_ids.count == valid_keyword_ids.allowed.size();
    const int32_t* __restrict__ mk_movie_data = mk_movie_id.i32.data();
    const int32_t* __restrict__ mk_keyword_data = mk_keyword_id.i32.data();
    const uint8_t* mk_movie_nulls = mk_movie_has_nulls ? mk_movie_id.is_null.data() : nullptr;
    const uint8_t* mk_keyword_nulls =
        mk_keyword_has_nulls ? mk_keyword_id.is_null.data() : nullptr;
    uint16_t* __restrict__ keyword_counts_data_local = keyword_counts.data();
    for (size_t idx = 0; idx < movie_keyword_rows; ++idx) {
#ifdef TRACE
        ++trace.movie_keyword_rows_scanned;
#endif
        if (mk_movie_nulls && mk_movie_nulls[idx] != 0) {
            continue;
        }
        if (mk_keyword_nulls && mk_keyword_nulls[idx] != 0) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(mk_movie_data[idx]);
        if (allowed_title_read[movie_idx] == 0) {
            continue;
        }
        if (!keyword_all_allowed) {
            const size_t keyword_idx = static_cast<size_t>(mk_keyword_data[idx]);
            if (keyword_allowed[keyword_idx] == 0) {
                continue;
            }
        }
#ifdef TRACE
        ++trace.movie_keyword_rows_emitted;
        ++trace.movie_keyword_agg_rows_in;
#endif
        auto& count = keyword_counts_data_local[movie_idx];
        if (count == 0) {
            ++keyword_group_count;
#ifdef TRACE
            ++trace.movie_keyword_groups_created;
#endif
        }
        ++count;
    }
#ifdef TRACE
    }
#endif
    if (keyword_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& company_name = db.tables.at("company_name");
    const auto& cn_name = company_name.columns.at("name");
    const auto name_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, cn_name, args.NAME);
    if (name_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto allowed_company_ids =
        q8a_internal::build_allowed_id_mask(company_name, "id", "name", name_filter);
    if (allowed_company_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& company_type = db.tables.at("company_type");
    const auto& ct_kind = company_type.columns.at("kind");
    const auto kind2_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, ct_kind, args.KIND2);
    if (kind2_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto allowed_company_type_ids =
        q8a_internal::build_allowed_id_mask(company_type, "id", "kind", kind2_filter);
    if (allowed_company_type_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_companies = db.tables.at("movie_companies");
    const auto& mc_movie_id = movie_companies.columns.at("movie_id");
    const auto& mc_company_id = movie_companies.columns.at("company_id");
    const auto& mc_company_type_id = movie_companies.columns.at("company_type_id");
    const bool mc_movie_has_nulls = mc_movie_id.has_nulls;
    const bool mc_company_has_nulls = mc_company_id.has_nulls;
    const bool mc_company_type_has_nulls = mc_company_type_id.has_nulls;
    const size_t movie_companies_rows = static_cast<size_t>(movie_companies.row_count);
    std::vector<uint16_t> company_counts(allowed_title_size, 0);
    size_t company_group_count = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q8a_movie_companies_scan");
#endif
    const auto* company_allowed = allowed_company_ids.allowed.data();
    const auto* company_type_allowed = allowed_company_type_ids.allowed.data();
    const int32_t* __restrict__ mc_movie_data = mc_movie_id.i32.data();
    const int32_t* __restrict__ mc_company_data = mc_company_id.i32.data();
    const int32_t* __restrict__ mc_company_type_data = mc_company_type_id.i32.data();
    const uint8_t* mc_movie_nulls = mc_movie_has_nulls ? mc_movie_id.is_null.data() : nullptr;
    const uint8_t* mc_company_nulls =
        mc_company_has_nulls ? mc_company_id.is_null.data() : nullptr;
    const uint8_t* mc_company_type_nulls =
        mc_company_type_has_nulls ? mc_company_type_id.is_null.data() : nullptr;
    uint16_t* __restrict__ company_counts_data_local = company_counts.data();
    for (size_t idx = 0; idx < movie_companies_rows; ++idx) {
#ifdef TRACE
        ++trace.movie_companies_rows_scanned;
#endif
        if (mc_company_nulls && mc_company_nulls[idx] != 0) {
            continue;
        }
        const size_t company_idx = static_cast<size_t>(mc_company_data[idx]);
        if (company_allowed[company_idx] == 0) {
            continue;
        }
        if (mc_company_type_nulls && mc_company_type_nulls[idx] != 0) {
            continue;
        }
        const size_t company_type_idx = static_cast<size_t>(mc_company_type_data[idx]);
        if (company_type_allowed[company_type_idx] == 0) {
            continue;
        }
        if (mc_movie_nulls && mc_movie_nulls[idx] != 0) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(mc_movie_data[idx]);
        if (allowed_title_read[movie_idx] == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_companies_rows_emitted;
        ++trace.movie_companies_agg_rows_in;
#endif
        auto& count = company_counts_data_local[movie_idx];
        if (count == 0) {
            ++company_group_count;
#ifdef TRACE
            ++trace.movie_companies_groups_created;
#endif
        }
        ++count;
    }
#ifdef TRACE
    }
#endif
    if (company_group_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    std::vector<uint64_t> movie_multiplier(allowed_title_size, 0);
    std::vector<uint8_t> movie_multiplier_mask(allowed_title_size, 0);
    size_t movie_multiplier_count = 0;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q8a_movie_multiplier_build");
#endif
    const auto* info_counts_data = info_counts.data();
    const auto* keyword_counts_data = keyword_counts.data();
    const auto* company_counts_data = company_counts.data();
    auto* movie_multiplier_data_build = movie_multiplier.data();
    auto* movie_multiplier_mask_build = movie_multiplier_mask.data();
    for (size_t movie_idx = 0; movie_idx < allowed_title_size; ++movie_idx) {
        if (allowed_title_read[movie_idx] == 0) {
            continue;
        }
        const auto info = info_counts_data[movie_idx];
        if (info == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.movie_multiplier_agg_rows_in;
#endif
        const auto keyword = keyword_counts_data[movie_idx];
        if (keyword == 0) {
            continue;
        }
        const auto company = company_counts_data[movie_idx];
        if (company == 0) {
            continue;
        }
        movie_multiplier_data_build[movie_idx] =
            static_cast<uint64_t>(info) * static_cast<uint64_t>(keyword) *
            static_cast<uint64_t>(company);
        movie_multiplier_mask_build[movie_idx] = 1;
        ++movie_multiplier_count;
#ifdef TRACE
        ++trace.movie_multiplier_groups_created;
#endif
    }
#ifdef TRACE
    }
#endif
    if (movie_multiplier_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& role_type = db.tables.at("role_type");
    const auto& role_col = role_type.columns.at("role");
    const auto role_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, role_col, args.ROLE);
    if (role_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    const auto allowed_role_ids =
        q8a_internal::build_allowed_id_mask(role_type, "id", "role", role_filter);
    if (allowed_role_ids.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& name = db.tables.at("name");
    const auto& gender_col = name.columns.at("gender");
    const auto& pcode_col = name.columns.at("name_pcode_cf");
    const auto gender_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, gender_col, args.GENDER);
    const auto pcode_filter =
        q8a_internal::build_string_filter_for_column(db.string_pool, pcode_col,
                                                     args.NAME_PCODE_CF);
    if (gender_filter.count == 0 || pcode_filter.count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const size_t name_rows = static_cast<size_t>(name.row_count);
    std::vector<uint8_t> allowed_person_ids(name_rows + 1, 0);
    size_t allowed_person_count = 0;
    const auto& name_id_col = name.columns.at("id");
    const bool name_id_has_nulls = name_id_col.has_nulls;
    const bool gender_has_nulls = gender_col.has_nulls;
    const bool pcode_has_nulls = pcode_col.has_nulls;
    const auto* gender_allowed = gender_filter.allowed.data();
    const auto* pcode_allowed = pcode_filter.allowed.data();
    const int32_t* __restrict__ name_id_data = name_id_col.i32.data();
    const int32_t* __restrict__ gender_ids = gender_col.str_ids.data();
    const int32_t* __restrict__ pcode_ids = pcode_col.str_ids.data();
    const uint8_t* name_id_nulls = name_id_has_nulls ? name_id_col.is_null.data() : nullptr;
    const uint8_t* gender_nulls = gender_has_nulls ? gender_col.is_null.data() : nullptr;
    const uint8_t* pcode_nulls = pcode_has_nulls ? pcode_col.is_null.data() : nullptr;
    uint8_t* __restrict__ allowed_person_data = allowed_person_ids.data();
#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q8a_name_scan");
        if (!skip_query) {
#endif
    for (size_t idx = 0; idx < name_rows; ++idx) {
#ifdef TRACE
        ++trace.name_rows_scanned;
#endif
        if (name_id_nulls && name_id_nulls[idx] != 0) {
            continue;
        }
        if (gender_nulls && gender_nulls[idx] != 0) {
            continue;
        }
        const size_t gender_idx = static_cast<size_t>(gender_ids[idx]);
        if (gender_allowed[gender_idx] == 0) {
            continue;
        }
        if (pcode_nulls && pcode_nulls[idx] != 0) {
            continue;
        }
        const size_t pcode_idx = static_cast<size_t>(pcode_ids[idx]);
        if (pcode_allowed[pcode_idx] == 0) {
            continue;
        }
        const size_t person_idx = static_cast<size_t>(name_id_data[idx]);
        if (allowed_person_data[person_idx] == 0) {
            allowed_person_data[person_idx] = 1;
            ++allowed_person_count;
        }
#ifdef TRACE
        ++trace.name_rows_emitted;
#endif
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

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const bool ci_person_has_nulls = ci_person_id.has_nulls;
    const bool ci_movie_has_nulls = ci_movie_id.has_nulls;
    const bool ci_role_has_nulls = ci_role_id.has_nulls;
    const size_t cast_info_rows = static_cast<size_t>(cast_info.row_count);

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q8a_cast_info_scan");
        if (!skip_query) {
#endif
    const auto* role_allowed = allowed_role_ids.allowed.data();
    const uint8_t* __restrict__ person_allowed = allowed_person_ids.data();
    const int32_t* __restrict__ ci_person_data = ci_person_id.i32.data();
    const int32_t* __restrict__ ci_movie_data = ci_movie_id.i32.data();
    const int32_t* __restrict__ ci_role_data = ci_role_id.i32.data();
    const uint8_t* ci_person_nulls = ci_person_has_nulls ? ci_person_id.is_null.data() : nullptr;
    const uint8_t* ci_movie_nulls = ci_movie_has_nulls ? ci_movie_id.is_null.data() : nullptr;
    const uint8_t* ci_role_nulls = ci_role_has_nulls ? ci_role_id.is_null.data() : nullptr;
    const auto* movie_multiplier_data = movie_multiplier.data();
    const auto* __restrict__ movie_multiplier_mask_data = movie_multiplier_mask.data();
    for (size_t idx = 0; idx < cast_info_rows; ++idx) {
#ifdef TRACE
        ++trace.cast_info_rows_scanned;
#endif
        if (ci_movie_nulls && ci_movie_nulls[idx] != 0) {
            continue;
        }
        const size_t movie_idx = static_cast<size_t>(ci_movie_data[idx]);
        if (movie_multiplier_mask_data[movie_idx] == 0) {
            continue;
        }
        if (ci_role_nulls && ci_role_nulls[idx] != 0) {
            continue;
        }
        const size_t role_idx = static_cast<size_t>(ci_role_data[idx]);
        if (role_allowed[role_idx] == 0) {
            continue;
        }
        if (ci_person_nulls && ci_person_nulls[idx] != 0) {
            continue;
        }
        const size_t person_idx = static_cast<size_t>(ci_person_data[idx]);
        if (person_allowed[person_idx] == 0) {
            continue;
        }
#ifdef TRACE
        ++trace.cast_info_rows_emitted;
        ++trace.cast_info_join_probe_rows_in;
#endif
        const auto multiplier = movie_multiplier_data[movie_idx];
#ifdef TRACE
        ++trace.cast_info_join_rows_emitted;
#endif
        result += multiplier;
    }
#ifdef TRACE
        }
    }
    trace.movie_info_agg_rows_emitted = info_group_count;
    trace.movie_keyword_agg_rows_emitted = keyword_group_count;
    trace.movie_companies_agg_rows_emitted = company_group_count;
    trace.movie_multiplier_agg_rows_emitted = movie_multiplier_count;
    trace.cast_info_join_build_rows_in = static_cast<uint64_t>(
        movie_multiplier_count + allowed_role_ids.count + allowed_person_count);
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q8a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q8a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q8a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q8a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q8a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q8a_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q8a_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q8a_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q8a_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q8a_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q8a_movie_companies_scan_rows_scanned", trace.movie_companies_rows_scanned);
    print_count("q8a_movie_companies_scan_rows_emitted", trace.movie_companies_rows_emitted);
    print_count("q8a_movie_companies_agg_rows_in", trace.movie_companies_agg_rows_in);
    print_count("q8a_movie_companies_groups_created", trace.movie_companies_groups_created);
    print_count("q8a_movie_companies_agg_rows_emitted", trace.movie_companies_agg_rows_emitted);
    print_count("q8a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q8a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q8a_movie_multiplier_agg_rows_in", trace.movie_multiplier_agg_rows_in);
    print_count("q8a_movie_multiplier_groups_created", trace.movie_multiplier_groups_created);
    print_count("q8a_movie_multiplier_agg_rows_emitted", trace.movie_multiplier_agg_rows_emitted);
    print_count("q8a_name_scan_rows_scanned", trace.name_rows_scanned);
    print_count("q8a_name_scan_rows_emitted", trace.name_rows_emitted);
    print_count("q8a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q8a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q8a_cast_info_join_build_rows_in", trace.cast_info_join_build_rows_in);
    print_count("q8a_cast_info_join_probe_rows_in", trace.cast_info_join_probe_rows_in);
    print_count("q8a_cast_info_join_rows_emitted", trace.cast_info_join_rows_emitted);
    print_count("q8a_query_output_rows", trace.query_output_rows);
#endif
    return result;
}