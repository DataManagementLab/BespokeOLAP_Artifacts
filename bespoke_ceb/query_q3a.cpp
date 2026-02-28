#include "query_q3a.hpp"

#include <algorithm>
#include <cstdlib>
#include <stdexcept>
#include <string>
#include <vector>

#include "trace.hpp"

namespace q3a_internal {

struct StringFilter {
    std::vector<int32_t> values;
    bool allow_null = false;

    bool contains(int32_t id) const {
        for (const auto value : values) {
            if (value == id) {
                return true;
            }
        }
        return false;
    }

    void add_value(int32_t id) {
        if (!contains(id)) {
            values.push_back(id);
        }
    }
};

StringFilter build_string_filter_for_column(const StringPool& pool,
                                            const std::vector<std::string>& values) {
    StringFilter filter;
    filter.values.reserve(values.size());
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
        if (found_id != -1) {
            filter.add_value(found_id);
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
    if (filter.values.empty()) {
        return false;
    }
    const int32_t id = col.str_ids[idx];
    return filter.contains(id);
}

struct IdFilter {
    std::vector<uint8_t> allowed;
    bool any = false;

    bool contains(int32_t id) const {
        if (id < 0) {
            return false;
        }
        const size_t idx = static_cast<size_t>(id);
        return idx < allowed.size() && allowed[idx] != 0;
    }
};

IdFilter build_allowed_ids_from_dim(const TableData& table,
                                    const std::string& id_col_name,
                                    const std::string& str_col_name,
                                    const StringFilter& filter) {
    IdFilter ids;
    if (table.row_count > 0) {
        ids.allowed.assign(static_cast<size_t>(table.row_count) + 1, 0);
    }
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
            if (idx >= ids.allowed.size()) {
                ids.allowed.resize(idx + 1, 0);
            }
            if (ids.allowed[idx] == 0) {
                ids.allowed[idx] = 1;
                ids.any = true;
            }
        }
    }
    return ids;
}

}  // namespace q3a_internal

int64_t run_q3a(const Database& db, const Q3aArgs& args) {
    int64_t total = 0;
#ifdef TRACE
    struct Q3aTrace {
        TraceRecorder recorder;
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
        uint64_t title_rows_scanned = 0;
        uint64_t title_rows_emitted = 0;
        uint64_t title_join_build_rows_in = 0;
        uint64_t title_join_probe_rows_in = 0;
        uint64_t title_join_rows_emitted = 0;
        uint64_t query_output_rows = 0;
    } trace;
    {
        PROFILE_SCOPE(&trace.recorder, "q3a_total");
#endif
    const int year1 = std::stoi(args.YEAR1);
    const int year2 = std::stoi(args.YEAR2);

    const auto& keyword = db.tables.at("keyword");
    const auto keyword_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.KEYWORD);

    const auto& company_name = db.tables.at("company_name");
    const auto country_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.COUNTRY);

    const auto& company_type = db.tables.at("company_type");
    const auto kind1_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.KIND1);

    const auto& kind_type = db.tables.at("kind_type");
    const auto kind2_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.KIND2);

    const auto& role_type = db.tables.at("role_type");
    const auto role_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.ROLE);

    const auto& name = db.tables.at("name");
    const auto gender_filter =
        q3a_internal::build_string_filter_for_column(db.string_pool, args.GENDER);

    const auto allowed_keyword_ids =
        q3a_internal::build_allowed_ids_from_dim(keyword, "id", "keyword", keyword_filter);
    const auto allowed_company_ids =
        q3a_internal::build_allowed_ids_from_dim(company_name, "id", "country_code", country_filter);
    const auto allowed_company_type_ids =
        q3a_internal::build_allowed_ids_from_dim(company_type, "id", "kind", kind1_filter);
    const auto allowed_kind_ids =
        q3a_internal::build_allowed_ids_from_dim(kind_type, "id", "kind", kind2_filter);
    const auto allowed_role_ids =
        q3a_internal::build_allowed_ids_from_dim(role_type, "id", "role", role_filter);
    const auto allowed_person_ids =
        q3a_internal::build_allowed_ids_from_dim(name, "id", "gender", gender_filter);

    const bool skip_query = !allowed_keyword_ids.any || !allowed_company_ids.any ||
        !allowed_company_type_ids.any || !allowed_kind_ids.any || !allowed_role_ids.any ||
        !allowed_person_ids.any;

    const auto& title = db.tables.at("title");
    const auto& t_id = title.columns.at("id");
    const auto& t_year = title.columns.at("production_year");
    const auto& t_kind_id = title.columns.at("kind_id");
    const bool t_id_has_nulls = t_id.has_nulls;
    const bool t_year_has_nulls = t_year.has_nulls;
    const bool t_kind_has_nulls = t_kind_id.has_nulls;
    const size_t initial_movie_capacity = static_cast<size_t>(title.row_count) + 1;

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");
    const bool mk_movie_has_nulls = mk_movie_id.has_nulls;
    const bool mk_keyword_has_nulls = mk_keyword_id.has_nulls;
    uint64_t mk_group_count = 0;
    uint64_t mc_group_count = 0;
    uint64_t ci_group_count = 0;

    const auto& movie_companies = db.tables.at("movie_companies");
    const auto& mc_movie_id = movie_companies.columns.at("movie_id");
    const auto& mc_company_id = movie_companies.columns.at("company_id");
    const auto& mc_company_type_id = movie_companies.columns.at("company_type_id");
    const bool mc_movie_has_nulls = mc_movie_id.has_nulls;
    const bool mc_company_has_nulls = mc_company_id.has_nulls;
    const bool mc_company_type_has_nulls = mc_company_type_id.has_nulls;

    const auto& cast_info = db.tables.at("cast_info");
    const auto& ci_movie_id = cast_info.columns.at("movie_id");
    const auto& ci_person_id = cast_info.columns.at("person_id");
    const auto& ci_role_id = cast_info.columns.at("role_id");
    const bool ci_movie_has_nulls = ci_movie_id.has_nulls;
    const bool ci_person_has_nulls = ci_person_id.has_nulls;
    const bool ci_role_has_nulls = ci_role_id.has_nulls;

    const bool enable_fast_path = false;
    bool used_fast_path = false;
    if (enable_fast_path && !skip_query && movie_keyword.sorted_by_movie_id &&
        movie_companies.sorted_by_movie_id && cast_info.sorted_by_movie_id) {
        used_fast_path = true;
        std::vector<int32_t> candidate_movies;
        candidate_movies.reserve(64);
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_title_scan");
#endif
        const auto* t_ids = t_id.i32.data();
        const auto* t_years = t_year.i32.data();
        const auto* t_kind_ids = t_kind_id.i32.data();
        const auto* t_id_nulls = t_id_has_nulls ? t_id.is_null.data() : nullptr;
        const auto* t_year_nulls = t_year_has_nulls ? t_year.is_null.data() : nullptr;
        const auto* t_kind_nulls = t_kind_has_nulls ? t_kind_id.is_null.data() : nullptr;
        const auto* allowed_kind_data = allowed_kind_ids.allowed.data();
        const size_t allowed_kind_size = allowed_kind_ids.allowed.size();
        const size_t t_row_count = static_cast<size_t>(title.row_count);
        if (!t_id_has_nulls && !t_year_has_nulls && !t_kind_has_nulls) {
            for (size_t idx = 0; idx < t_row_count; ++idx) {
#ifdef TRACE
                ++trace.title_rows_scanned;
#endif
                const int32_t year = t_years[idx];
                if (year > year1 || year < year2) {
                    continue;
                }
                const int32_t kind_id = t_kind_ids[idx];
                if (kind_id < 0 ||
                    static_cast<size_t>(kind_id) >= allowed_kind_size ||
                    allowed_kind_data[kind_id] == 0) {
                    continue;
                }
                const int32_t movie_id = t_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                candidate_movies.push_back(movie_id);
            }
        } else {
            for (size_t idx = 0; idx < t_row_count; ++idx) {
#ifdef TRACE
                ++trace.title_rows_scanned;
#endif
                if ((t_id_has_nulls && t_id_nulls[idx] != 0) ||
                    (t_year_has_nulls && t_year_nulls[idx] != 0) ||
                    (t_kind_has_nulls && t_kind_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t year = t_years[idx];
                if (year > year1 || year < year2) {
                    continue;
                }
                const int32_t kind_id = t_kind_ids[idx];
                if (kind_id < 0 ||
                    static_cast<size_t>(kind_id) >= allowed_kind_size ||
                    allowed_kind_data[kind_id] == 0) {
                    continue;
                }
                const int32_t movie_id = t_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                candidate_movies.push_back(movie_id);
            }
        }
#ifdef TRACE
        }
#endif

        const auto* mk_movie_ids = mk_movie_id.i32.data();
        const auto* mk_keyword_ids = mk_keyword_id.i32.data();
        const auto* mk_movie_nulls = mk_movie_has_nulls ? mk_movie_id.is_null.data() : nullptr;
        const auto* mk_keyword_nulls = mk_keyword_has_nulls ? mk_keyword_id.is_null.data() : nullptr;
        const auto* allowed_keyword_data = allowed_keyword_ids.allowed.data();
        const size_t allowed_keyword_size = allowed_keyword_ids.allowed.size();
        const size_t mk_row_count = static_cast<size_t>(movie_keyword.row_count);

        const auto* mc_movie_ids = mc_movie_id.i32.data();
        const auto* mc_company_ids = mc_company_id.i32.data();
        const auto* mc_company_type_ids = mc_company_type_id.i32.data();
        const auto* mc_movie_nulls = mc_movie_has_nulls ? mc_movie_id.is_null.data() : nullptr;
        const auto* mc_company_nulls = mc_company_has_nulls ? mc_company_id.is_null.data() : nullptr;
        const auto* mc_company_type_nulls =
            mc_company_type_has_nulls ? mc_company_type_id.is_null.data() : nullptr;
        const auto* allowed_company_data = allowed_company_ids.allowed.data();
        const size_t allowed_company_size = allowed_company_ids.allowed.size();
        const auto* allowed_company_type_data = allowed_company_type_ids.allowed.data();
        const size_t allowed_company_type_size = allowed_company_type_ids.allowed.size();
        const size_t mc_row_count = static_cast<size_t>(movie_companies.row_count);

        const auto* ci_movie_ids = ci_movie_id.i32.data();
        const auto* ci_person_ids = ci_person_id.i32.data();
        const auto* ci_role_ids = ci_role_id.i32.data();
        const auto* ci_movie_nulls = ci_movie_has_nulls ? ci_movie_id.is_null.data() : nullptr;
        const auto* ci_person_nulls = ci_person_has_nulls ? ci_person_id.is_null.data() : nullptr;
        const auto* ci_role_nulls = ci_role_has_nulls ? ci_role_id.is_null.data() : nullptr;
        const auto* allowed_person_data = allowed_person_ids.allowed.data();
        const size_t allowed_person_size = allowed_person_ids.allowed.size();
        const auto* allowed_role_data = allowed_role_ids.allowed.data();
        const size_t allowed_role_size = allowed_role_ids.allowed.size();
        const size_t ci_row_count = static_cast<size_t>(cast_info.row_count);

        auto count_movie_keyword = [&](int32_t movie_id) -> uint32_t {
            if (mk_row_count == 0) {
                return 0;
            }
            const int32_t* begin = mk_movie_ids;
            const int32_t* end = mk_movie_ids + mk_row_count;
            const int32_t* range_start = std::lower_bound(begin, end, movie_id);
            if (range_start == end || *range_start != movie_id) {
                return 0;
            }
            const int32_t* range_end = std::upper_bound(range_start, end, movie_id);
            uint32_t count = 0;
            for (const int32_t* ptr = range_start; ptr != range_end; ++ptr) {
                const size_t idx = static_cast<size_t>(ptr - mk_movie_ids);
#ifdef TRACE
                ++trace.movie_keyword_rows_scanned;
#endif
                if ((mk_movie_nulls && mk_movie_nulls[idx] != 0) ||
                    (mk_keyword_nulls && mk_keyword_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t keyword_id = mk_keyword_ids[idx];
                if (keyword_id < 0 ||
                    static_cast<size_t>(keyword_id) >= allowed_keyword_size ||
                    allowed_keyword_data[keyword_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_keyword_rows_emitted;
                ++trace.movie_keyword_agg_rows_in;
#endif
                ++count;
            }
            return count;
        };

        auto count_movie_companies = [&](int32_t movie_id) -> uint32_t {
            if (mc_row_count == 0) {
                return 0;
            }
            const int32_t* begin = mc_movie_ids;
            const int32_t* end = mc_movie_ids + mc_row_count;
            const int32_t* range_start = std::lower_bound(begin, end, movie_id);
            if (range_start == end || *range_start != movie_id) {
                return 0;
            }
            const int32_t* range_end = std::upper_bound(range_start, end, movie_id);
            uint32_t count = 0;
            for (const int32_t* ptr = range_start; ptr != range_end; ++ptr) {
                const size_t idx = static_cast<size_t>(ptr - mc_movie_ids);
#ifdef TRACE
                ++trace.movie_companies_rows_scanned;
#endif
                if ((mc_movie_nulls && mc_movie_nulls[idx] != 0) ||
                    (mc_company_nulls && mc_company_nulls[idx] != 0) ||
                    (mc_company_type_nulls && mc_company_type_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t company_id = mc_company_ids[idx];
                const int32_t company_type_id = mc_company_type_ids[idx];
                if (company_id < 0 ||
                    static_cast<size_t>(company_id) >= allowed_company_size ||
                    allowed_company_data[company_id] == 0) {
                    continue;
                }
                if (company_type_id < 0 ||
                    static_cast<size_t>(company_type_id) >= allowed_company_type_size ||
                    allowed_company_type_data[company_type_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_companies_rows_emitted;
                ++trace.movie_companies_agg_rows_in;
#endif
                ++count;
            }
            return count;
        };

        auto count_cast_info = [&](int32_t movie_id) -> uint32_t {
            if (ci_row_count == 0) {
                return 0;
            }
            const int32_t* begin = ci_movie_ids;
            const int32_t* end = ci_movie_ids + ci_row_count;
            const int32_t* range_start = std::lower_bound(begin, end, movie_id);
            if (range_start == end || *range_start != movie_id) {
                return 0;
            }
            const int32_t* range_end = std::upper_bound(range_start, end, movie_id);
            uint32_t count = 0;
            for (const int32_t* ptr = range_start; ptr != range_end; ++ptr) {
                const size_t idx = static_cast<size_t>(ptr - ci_movie_ids);
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                if ((ci_movie_nulls && ci_movie_nulls[idx] != 0) ||
                    (ci_person_nulls && ci_person_nulls[idx] != 0) ||
                    (ci_role_nulls && ci_role_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t person_id = ci_person_ids[idx];
                const int32_t role_id = ci_role_ids[idx];
                if (person_id < 0 ||
                    static_cast<size_t>(person_id) >= allowed_person_size ||
                    allowed_person_data[person_id] == 0) {
                    continue;
                }
                if (role_id < 0 ||
                    static_cast<size_t>(role_id) >= allowed_role_size ||
                    allowed_role_data[role_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.cast_info_rows_emitted;
                ++trace.cast_info_agg_rows_in;
#endif
                ++count;
            }
            return count;
        };

        std::vector<uint32_t> mk_counts_for_candidates(candidate_movies.size(), 0);
        std::vector<uint32_t> mc_counts_for_candidates(candidate_movies.size(), 0);
        std::vector<uint32_t> ci_counts_for_candidates(candidate_movies.size(), 0);
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_movie_keyword_scan");
#endif
        for (size_t i = 0; i < candidate_movies.size(); ++i) {
            const uint32_t count = count_movie_keyword(candidate_movies[i]);
            mk_counts_for_candidates[i] = count;
            if (count > 0) {
                ++mk_group_count;
#ifdef TRACE
                ++trace.movie_keyword_groups_created;
#endif
            }
        }
#ifdef TRACE
        }
#endif
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_movie_companies_scan");
#endif
        for (size_t i = 0; i < candidate_movies.size(); ++i) {
            if (mk_counts_for_candidates[i] == 0) {
                continue;
            }
            const uint32_t count = count_movie_companies(candidate_movies[i]);
            mc_counts_for_candidates[i] = count;
            if (count > 0) {
                ++mc_group_count;
#ifdef TRACE
                ++trace.movie_companies_groups_created;
#endif
            }
        }
#ifdef TRACE
        }
#endif
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_cast_info_scan");
#endif
        for (size_t i = 0; i < candidate_movies.size(); ++i) {
            if (mk_counts_for_candidates[i] == 0 || mc_counts_for_candidates[i] == 0) {
                continue;
            }
            const uint32_t count = count_cast_info(candidate_movies[i]);
            ci_counts_for_candidates[i] = count;
            if (count > 0) {
                ++ci_group_count;
#ifdef TRACE
                ++trace.cast_info_groups_created;
#endif
            }
        }
#ifdef TRACE
        }
#endif
        for (size_t i = 0; i < candidate_movies.size(); ++i) {
            const uint32_t mk_count = mk_counts_for_candidates[i];
            const uint32_t mc_count = mc_counts_for_candidates[i];
            const uint32_t ci_count = ci_counts_for_candidates[i];
            if (mk_count == 0 || mc_count == 0 || ci_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
            ++trace.title_join_rows_emitted;
#endif
            total += static_cast<int64_t>(mk_count) * static_cast<int64_t>(mc_count) *
                static_cast<int64_t>(ci_count);
        }
    }

    if (!used_fast_path) {
        std::vector<uint32_t> mk_counts;
        std::vector<uint8_t> mk_present;
        std::vector<int32_t> mk_movie_id_list;
        if (!skip_query) {
        mk_counts.assign(initial_movie_capacity, 0);
        mk_present.assign(initial_movie_capacity, 0);
            mk_movie_id_list.reserve(65536);
        size_t mk_counts_size = mk_counts.size();
        uint32_t* mk_counts_data = mk_counts.data();
        size_t mk_present_size = mk_present.size();
        uint8_t* mk_present_data = mk_present.data();
        const auto* mk_movie_ids = mk_movie_id.i32.data();
        const auto* mk_keyword_ids = mk_keyword_id.i32.data();
        const auto* mk_movie_nulls = mk_movie_has_nulls ? mk_movie_id.is_null.data() : nullptr;
        const auto* mk_keyword_nulls = mk_keyword_has_nulls ? mk_keyword_id.is_null.data() : nullptr;
        const auto* allowed_keyword_data = allowed_keyword_ids.allowed.data();
        const size_t allowed_keyword_size = allowed_keyword_ids.allowed.size();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_movie_keyword_scan");
#endif
        const size_t mk_row_count = static_cast<size_t>(movie_keyword.row_count);
        if (movie_keyword.sorted_by_keyword_id && !mk_keyword_has_nulls) {
            std::vector<int32_t> keyword_id_list;
            keyword_id_list.reserve(allowed_keyword_size);
            for (size_t id = 0; id < allowed_keyword_size; ++id) {
                if (allowed_keyword_data[id] != 0) {
                    keyword_id_list.push_back(static_cast<int32_t>(id));
                }
            }
            std::sort(keyword_id_list.begin(), keyword_id_list.end());
            size_t search_start = 0;
            for (const int32_t keyword_id : keyword_id_list) {
                if (keyword_id < 0) {
                    continue;
                }
                const int32_t* search_begin = mk_keyword_ids + search_start;
                const int32_t* search_end = mk_keyword_ids + mk_row_count;
                const int32_t* range_start =
                    std::lower_bound(search_begin, search_end, keyword_id);
                if (range_start == search_end || *range_start != keyword_id) {
                    continue;
                }
                const int32_t* range_end =
                    std::upper_bound(range_start, search_end, keyword_id);
                const size_t idx_start = static_cast<size_t>(range_start - mk_keyword_ids);
                const size_t idx_end = static_cast<size_t>(range_end - mk_keyword_ids);
                search_start = idx_end;
                for (size_t idx = idx_start; idx < idx_end; ++idx) {
#ifdef TRACE
                    ++trace.movie_keyword_rows_scanned;
#endif
                    if (mk_movie_has_nulls && mk_movie_nulls[idx] != 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.movie_keyword_rows_emitted;
                    ++trace.movie_keyword_agg_rows_in;
#endif
                    const int32_t movie_id = mk_movie_ids[idx];
                    if (movie_id < 0) {
                        continue;
                    }
                    const size_t movie_idx = static_cast<size_t>(movie_id);
                    if (movie_idx >= mk_counts_size) {
                        mk_counts.resize(movie_idx + 1, 0);
                        mk_counts_size = mk_counts.size();
                        mk_counts_data = mk_counts.data();
                        mk_present.resize(mk_counts_size, 0);
                        mk_present_size = mk_present.size();
                        mk_present_data = mk_present.data();
                    }
                    if (mk_counts_data[movie_idx] == 0) {
#ifdef TRACE
                        ++trace.movie_keyword_groups_created;
#endif
                        ++mk_group_count;
                        mk_present_data[movie_idx] = 1;
                        mk_movie_id_list.push_back(movie_id);
                    }
                    ++mk_counts_data[movie_idx];
                }
            }
        } else if (!mk_movie_has_nulls && !mk_keyword_has_nulls) {
            for (size_t idx = 0; idx < mk_row_count; ++idx) {
#ifdef TRACE
                ++trace.movie_keyword_rows_scanned;
#endif
                const int32_t keyword_id = mk_keyword_ids[idx];
                if (keyword_id < 0 ||
                    static_cast<size_t>(keyword_id) >= allowed_keyword_size ||
                    allowed_keyword_data[keyword_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_keyword_rows_emitted;
                ++trace.movie_keyword_agg_rows_in;
#endif
                const int32_t movie_id = mk_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_counts_size) {
                    mk_counts.resize(movie_idx + 1, 0);
                    mk_counts_size = mk_counts.size();
                    mk_counts_data = mk_counts.data();
                    mk_present.resize(mk_counts_size, 0);
                    mk_present_size = mk_present.size();
                    mk_present_data = mk_present.data();
                }
                if (mk_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.movie_keyword_groups_created;
#endif
                    ++mk_group_count;
                    mk_present_data[movie_idx] = 1;
                    mk_movie_id_list.push_back(movie_id);
                }
                ++mk_counts_data[movie_idx];
            }
        } else {
            for (size_t idx = 0; idx < mk_row_count; ++idx) {
#ifdef TRACE
                ++trace.movie_keyword_rows_scanned;
#endif
                if ((mk_movie_has_nulls && mk_movie_nulls[idx] != 0) ||
                    (mk_keyword_has_nulls && mk_keyword_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t keyword_id = mk_keyword_ids[idx];
                if (keyword_id < 0 ||
                    static_cast<size_t>(keyword_id) >= allowed_keyword_size ||
                    allowed_keyword_data[keyword_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_keyword_rows_emitted;
                ++trace.movie_keyword_agg_rows_in;
#endif
                const int32_t movie_id = mk_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_counts_size) {
                    mk_counts.resize(movie_idx + 1, 0);
                    mk_counts_size = mk_counts.size();
                    mk_counts_data = mk_counts.data();
                    mk_present.resize(mk_counts_size, 0);
                    mk_present_size = mk_present.size();
                    mk_present_data = mk_present.data();
                }
                if (mk_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.movie_keyword_groups_created;
#endif
                    ++mk_group_count;
                    mk_present_data[movie_idx] = 1;
                    mk_movie_id_list.push_back(movie_id);
                }
                ++mk_counts_data[movie_idx];
            }
        }
#ifdef TRACE
        }
#endif
    }

        std::vector<uint32_t> mc_counts;
        std::vector<uint8_t> mk_mc_present;
        std::vector<int32_t> mk_mc_movie_ids;
        if (!skip_query && !mk_counts.empty()) {
        mc_counts.assign(initial_movie_capacity, 0);
        mk_mc_present.assign(initial_movie_capacity, 0);
        mk_mc_movie_ids.reserve(512);
        size_t mc_counts_size = mc_counts.size();
        uint32_t* mc_counts_data = mc_counts.data();
        const uint8_t* mk_present_data = mk_present.data();
        const size_t mk_present_size = mk_present.size();
        size_t mk_mc_present_size = mk_mc_present.size();
        uint8_t* mk_mc_present_data = mk_mc_present.data();
        const auto* mc_movie_ids = mc_movie_id.i32.data();
        const auto* mc_company_ids = mc_company_id.i32.data();
        const auto* mc_company_type_ids = mc_company_type_id.i32.data();
        const auto* mc_movie_nulls = mc_movie_has_nulls ? mc_movie_id.is_null.data() : nullptr;
        const auto* mc_company_nulls = mc_company_has_nulls ? mc_company_id.is_null.data() : nullptr;
        const auto* mc_company_type_nulls =
            mc_company_type_has_nulls ? mc_company_type_id.is_null.data() : nullptr;
        const auto* allowed_company_data = allowed_company_ids.allowed.data();
        const size_t allowed_company_size = allowed_company_ids.allowed.size();
        const auto* allowed_company_type_data = allowed_company_type_ids.allowed.data();
        const size_t allowed_company_type_size = allowed_company_type_ids.allowed.size();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_movie_companies_scan");
#endif
        const size_t mc_row_count = static_cast<size_t>(movie_companies.row_count);
        if (movie_companies.sorted_by_movie_id && !mc_movie_has_nulls &&
            !mk_movie_id_list.empty()) {
            std::sort(mk_movie_id_list.begin(), mk_movie_id_list.end());
            size_t search_start = 0;
            for (const int32_t movie_id : mk_movie_id_list) {
                if (movie_id < 0) {
                    continue;
                }
                const int32_t* search_begin = mc_movie_ids + search_start;
                const int32_t* search_end = mc_movie_ids + mc_row_count;
                const int32_t* range_start =
                    std::lower_bound(search_begin, search_end, movie_id);
                if (range_start == search_end || *range_start != movie_id) {
                    continue;
                }
                const int32_t* range_end =
                    std::upper_bound(range_start, search_end, movie_id);
                const size_t idx_start = static_cast<size_t>(range_start - mc_movie_ids);
                const size_t idx_end = static_cast<size_t>(range_end - mc_movie_ids);
                search_start = idx_end;
                const size_t movie_idx = static_cast<size_t>(movie_id);
                for (size_t idx = idx_start; idx < idx_end; ++idx) {
#ifdef TRACE
                    ++trace.movie_companies_rows_scanned;
#endif
                    if ((mc_company_has_nulls && mc_company_nulls[idx] != 0) ||
                        (mc_company_type_has_nulls && mc_company_type_nulls[idx] != 0)) {
                        continue;
                    }
                    const int32_t company_id = mc_company_ids[idx];
                    const int32_t company_type_id = mc_company_type_ids[idx];
                    if (company_id < 0 ||
                        static_cast<size_t>(company_id) >= allowed_company_size ||
                        allowed_company_data[company_id] == 0) {
                        continue;
                    }
                    if (company_type_id < 0 ||
                        static_cast<size_t>(company_type_id) >= allowed_company_type_size ||
                        allowed_company_type_data[company_type_id] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.movie_companies_rows_emitted;
                    ++trace.movie_companies_agg_rows_in;
#endif
                    if (movie_idx >= mc_counts_size) {
                        mc_counts.resize(movie_idx + 1, 0);
                        mc_counts_size = mc_counts.size();
                        mc_counts_data = mc_counts.data();
                        mk_mc_present.resize(mc_counts_size, 0);
                        mk_mc_present_size = mk_mc_present.size();
                        mk_mc_present_data = mk_mc_present.data();
                    }
                    if (mc_counts_data[movie_idx] == 0) {
#ifdef TRACE
                        ++trace.movie_companies_groups_created;
#endif
                        ++mc_group_count;
                        mk_mc_present_data[movie_idx] = 1;
                        mk_mc_movie_ids.push_back(movie_id);
                    }
                    ++mc_counts_data[movie_idx];
                }
            }
        } else if (!mc_movie_has_nulls && !mc_company_has_nulls && !mc_company_type_has_nulls) {
            for (size_t idx = 0; idx < mc_row_count; ++idx) {
#ifdef TRACE
                ++trace.movie_companies_rows_scanned;
#endif
                const int32_t movie_id = mc_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_present_size || mk_present_data[movie_idx] == 0) {
                    continue;
                }
                const int32_t company_id = mc_company_ids[idx];
                const int32_t company_type_id = mc_company_type_ids[idx];
                if (company_id < 0 ||
                    static_cast<size_t>(company_id) >= allowed_company_size ||
                    allowed_company_data[company_id] == 0) {
                    continue;
                }
                if (company_type_id < 0 ||
                    static_cast<size_t>(company_type_id) >= allowed_company_type_size ||
                    allowed_company_type_data[company_type_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_companies_rows_emitted;
                ++trace.movie_companies_agg_rows_in;
#endif
                if (movie_idx >= mc_counts_size) {
                    mc_counts.resize(movie_idx + 1, 0);
                    mc_counts_size = mc_counts.size();
                    mc_counts_data = mc_counts.data();
                    mk_mc_present.resize(mc_counts_size, 0);
                    mk_mc_present_size = mk_mc_present.size();
                    mk_mc_present_data = mk_mc_present.data();
                }
                if (mc_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.movie_companies_groups_created;
#endif
                    ++mc_group_count;
                    mk_mc_present_data[movie_idx] = 1;
                    mk_mc_movie_ids.push_back(movie_id);
                }
                ++mc_counts_data[movie_idx];
            }
        } else {
            for (size_t idx = 0; idx < mc_row_count; ++idx) {
#ifdef TRACE
                ++trace.movie_companies_rows_scanned;
#endif
                if ((mc_movie_has_nulls && mc_movie_nulls[idx] != 0) ||
                    (mc_company_has_nulls && mc_company_nulls[idx] != 0) ||
                    (mc_company_type_has_nulls && mc_company_type_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t movie_id = mc_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_present_size || mk_present_data[movie_idx] == 0) {
                    continue;
                }
                const int32_t company_id = mc_company_ids[idx];
                const int32_t company_type_id = mc_company_type_ids[idx];
                if (company_id < 0 ||
                    static_cast<size_t>(company_id) >= allowed_company_size ||
                    allowed_company_data[company_id] == 0) {
                    continue;
                }
                if (company_type_id < 0 ||
                    static_cast<size_t>(company_type_id) >= allowed_company_type_size ||
                    allowed_company_type_data[company_type_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.movie_companies_rows_emitted;
                ++trace.movie_companies_agg_rows_in;
#endif
                if (movie_idx >= mc_counts_size) {
                    mc_counts.resize(movie_idx + 1, 0);
                    mc_counts_size = mc_counts.size();
                    mc_counts_data = mc_counts.data();
                    mk_mc_present.resize(mc_counts_size, 0);
                    mk_mc_present_size = mk_mc_present.size();
                    mk_mc_present_data = mk_mc_present.data();
                }
                if (mc_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.movie_companies_groups_created;
#endif
                    ++mc_group_count;
                    mk_mc_present_data[movie_idx] = 1;
                    mk_mc_movie_ids.push_back(movie_id);
                }
                ++mc_counts_data[movie_idx];
            }
        }
#ifdef TRACE
        }
#endif
    }

        std::vector<uint32_t> ci_counts;
        std::vector<uint8_t> mk_mc_ci_present;
        std::vector<int32_t> mk_mc_ci_movie_ids;
        if (!skip_query && !mk_counts.empty() && !mc_counts.empty()) {
        ci_counts.assign(initial_movie_capacity, 0);
        mk_mc_ci_present.assign(initial_movie_capacity, 0);
            mk_mc_ci_movie_ids.reserve(256);
        size_t ci_counts_size = ci_counts.size();
        uint32_t* ci_counts_data = ci_counts.data();
        const uint8_t* mk_mc_present_data = mk_mc_present.data();
        const size_t mk_mc_present_size = mk_mc_present.size();
        size_t mk_mc_ci_present_size = mk_mc_ci_present.size();
        uint8_t* mk_mc_ci_present_data = mk_mc_ci_present.data();
        const auto* ci_movie_ids = ci_movie_id.i32.data();
        const auto* ci_person_ids = ci_person_id.i32.data();
        const auto* ci_role_ids = ci_role_id.i32.data();
        const auto* ci_movie_nulls = ci_movie_has_nulls ? ci_movie_id.is_null.data() : nullptr;
        const auto* ci_person_nulls = ci_person_has_nulls ? ci_person_id.is_null.data() : nullptr;
        const auto* ci_role_nulls = ci_role_has_nulls ? ci_role_id.is_null.data() : nullptr;
        const auto* allowed_person_data = allowed_person_ids.allowed.data();
        const size_t allowed_person_size = allowed_person_ids.allowed.size();
        const auto* allowed_role_data = allowed_role_ids.allowed.data();
        const size_t allowed_role_size = allowed_role_ids.allowed.size();
#ifdef TRACE
        {
            PROFILE_SCOPE(&trace.recorder, "q3a_cast_info_scan");
#endif
        const size_t ci_row_count = static_cast<size_t>(cast_info.row_count);
        if (cast_info.sorted_by_movie_id && !ci_movie_has_nulls && !mk_mc_movie_ids.empty()) {
            std::sort(mk_mc_movie_ids.begin(), mk_mc_movie_ids.end());
            size_t search_start = 0;
            for (const int32_t movie_id : mk_mc_movie_ids) {
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_mc_present_size || mk_mc_present_data[movie_idx] == 0) {
                    continue;
                }
                const int32_t* search_begin = ci_movie_ids + search_start;
                const int32_t* search_end = ci_movie_ids + ci_row_count;
                const int32_t* range_start =
                    std::lower_bound(search_begin, search_end, movie_id);
                if (range_start == search_end || *range_start != movie_id) {
                    continue;
                }
                const int32_t* range_end =
                    std::upper_bound(range_start, search_end, movie_id);
                const size_t idx_start = static_cast<size_t>(range_start - ci_movie_ids);
                const size_t idx_end = static_cast<size_t>(range_end - ci_movie_ids);
                search_start = idx_end;
                for (size_t idx = idx_start; idx < idx_end; ++idx) {
#ifdef TRACE
                    ++trace.cast_info_rows_scanned;
#endif
                    if ((ci_person_has_nulls && ci_person_nulls[idx] != 0) ||
                        (ci_role_has_nulls && ci_role_nulls[idx] != 0)) {
                        continue;
                    }
                    const int32_t person_id = ci_person_ids[idx];
                    const int32_t role_id = ci_role_ids[idx];
                    if (person_id < 0 ||
                        static_cast<size_t>(person_id) >= allowed_person_size ||
                        allowed_person_data[person_id] == 0) {
                        continue;
                    }
                    if (role_id < 0 ||
                        static_cast<size_t>(role_id) >= allowed_role_size ||
                        allowed_role_data[role_id] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.cast_info_rows_emitted;
                    ++trace.cast_info_agg_rows_in;
#endif
                    if (movie_idx >= ci_counts_size) {
                        ci_counts.resize(movie_idx + 1, 0);
                        ci_counts_size = ci_counts.size();
                        ci_counts_data = ci_counts.data();
                        mk_mc_ci_present.resize(ci_counts_size, 0);
                        mk_mc_ci_present_size = mk_mc_ci_present.size();
                        mk_mc_ci_present_data = mk_mc_ci_present.data();
                    }
                    if (ci_counts_data[movie_idx] == 0) {
#ifdef TRACE
                        ++trace.cast_info_groups_created;
#endif
                        ++ci_group_count;
                        mk_mc_ci_present_data[movie_idx] = 1;
                        mk_mc_ci_movie_ids.push_back(movie_id);
                    }
                    ++ci_counts_data[movie_idx];
                }
            }
        } else if (!ci_movie_has_nulls && !ci_person_has_nulls && !ci_role_has_nulls) {
            for (size_t idx = 0; idx < ci_row_count; ++idx) {
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                const int32_t movie_id = ci_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_mc_present_size || mk_mc_present_data[movie_idx] == 0) {
                    continue;
                }
                const int32_t person_id = ci_person_ids[idx];
                const int32_t role_id = ci_role_ids[idx];
                if (person_id < 0 ||
                    static_cast<size_t>(person_id) >= allowed_person_size ||
                    allowed_person_data[person_id] == 0) {
                    continue;
                }
                if (role_id < 0 ||
                    static_cast<size_t>(role_id) >= allowed_role_size ||
                    allowed_role_data[role_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.cast_info_rows_emitted;
                ++trace.cast_info_agg_rows_in;
#endif
                if (movie_idx >= ci_counts_size) {
                    ci_counts.resize(movie_idx + 1, 0);
                    ci_counts_size = ci_counts.size();
                    ci_counts_data = ci_counts.data();
                    mk_mc_ci_present.resize(ci_counts_size, 0);
                    mk_mc_ci_present_size = mk_mc_ci_present.size();
                    mk_mc_ci_present_data = mk_mc_ci_present.data();
                }
                if (ci_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.cast_info_groups_created;
#endif
                    ++ci_group_count;
                    mk_mc_ci_present_data[movie_idx] = 1;
                    mk_mc_ci_movie_ids.push_back(movie_id);
                }
                ++ci_counts_data[movie_idx];
            }
        } else {
            for (size_t idx = 0; idx < ci_row_count; ++idx) {
#ifdef TRACE
                ++trace.cast_info_rows_scanned;
#endif
                if ((ci_movie_has_nulls && ci_movie_nulls[idx] != 0) ||
                    (ci_person_has_nulls && ci_person_nulls[idx] != 0) ||
                    (ci_role_has_nulls && ci_role_nulls[idx] != 0)) {
                    continue;
                }
                const int32_t movie_id = ci_movie_ids[idx];
                if (movie_id < 0) {
                    continue;
                }
                const size_t movie_idx = static_cast<size_t>(movie_id);
                if (movie_idx >= mk_mc_present_size || mk_mc_present_data[movie_idx] == 0) {
                    continue;
                }
                const int32_t person_id = ci_person_ids[idx];
                const int32_t role_id = ci_role_ids[idx];
                if (person_id < 0 ||
                    static_cast<size_t>(person_id) >= allowed_person_size ||
                    allowed_person_data[person_id] == 0) {
                    continue;
                }
                if (role_id < 0 ||
                    static_cast<size_t>(role_id) >= allowed_role_size ||
                    allowed_role_data[role_id] == 0) {
                    continue;
                }
#ifdef TRACE
                ++trace.cast_info_rows_emitted;
                ++trace.cast_info_agg_rows_in;
#endif
                if (movie_idx >= ci_counts_size) {
                    ci_counts.resize(movie_idx + 1, 0);
                    ci_counts_size = ci_counts.size();
                    ci_counts_data = ci_counts.data();
                    mk_mc_ci_present.resize(ci_counts_size, 0);
                    mk_mc_ci_present_size = mk_mc_ci_present.size();
                    mk_mc_ci_present_data = mk_mc_ci_present.data();
                }
                if (ci_counts_data[movie_idx] == 0) {
#ifdef TRACE
                    ++trace.cast_info_groups_created;
#endif
                    ++ci_group_count;
                    mk_mc_ci_present_data[movie_idx] = 1;
                    mk_mc_ci_movie_ids.push_back(movie_id);
                }
                ++ci_counts_data[movie_idx];
            }
        }
#ifdef TRACE
        }
#endif
    }

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q3a_title_scan");
#endif
        if (!skip_query) {
            const uint32_t* mk_counts_data = mk_counts.data();
            const size_t mk_counts_size = mk_counts.size();
            const uint32_t* mc_counts_data = mc_counts.data();
            const size_t mc_counts_size = mc_counts.size();
            const uint32_t* ci_counts_data = ci_counts.data();
            const size_t ci_counts_size = ci_counts.size();
            const uint8_t* mk_mc_ci_present_data = mk_mc_ci_present.data();
            const size_t mk_mc_ci_present_size = mk_mc_ci_present.size();
            const auto* t_ids = t_id.i32.data();
            const auto* t_years = t_year.i32.data();
            const auto* t_kind_ids = t_kind_id.i32.data();
            const auto* t_id_nulls = t_id_has_nulls ? t_id.is_null.data() : nullptr;
            const auto* t_year_nulls = t_year_has_nulls ? t_year.is_null.data() : nullptr;
            const auto* t_kind_nulls = t_kind_has_nulls ? t_kind_id.is_null.data() : nullptr;
            const auto* allowed_kind_data = allowed_kind_ids.allowed.data();
            const size_t allowed_kind_size = allowed_kind_ids.allowed.size();
            const size_t t_row_count = static_cast<size_t>(title.row_count);
            if (title.sorted_by_movie_id && !t_id_has_nulls && !mk_mc_ci_movie_ids.empty()) {
                std::sort(mk_mc_ci_movie_ids.begin(), mk_mc_ci_movie_ids.end());
                size_t search_start = 0;
                for (const int32_t movie_id : mk_mc_ci_movie_ids) {
                    if (movie_id < 0) {
                        continue;
                    }
                    const size_t movie_idx = static_cast<size_t>(movie_id);
                    if (movie_idx >= mk_counts_size || movie_idx >= mc_counts_size ||
                        movie_idx >= ci_counts_size) {
                        continue;
                    }
                    if (mk_counts_data[movie_idx] == 0 || mc_counts_data[movie_idx] == 0 ||
                        ci_counts_data[movie_idx] == 0) {
                        continue;
                    }
                    const int32_t* search_begin = t_ids + search_start;
                    const int32_t* search_end = t_ids + t_row_count;
                    const int32_t* row_it =
                        std::lower_bound(search_begin, search_end, movie_id);
                    if (row_it == search_end || *row_it != movie_id) {
                        continue;
                    }
                    const size_t idx = static_cast<size_t>(row_it - t_ids);
                    search_start = idx;
#ifdef TRACE
                    ++trace.title_rows_scanned;
#endif
                    if ((t_year_has_nulls && t_year_nulls[idx] != 0) ||
                        (t_kind_has_nulls && t_kind_nulls[idx] != 0)) {
                        continue;
                    }
                    const int32_t year = t_years[idx];
                    if (year > year1 || year < year2) {
                        continue;
                    }
                    const int32_t kind_id = t_kind_ids[idx];
                    if (kind_id < 0 ||
                        static_cast<size_t>(kind_id) >= allowed_kind_size ||
                        allowed_kind_data[kind_id] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.title_rows_emitted;
                    ++trace.title_join_probe_rows_in;
                    ++trace.title_join_rows_emitted;
#endif
                    total += static_cast<int64_t>(mk_counts_data[movie_idx]) *
                        static_cast<int64_t>(mc_counts_data[movie_idx]) *
                        static_cast<int64_t>(ci_counts_data[movie_idx]);
                }
            } else if (!t_id_has_nulls && !t_year_has_nulls && !t_kind_has_nulls) {
                for (size_t idx = 0; idx < t_row_count; ++idx) {
#ifdef TRACE
                    ++trace.title_rows_scanned;
#endif
                    const int32_t year = t_years[idx];
                    if (year > year1 || year < year2) {
                        continue;
                    }
                    const int32_t kind_id = t_kind_ids[idx];
                    if (kind_id < 0 ||
                        static_cast<size_t>(kind_id) >= allowed_kind_size ||
                        allowed_kind_data[kind_id] == 0) {
                        continue;
                    }
                    const int32_t movie_id = t_ids[idx];
                    if (movie_id < 0) {
                        continue;
                    }
                    const size_t movie_idx = static_cast<size_t>(movie_id);
                    if (movie_idx >= mk_mc_ci_present_size ||
                        mk_mc_ci_present_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= mk_counts_size || mk_counts_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= mc_counts_size || mc_counts_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= ci_counts_size || ci_counts_data[movie_idx] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.title_rows_emitted;
                    ++trace.title_join_probe_rows_in;
                    ++trace.title_join_rows_emitted;
#endif
                    total += static_cast<int64_t>(mk_counts_data[movie_idx]) *
                        static_cast<int64_t>(mc_counts_data[movie_idx]) *
                        static_cast<int64_t>(ci_counts_data[movie_idx]);
                }
            } else {
                for (size_t idx = 0; idx < t_row_count; ++idx) {
#ifdef TRACE
                    ++trace.title_rows_scanned;
#endif
                    if ((t_id_has_nulls && t_id_nulls[idx] != 0) ||
                        (t_year_has_nulls && t_year_nulls[idx] != 0) ||
                        (t_kind_has_nulls && t_kind_nulls[idx] != 0)) {
                        continue;
                    }
                    const int32_t year = t_years[idx];
                    if (year > year1 || year < year2) {
                        continue;
                    }
                    const int32_t kind_id = t_kind_ids[idx];
                    if (kind_id < 0 ||
                        static_cast<size_t>(kind_id) >= allowed_kind_size ||
                        allowed_kind_data[kind_id] == 0) {
                        continue;
                    }
                    const int32_t movie_id = t_ids[idx];
                    if (movie_id < 0) {
                        continue;
                    }
                    const size_t movie_idx = static_cast<size_t>(movie_id);
                    if (movie_idx >= mk_mc_ci_present_size ||
                        mk_mc_ci_present_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= mk_counts_size || mk_counts_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= mc_counts_size || mc_counts_data[movie_idx] == 0) {
                        continue;
                    }
                    if (movie_idx >= ci_counts_size || ci_counts_data[movie_idx] == 0) {
                        continue;
                    }
#ifdef TRACE
                    ++trace.title_rows_emitted;
                    ++trace.title_join_probe_rows_in;
                    ++trace.title_join_rows_emitted;
#endif
                    total += static_cast<int64_t>(mk_counts_data[movie_idx]) *
                        static_cast<int64_t>(mc_counts_data[movie_idx]) *
                        static_cast<int64_t>(ci_counts_data[movie_idx]);
                }
            }
        }
#ifdef TRACE
        }
#endif
    }
#ifdef TRACE
    trace.movie_keyword_agg_rows_emitted = mk_group_count;
    trace.movie_companies_agg_rows_emitted = mc_group_count;
    trace.cast_info_agg_rows_emitted = ci_group_count;
    trace.title_join_build_rows_in = mk_group_count + mc_group_count + ci_group_count;
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q3a_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q3a_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q3a_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q3a_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q3a_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q3a_movie_companies_scan_rows_scanned", trace.movie_companies_rows_scanned);
    print_count("q3a_movie_companies_scan_rows_emitted", trace.movie_companies_rows_emitted);
    print_count("q3a_movie_companies_agg_rows_in", trace.movie_companies_agg_rows_in);
    print_count("q3a_movie_companies_groups_created", trace.movie_companies_groups_created);
    print_count("q3a_movie_companies_agg_rows_emitted", trace.movie_companies_agg_rows_emitted);
    print_count("q3a_cast_info_scan_rows_scanned", trace.cast_info_rows_scanned);
    print_count("q3a_cast_info_scan_rows_emitted", trace.cast_info_rows_emitted);
    print_count("q3a_cast_info_agg_rows_in", trace.cast_info_agg_rows_in);
    print_count("q3a_cast_info_groups_created", trace.cast_info_groups_created);
    print_count("q3a_cast_info_agg_rows_emitted", trace.cast_info_agg_rows_emitted);
    print_count("q3a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q3a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q3a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q3a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q3a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q3a_query_output_rows", trace.query_output_rows);
#endif
    return total;
}