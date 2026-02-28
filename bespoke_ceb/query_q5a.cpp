#include "query_q5a.hpp"

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "trace.hpp"

namespace q5a_internal {

std::vector<int32_t> build_int_list(const std::vector<std::string>& values) {
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

bool id_in_list(int32_t id, const std::vector<int32_t>& values) {
    for (int32_t value : values) {
        if (value == id) {
            return true;
        }
    }
    return false;
}

bool parse_numeric_value(const std::string& value, double& out) {
    const size_t size = value.size();
    if (size == 0) {
        return false;
    }
    const char* data = value.data();
    size_t pos = 0;
    bool has_int = false;
    if (data[pos] == '.') {
        // integer part omitted
    } else if (data[pos] == '0') {
        has_int = true;
        ++pos;
        if (pos < size && data[pos] != '.') {
            return false;
        }
    } else if (data[pos] >= '1' && data[pos] <= '9') {
        has_int = true;
        ++pos;
        while (pos < size && data[pos] >= '0' && data[pos] <= '9') {
            ++pos;
        }
    } else {
        return false;
    }

    bool has_frac = false;
    if (pos < size && data[pos] == '.') {
        ++pos;
        const size_t frac_start = pos;
        while (pos < size && data[pos] >= '0' && data[pos] <= '9') {
            ++pos;
        }
        if (pos == frac_start) {
            return false;
        }
        has_frac = true;
    }
    if (pos != size || (!has_int && !has_frac)) {
        return false;
    }

    double result = 0.0;
    size_t i = 0;
    if (data[i] == '.') {
        i = 1;
    } else {
        for (; i < size && data[i] != '.'; ++i) {
            result = result * 10.0 + static_cast<double>(data[i] - '0');
        }
        if (i < size && data[i] == '.') {
            ++i;
        }
    }
    if (i < size) {
        double scale = 0.1;
        for (; i < size; ++i) {
            result += static_cast<double>(data[i] - '0') * scale;
            scale *= 0.1;
        }
    }
    out = result;
    return true;
}

struct Q5aScratch {
    std::vector<uint32_t> info1_stamp;
    uint32_t info1_gen = 1;

    std::unique_ptr<double[]> numeric_cache;
    std::unique_ptr<uint8_t[]> numeric_state;
    size_t numeric_size = 0;
    std::vector<uint32_t> candidate_stamp;
    uint32_t candidate_gen = 1;

    std::vector<uint32_t> counts_info;
    std::vector<uint32_t> counts_idx1;
    std::vector<uint32_t> counts_idx2;
    std::vector<uint32_t> counts_kw;
    bool counts_ready = false;
};

inline Q5aScratch& scratch() {
    static thread_local Q5aScratch scratch_state;
    return scratch_state;
}

}  // namespace q5a_internal

#ifdef TRACE
struct Q5aTrace {
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
};
#endif

void prepare_q5a(const Database& db) {
    auto& scratch = q5a_internal::scratch();
    const size_t pool_size = db.string_pool.values.size();
    if (scratch.info1_stamp.size() < pool_size) {
        scratch.info1_stamp.resize(pool_size, 0);
    }
    if (scratch.numeric_size < pool_size) {
        auto new_cache = std::unique_ptr<double[]>(new double[pool_size]);
        auto new_state = std::unique_ptr<uint8_t[]>(new uint8_t[pool_size]());
        if (scratch.numeric_size > 0) {
            std::memcpy(new_cache.get(),
                        scratch.numeric_cache.get(),
                        scratch.numeric_size * sizeof(double));
            std::memcpy(new_state.get(),
                        scratch.numeric_state.get(),
                        scratch.numeric_size * sizeof(uint8_t));
        }
        scratch.numeric_cache = std::move(new_cache);
        scratch.numeric_state = std::move(new_state);
        scratch.numeric_size = pool_size;
    }
    const auto& title = db.tables.at("title");
    const size_t movie_size = static_cast<size_t>(title.row_count + 1);
    if (scratch.candidate_stamp.size() < movie_size) {
        scratch.candidate_stamp.resize(movie_size, 0);
    }
    if (scratch.counts_info.size() < movie_size) {
        scratch.counts_info.resize(movie_size);
        scratch.counts_idx1.resize(movie_size);
        scratch.counts_idx2.resize(movie_size);
        scratch.counts_kw.resize(movie_size);
    }
    std::fill(scratch.counts_info.begin(), scratch.counts_info.begin() + movie_size, 0);
    std::fill(scratch.counts_idx1.begin(), scratch.counts_idx1.begin() + movie_size, 0);
    std::fill(scratch.counts_idx2.begin(), scratch.counts_idx2.begin() + movie_size, 0);
    std::fill(scratch.counts_kw.begin(), scratch.counts_kw.begin() + movie_size, 0);
    scratch.counts_ready = true;
}

int64_t run_q5a(const Database& db, const Q5aArgs& args) {
    int64_t total_count = 0;
#ifdef TRACE
    Q5aTrace trace;
    bool skip_query = false;
    {
        PROFILE_SCOPE(&trace.recorder, "q5a_total");
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
    const auto& kind_id_col = kind_type.columns.at("id");
    const auto& kind_col = kind_type.columns.at("kind");
    const auto kind_filter_ids = q5a_internal::build_string_id_list(db.string_pool, args.KIND);
    if (kind_filter_ids.empty()) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }
    int32_t max_kind_id = 0;
    for (int64_t i = 0; i < kind_type.row_count; ++i) {
        if (kind_id_col.is_null[i] == 0) {
            max_kind_id = std::max(max_kind_id, kind_id_col.i32[static_cast<size_t>(i)]);
        }
    }
    std::vector<uint8_t> allowed_kind_ids(static_cast<size_t>(max_kind_id + 1), 0);
    int64_t allowed_kind_count = 0;
    for (int64_t i = 0; i < kind_type.row_count; ++i) {
        if (kind_id_col.is_null[i] != 0 || kind_col.is_null[i] != 0) {
            continue;
        }
        const int32_t kind_id = kind_id_col.i32[static_cast<size_t>(i)];
        if (kind_id < 0 || kind_id > max_kind_id) {
            continue;
        }
        if (q5a_internal::id_in_list(kind_col.str_ids[static_cast<size_t>(i)], kind_filter_ids)) {
            if (allowed_kind_ids[static_cast<size_t>(kind_id)] == 0) {
                allowed_kind_ids[static_cast<size_t>(kind_id)] = 1;
                ++allowed_kind_count;
            }
        }
    }
    if (allowed_kind_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& info_type = db.tables.at("info_type");
    const auto& info_type_id_col = info_type.columns.at("id");
    int32_t max_info_type_id = 0;
    for (int64_t i = 0; i < info_type.row_count; ++i) {
        if (info_type_id_col.is_null[i] == 0) {
            max_info_type_id =
                std::max(max_info_type_id, info_type_id_col.i32[static_cast<size_t>(i)]);
        }
    }
    std::vector<uint8_t> valid_info_type_ids(static_cast<size_t>(max_info_type_id + 1), 0);
    for (int64_t i = 0; i < info_type.row_count; ++i) {
        if (info_type_id_col.is_null[i] != 0) {
            continue;
        }
        const int32_t info_id = info_type_id_col.i32[static_cast<size_t>(i)];
        if (info_id >= 0 && info_id <= max_info_type_id) {
            valid_info_type_ids[static_cast<size_t>(info_id)] = 1;
        }
    }
    if (id2 < 0 || id3 < 0 || id2 > max_info_type_id || id3 > max_info_type_id ||
        valid_info_type_ids[static_cast<size_t>(id2)] == 0 ||
        valid_info_type_ids[static_cast<size_t>(id3)] == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto info_type_ids1 = q5a_internal::build_int_list(args.ID1);
    std::vector<uint8_t> info_type_filter1(static_cast<size_t>(max_info_type_id + 1), 0);
    int64_t info_type_filter_count = 0;
    for (int32_t value : info_type_ids1) {
        if (value < 0 || value > max_info_type_id) {
            continue;
        }
        if (valid_info_type_ids[static_cast<size_t>(value)] == 0) {
            continue;
        }
        if (info_type_filter1[static_cast<size_t>(value)] == 0) {
            info_type_filter1[static_cast<size_t>(value)] = 1;
            ++info_type_filter_count;
        }
    }
    if (info_type_filter_count == 0) {
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
    const auto info1_filter_ids = q5a_internal::build_string_id_list(db.string_pool, args.INFO1);
    const size_t pool_size = db.string_pool.values.size();
    auto& scratch = q5a_internal::scratch();
    if (scratch.info1_stamp.size() < pool_size) {
        scratch.info1_stamp.resize(pool_size, 0);
    }
    uint32_t info1_gen = ++scratch.info1_gen;
    if (info1_gen == 0) {
        scratch.info1_gen = info1_gen = 1;
        std::fill(scratch.info1_stamp.begin(), scratch.info1_stamp.end(), 0);
    }
    auto* info1_stamp = scratch.info1_stamp.data();
    int64_t info1_filter_count = 0;
    for (int32_t id : info1_filter_ids) {
        if (id < 0) {
            continue;
        }
        const size_t idx = static_cast<size_t>(id);
        if (idx >= pool_size) {
            continue;
        }
        if (info1_stamp[idx] != info1_gen) {
            info1_stamp[idx] = info1_gen;
            ++info1_filter_count;
        }
    }
    if (info1_filter_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& title = db.tables.at("title");
    const int64_t max_movie_id = title.row_count;
    const size_t movie_size = static_cast<size_t>(max_movie_id + 1);
    if (scratch.counts_info.size() < movie_size) {
        scratch.counts_info.resize(movie_size);
        scratch.counts_idx1.resize(movie_size);
        scratch.counts_idx2.resize(movie_size);
        scratch.counts_kw.resize(movie_size);
        scratch.counts_ready = false;
    }
    if (!scratch.counts_ready) {
        std::fill(scratch.counts_info.begin(), scratch.counts_info.begin() + movie_size, 0);
        std::fill(scratch.counts_idx1.begin(), scratch.counts_idx1.begin() + movie_size, 0);
        std::fill(scratch.counts_idx2.begin(), scratch.counts_idx2.begin() + movie_size, 0);
        std::fill(scratch.counts_kw.begin(), scratch.counts_kw.begin() + movie_size, 0);
    }
    scratch.counts_ready = false;
    auto* info_counts = scratch.counts_info.data();
    auto* idx_counts1 = scratch.counts_idx1.data();
    auto* idx_counts2 = scratch.counts_idx2.data();
    auto* keyword_counts = scratch.counts_kw.data();
    std::vector<int32_t> idx1_movie_ids;
    idx1_movie_ids.reserve(20000);
    uint32_t info_groups = 0;
    const auto* mi_movie_id_data = mi_movie_id.i32.data();
    const auto* mi_movie_id_null = mi_movie_id.is_null.data();
    const auto* mi_info_type_id_data = mi_info_type_id.i32.data();
    const auto* mi_info_type_id_null = mi_info_type_id.is_null.data();
    const auto* mi_info_data = mi_info.str_ids.data();
    const auto* mi_info_null = mi_info.is_null.data();
    const bool mi_has_nulls = mi_movie_id.has_nulls || mi_info_type_id.has_nulls ||
        mi_info.has_nulls;
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q5a_movie_info_scan");
#endif
    if (!mi_has_nulls) {
        for (int64_t i = 0; i < movie_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (info_type_filter1[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info1_stamp[static_cast<size_t>(info_id)] != info1_gen) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
            if (info_counts[movie_idx]++ == 0) {
                ++trace.movie_info_groups_created;
            }
#else
            if (info_counts[movie_idx]++ == 0) {
                ++info_groups;
            }
#endif
        }
    } else {
        for (int64_t i = 0; i < movie_info.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_rows_scanned;
#endif
            if (mi_movie_id_null[idx] != 0 || mi_info_type_id_null[idx] != 0 ||
                mi_info_null[idx] != 0) {
                continue;
            }
            const int32_t info_type_id = mi_info_type_id_data[idx];
            if (info_type_filter1[static_cast<size_t>(info_type_id)] == 0) {
                continue;
            }
            const int32_t info_id = mi_info_data[idx];
            if (info1_stamp[static_cast<size_t>(info_id)] != info1_gen) {
                continue;
            }
            const int32_t movie_id = mi_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
#ifdef TRACE
            ++trace.movie_info_rows_emitted;
            ++trace.movie_info_agg_rows_in;
            if (info_counts[movie_idx]++ == 0) {
                ++trace.movie_info_groups_created;
            }
#else
            if (info_counts[movie_idx]++ == 0) {
                ++info_groups;
            }
#endif
        }
    }
#ifdef TRACE
    }
#endif
#ifndef TRACE
    if (info_groups == 0) {
#else
    if (trace.movie_info_groups_created == 0) {
#endif
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_info_idx = db.tables.at("movie_info_idx");
    const auto& mii_movie_id = movie_info_idx.columns.at("movie_id");
    const auto& mii_info_type_id = movie_info_idx.columns.at("info_type_id");
    const auto& mii_info = movie_info_idx.columns.at("info");

    uint32_t idx_groups1 = 0;
    uint32_t idx_groups2 = 0;
    if (scratch.numeric_size < pool_size) {
        auto new_cache = std::unique_ptr<double[]>(new double[pool_size]);
        auto new_state = std::unique_ptr<uint8_t[]>(new uint8_t[pool_size]());
        if (scratch.numeric_size > 0) {
            std::memcpy(new_cache.get(),
                        scratch.numeric_cache.get(),
                        scratch.numeric_size * sizeof(double));
            std::memcpy(new_state.get(),
                        scratch.numeric_state.get(),
                        scratch.numeric_size * sizeof(uint8_t));
        }
        scratch.numeric_cache = std::move(new_cache);
        scratch.numeric_state = std::move(new_state);
        scratch.numeric_size = pool_size;
    }
    auto* numeric_cache = scratch.numeric_cache.get();
    auto* numeric_state = scratch.numeric_state.get();
    const size_t numeric_state_size = scratch.numeric_size;
    auto parse_numeric_cached = [&](int32_t str_id, double& out) -> bool {
        if (str_id < 0) {
            return false;
        }
        const size_t idx = static_cast<size_t>(str_id);
        if (idx >= numeric_state_size) {
            return false;
        }
        const uint8_t state = numeric_state[idx];
        if (state == 1) {
            out = numeric_cache[idx];
            return true;
        }
        if (state == 2) {
            return false;
        }
        const std::string& value = db.string_pool.get(str_id);
        if (q5a_internal::parse_numeric_value(value, out)) {
            numeric_cache[idx] = out;
            numeric_state[idx] = 1;
            return true;
        }
        numeric_state[idx] = 2;
        return false;
    };

#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q5a_movie_info_idx_scan");
#endif
    const auto* mii_movie_id_data = mii_movie_id.i32.data();
    const auto* mii_movie_id_null = mii_movie_id.is_null.data();
    const auto* mii_info_type_id_data = mii_info_type_id.i32.data();
    const auto* mii_info_type_id_null = mii_info_type_id.is_null.data();
    const auto* mii_info_data = mii_info.str_ids.data();
    const auto* mii_info_null = mii_info.is_null.data();
    const bool mii_has_nulls = mii_movie_id.has_nulls || mii_info_type_id.has_nulls ||
        mii_info.has_nulls;
    if (!mii_has_nulls) {
        for (int64_t i = 0; i < movie_info_idx.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_idx_rows_scanned;
#endif
            const int32_t info_type_id = mii_info_type_id_data[idx];
            double info_num = 0.0;
            const int32_t movie_id = mii_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_type_id == id2) {
                if (!parse_numeric_cached(mii_info_data[idx], info_num)) {
                    continue;
                }
                if (info_num >= info4 && info_num <= info5) {
#ifdef TRACE
                    ++trace.movie_info_idx_rows_emitted_id2;
                    ++trace.movie_info_idx_agg_rows_in_id2;
                    if (idx_counts1[movie_idx]++ == 0) {
                        ++trace.movie_info_idx_groups_created_id2;
                        idx1_movie_ids.push_back(movie_id);
                    }
#else
                    if (idx_counts1[movie_idx]++ == 0) {
                        ++idx_groups1;
                        idx1_movie_ids.push_back(movie_id);
                    }
#endif
                }
            } else if (info_type_id == id3) {
                if (!parse_numeric_cached(mii_info_data[idx], info_num)) {
                    continue;
                }
                if (info_num >= info3 && info_num <= info2) {
#ifdef TRACE
                    ++trace.movie_info_idx_rows_emitted_id3;
                    ++trace.movie_info_idx_agg_rows_in_id3;
                    if (idx_counts2[movie_idx]++ == 0) {
                        ++trace.movie_info_idx_groups_created_id3;
                    }
#else
                    if (idx_counts2[movie_idx]++ == 0) {
                        ++idx_groups2;
                    }
#endif
                }
            }
        }
    } else {
        for (int64_t i = 0; i < movie_info_idx.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_info_idx_rows_scanned;
#endif
            if (mii_movie_id_null[idx] != 0 || mii_info_type_id_null[idx] != 0 ||
                mii_info_null[idx] != 0) {
                continue;
            }
            const int32_t info_type_id = mii_info_type_id_data[idx];
            double info_num = 0.0;
            const int32_t movie_id = mii_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (info_type_id == id2) {
                if (!parse_numeric_cached(mii_info_data[idx], info_num)) {
                    continue;
                }
                if (info_num >= info4 && info_num <= info5) {
#ifdef TRACE
                    ++trace.movie_info_idx_rows_emitted_id2;
                    ++trace.movie_info_idx_agg_rows_in_id2;
                    if (idx_counts1[movie_idx]++ == 0) {
                        ++trace.movie_info_idx_groups_created_id2;
                        idx1_movie_ids.push_back(movie_id);
                    }
#else
                    if (idx_counts1[movie_idx]++ == 0) {
                        ++idx_groups1;
                        idx1_movie_ids.push_back(movie_id);
                    }
#endif
                }
            } else if (info_type_id == id3) {
                if (!parse_numeric_cached(mii_info_data[idx], info_num)) {
                    continue;
                }
                if (info_num >= info3 && info_num <= info2) {
#ifdef TRACE
                    ++trace.movie_info_idx_rows_emitted_id3;
                    ++trace.movie_info_idx_agg_rows_in_id3;
                    if (idx_counts2[movie_idx]++ == 0) {
                        ++trace.movie_info_idx_groups_created_id3;
                    }
#else
                    if (idx_counts2[movie_idx]++ == 0) {
                        ++idx_groups2;
                    }
#endif
                }
            }
        }
    }
#ifdef TRACE
    }
#endif
#ifndef TRACE
    if (idx_groups1 == 0 || idx_groups2 == 0) {
#else
    if (trace.movie_info_idx_groups_created_id2 == 0 ||
        trace.movie_info_idx_groups_created_id3 == 0) {
#endif
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    if (scratch.candidate_stamp.size() < movie_size) {
        scratch.candidate_stamp.resize(movie_size, 0);
    }
    uint32_t candidate_gen = ++scratch.candidate_gen;
    if (candidate_gen == 0) {
        scratch.candidate_gen = candidate_gen = 1;
        std::fill(scratch.candidate_stamp.begin(), scratch.candidate_stamp.end(), 0);
    }
    auto* candidate_stamp = scratch.candidate_stamp.data();
    int64_t candidate_count = 0;
    for (int32_t movie_id : idx1_movie_ids) {
        const size_t movie_idx = static_cast<size_t>(movie_id);
        if (idx_counts2[movie_idx] == 0) {
            continue;
        }
        if (info_counts[movie_idx] == 0) {
            continue;
        }
        candidate_stamp[movie_idx] = candidate_gen;
        ++candidate_count;
    }
    if (candidate_count == 0) {
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& movie_keyword = db.tables.at("movie_keyword");
    const auto& mk_movie_id = movie_keyword.columns.at("movie_id");
    const auto& mk_keyword_id = movie_keyword.columns.at("keyword_id");
    uint32_t keyword_groups = 0;
    const auto* mk_movie_id_data = mk_movie_id.i32.data();
    const auto* mk_movie_id_null = mk_movie_id.is_null.data();
    const auto* mk_keyword_id_data = mk_keyword_id.i32.data();
    const auto* mk_keyword_id_null = mk_keyword_id.is_null.data();
#ifdef TRACE
    if (!skip_query) {
        PROFILE_SCOPE(&trace.recorder, "q5a_movie_keyword_scan");
#endif
    const bool mk_has_nulls = mk_movie_id.has_nulls || mk_keyword_id.has_nulls;
    if (!mk_has_nulls) {
        for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_keyword_rows_scanned;
#endif
            const int32_t movie_id = mk_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (candidate_stamp[movie_idx] != candidate_gen) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_keyword_rows_emitted;
            ++trace.movie_keyword_agg_rows_in;
            if (keyword_counts[movie_idx]++ == 0) {
                ++trace.movie_keyword_groups_created;
            }
#else
            if (keyword_counts[movie_idx]++ == 0) {
                ++keyword_groups;
            }
#endif
        }
    } else {
        for (int64_t i = 0; i < movie_keyword.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.movie_keyword_rows_scanned;
#endif
            if (mk_movie_id_null[idx] != 0 || mk_keyword_id_null[idx] != 0) {
                continue;
            }
            const int32_t movie_id = mk_movie_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
            if (candidate_stamp[movie_idx] != candidate_gen) {
                continue;
            }
#ifdef TRACE
            ++trace.movie_keyword_rows_emitted;
            ++trace.movie_keyword_agg_rows_in;
            if (keyword_counts[movie_idx]++ == 0) {
                ++trace.movie_keyword_groups_created;
            }
#else
            if (keyword_counts[movie_idx]++ == 0) {
                ++keyword_groups;
            }
#endif
        }
    }
#ifdef TRACE
    }
#endif
#ifndef TRACE
    if (keyword_groups == 0) {
#else
    if (trace.movie_keyword_groups_created == 0) {
#endif
#ifdef TRACE
        skip_query = true;
#else
        return 0;
#endif
    }

    const auto& t_id = title.columns.at("id");
    const auto& t_kind_id = title.columns.at("kind_id");
    const auto& t_year = title.columns.at("production_year");
    const auto* t_id_data = t_id.i32.data();
    const auto* t_id_null = t_id.is_null.data();
    const auto* t_kind_id_data = t_kind_id.i32.data();
    const auto* t_kind_id_null = t_kind_id.is_null.data();
    const auto* t_year_data = t_year.i32.data();
    const auto* t_year_null = t_year.is_null.data();

#ifdef TRACE
    {
        PROFILE_SCOPE(&trace.recorder, "q5a_title_scan");
        if (!skip_query) {
#endif
    const bool title_has_nulls = t_id.has_nulls || t_kind_id.has_nulls || t_year.has_nulls;
    if (!title_has_nulls) {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            const int32_t year = t_year_data[idx];
            if (year > year1 || year < year2) {
                continue;
            }
            const int32_t kind_id = t_kind_id_data[idx];
            if (allowed_kind_ids[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t movie_id = t_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
#endif
            if (candidate_stamp[movie_idx] != candidate_gen) {
                continue;
            }
            const uint32_t info_count = info_counts[movie_idx];
            const uint32_t idx_count1 = idx_counts1[movie_idx];
            const uint32_t idx_count2 = idx_counts2[movie_idx];
            const uint32_t kw_count = keyword_counts[movie_idx];
            if (kw_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_join_rows_emitted;
#endif
            total_count += static_cast<int64_t>(
                static_cast<uint64_t>(info_count) * idx_count1 * idx_count2 * kw_count);
        }
    } else {
        for (int64_t i = 0; i < title.row_count; ++i) {
            const size_t idx = static_cast<size_t>(i);
#ifdef TRACE
            ++trace.title_rows_scanned;
#endif
            if (t_id_null[idx] != 0 || t_kind_id_null[idx] != 0 || t_year_null[idx] != 0) {
                continue;
            }
            const int32_t year = t_year_data[idx];
            if (year > year1 || year < year2) {
                continue;
            }
            const int32_t kind_id = t_kind_id_data[idx];
            if (allowed_kind_ids[static_cast<size_t>(kind_id)] == 0) {
                continue;
            }
            const int32_t movie_id = t_id_data[idx];
            const size_t movie_idx = static_cast<size_t>(movie_id);
#ifdef TRACE
            ++trace.title_rows_emitted;
            ++trace.title_join_probe_rows_in;
#endif
            if (candidate_stamp[movie_idx] != candidate_gen) {
                continue;
            }
            const uint32_t info_count = info_counts[movie_idx];
            const uint32_t idx_count1 = idx_counts1[movie_idx];
            const uint32_t idx_count2 = idx_counts2[movie_idx];
            const uint32_t kw_count = keyword_counts[movie_idx];
            if (kw_count == 0) {
                continue;
            }
#ifdef TRACE
            ++trace.title_join_rows_emitted;
#endif
            total_count += static_cast<int64_t>(
                static_cast<uint64_t>(info_count) * idx_count1 * idx_count2 * kw_count);
        }
    }
#ifdef TRACE
        }
    }
    trace.movie_info_agg_rows_emitted = trace.movie_info_groups_created;
    trace.movie_info_idx_agg_rows_emitted_id2 = trace.movie_info_idx_groups_created_id2;
    trace.movie_info_idx_agg_rows_emitted_id3 = trace.movie_info_idx_groups_created_id3;
    trace.movie_keyword_agg_rows_emitted = trace.movie_keyword_groups_created;
    trace.title_join_build_rows_in = static_cast<uint64_t>(
        trace.movie_info_groups_created + trace.movie_info_idx_groups_created_id2 +
        trace.movie_info_idx_groups_created_id3 + trace.movie_keyword_groups_created);
    trace.query_output_rows = 1;
#endif

#ifdef TRACE
    }
    trace.recorder.dump_timings();
    print_count("q5a_movie_info_scan_rows_scanned", trace.movie_info_rows_scanned);
    print_count("q5a_movie_info_scan_rows_emitted", trace.movie_info_rows_emitted);
    print_count("q5a_movie_info_agg_rows_in", trace.movie_info_agg_rows_in);
    print_count("q5a_movie_info_groups_created", trace.movie_info_groups_created);
    print_count("q5a_movie_info_agg_rows_emitted", trace.movie_info_agg_rows_emitted);
    print_count("q5a_movie_info_idx_scan_rows_scanned", trace.movie_info_idx_rows_scanned);
    print_count("q5a_movie_info_idx_scan_rows_emitted_id2", trace.movie_info_idx_rows_emitted_id2);
    print_count("q5a_movie_info_idx_scan_rows_emitted_id3", trace.movie_info_idx_rows_emitted_id3);
    print_count("q5a_movie_info_idx_agg_rows_in_id2", trace.movie_info_idx_agg_rows_in_id2);
    print_count("q5a_movie_info_idx_agg_rows_in_id3", trace.movie_info_idx_agg_rows_in_id3);
    print_count("q5a_movie_info_idx_groups_created_id2",
                trace.movie_info_idx_groups_created_id2);
    print_count("q5a_movie_info_idx_groups_created_id3",
                trace.movie_info_idx_groups_created_id3);
    print_count("q5a_movie_info_idx_agg_rows_emitted_id2",
                trace.movie_info_idx_agg_rows_emitted_id2);
    print_count("q5a_movie_info_idx_agg_rows_emitted_id3",
                trace.movie_info_idx_agg_rows_emitted_id3);
    print_count("q5a_movie_keyword_scan_rows_scanned", trace.movie_keyword_rows_scanned);
    print_count("q5a_movie_keyword_scan_rows_emitted", trace.movie_keyword_rows_emitted);
    print_count("q5a_movie_keyword_agg_rows_in", trace.movie_keyword_agg_rows_in);
    print_count("q5a_movie_keyword_groups_created", trace.movie_keyword_groups_created);
    print_count("q5a_movie_keyword_agg_rows_emitted", trace.movie_keyword_agg_rows_emitted);
    print_count("q5a_title_scan_rows_scanned", trace.title_rows_scanned);
    print_count("q5a_title_scan_rows_emitted", trace.title_rows_emitted);
    print_count("q5a_title_join_build_rows_in", trace.title_join_build_rows_in);
    print_count("q5a_title_join_probe_rows_in", trace.title_join_probe_rows_in);
    print_count("q5a_title_join_rows_emitted", trace.title_join_rows_emitted);
    print_count("q5a_query_output_rows", trace.query_output_rows);
#endif
    return total_count;
}