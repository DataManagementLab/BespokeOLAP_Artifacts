#include "db_loader.hpp"

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arrow/array.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_primitive.h>
#include <arrow/chunked_array.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>

// ===========================================================================
// Low-level Arrow extraction helpers
// ===========================================================================

// Extract a column of int32_t from a ChunkedArray.
// Dispatches on chunk type_id to handle INT8/INT16/INT32/INT64/UINT*/FLOAT/DOUBLE.
// Null values become null_sentinel (default -1).
static std::vector<int32_t> extract_int32(
        const arrow::ChunkedArray& col, int32_t null_sentinel = -1) {
    int64_t total = col.length();
    std::vector<int32_t> out;
    out.reserve((size_t)total);

    for (int ci = 0; ci < col.num_chunks(); ++ci) {
        const auto& chunk = *col.chunk(ci);
        int64_t n = chunk.length();
        auto type_id = chunk.type_id();

        switch (type_id) {
            case arrow::Type::INT8: {
                auto& arr = static_cast<const arrow::Int8Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::INT16: {
                auto& arr = static_cast<const arrow::Int16Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::INT32: {
                auto& arr = static_cast<const arrow::Int32Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : arr.Value(i));
                break;
            }
            case arrow::Type::INT64: {
                auto& arr = static_cast<const arrow::Int64Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::UINT8: {
                auto& arr = static_cast<const arrow::UInt8Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::UINT16: {
                auto& arr = static_cast<const arrow::UInt16Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::UINT32: {
                auto& arr = static_cast<const arrow::UInt32Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::UINT64: {
                auto& arr = static_cast<const arrow::UInt64Array&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::FLOAT: {
                auto& arr = static_cast<const arrow::FloatArray&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            case arrow::Type::DOUBLE: {
                auto& arr = static_cast<const arrow::DoubleArray&>(chunk);
                for (int64_t i = 0; i < n; ++i)
                    out.push_back(arr.IsNull(i) ? null_sentinel : (int32_t)arr.Value(i));
                break;
            }
            default:
                throw std::runtime_error(
                    std::string("extract_int32: unsupported type_id=") +
                    std::to_string((int)type_id));
        }
    }
    return out;
}

// Extract a column of strings from a ChunkedArray.
// Handles STRING and LARGE_STRING. Null values become "".
static std::vector<std::string> extract_str(const arrow::ChunkedArray& col) {
    int64_t total = col.length();
    std::vector<std::string> out;
    out.reserve((size_t)total);

    for (int ci = 0; ci < col.num_chunks(); ++ci) {
        const auto& chunk = *col.chunk(ci);
        int64_t n = chunk.length();
        auto type_id = chunk.type_id();

        if (type_id == arrow::Type::STRING) {
            auto& arr = static_cast<const arrow::StringArray&>(chunk);
            for (int64_t i = 0; i < n; ++i) {
                if (arr.IsNull(i)) {
                    out.emplace_back();
                } else {
                    auto sv = arr.GetView(i);
                    out.emplace_back(sv.data(), sv.size());
                }
            }
        } else if (type_id == arrow::Type::LARGE_STRING) {
            auto& arr = static_cast<const arrow::LargeStringArray&>(chunk);
            for (int64_t i = 0; i < n; ++i) {
                if (arr.IsNull(i)) {
                    out.emplace_back();
                } else {
                    auto sv = arr.GetView(i);
                    out.emplace_back(sv.data(), sv.size());
                }
            }
        } else if (type_id == arrow::Type::BINARY) {
            auto& arr = static_cast<const arrow::BinaryArray&>(chunk);
            for (int64_t i = 0; i < n; ++i) {
                if (arr.IsNull(i)) {
                    out.emplace_back();
                } else {
                    auto sv = arr.GetView(i);
                    out.emplace_back(sv.data(), sv.size());
                }
            }
        } else if (type_id == arrow::Type::LARGE_BINARY) {
            auto& arr = static_cast<const arrow::LargeBinaryArray&>(chunk);
            for (int64_t i = 0; i < n; ++i) {
                if (arr.IsNull(i)) {
                    out.emplace_back();
                } else {
                    auto sv = arr.GetView(i);
                    out.emplace_back(sv.data(), sv.size());
                }
            }
        } else {
            throw std::runtime_error(
                std::string("extract_str: unsupported type_id=") +
                std::to_string((int)type_id));
        }
    }
    return out;
}

// Helper: get a ChunkedArray column by name from an Arrow table.
static const arrow::ChunkedArray& get_col(const arrow::Table& tbl, const char* name) {
    auto col = tbl.GetColumnByName(name);
    if (!col) throw std::runtime_error(std::string("Column not found: ") + name);
    return *col;
}

// ===========================================================================
// CSR building helpers
// ===========================================================================

// Build a CSR from a key column that is already sorted ascending.
// Null keys (-1) are skipped.
static Csr build_csr_sorted(const std::vector<int32_t>& key_col) {
    if (key_col.empty()) return {};
    int32_t max_key = 0;
    for (int32_t k : key_col) if (k > max_key) max_key = k;

    Csr csr;
    int32_t N = (int32_t)key_col.size();
    csr.offsets.assign((size_t)max_key + 2, N); // default: end sentinel
    csr.values.resize((size_t)N);
    std::iota(csr.values.begin(), csr.values.end(), 0);

    // Fill offsets: offsets[k] = first position where key_col[pos] == k
    // Walk backwards so we correctly fill each key's start.
    for (int32_t pos = N - 1; pos >= 0; --pos) {
        int32_t k = key_col[pos];
        if (k >= 0) csr.offsets[(size_t)k] = pos;
    }
    // Fill forward for any gaps (keys with zero rows keep the next key's offset)
    for (int32_t k = (int32_t)csr.offsets.size() - 2; k >= 0; --k) {
        if (csr.offsets[(size_t)k] > csr.offsets[(size_t)k + 1])
            csr.offsets[(size_t)k] = csr.offsets[(size_t)k + 1];
    }
    return csr;
}

// Build a CSR from an unsorted key column.
// Null keys (-1) are skipped.
static Csr build_csr_unsorted(const std::vector<int32_t>& key_col) {
    if (key_col.empty()) return {};
    int32_t max_key = 0;
    for (int32_t k : key_col) if (k > max_key) max_key = k;

    int32_t N = (int32_t)key_col.size();
    // Count occurrences
    std::vector<int32_t> cnt((size_t)max_key + 2, 0);
    for (int32_t k : key_col) if (k >= 0) ++cnt[(size_t)k];

    Csr csr;
    csr.offsets.resize((size_t)max_key + 2);
    csr.offsets[0] = 0;
    for (size_t k = 0; k <= (size_t)max_key; ++k)
        csr.offsets[k + 1] = csr.offsets[k] + cnt[k];
    int32_t total_valid = csr.offsets[max_key + 1];
    csr.values.resize((size_t)total_valid);

    // Fill values using a cursor array
    std::vector<int32_t> cursor(csr.offsets.begin(), csr.offsets.begin() + max_key + 1);
    for (int32_t pos = 0; pos < N; ++pos) {
        int32_t k = key_col[pos];
        if (k >= 0) csr.values[(size_t)cursor[(size_t)k]++] = pos;
    }
    return csr;
}

// Build id_to_row direct array.
static std::vector<int32_t> build_id_to_row(const std::vector<int32_t>& id_col) {
    if (id_col.empty()) return {};
    int32_t max_id = *std::max_element(id_col.begin(), id_col.end());
    std::vector<int32_t> itr((size_t)max_id + 1, -1);
    for (int32_t row = 0; row < (int32_t)id_col.size(); ++row) {
        int32_t id = id_col[row];
        if (id >= 0) itr[(size_t)id] = row;
    }
    return itr;
}

// ===========================================================================
// Radix sort helpers
// ===========================================================================

// 1-key radix argsort: 2 passes of 16 bits, signed-correct via sign-bit flip.
static std::vector<int32_t> radix_argsort(const std::vector<int32_t>& keys) {
    int32_t N = (int32_t)keys.size();
    if (N == 0) return {};
    // Flip sign bit so unsigned order == signed order
    auto uk = [&](int32_t i) -> uint32_t { return (uint32_t)keys[i] ^ 0x80000000u; };

    std::vector<int32_t> idx(N), buf(N);
    std::iota(idx.begin(), idx.end(), 0);

    // Pass 1: lower 16 bits
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[uk(i) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) buf[cnt[uk(i) & 0xFFFFu]++] = i;
    }
    // Pass 2: upper 16 bits (stable over pass 1)
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[(uk(buf[i]) >> 16) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) idx[cnt[(uk(buf[i]) >> 16) & 0xFFFFu]++] = buf[i];
    }
    return idx;
}

// 2-key radix argsort: primary=k1, secondary=k2.
// 4 passes of 16 bits (LSB-first: k2_lo16, k2_hi16, k1_lo16, k1_hi16).
static std::vector<int32_t> radix_argsort2(const std::vector<int32_t>& k1,
                                            const std::vector<int32_t>& k2) {
    int32_t N = (int32_t)k1.size();
    if (N == 0) return {};
    auto uk1 = [&](int32_t i) -> uint32_t { return (uint32_t)k1[i] ^ 0x80000000u; };
    auto uk2 = [&](int32_t i) -> uint32_t { return (uint32_t)k2[i] ^ 0x80000000u; };

    std::vector<int32_t> idx(N), buf(N);
    std::iota(idx.begin(), idx.end(), 0);

    // Pass 1: k2 lower 16
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[uk2(idx[i]) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) buf[cnt[uk2(idx[i]) & 0xFFFFu]++] = idx[i];
    }
    // Pass 2: k2 upper 16
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[(uk2(buf[i]) >> 16) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) idx[cnt[(uk2(buf[i]) >> 16) & 0xFFFFu]++] = buf[i];
    }
    // Pass 3: k1 lower 16
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[uk1(idx[i]) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) buf[cnt[uk1(idx[i]) & 0xFFFFu]++] = idx[i];
    }
    // Pass 4: k1 upper 16
    {
        uint32_t cnt[65536] = {};
        for (int32_t i = 0; i < N; ++i) ++cnt[(uk1(buf[i]) >> 16) & 0xFFFFu];
        uint32_t p = 0;
        for (int k = 0; k < 65536; ++k) { uint32_t c = cnt[k]; cnt[k] = p; p += c; }
        for (int32_t i = 0; i < N; ++i) idx[cnt[(uk1(buf[i]) >> 16) & 0xFFFFu]++] = buf[i];
    }
    return idx;
}

static std::vector<int32_t> argsort(const std::vector<int32_t>& keys) {
    return radix_argsort(keys);
}
static std::vector<int32_t> argsort2(const std::vector<int32_t>& k1,
                                      const std::vector<int32_t>& k2) {
    return radix_argsort2(k1, k2);
}

// ===========================================================================
// Intra-table parallel task runner
// Launches each task in its own thread. Collects and rethrows first exception.
// Used for parallel column extraction within a single table's build task.
// ===========================================================================
static void run_parallel_tasks(std::vector<std::function<void()>> tasks) {
    if (tasks.size() <= 1) {
        for (auto& t : tasks) t();
        return;
    }
    std::vector<std::thread> threads;
    threads.reserve(tasks.size());
    std::exception_ptr first_exc;
    std::mutex exc_mu;
    for (auto& task : tasks) {
        threads.emplace_back([&task, &first_exc, &exc_mu]() {
            try { task(); }
            catch (...) {
                std::lock_guard<std::mutex> lk(exc_mu);
                if (!first_exc) first_exc = std::current_exception();
            }
        });
    }
    for (auto& th : threads) th.join();
    if (first_exc) std::rethrow_exception(first_exc);
}

// ===========================================================================
// Combined multi-column permute helpers
// Perform a single random-access pass over the order array, writing to all
// output columns simultaneously. Saves N-1 extra cache-miss passes vs
// calling permute_into N times separately.
// ===========================================================================

// In-place multi-permute for int32 columns.
static void multi_permute_int32(const std::vector<int32_t>& order,
                                std::vector<std::vector<int32_t>*> cols) {
    size_t N = order.size();
    size_t C = cols.size();
    std::vector<std::vector<int32_t>> out(C);
    for (size_t c = 0; c < C; ++c) out[c].resize(N);
    for (size_t i = 0; i < N; ++i) {
        size_t src_idx = (size_t)order[i];
        for (size_t c = 0; c < C; ++c)
            out[c][i] = (*cols[c])[src_idx];
    }
    for (size_t c = 0; c < C; ++c) *cols[c] = std::move(out[c]);
}

// Parallel version: each column permuted in its own thread.
static void multi_permute_int32_parallel(const std::vector<int32_t>& order,
                                          std::vector<std::vector<int32_t>*> cols) {
    std::vector<std::function<void()>> tasks;
    tasks.reserve(cols.size());
    for (auto* col : cols) {
        tasks.push_back([&order, col]() {
            size_t N = order.size();
            std::vector<int32_t> out(N);
            for (size_t i = 0; i < N; ++i) out[i] = (*col)[(size_t)order[i]];
            *col = std::move(out);
        });
    }
    run_parallel_tasks(std::move(tasks));
}

// In-place multi-permute (move) for string columns.
static void multi_permute_str(const std::vector<int32_t>& order,
                              std::vector<std::vector<std::string>*> cols) {
    size_t N = order.size();
    size_t C = cols.size();
    std::vector<std::vector<std::string>> out(C);
    for (size_t c = 0; c < C; ++c) out[c].resize(N);
    for (size_t i = 0; i < N; ++i) {
        size_t src_idx = (size_t)order[i];
        for (size_t c = 0; c < C; ++c)
            out[c][i] = std::move((*cols[c])[src_idx]);
    }
    for (size_t c = 0; c < C; ++c) *cols[c] = std::move(out[c]);
}

// Permute vec in-place (classic move-based).
template<typename T>
static void permute(std::vector<T>& vec, const std::vector<int32_t>& order) {
    std::vector<T> tmp;
    tmp.reserve(vec.size());
    for (int32_t i : order) tmp.push_back(std::move(vec[(size_t)i]));
    vec = std::move(tmp);
}

// Gather: dst[i] = src[order[i]].  dst is resized to order.size().
template<typename T>
static void permute_into(std::vector<T>& dst, const std::vector<T>& src,
                         const std::vector<int32_t>& order) {
    dst.resize(order.size());
    for (size_t i = 0; i < order.size(); ++i) dst[i] = src[(size_t)order[i]];
}

// Move-gather: dst[i] = move(src[order[i]]).
template<typename T>
static void permute_move_into(std::vector<T>& dst, std::vector<T>& src,
                              const std::vector<int32_t>& order) {
    dst.resize(order.size());
    for (size_t i = 0; i < order.size(); ++i) dst[i] = std::move(src[(size_t)order[i]]);
}

// ===========================================================================
// Parallel task runner — launches each task in its own thread.
// Collects the first exception and rethrows after all threads join.
// ===========================================================================
static void run_parallel(std::vector<std::function<void()>> tasks) {
    std::vector<std::thread> threads;
    threads.reserve(tasks.size());
    std::exception_ptr first_exc;
    std::mutex exc_mu;
    for (auto& task : tasks) {
        threads.emplace_back([&task, &first_exc, &exc_mu]() {
            try { task(); }
            catch (...) {
                std::lock_guard<std::mutex> lk(exc_mu);
                if (!first_exc) first_exc = std::current_exception();
            }
        });
    }
    for (auto& th : threads) th.join();
    if (first_exc) std::rethrow_exception(first_exc);
}

// ===========================================================================
// Float parsing for movie_info_idx shadow column
// ===========================================================================
static float parse_float_or_nan(const std::string& s) {
    if (s.empty()) return std::numeric_limits<float>::quiet_NaN();
    char* end = nullptr;
    float v = (float)std::strtod(s.c_str(), &end);
    if (end == s.c_str() || *end != '\0') return std::numeric_limits<float>::quiet_NaN();
    return v;
}

// ===========================================================================
// Partition boundary builder (for info_type_id sorted tables)
// ===========================================================================
static void build_type_partitions(const std::vector<int32_t>& type_col,
                                   std::vector<int32_t>& part_start,
                                   std::vector<int32_t>& part_end) {
    int32_t N = (int32_t)type_col.size();
    if (N == 0) return;
    int32_t max_type = 0;
    for (int32_t t : type_col) if (t > max_type) max_type = t;

    part_start.assign((size_t)max_type + 2, N);
    part_end.assign((size_t)max_type + 2, 0);

    // Walk forwards: first occurrence of each type_id
    for (int32_t pos = N - 1; pos >= 0; --pos) {
        int32_t t = type_col[(size_t)pos];
        if (t >= 0) part_start[(size_t)t] = pos;
    }
    // Walk backwards: one-past-last occurrence of each type_id
    for (int32_t pos = 0; pos < N; ++pos) {
        int32_t t = type_col[(size_t)pos];
        if (t >= 0) part_end[(size_t)t] = pos + 1;
    }
    // Fix empty partitions
    for (int32_t t = 0; t <= max_type; ++t) {
        if (part_start[(size_t)t] > part_end[(size_t)t]) {
            part_start[(size_t)t] = 0;
            part_end[(size_t)t] = 0;
        }
    }
}

// ===========================================================================
// build()
// ===========================================================================

Database* build(ParquetTables* pt) {
    auto* db = new Database{};

    // -----------------------------------------------------------------------
    // Micro tables (tiny — fast sequential load)
    // -----------------------------------------------------------------------
    {
        auto& t = *pt->kind_type;
        db->kind_type.id   = extract_int32(get_col(t, "id"));
        db->kind_type.kind = extract_str(get_col(t, "kind"));
    }
    {
        auto& t = *pt->role_type;
        db->role_type.id   = extract_int32(get_col(t, "id"));
        db->role_type.role = extract_str(get_col(t, "role"));
    }
    {
        auto& t = *pt->info_type;
        db->info_type.id   = extract_int32(get_col(t, "id"));
        db->info_type.info = extract_str(get_col(t, "info"));
    }
    {
        auto& t = *pt->company_type;
        db->company_type.id   = extract_int32(get_col(t, "id"));
        db->company_type.kind = extract_str(get_col(t, "kind"));
    }
    {
        auto& t = *pt->comp_cast_type;
        db->comp_cast_type.id   = extract_int32(get_col(t, "id"));
        db->comp_cast_type.kind = extract_str(get_col(t, "kind"));
    }
    {
        auto& t = *pt->link_type;
        db->link_type.id   = extract_int32(get_col(t, "id"));
        db->link_type.link = extract_str(get_col(t, "link"));
    }

    // -----------------------------------------------------------------------
    // All large tables built in parallel (no cross-table dependencies).
    // -----------------------------------------------------------------------
    run_parallel({

    // title
    [&]() {
        auto& t = *pt->title;
        std::vector<int32_t> id, kind_id, production_year;
        std::vector<std::string> title_str;
        run_parallel_tasks({
            [&]() { id              = extract_int32(get_col(t, "id")); },
            [&]() { title_str       = extract_str(get_col(t, "title")); },
            [&]() { kind_id         = extract_int32(get_col(t, "kind_id")); },
            [&]() { production_year = extract_int32(get_col(t, "production_year")); },
        });

        // Sort all parallel arrays by (kind_id, production_year).
        // Sorting by kind_id enables kind-partition scans (skip irrelevant kinds).
        // Secondary sort by production_year enables binary-search year-range lookup
        // within each kind partition, avoiding linear scans over all years.
        // NULL kind (-1) sorts first; NULL year (-1) sorts before valid years.
        // This is a general-purpose storage layout: any SQL with kind/year predicates benefits.
        int32_t N = (int32_t)id.size();
        std::vector<int32_t> order(N);
        std::iota(order.begin(), order.end(), 0);
        std::stable_sort(order.begin(), order.end(),
            [&](int32_t a, int32_t b) {
                if (kind_id[a] != kind_id[b]) return kind_id[a] < kind_id[b];
                return production_year[a] < production_year[b];
            });

        // Apply permutation to all columns
        std::vector<int32_t>     id_s(N), kind_id_s(N), py_s(N);
        std::vector<std::string> title_str_s(N);
        for (int32_t i = 0; i < N; ++i) {
            int32_t src = order[i];
            id_s[i]         = id[src];
            kind_id_s[i]    = kind_id[src];
            py_s[i]         = production_year[src];
            title_str_s[i]  = std::move(title_str[src]);
        }

        // Build kind partition index
        // Find max kind_id (nulls are -1, valid IDs are 1..~14)
        int32_t max_kid = 0;
        for (int32_t k : kind_id_s) if (k > max_kid) max_kid = k;
        // Allocate with sentinel: size = max_kid + 2
        // Null kind (-1) is not included in the partition index (queries handle
        // null separately via kind_null_ok flag).
        std::vector<int32_t> kp_start(max_kid + 2, N); // default: empty range (end sentinel)
        std::vector<int32_t> kp_end(max_kid + 2, N);
        // Walk sorted array and fill partition boundaries
        {
            int32_t prev_kid = INT_MIN;
            for (int32_t r = 0; r < N; ++r) {
                int32_t k = kind_id_s[r];
                if (k != prev_kid) {
                    if (k >= 0 && k <= max_kid) {
                        kp_start[k] = r;
                    }
                    // Close previous non-null partition
                    if (prev_kid >= 0 && prev_kid <= max_kid) {
                        kp_end[prev_kid] = r;
                    }
                    prev_kid = k;
                }
            }
            // Close the last non-null partition
            if (prev_kid >= 0 && prev_kid <= max_kid) {
                kp_end[prev_kid] = N;
            }
        }

        db->title.id              = std::move(id_s);
        db->title.title_str       = std::move(title_str_s);
        db->title.kind_id         = std::move(kind_id_s);
        db->title.production_year = std::move(py_s);
        db->title.id_to_row       = build_id_to_row(db->title.id);
        db->title.kind_part_start = std::move(kp_start);
        db->title.kind_part_end   = std::move(kp_end);
    },

    // name
    [&]() {
        auto& t = *pt->name;
        std::vector<int32_t> id;
        std::vector<std::string> name_str, gender, name_pcode_cf, name_pcode_nf;
        run_parallel_tasks({
            [&]() { id            = extract_int32(get_col(t, "id")); },
            [&]() { name_str      = extract_str(get_col(t, "name")); },
            [&]() { gender        = extract_str(get_col(t, "gender")); },
            [&]() { name_pcode_cf = extract_str(get_col(t, "name_pcode_cf")); },
            [&]() { name_pcode_nf = extract_str(get_col(t, "name_pcode_nf")); },
        });
        // Sort by id so that id_to_row becomes an identity-like mapping.
        // This makes any subsequent pid-sorted access (e.g. after radix-sorting
        // cast_info by person_id) sequential in name_str, improving cache
        // efficiency for ILIKE evaluations in queries 9b, 10a, 11a, 11b.
        {
            auto order = argsort(id);
            // Permute all columns in parallel using worker threads
            run_parallel_tasks({
                [&]() { multi_permute_int32(order, {&id}); },
                [&]() { multi_permute_str(order, {&name_str}); },
                [&]() { multi_permute_str(order, {&gender}); },
                [&]() { multi_permute_str(order, {&name_pcode_cf}); },
                [&]() { multi_permute_str(order, {&name_pcode_nf}); },
            });
        }
        auto id_to_row = build_id_to_row(id);
        db->name.id            = std::move(id);
        db->name.name_str      = std::move(name_str);
        db->name.gender        = std::move(gender);
        db->name.name_pcode_cf = std::move(name_pcode_cf);
        db->name.name_pcode_nf = std::move(name_pcode_nf);
        db->name.id_to_row     = std::move(id_to_row);
        // Build compact gender_byte (first char of gender string, 0 for empty)
        {
            const auto& g = db->name.gender;
            db->name.gender_byte.resize(g.size());
            for (size_t i = 0; i < g.size(); ++i)
                db->name.gender_byte[i] = g[i].empty() ? 0 : (uint8_t)g[i][0];
        }
    },

    // keyword
    [&]() {
        auto& t = *pt->keyword;
        std::vector<int32_t> id;
        std::vector<std::string> keyword_str;
        run_parallel_tasks({
            [&]() { id          = extract_int32(get_col(t, "id")); },
            [&]() { keyword_str = extract_str(get_col(t, "keyword")); },
        });
        auto id_to_row = build_id_to_row(id);
        db->keyword.id          = std::move(id);
        db->keyword.keyword_str = std::move(keyword_str);
        db->keyword.id_to_row   = std::move(id_to_row);
        // Build reverse lookup: keyword_str -> all ids (handles duplicates at higher SF)
        {
            auto& kw = db->keyword;
            kw.str_to_ids.reserve(kw.id.size());
            for (size_t i = 0; i < kw.id.size(); ++i)
                kw.str_to_ids[kw.keyword_str[i]].push_back(kw.id[i]);
        }
    },

    // company_name
    [&]() {
        auto& t = *pt->company_name;
        std::vector<int32_t> id;
        std::vector<std::string> name_str, country_code;
        run_parallel_tasks({
            [&]() { id           = extract_int32(get_col(t, "id")); },
            [&]() { name_str     = extract_str(get_col(t, "name")); },
            [&]() { country_code = extract_str(get_col(t, "country_code")); },
        });
        auto id_to_row = build_id_to_row(id);
        // Build compact country_code_u64: copy up to 8 bytes of country code string
        // into a uint64_t for fast scalar comparison in query loops.
        // 0 = null/empty. Avoids std::string hash/comparison overhead per row.
        std::vector<uint64_t> country_code_u64(country_code.size(), 0ULL);
        for (size_t i = 0; i < country_code.size(); ++i) {
            const std::string& cc = country_code[i];
            if (!cc.empty()) {
                uint64_t v = 0;
                size_t n = (cc.size() < 8) ? cc.size() : 8;
                std::memcpy(&v, cc.data(), n);
                country_code_u64[i] = v;
            }
        }
        db->company_name.id               = std::move(id);
        db->company_name.name_str         = std::move(name_str);
        db->company_name.country_code     = std::move(country_code);
        db->company_name.country_code_u64 = std::move(country_code_u64);
        db->company_name.id_to_row        = std::move(id_to_row);
        // Build reverse name->ids map for O(1) company name lookup.
        // General-purpose: avoids O(N) linear scan in any query filtering by cn.name.
        {
            auto& cn = db->company_name;
            for (size_t i = 0; i < cn.name_str.size(); ++i) {
                cn.name_to_ids[cn.name_str[i]].push_back(cn.id[i]);
            }
        }
        // Build pre-lowercased company names for fast ILIKE scanning.
        // Eliminates per-character tolower() calls in hot ILIKE match loops.
        {
            auto& ns = db->company_name.name_str;
            auto& nsl = db->company_name.name_str_lower;
            nsl.resize(ns.size());
            for (size_t i = 0; i < ns.size(); ++i) {
                nsl[i] = ns[i];
                for (char& c : nsl[i]) c = (char)std::tolower((unsigned char)c);
            }
        }
    },

    // cast_info — sort by movie_id
    [&]() {
        auto& t = *pt->cast_info;
        // Phase 1: Extract movie_id (needed for sort) and other columns in parallel.
        // Overlap: start argsort of movie_id as soon as it is ready,
        // while remaining columns still extract.
        std::vector<int32_t> person_id, movie_id, person_role_id, role_id;
        std::vector<std::string> note;
        std::vector<int32_t> order; // will be filled by movie_id thread
        run_parallel_tasks({
            // movie_id thread: extract then immediately sort
            [&]() {
                movie_id = extract_int32(get_col(t, "movie_id"));
                order    = argsort(movie_id); // sort while others still extract
            },
            [&]() { person_id      = extract_int32(get_col(t, "person_id")); },
            [&]() { person_role_id = extract_int32(get_col(t, "person_role_id")); },
            [&]() { role_id        = extract_int32(get_col(t, "role_id")); },
            [&]() { note           = extract_str(get_col(t, "note")); },
        });
        // Phase 2: Permute each column in its own thread (fully parallel).
        // Each thread does its own random-read gather independently.
        Csr person_id_csr, movie_id_csr;
        run_parallel_tasks({
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = person_id[(size_t)order[i]];
                person_id = std::move(out);
                // Build person_id CSR right after permute while data is hot
                person_id_csr = build_csr_unsorted(person_id);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = movie_id[(size_t)order[i]];
                movie_id = std::move(out);
                // Build movie_id CSR right after permute while data is hot
                movie_id_csr = build_csr_sorted(movie_id);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = person_role_id[(size_t)order[i]];
                person_role_id = std::move(out);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = role_id[(size_t)order[i]];
                role_id = std::move(out);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(note[(size_t)order[i]]);
                note = std::move(out);
            },
        });

        db->cast_info.person_id      = std::move(person_id);
        db->cast_info.movie_id       = std::move(movie_id);
        db->cast_info.person_role_id = std::move(person_role_id);
        db->cast_info.note           = std::move(note);
        db->cast_info.role_id        = std::move(role_id);
        db->cast_info.movie_id_csr   = std::move(movie_id_csr);
        db->cast_info.person_id_csr  = std::move(person_id_csr);
    },

    // movie_keyword — sort by movie_id; also build keyword_id CSR
    [&]() {
        auto& t = *pt->movie_keyword;
        std::vector<int32_t> movie_id, keyword_id;
        run_parallel_tasks({
            [&]() { movie_id   = extract_int32(get_col(t, "movie_id")); },
            [&]() { keyword_id = extract_int32(get_col(t, "keyword_id")); },
        });

        auto order = argsort(movie_id);
        multi_permute_int32(order, {&movie_id, &keyword_id});

        Csr keyword_id_csr, movie_id_csr;
        run_parallel_tasks({
            [&]() { keyword_id_csr = build_csr_unsorted(keyword_id); },
            [&]() { movie_id_csr   = build_csr_sorted(movie_id); },
        });

        db->movie_keyword.movie_id       = std::move(movie_id);
        db->movie_keyword.keyword_id     = std::move(keyword_id);
        db->movie_keyword.movie_id_csr   = std::move(movie_id_csr);
        db->movie_keyword.keyword_id_csr = std::move(keyword_id_csr);
    },

    // movie_companies — sort by movie_id
    [&]() {
        auto& t = *pt->movie_companies;
        std::vector<int32_t> movie_id, company_id, company_type_id;
        std::vector<std::string> note;
        run_parallel_tasks({
            [&]() { movie_id        = extract_int32(get_col(t, "movie_id")); },
            [&]() { company_id      = extract_int32(get_col(t, "company_id")); },
            [&]() { company_type_id = extract_int32(get_col(t, "company_type_id")); },
            [&]() { note            = extract_str(get_col(t, "note")); },
        });

        auto order = argsort(movie_id);
        multi_permute_int32(order, {&movie_id, &company_id, &company_type_id});
        multi_permute_str(order, {&note});

        Csr movie_id_csr, company_id_csr;
        run_parallel_tasks({
            [&]() { movie_id_csr   = build_csr_sorted(movie_id); },
            [&]() { company_id_csr = build_csr_unsorted(company_id); },
        });

        db->movie_companies.movie_id        = std::move(movie_id);
        db->movie_companies.company_id      = std::move(company_id);
        db->movie_companies.company_type_id = std::move(company_type_id);
        db->movie_companies.note            = std::move(note);
        db->movie_companies.movie_id_csr    = std::move(movie_id_csr);
        db->movie_companies.company_id_csr  = std::move(company_id_csr);
    },

    // aka_name — sort by person_id
    [&]() {
        auto& t = *pt->aka_name;
        std::vector<int32_t> person_id;
        std::vector<std::string> name_str;
        run_parallel_tasks({
            [&]() { person_id = extract_int32(get_col(t, "person_id")); },
            [&]() { name_str  = extract_str(get_col(t, "name")); },
        });

        auto order = argsort(person_id);
        multi_permute_int32(order, {&person_id});
        multi_permute_str(order, {&name_str});

        auto person_id_csr = build_csr_sorted(person_id);

        db->aka_name.person_id     = std::move(person_id);
        db->aka_name.name_str      = std::move(name_str);
        db->aka_name.person_id_csr = std::move(person_id_csr);
    },

    // movie_info — sort by (info_type_id, movie_id)
    [&]() {
        auto& t = *pt->movie_info;
        std::vector<int32_t> movie_id, info_type_id;
        std::vector<std::string> info_str, note;
        std::vector<int32_t> order;
        run_parallel_tasks({
            // Sort key columns extracted then sorted inline; string cols extract in parallel
            [&]() {
                movie_id     = extract_int32(get_col(t, "movie_id"));
                info_type_id = extract_int32(get_col(t, "info_type_id"));
                order = argsort2(info_type_id, movie_id);
            },
            [&]() { info_str     = extract_str(get_col(t, "info")); },
            [&]() { note         = extract_str(get_col(t, "note")); },
        });
        // Permute all columns in parallel; build CSR and partitions inline
        Csr movie_id_csr;
        run_parallel_tasks({
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = movie_id[(size_t)order[i]];
                movie_id = std::move(out);
                movie_id_csr = build_csr_unsorted(movie_id);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = info_type_id[(size_t)order[i]];
                info_type_id = std::move(out);
                build_type_partitions(info_type_id,
                                      db->movie_info.type_part_start,
                                      db->movie_info.type_part_end);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(info_str[(size_t)order[i]]);
                info_str = std::move(out);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(note[(size_t)order[i]]);
                note = std::move(out);
            },
        });

        db->movie_info.movie_id      = std::move(movie_id);
        db->movie_info.info_type_id  = std::move(info_type_id);
        db->movie_info.info_str      = std::move(info_str);
        db->movie_info.note          = std::move(note);
        db->movie_info.movie_id_csr  = std::move(movie_id_csr);
        // info_float left empty for movie_info

        // Build info_id interning: assign each unique info_str an integer ID.
        // This allows query loops to compare integers instead of strings.
        {
            const auto& is = db->movie_info.info_str;
            const size_t n = is.size();
            std::unordered_map<std::string, int32_t>& intern_map = db->movie_info.info_dict_map;
            intern_map.reserve(n / 4); // rough estimate: many duplicates
            std::vector<std::string> dict;
            std::vector<int32_t> ids(n);
            for (size_t i = 0; i < n; ++i) {
                auto [it, inserted] = intern_map.emplace(is[i], (int32_t)dict.size());
                if (inserted) dict.push_back(is[i]);
                ids[i] = it->second;
            }
            dict.shrink_to_fit();
            ids.shrink_to_fit();
            db->movie_info.info_dict_vec = std::move(dict);
            db->movie_info.info_id       = std::move(ids);
        }

                // Build per-type unique info_str sets for fast existence checks
        {
            const auto& ts = db->movie_info.type_part_start;
            const auto& te = db->movie_info.type_part_end;
            const auto& is = db->movie_info.info_str;
            int32_t max_type = (int32_t)ts.size() > 0 ? (int32_t)ts.size() - 1 : 0;
            auto& uinfo = db->movie_info.type_unique_info;
            uinfo.resize((size_t)max_type);
            for (int32_t t = 0; t < max_type; ++t) {
                int32_t beg = ts[(size_t)t], end = te[(size_t)t];
                for (int32_t r = beg; r < end; ++r)
                    uinfo[(size_t)t].insert(is[(size_t)r]);
            }
            // Build cache-friendly flat vector version for iteration
            auto& uvec = db->movie_info.type_unique_info_vec;
            uvec.resize((size_t)max_type);
            for (int32_t t = 0; t < max_type; ++t) {
                uvec[(size_t)t].reserve(uinfo[(size_t)t].size());
                for (const auto& s : uinfo[(size_t)t])
                    uvec[(size_t)t].push_back(s);
            }
        }

        // Build per-type inverted index: (type, intern_id) -> list of row indices.
        // Allows query loops to jump directly to rows matching a given info value
        // without scanning the entire type partition. General-purpose: any query
        // filtering by (info_type_id, info) can use this index.
        {
            const auto& ts  = db->movie_info.type_part_start;
            const auto& te  = db->movie_info.type_part_end;
            const auto& iid = db->movie_info.info_id;
            int32_t max_type = (int32_t)ts.size() > 0 ? (int32_t)ts.size() - 1 : 0;

            auto& iid_keys    = db->movie_info.type_iid_keys;
            auto& iid_offsets = db->movie_info.type_iid_offsets;
            auto& iid_rows    = db->movie_info.type_iid_rows;
            iid_keys.resize((size_t)max_type);
            iid_offsets.resize((size_t)max_type);
            iid_rows.resize((size_t)max_type);

            for (int32_t tp = 0; tp < max_type; ++tp) {
                int32_t beg = ts[(size_t)tp], end = te[(size_t)tp];
                int32_t n = end - beg;
                if (n == 0) continue;

                // Count occurrences of each intern ID in this partition
                // using a local hash map (intern IDs can be up to 2.7M, too sparse for flat array)
                std::unordered_map<int32_t, int32_t> cnt;
                cnt.reserve((size_t)n / 4 + 1);
                for (int32_t r = beg; r < end; ++r) {
                    ++cnt[iid[(size_t)r]];
                }

                // Sort intern IDs for binary search
                std::vector<int32_t>& keys = iid_keys[(size_t)tp];
                keys.reserve(cnt.size());
                for (const auto& [k, _] : cnt) keys.push_back(k);
                std::sort(keys.begin(), keys.end());

                // Build offsets
                std::vector<int32_t>& offsets = iid_offsets[(size_t)tp];
                offsets.resize(keys.size() + 1);
                offsets[0] = 0;
                for (size_t i = 0; i < keys.size(); ++i)
                    offsets[i + 1] = offsets[i] + cnt[keys[i]];
                int32_t total = offsets[keys.size()];

                // Fill movie_id list (store movie_id directly, not row index).
                // This avoids an extra mid_ptr[row] indirection in query loops.
                // Values are sorted (movie_id order) since we iterate r in [beg,end)
                // which is sorted by movie_id.
                const auto& mid_data = db->movie_info.movie_id;
                std::vector<int32_t>& rows = iid_rows[(size_t)tp];
                rows.resize((size_t)total);
                // Cursor for filling
                std::vector<int32_t> cursor(offsets.begin(), offsets.begin() + (int32_t)keys.size());
                // Build local lookup: intern_id -> local index in keys
                std::unordered_map<int32_t, int32_t> key_to_local;
                key_to_local.reserve(keys.size());
                for (int32_t i = 0; i < (int32_t)keys.size(); ++i)
                    key_to_local[keys[(size_t)i]] = i;

                for (int32_t r = beg; r < end; ++r) {
                    int32_t local = key_to_local[iid[(size_t)r]];
                    // Store movie_id directly — no indirection needed in query
                    rows[(size_t)cursor[(size_t)local]++] = mid_data[(size_t)r];
                }
                // Each rows subarray is sorted (movie_id values in ascending order)
                // because the partition is sorted by movie_id and we iterate forward.
            }
        }
    },

    // movie_info_idx — sort by (info_type_id, movie_id); build float shadow
    [&]() {
        auto& t = *pt->movie_info_idx;
        std::vector<int32_t> movie_id, info_type_id;
        std::vector<std::string> info_str, note;
        std::vector<int32_t> order;
        run_parallel_tasks({
            [&]() {
                movie_id     = extract_int32(get_col(t, "movie_id"));
                info_type_id = extract_int32(get_col(t, "info_type_id"));
                order = argsort2(info_type_id, movie_id);
            },
            [&]() { info_str     = extract_str(get_col(t, "info")); },
            [&]() { note         = extract_str(get_col(t, "note")); },
        });
        // Permute all columns in parallel; build derived structures inline
        std::vector<float> info_float;
        Csr movie_id_csr;
        run_parallel_tasks({
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = movie_id[(size_t)order[i]];
                movie_id = std::move(out);
                movie_id_csr = build_csr_unsorted(movie_id);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = info_type_id[(size_t)order[i]];
                info_type_id = std::move(out);
                build_type_partitions(info_type_id,
                                      db->movie_info_idx.type_part_start,
                                      db->movie_info_idx.type_part_end);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(info_str[(size_t)order[i]]);
                info_str = std::move(out);
                // Build float shadow right after info_str permute while data is warm
                info_float.resize(info_str.size());
                for (size_t i = 0; i < info_str.size(); ++i)
                    info_float[i] = parse_float_or_nan(info_str[i]);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(note[(size_t)order[i]]);
                note = std::move(out);
            },
        });

        db->movie_info_idx.movie_id      = std::move(movie_id);
        db->movie_info_idx.info_type_id  = std::move(info_type_id);
        db->movie_info_idx.info_str      = std::move(info_str);
        db->movie_info_idx.note          = std::move(note);
        db->movie_info_idx.info_float    = std::move(info_float);
        db->movie_info_idx.movie_id_csr  = std::move(movie_id_csr);
    },

    // person_info — sort by (info_type_id, person_id)
    [&]() {
        auto& t = *pt->person_info;
        std::vector<int32_t> person_id, info_type_id;
        std::vector<std::string> info_str, note;
        std::vector<int32_t> order;
        run_parallel_tasks({
            [&]() {
                person_id    = extract_int32(get_col(t, "person_id"));
                info_type_id = extract_int32(get_col(t, "info_type_id"));
                order = argsort2(info_type_id, person_id);
            },
            [&]() { info_str     = extract_str(get_col(t, "info")); },
            [&]() { note         = extract_str(get_col(t, "note")); },
        });


        // Permute all columns in parallel; build derived structures inline
        Csr person_id_csr;
        run_parallel_tasks({
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = person_id[(size_t)order[i]];
                person_id = std::move(out);
                person_id_csr = build_csr_unsorted(person_id);
            },
            [&]() {
                std::vector<int32_t> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = info_type_id[(size_t)order[i]];
                info_type_id = std::move(out);
                build_type_partitions(info_type_id,
                                      db->person_info.type_part_start,
                                      db->person_info.type_part_end);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(info_str[(size_t)order[i]]);
                info_str = std::move(out);
            },
            [&]() {
                std::vector<std::string> out(order.size());
                for (size_t i = 0; i < order.size(); ++i) out[i] = std::move(note[(size_t)order[i]]);
                note = std::move(out);
            },
        });

        db->person_info.person_id     = std::move(person_id);
        db->person_info.info_type_id  = std::move(info_type_id);
        db->person_info.info_str      = std::move(info_str);
        db->person_info.note          = std::move(note);
        db->person_info.person_id_csr = std::move(person_id_csr);
    }

    }); // end run_parallel

    return db;
}

void destroy_database(Database* db) {
    delete db;
}
