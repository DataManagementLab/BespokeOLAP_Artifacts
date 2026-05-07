#include "db_loader.hpp"
#include <cstdio>
#include "trace.hpp"

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cinttypes>
#include <numeric>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>
#include <thread>
#include <functional>
#include <exception>
#include <thread>
#include <mutex>
#include <atomic>
#include <functional>
#include <exception>

#include <arrow/array.h>
#include <arrow/array/array_binary.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_primitive.h>
#include <arrow/chunked_array.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/decimal.h>

#include <cmath>
#include <cstring>

namespace {

// =============================================================================
// Arrow extraction helpers — bulk chunk-level dispatch
// =============================================================================

// Dispatch once per chunk on type_id, then run a tight typed loop.
// This eliminates per-row virtual switch overhead from the old single-value approach.

static void chunk_extract_double(const arrow::Array* arr, std::vector<double>& out) {
    using T = arrow::Type;
    int64_t len = arr->length();
    switch (arr->type_id()) {
        case T::DOUBLE: {
            const double* buf = static_cast<const arrow::DoubleArray*>(arr)->raw_values();
            out.insert(out.end(), buf, buf + len);
            break;
        }
        case T::FLOAT: {
            auto* a = static_cast<const arrow::FloatArray*>(arr);
            for (int64_t i = 0; i < len; ++i) out.push_back(static_cast<double>(a->Value(i)));
            break;
        }
        case T::INT64: {
            auto* a = static_cast<const arrow::Int64Array*>(arr);
            for (int64_t i = 0; i < len; ++i) out.push_back(static_cast<double>(a->Value(i)));
            break;
        }
        case T::INT32: {
            auto* a = static_cast<const arrow::Int32Array*>(arr);
            for (int64_t i = 0; i < len; ++i) out.push_back(static_cast<double>(a->Value(i)));
            break;
        }
        case T::DECIMAL128: {
            auto* a = static_cast<const arrow::Decimal128Array*>(arr);
            int32_t scale = static_cast<const arrow::Decimal128Type*>(arr->type().get())->scale();
            if (scale == 2) {
                // Fast path: scale=2 is by far the most common (TPC-H monetary columns).
                // Decimal128 for TPC-H values fits in int64 low bits; divide by 100.0.
                static constexpr double inv100 = 1.0 / 100.0;
                for (int64_t i = 0; i < len; ++i)
                    out.push_back(static_cast<int64_t>(arrow::Decimal128(a->GetValue(i)).low_bits()) * inv100);
            } else {
                for (int64_t i = 0; i < len; ++i)
                    out.push_back(arrow::Decimal128(a->GetValue(i)).ToDouble(scale));
            }
            break;
        }
        case T::DECIMAL256: {
            auto* a = static_cast<const arrow::Decimal256Array*>(arr);
            int32_t scale = static_cast<const arrow::Decimal256Type*>(arr->type().get())->scale();
            for (int64_t i = 0; i < len; ++i)
                out.push_back(arrow::Decimal256(a->GetValue(i)).ToDouble(scale));
            break;
        }
        case T::INT8:  { auto* a = static_cast<const arrow::Int8Array*>(arr);   for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        case T::INT16: { auto* a = static_cast<const arrow::Int16Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        case T::UINT8: { auto* a = static_cast<const arrow::UInt8Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        case T::UINT16:{ auto* a = static_cast<const arrow::UInt16Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        case T::UINT32:{ auto* a = static_cast<const arrow::UInt32Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        case T::UINT64:{ auto* a = static_cast<const arrow::UInt64Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<double>(a->Value(i))); break; }
        default: throw std::runtime_error("chunk_extract_double: unsupported type " + arr->type()->ToString());
    }
}

static void chunk_extract_int64(const arrow::Array* arr, std::vector<int64_t>& out) {
    using T = arrow::Type;
    int64_t len = arr->length();
    switch (arr->type_id()) {
        case T::INT64: {
            // memcpy-style fast path: contiguous int64_t buffer
            const int64_t* buf = static_cast<const arrow::Int64Array*>(arr)->raw_values();
            out.insert(out.end(), buf, buf + len);
            break;
        }
        case T::INT32: {
            auto* a = static_cast<const arrow::Int32Array*>(arr);
            for (int64_t i = 0; i < len; ++i) out.push_back(static_cast<int64_t>(a->Value(i)));
            break;
        }
        case T::DECIMAL128: {
            auto* a = static_cast<const arrow::Decimal128Array*>(arr);
            for (int64_t i = 0; i < len; ++i)
                out.push_back(static_cast<int64_t>(arrow::Decimal128(a->GetValue(i)).low_bits()));
            break;
        }
        case T::INT8:  { auto* a = static_cast<const arrow::Int8Array*>(arr);   for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        case T::INT16: { auto* a = static_cast<const arrow::Int16Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        case T::UINT8: { auto* a = static_cast<const arrow::UInt8Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        case T::UINT16:{ auto* a = static_cast<const arrow::UInt16Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        case T::UINT32:{ auto* a = static_cast<const arrow::UInt32Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        case T::UINT64:{ auto* a = static_cast<const arrow::UInt64Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<int64_t>(a->Value(i))); break; }
        default: throw std::runtime_error("chunk_extract_int64: unsupported type " + arr->type()->ToString());
    }
}

static void chunk_extract_int32(const arrow::Array* arr, std::vector<int32_t>& out) {
    using T = arrow::Type;
    int64_t len = arr->length();
    switch (arr->type_id()) {
        case T::INT32:
        {
            const int32_t* buf = static_cast<const arrow::Int32Array*>(arr)->raw_values();
            out.insert(out.end(), buf, buf + len);
            break;
        }
        case T::DATE32: {
            const int32_t* buf = static_cast<const arrow::Date32Array*>(arr)->raw_values();
            out.insert(out.end(), buf, buf + len);
            break;
        }
        case T::INT64: {
            auto* a = static_cast<const arrow::Int64Array*>(arr);
            for (int64_t i = 0; i < len; ++i) out.push_back(static_cast<int32_t>(a->Value(i)));
            break;
        }
        case T::INT8:  { auto* a = static_cast<const arrow::Int8Array*>(arr);   for (int64_t i=0;i<len;++i) out.push_back(static_cast<int32_t>(a->Value(i))); break; }
        case T::INT16: { auto* a = static_cast<const arrow::Int16Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<int32_t>(a->Value(i))); break; }
        case T::UINT8: { auto* a = static_cast<const arrow::UInt8Array*>(arr);  for (int64_t i=0;i<len;++i) out.push_back(static_cast<int32_t>(a->Value(i))); break; }
        case T::UINT16:{ auto* a = static_cast<const arrow::UInt16Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<int32_t>(a->Value(i))); break; }
        case T::UINT32:{ auto* a = static_cast<const arrow::UInt32Array*>(arr); for (int64_t i=0;i<len;++i) out.push_back(static_cast<int32_t>(a->Value(i))); break; }
        default: throw std::runtime_error("chunk_extract_int32: unsupported type " + arr->type()->ToString());
    }
}

static void chunk_extract_string(const arrow::Array* arr, std::vector<std::string>& out) {
    using T = arrow::Type;
    int64_t len = arr->length();
    switch (arr->type_id()) {
        case T::STRING: {
            auto* a = static_cast<const arrow::StringArray*>(arr);
            for (int64_t i = 0; i < len; ++i) { auto sv = a->GetView(i); out.emplace_back(sv.data(), sv.size()); }
            break;
        }
        case T::LARGE_STRING: {
            auto* a = static_cast<const arrow::LargeStringArray*>(arr);
            for (int64_t i = 0; i < len; ++i) { auto sv = a->GetView(i); out.emplace_back(sv.data(), sv.size()); }
            break;
        }
        case T::BINARY: {
            auto* a = static_cast<const arrow::BinaryArray*>(arr);
            for (int64_t i = 0; i < len; ++i) { auto sv = a->GetView(i); out.emplace_back(sv.data(), sv.size()); }
            break;
        }
        case T::LARGE_BINARY: {
            auto* a = static_cast<const arrow::LargeBinaryArray*>(arr);
            for (int64_t i = 0; i < len; ++i) { auto sv = a->GetView(i); out.emplace_back(sv.data(), sv.size()); }
            break;
        }
        default: throw std::runtime_error("chunk_extract_string: unsupported type " + arr->type()->ToString());
    }
}

// Column-level wrappers
static void extract_double_col(const arrow::ChunkedArray* ca, std::vector<double>& out) {
    out.reserve(out.size() + static_cast<size_t>(ca->length()));
    for (int ci = 0; ci < ca->num_chunks(); ++ci) chunk_extract_double(ca->chunk(ci).get(), out);
}
static void extract_int64_col(const arrow::ChunkedArray* ca, std::vector<int64_t>& out) {
    out.reserve(out.size() + static_cast<size_t>(ca->length()));
    for (int ci = 0; ci < ca->num_chunks(); ++ci) chunk_extract_int64(ca->chunk(ci).get(), out);
}
static void extract_int32_col(const arrow::ChunkedArray* ca, std::vector<int32_t>& out) {
    out.reserve(out.size() + static_cast<size_t>(ca->length()));
    for (int ci = 0; ci < ca->num_chunks(); ++ci) chunk_extract_int32(ca->chunk(ci).get(), out);
}
static void extract_string_col(const arrow::ChunkedArray* ca, std::vector<std::string>& out) {
    out.reserve(out.size() + static_cast<size_t>(ca->length()));
    for (int ci = 0; ci < ca->num_chunks(); ++ci) chunk_extract_string(ca->chunk(ci).get(), out);
}

// Enum extraction: dispatch once per chunk on string type, avoid per-row heap alloc on hit
static void extract_enum_col(const arrow::ChunkedArray* ca,
                              std::unordered_map<std::string, uint8_t>& dict,
                              std::vector<std::string>& names,
                              std::vector<uint8_t>& out) {
    using T = arrow::Type;
    out.reserve(out.size() + static_cast<size_t>(ca->length()));

    auto lookup = [&](std::string_view sv) -> uint8_t {
        // Avoid allocation on lookup hit by using find with a temporary string
        std::string key(sv.data(), sv.size());
        auto it = dict.find(key);
        if (it != dict.end()) return it->second;
        uint8_t code = static_cast<uint8_t>(names.size());
        names.push_back(key);
        dict.emplace(std::move(key), code);
        return code;
    };

    for (int ci = 0; ci < ca->num_chunks(); ++ci) {
        const arrow::Array* arr = ca->chunk(ci).get();
        int64_t len = arr->length();
        switch (arr->type_id()) {
            case T::STRING: {
                auto* a = static_cast<const arrow::StringArray*>(arr);
                for (int64_t i = 0; i < len; ++i) out.push_back(lookup(a->GetView(i)));
                break;
            }
            case T::LARGE_STRING: {
                auto* a = static_cast<const arrow::LargeStringArray*>(arr);
                for (int64_t i = 0; i < len; ++i) out.push_back(lookup(a->GetView(i)));
                break;
            }
            case T::BINARY: {
                auto* a = static_cast<const arrow::BinaryArray*>(arr);
                for (int64_t i = 0; i < len; ++i) out.push_back(lookup(a->GetView(i)));
                break;
            }
            default:
                throw std::runtime_error("extract_enum_col: unsupported type " +
                                         arr->type()->ToString());
        }
    }
}

// =============================================================================
// Multi-column permutation helper
// Permuter holds pre-allocated scratch buffers per type to avoid per-column
// heap allocation.  Each .apply() shuffles one column using the stored index.
// =============================================================================
struct Permuter {
    const std::vector<int32_t>& perm;
    // Per-type scratch buffers (allocated on first use, reused for subsequent columns)
    std::vector<double>      sd;
    std::vector<int64_t>     si64;
    std::vector<int32_t>     si32;
    std::vector<uint8_t>     su8;
    std::vector<std::string> sstr;

    explicit Permuter(const std::vector<int32_t>& p) : perm(p) {}

    void apply(std::vector<double>& v) {
        sd.resize(v.size());
        for (size_t i = 0; i < perm.size(); ++i) sd[i] = v[static_cast<size_t>(perm[i])];
        v.swap(sd);
    }
    void apply(std::vector<int64_t>& v) {
        si64.resize(v.size());
        for (size_t i = 0; i < perm.size(); ++i) si64[i] = v[static_cast<size_t>(perm[i])];
        v.swap(si64);
    }
    void apply(std::vector<int32_t>& v) {
        si32.resize(v.size());
        for (size_t i = 0; i < perm.size(); ++i) si32[i] = v[static_cast<size_t>(perm[i])];
        v.swap(si32);
    }
    void apply(std::vector<uint8_t>& v) {
        su8.resize(v.size());
        for (size_t i = 0; i < perm.size(); ++i) su8[i] = v[static_cast<size_t>(perm[i])];
        v.swap(su8);
    }
    void apply(std::vector<uint16_t>& v) {
        std::vector<uint16_t> tmp(v.size());
        for (size_t i = 0; i < perm.size(); ++i) tmp[i] = v[static_cast<size_t>(perm[i])];
        v.swap(tmp);
    }
    void apply(std::vector<std::string>& v) {
        sstr.resize(v.size());
        for (size_t i = 0; i < perm.size(); ++i) sstr[i] = std::move(v[static_cast<size_t>(perm[i])]);
        v.swap(sstr);
    }
    void apply(std::vector<bool>& v) {
        std::vector<bool> tmp(v.size());
        for (size_t i = 0; i < perm.size(); ++i) tmp[i] = v[static_cast<size_t>(perm[i])];
        v.swap(tmp);
    }
};

// =============================================================================
// Counting-sort (radix-like) permutation for when the primary key has small range.
// Used for l_shipdate and o_orderdate which have only ~2920 distinct values over
// the TPC-H date range.  This sorts in O(N + K) where K = date range.
// Returns a permutation index sorted by key[i] ASC.
// =============================================================================
static std::vector<int32_t> counting_sort_permutation(const std::vector<int32_t>& key) {
    if (key.empty()) return {};
    int32_t mn = key[0], mx = key[0];
    for (int32_t v : key) {
        if (v < mn) mn = v;
        if (v > mx) mx = v;
    }
    size_t range = static_cast<size_t>(mx - mn + 1);
    size_t n = key.size();

    // Count per bucket
    std::vector<int32_t> cnt(range, 0);
    for (int32_t v : key) ++cnt[static_cast<size_t>(v - mn)];

    // Prefix-sum → start offsets
    std::vector<int32_t> off(range + 1, 0);
    for (size_t i = 0; i < range; ++i) off[i + 1] = off[i] + cnt[i];

    // Fill permutation
    std::vector<int32_t> perm(n);
    std::vector<int32_t> pos(off.begin(), off.begin() + static_cast<ptrdiff_t>(range));
    for (size_t i = 0; i < n; ++i) {
        size_t b = static_cast<size_t>(key[i] - mn);
        perm[static_cast<size_t>(pos[b]++)] = static_cast<int32_t>(i);
    }
    return perm;
}

// counting_sort_scatter: returns scatter[i] = output position for input row i
// Directly usable as: out[scatter[i]] = in[i]  (sequential reads, scattered writes)
static std::vector<int32_t> counting_sort_scatter(const std::vector<int32_t>& key) {
    if (key.empty()) return {};
    int32_t mn = key[0], mx = key[0];
    for (int32_t v : key) {
        if (v < mn) mn = v;
        if (v > mx) mx = v;
    }
    size_t range = static_cast<size_t>(mx - mn + 1);
    size_t n = key.size();
    std::vector<int32_t> cnt(range, 0);
    for (int32_t v : key) ++cnt[static_cast<size_t>(v - mn)];
    // pos[b] = first output position for bucket b
    std::vector<int32_t> pos(range, 0);
    int32_t acc = 0;
    for (size_t i = 0; i < range; ++i) { pos[i] = acc; acc += cnt[i]; }
    // scatter[i] = output position for input row i
    std::vector<int32_t> scatter(n);
    for (size_t i = 0; i < n; ++i) {
        size_t b = static_cast<size_t>(key[i] - mn);
        scatter[i] = pos[b]++;
    }
    return scatter;
}

// =============================================================================
// CSR builder — counting-sort directly from key column; no pair<> intermediary
// =============================================================================
static CsrIndex build_csr_from_col(const std::vector<int64_t>& keys, int64_t max_key) {
    CsrIndex csr;
    if (max_key < 0 || keys.empty()) return csr;
    int32_t n = static_cast<int32_t>(keys.size());
    size_t sz = static_cast<size_t>(max_key + 2);
    csr.offsets.assign(sz, 0);
    csr.row_ids.resize(static_cast<size_t>(n));
    for (int32_t i = 0; i < n; ++i)
        ++csr.offsets[static_cast<size_t>(keys[static_cast<size_t>(i)]) + 1];
    for (size_t i = 1; i < sz; ++i)
        csr.offsets[i] += csr.offsets[i - 1];
    // write pointers: copy first sz-1 offsets (= the starts)
    std::vector<int32_t> pos(csr.offsets.begin(), csr.offsets.begin() + static_cast<ptrdiff_t>(sz - 1));
    for (int32_t i = 0; i < n; ++i) {
        size_t k = static_cast<size_t>(keys[static_cast<size_t>(i)]);
        csr.row_ids[static_cast<size_t>(pos[k]++)] = i;
    }
    return csr;
}

// =============================================================================
// Table loaders
// =============================================================================

static void load_nation(const arrow::Table* tbl, NationTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = static_cast<int32_t>(n);

    extract_int32_col(tbl->GetColumnByName("n_nationkey").get(), t.n_nationkey);
    extract_int32_col(tbl->GetColumnByName("n_regionkey").get(), t.n_regionkey);
    extract_string_col(tbl->GetColumnByName("n_name").get(),    t.n_name);
    extract_string_col(tbl->GetColumnByName("n_comment").get(), t.n_comment);

    t.nationkey_to_regionkey.fill(-1);
    for (int32_t i = 0; i < t.num_rows; ++i) {
        int32_t nk = t.n_nationkey[static_cast<size_t>(i)];
        int32_t rk = t.n_regionkey[static_cast<size_t>(i)];
        t.name_to_nationkey[t.n_name[static_cast<size_t>(i)]] = nk;
        if (nk >= 0 && nk < 25)
            t.nationkey_to_regionkey[static_cast<size_t>(nk)] = rk;
    }
}

static void load_region(const arrow::Table* tbl, RegionTable& t) {
    t.num_rows = static_cast<int32_t>(tbl->num_rows());

    extract_int32_col(tbl->GetColumnByName("r_regionkey").get(), t.r_regionkey);
    extract_string_col(tbl->GetColumnByName("r_name").get(),    t.r_name);
    extract_string_col(tbl->GetColumnByName("r_comment").get(), t.r_comment);

    for (int32_t i = 0; i < t.num_rows; ++i)
        t.name_to_regionkey[t.r_name[static_cast<size_t>(i)]] =
            t.r_regionkey[static_cast<size_t>(i)];
}

static void load_supplier(const arrow::Table* tbl, SupplierTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;

    extract_int64_col( tbl->GetColumnByName("s_suppkey").get(),   t.s_suppkey);
    extract_int32_col( tbl->GetColumnByName("s_nationkey").get(), t.s_nationkey);
    extract_double_col(tbl->GetColumnByName("s_acctbal").get(),   t.s_acctbal);
    extract_string_col(tbl->GetColumnByName("s_name").get(),      t.s_name);
    extract_string_col(tbl->GetColumnByName("s_address").get(),   t.s_address);
    extract_string_col(tbl->GetColumnByName("s_phone").get(),     t.s_phone);
    extract_string_col(tbl->GetColumnByName("s_comment").get(),   t.s_comment);

    {
        std::vector<int32_t> idx(static_cast<size_t>(n));
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](int32_t a, int32_t b) {
            return t.s_suppkey[static_cast<size_t>(a)] < t.s_suppkey[static_cast<size_t>(b)];
        });
        Permuter p(idx);
        p.apply(t.s_suppkey); p.apply(t.s_nationkey); p.apply(t.s_acctbal);
        p.apply(t.s_name);    p.apply(t.s_address);   p.apply(t.s_phone);  p.apply(t.s_comment);
    }

    // Pre-compute complaint bitmask
    t.s_has_complaint.resize(static_cast<size_t>(n));
    for (int64_t i = 0; i < n; ++i) {
        const auto& c = t.s_comment[static_cast<size_t>(i)];
        auto p1 = c.find("Customer");
        t.s_has_complaint[static_cast<size_t>(i)] =
            (p1 != std::string::npos) &&
            (c.find("Complaints", p1) != std::string::npos);
    }

    // Pre-format s_acctbal as "%.2f" string for fast output (Q2 and any query outputting acctbal).
    // Avoids per-row snprintf in the output phase.
    {
        size_t nn = static_cast<size_t>(n);
        t.s_acctbal_str.resize(nn);
        char buf[32];
        for (size_t i = 0; i < nn; ++i) {
            int len = std::snprintf(buf, sizeof(buf), "%.2f", t.s_acctbal[i]);
            t.s_acctbal_str[i].assign(buf, static_cast<size_t>(len > 0 ? len : 0));
        }
    }
}

static void load_customer(const arrow::Table* tbl, CustomerTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;

    extract_int64_col( tbl->GetColumnByName("c_custkey").get(),   t.c_custkey);
    extract_int32_col( tbl->GetColumnByName("c_nationkey").get(), t.c_nationkey);
    extract_double_col(tbl->GetColumnByName("c_acctbal").get(),   t.c_acctbal);
    extract_string_col(tbl->GetColumnByName("c_phone").get(),     t.c_phone);
    extract_string_col(tbl->GetColumnByName("c_name").get(),      t.c_name);
    extract_string_col(tbl->GetColumnByName("c_address").get(),   t.c_address);
    extract_string_col(tbl->GetColumnByName("c_comment").get(),   t.c_comment);

    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("c_mktsegment").get(),
                         dict, t.mktsegment_names, t.c_mktsegment);
    }

    // Materialise phone country code (first 2 chars)
    t.c_phone_cc.reserve(static_cast<size_t>(n));
    for (const auto& ph : t.c_phone)
        t.c_phone_cc.push_back(ph.size() >= 2 ? ph.substr(0, 2) : ph);

    // Materialise packed uint16 phone country code: low byte = c_phone[0], high byte = c_phone[1].
    // General-purpose: faster integer comparison vs. string for phone-code filters.
    t.c_phone_cc_u16.resize(static_cast<size_t>(n));
    for (size_t i = 0; i < static_cast<size_t>(n); ++i) {
        const auto& ph = t.c_phone[i];
        t.c_phone_cc_u16[i] = (ph.size() >= 2)
            ? static_cast<uint16_t>(static_cast<uint8_t>(ph[0]) | (static_cast<uint16_t>(static_cast<uint8_t>(ph[1])) << 8))
            : 0u;
    }

    {
        std::vector<int32_t> idx(static_cast<size_t>(n));
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](int32_t a, int32_t b) {
            return t.c_custkey[static_cast<size_t>(a)] < t.c_custkey[static_cast<size_t>(b)];
        });
        Permuter p(idx);
        p.apply(t.c_custkey);    p.apply(t.c_nationkey);  p.apply(t.c_acctbal);
        p.apply(t.c_mktsegment); p.apply(t.c_phone);      p.apply(t.c_phone_cc);
        p.apply(t.c_phone_cc_u16);
        p.apply(t.c_name);       p.apply(t.c_address);    p.apply(t.c_comment);
    }

    // Pre-format c_custkey and c_acctbal as strings (general-purpose string output).
    // Also materialise c_nationkey as uint8 for fast dense lookup.
    {
        const size_t nn = static_cast<size_t>(n);
        t.c_custkey_str.resize(nn);
        t.c_acctbal_str.resize(nn);
        t.c_nationkey_u8.resize(nn);
        char buf[32];
        for (size_t i = 0; i < nn; ++i) {
            t.c_custkey_str[i] = std::to_string(t.c_custkey[i]);
            int len = std::snprintf(buf, sizeof(buf), "%.2f", t.c_acctbal[i]);
            t.c_acctbal_str[i] = std::string(buf, static_cast<size_t>(len > 0 ? len : 0));
            t.c_nationkey_u8[i] = static_cast<uint8_t>(t.c_nationkey[i] >= 0 ? t.c_nationkey[i] : 255);
        }
    }

}

static void load_part(const arrow::Table* tbl, PartTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;

    extract_int64_col( tbl->GetColumnByName("p_partkey").get(),    t.p_partkey);
    extract_int32_col( tbl->GetColumnByName("p_size").get(),       t.p_size);
    extract_double_col(tbl->GetColumnByName("p_retailprice").get(), t.p_retailprice);
    extract_string_col(tbl->GetColumnByName("p_type").get(),       t.p_type);
    extract_string_col(tbl->GetColumnByName("p_name").get(),       t.p_name);
    extract_string_col(tbl->GetColumnByName("p_comment").get(),    t.p_comment);

    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("p_brand").get(),
                         dict, t.brand_names, t.p_brand);
    }
    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("p_mfgr").get(),
                         dict, t.mfgr_names, t.p_mfgr);
    }
    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("p_container").get(),
                         dict, t.container_names, t.p_container);
    }
    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("p_type").get(),
                         dict, t.type_names, t.p_type_enum);
    }

    {
        std::vector<int32_t> idx(static_cast<size_t>(n));
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](int32_t a, int32_t b) {
            return t.p_partkey[static_cast<size_t>(a)] < t.p_partkey[static_cast<size_t>(b)];
        });
        Permuter p(idx);
        p.apply(t.p_partkey);  p.apply(t.p_size);       p.apply(t.p_retailprice);
        p.apply(t.p_brand);    p.apply(t.p_mfgr);       p.apply(t.p_container);
        p.apply(t.p_type);     p.apply(t.p_type_enum);  p.apply(t.p_name);    p.apply(t.p_comment);
    }

    // Pre-format p_partkey as decimal string for fast output (Q2 and any query outputting partkey).
    // Avoids per-row snprintf in the output phase.
    {
        size_t nn = static_cast<size_t>(n);
        t.p_partkey_str.resize(nn);
        char buf[24];
        for (size_t i = 0; i < nn; ++i) {
            int len = std::snprintf(buf, sizeof(buf), "%" PRId64, t.p_partkey[i]);
            t.p_partkey_str[i].assign(buf, static_cast<size_t>(len > 0 ? len : 0));
        }
    }

    // Build p_name_word1_u8: encode the first word of each p_name as a uint8_t code.
    // TPC-H p_name = "word1 word2 ... word5"; we extract word1 (up to first space).
    // Builds a dictionary mapping word1_string → uint8_t code, and the inverse table.
    // Used by Q20 (and any LIKE 'COLOR%' query) to avoid string pointer indirection.
    {
        PROFILE_SCOPE("build_p_name_word1");
        size_t nn = static_cast<size_t>(n);
        auto& dict  = t.p_name_word1_to_code;
        auto& table = t.p_name_word1_code_table;
        t.p_name_word1_u8.resize(nn, 0xFFu);
        for (size_t i = 0; i < nn; ++i) {
            const std::string& nm = t.p_name[i];
            // Extract first word: everything before first space (or entire string)
            size_t sp = nm.find(' ');
            std::string w1 = (sp != std::string::npos) ? nm.substr(0, sp) : nm;
            auto it = dict.find(w1);
            if (it == dict.end()) {
                uint8_t code = static_cast<uint8_t>(table.size());
                dict[w1] = code;
                table.push_back(w1);
                t.p_name_word1_u8[i] = code;
            } else {
                t.p_name_word1_u8[i] = it->second;
            }
        }
        TRACE_COUNT("p_name_word1_distinct_words", static_cast<long long>(table.size()));
    }
}

static void load_partsupp(const arrow::Table* tbl, PartsuppTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;

    extract_int64_col( tbl->GetColumnByName("ps_partkey").get(),    t.ps_partkey);
    extract_int64_col( tbl->GetColumnByName("ps_suppkey").get(),    t.ps_suppkey);
    extract_int32_col( tbl->GetColumnByName("ps_availqty").get(),   t.ps_availqty);
    extract_double_col(tbl->GetColumnByName("ps_supplycost").get(), t.ps_supplycost);
    extract_string_col(tbl->GetColumnByName("ps_comment").get(),    t.ps_comment);

    // Sort by (ps_partkey ASC, ps_suppkey ASC)
    // Pack composite sort key into int64 for single comparison per sort step.
    // TPC-H at SF=20: partkey <= 400000, suppkey <= 20000 — fits in int64.
    {
        size_t sz = static_cast<size_t>(n);
        std::vector<int64_t> skey(sz);
        for (size_t i = 0; i < sz; ++i)
            skey[i] = (t.ps_partkey[i] << 20) | (t.ps_suppkey[i] & 0xFFFFF);
        std::vector<int32_t> idx(sz);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](int32_t a, int32_t b) {
            return skey[static_cast<size_t>(a)] < skey[static_cast<size_t>(b)];
        });
        Permuter p(idx);
        p.apply(t.ps_partkey);   p.apply(t.ps_suppkey);
        p.apply(t.ps_availqty);  p.apply(t.ps_supplycost);  p.apply(t.ps_comment);
    }

    // Derive compact/integer columns for fast Q11 aggregation.
    size_t sz = static_cast<size_t>(n);

    // ps_suppkey_i32: int32 cast of suppkey (TPC-H suppkey ≤ 200K << 2^31)
    t.ps_suppkey_i32.resize(sz);
    for (size_t i = 0; i < sz; ++i)
        t.ps_suppkey_i32[i] = static_cast<int32_t>(t.ps_suppkey[i]);

    // ps_supplycost_cents: round(supplycost * 100) → int32_t
    // TPC-H supplycost ∈ [1.00, 1000.00] → cents ∈ [100, 100000] → fits int32_t.
    t.ps_supplycost_cents.resize(sz);
    for (size_t i = 0; i < sz; ++i)
        t.ps_supplycost_cents[i] = static_cast<int32_t>(
            t.ps_supplycost[i] * 100.0 + 0.5);

    // ps_partkey_i32: int32 cast of partkey (TPC-H partkey ≤ 4M at SF=20 << 2^31)
    t.ps_partkey_i32.resize(sz);
    for (size_t i = 0; i < sz; ++i)
        t.ps_partkey_i32[i] = static_cast<int32_t>(t.ps_partkey[i]);

    // ps_packed: AoS layout combining 4 hot int32 columns for cache-efficient scans.
    // Packs (suppkey, partkey, availqty, cost_cents) into 16-byte structs so that
    // sequential row scans touch one cache line per 4 rows instead of 4 separate arrays.
    t.ps_packed.resize(sz);
    for (size_t i = 0; i < sz; ++i) {
        t.ps_packed[i] = { t.ps_suppkey_i32[i], t.ps_partkey_i32[i],
                           t.ps_availqty[i],     t.ps_supplycost_cents[i] };
    }
}

static void load_orders(const arrow::Table* tbl, OrdersTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;

    extract_int32_col( tbl->GetColumnByName("o_orderdate").get(),    t.o_orderdate);
    extract_int64_col( tbl->GetColumnByName("o_orderkey").get(),     t.o_orderkey);
    extract_int64_col( tbl->GetColumnByName("o_custkey").get(),      t.o_custkey);
    extract_double_col(tbl->GetColumnByName("o_totalprice").get(),   t.o_totalprice);
    extract_int32_col( tbl->GetColumnByName("o_shippriority").get(), t.o_shippriority);
    extract_string_col(tbl->GetColumnByName("o_clerk").get(),        t.o_clerk);
    extract_string_col(tbl->GetColumnByName("o_comment").get(),      t.o_comment);

    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("o_orderstatus").get(),
                         dict, t.orderstatus_names, t.o_orderstatus);
    }
    {
        std::unordered_map<std::string, uint8_t> dict;
        extract_enum_col(tbl->GetColumnByName("o_orderpriority").get(),
                         dict, t.orderpriority_names, t.o_orderpriority);
    }

    // Sort by o_orderdate ASC — pack int64 sort key: date(32 hi) | custkey_low(32 lo)
    {
        // Counting scatter sort: sequential reads of src + scat, scattered writes to dst
        std::vector<int32_t> scat = counting_sort_scatter(t.o_orderdate);
        size_t nn2 = static_cast<size_t>(t.num_rows);
        auto sc_i32 = [&](std::vector<int32_t>& v){ std::vector<int32_t> tmp(nn2); for(size_t i=0;i<nn2;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
        auto sc_i64 = [&](std::vector<int64_t>& v){ std::vector<int64_t> tmp(nn2); for(size_t i=0;i<nn2;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
        auto sc_dbl = [&](std::vector<double>&  v){ std::vector<double>  tmp(nn2); for(size_t i=0;i<nn2;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
        auto sc_u8  = [&](std::vector<uint8_t>& v){ std::vector<uint8_t> tmp(nn2); for(size_t i=0;i<nn2;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
        auto sc_str = [&](std::vector<std::string>& v){ std::vector<std::string> tmp(nn2); for(size_t i=0;i<nn2;++i) tmp[(size_t)scat[i]]=std::move(v[i]); v.swap(tmp); };
        sc_i32(t.o_orderdate); sc_i64(t.o_orderkey); sc_i64(t.o_custkey);
        sc_dbl(t.o_totalprice); sc_i32(t.o_shippriority);
        sc_u8(t.o_orderstatus); sc_u8(t.o_orderpriority);
        sc_str(t.o_clerk); sc_str(t.o_comment);
    }

    // ── Build flat comment buffer ─────────────────────────────────────────────
    // Concatenate all o_comment strings into a single contiguous char array.
    // This eliminates per-string pointer chasing during the full-table comment scan
    // in Q13, converting 30M scattered heap reads into one sequential memory scan.
    {
        size_t nn2 = static_cast<size_t>(t.num_rows);
        // First pass: compute total bytes and fill offsets
        t.o_comment_offs.resize(nn2 + 1);
        uint64_t running = 0;
        for (size_t i = 0; i < nn2; ++i) {
            t.o_comment_offs[i] = running;
            running += static_cast<uint64_t>(t.o_comment[i].size());
        }
        t.o_comment_offs[nn2] = running;
        // Second pass: copy string data
        t.o_comment_flat.resize(running);
        char* dst = t.o_comment_flat.data();
        for (size_t i = 0; i < nn2; ++i) {
            const std::string& s = t.o_comment[i];
            std::memcpy(dst + t.o_comment_offs[i], s.data(), s.size());
        }
    }
    // ── Free o_comment string vector after building flat buffer ──────────────
    // No query reads o_comment directly (only o_comment_flat + o_comment_offs).
    // Clearing the 30M-string vector frees ~2GB of memory at SF=20, reducing
    // memory pressure and improving cache utilization for subsequent queries.
    // The flat buffer is the canonical storage; o_comment is a build artifact.
    // We shrink_to_fit to actually release the heap memory.
    {
        std::vector<std::string>().swap(t.o_comment);
    }

    // ── Build narrow custkey (int32_t) ────────────────────────────────────────
    // TPC-H custkey ≤ 150000*SF which fits easily in int32_t.
    // Halves memory bandwidth vs. int64 for custkey lookups.
    {
        size_t nn2 = static_cast<size_t>(t.num_rows);
        t.o_custkey_i32.resize(nn2);
        for (size_t i = 0; i < nn2; ++i)
            t.o_custkey_i32[i] = static_cast<int32_t>(t.o_custkey[i]);
    }
}

static void load_lineitem(const arrow::Table* tbl, LineitemTable& t) {
    int64_t n = tbl->num_rows();
    t.num_rows = n;
    size_t nn = static_cast<size_t>(n);

    // Phase 1: Extract all columns in parallel (4 threads).
    // Thread A: int32 columns (dates + linenumber)  — ~4 passes, moderate cost
    // Thread B: double columns (quantity, price, discount, tax) — 4 passes, moderate cost
    // Thread C: int64 keys + enum uint8 columns — 3+4 passes, moderate cost
    // Thread D: l_comment alone — single string column but very expensive (~9s at SF=20)
    std::exception_ptr ex_a, ex_b, ex_c, ex_d2;
    auto ext_a = std::thread([&]() noexcept {
        try {
            extract_int32_col(tbl->GetColumnByName("l_shipdate").get(),    t.l_shipdate);
            extract_int32_col(tbl->GetColumnByName("l_commitdate").get(),  t.l_commitdate);
            extract_int32_col(tbl->GetColumnByName("l_receiptdate").get(), t.l_receiptdate);
            extract_int32_col(tbl->GetColumnByName("l_linenumber").get(),  t.l_linenumber);
        } catch (...) { ex_a = std::current_exception(); }
    });
    auto ext_b = std::thread([&]() noexcept {
        try {
            extract_double_col(tbl->GetColumnByName("l_quantity").get(),      t.l_quantity);
            extract_double_col(tbl->GetColumnByName("l_extendedprice").get(), t.l_extendedprice);
            extract_double_col(tbl->GetColumnByName("l_discount").get(),      t.l_discount);
            extract_double_col(tbl->GetColumnByName("l_tax").get(),           t.l_tax);
        } catch (...) { ex_b = std::current_exception(); }
    });
    auto ext_c = std::thread([&]() noexcept {
        try {
            extract_int64_col( tbl->GetColumnByName("l_orderkey").get(), t.l_orderkey);
            extract_int64_col( tbl->GetColumnByName("l_partkey").get(),  t.l_partkey);
            extract_int64_col( tbl->GetColumnByName("l_suppkey").get(),  t.l_suppkey);
            { std::unordered_map<std::string,uint8_t> d; extract_enum_col(tbl->GetColumnByName("l_returnflag").get(),   d, t.returnflag_names,   t.l_returnflag); }
            { std::unordered_map<std::string,uint8_t> d; extract_enum_col(tbl->GetColumnByName("l_linestatus").get(),   d, t.linestatus_names,   t.l_linestatus); }
            { std::unordered_map<std::string,uint8_t> d; extract_enum_col(tbl->GetColumnByName("l_shipmode").get(),     d, t.shipmode_names,     t.l_shipmode); }
            { std::unordered_map<std::string,uint8_t> d; extract_enum_col(tbl->GetColumnByName("l_shipinstruct").get(), d, t.shipinstruct_names, t.l_shipinstruct); }
        } catch (...) { ex_c = std::current_exception(); }
    });
    auto ext_d2 = std::thread([&]() noexcept {
        try {
            extract_string_col(tbl->GetColumnByName("l_comment").get(), t.l_comment);
        } catch (...) { ex_d2 = std::current_exception(); }
    });
    ext_a.join(); ext_b.join(); ext_c.join(); ext_d2.join();
    if (ex_a)  std::rethrow_exception(ex_a);
    if (ex_b)  std::rethrow_exception(ex_b);
    if (ex_c)  std::rethrow_exception(ex_c);
    if (ex_d2) std::rethrow_exception(ex_d2);

    // Phase 2: Sort by l_shipdate ASC.
    // Scatter permutation: out[scat[i]] = in[i]
    // Sequential reads of src + scat index; scattered writes to dst.
    // 4 column-group threads run concurrently.
    {
        std::vector<int32_t> scat = counting_sort_scatter(t.l_shipdate);
        auto pg1 = std::thread([&]{
            auto si32 = [&](std::vector<int32_t>& v){ std::vector<int32_t> tmp(nn); for(size_t i=0;i<nn;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
            si32(t.l_shipdate); si32(t.l_commitdate); si32(t.l_receiptdate); si32(t.l_linenumber);
        });
        auto pg2 = std::thread([&]{
            auto sdbl = [&](std::vector<double>& v){ std::vector<double> tmp(nn); for(size_t i=0;i<nn;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
            sdbl(t.l_quantity); sdbl(t.l_extendedprice); sdbl(t.l_discount); sdbl(t.l_tax);
        });
        auto pg3 = std::thread([&]{
            auto si64 = [&](std::vector<int64_t>& v){ std::vector<int64_t> tmp(nn); for(size_t i=0;i<nn;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
            si64(t.l_orderkey); si64(t.l_partkey); si64(t.l_suppkey);
        });
        auto pg4 = std::thread([&]{
            auto su8  = [&](std::vector<uint8_t>& v){ std::vector<uint8_t> tmp(nn); for(size_t i=0;i<nn;++i) tmp[(size_t)scat[i]]=v[i]; v.swap(tmp); };
            auto sstr = [&](std::vector<std::string>& v){ std::vector<std::string> tmp(nn); for(size_t i=0;i<nn;++i) tmp[(size_t)scat[i]]=std::move(v[i]); v.swap(tmp); };
            su8(t.l_returnflag); su8(t.l_linestatus); su8(t.l_shipmode); su8(t.l_shipinstruct);
            sstr(t.l_comment);
        });
        pg1.join(); pg2.join(); pg3.join(); pg4.join();
    }

    // Phase 3: Compute precomputed comparison column (general-purpose)
    t.l_commitdate_lt_receiptdate.resize(nn);
    for (size_t i = 0; i < nn; ++i) {
        t.l_commitdate_lt_receiptdate[i] =
            (t.l_commitdate[i] < t.l_receiptdate[i]) ? 1u : 0u;
    }

    // Precompute l_shipdate < l_commitdate (general-purpose derived column).
    t.l_shipdate_lt_commitdate.resize(nn);
    for (size_t i = 0; i < nn; ++i) {
        t.l_shipdate_lt_commitdate[i] =
            (t.l_shipdate[i] < t.l_commitdate[i]) ? 1u : 0u;
    }

    // Phase 3b: Build receiptdate sorted index for range scans (e.g. q12).
    // This is a general-purpose permutation: l_receiptdate[l_receiptdate_sorted_idx[i]]
    // is non-decreasing in i. Allows binary-search range access for any query
    // filtering on l_receiptdate range.
    {
        std::vector<int32_t>& idx = t.l_receiptdate_sorted_idx;
        idx.resize(nn);
        std::iota(idx.begin(), idx.end(), 0);
        const int32_t* rd = t.l_receiptdate.data();
        std::sort(idx.begin(), idx.end(),
                  [rd](int32_t a, int32_t b){ return rd[a] < rd[b]; });
    }

    // Phase 3c: Build compact parallel arrays in receiptdate-sorted order.
    // These eliminate the scatter-read cache penalty when iterating the
    // receiptdate range in q12 (and any future query with rdate range filter).
    // Each array li_rds_X[k] holds the value for row rdate_sorted_idx[k].
    {
        const std::vector<int32_t>& idx = t.l_receiptdate_sorted_idx;
        const uint8_t* sm_src   = t.l_shipmode.data();
        const uint8_t* clt_src  = t.l_commitdate_lt_receiptdate.data();
        const uint8_t* slt_src  = t.l_shipdate_lt_commitdate.data();
        const int64_t* ok_src   = t.l_orderkey.data();
        const int32_t* rd_src   = t.l_receiptdate.data();

        t.li_rds_shipmode.resize(nn);
        t.li_rds_clt_rd.resize(nn);
        t.li_rds_slt_cd.resize(nn);
        t.li_rds_orderkey_i32.resize(nn);
        t.li_rds_receiptdate.resize(nn);

        uint8_t*  rds_sm  = t.li_rds_shipmode.data();
        uint8_t*  rds_clt = t.li_rds_clt_rd.data();
        uint8_t*  rds_slt = t.li_rds_slt_cd.data();
        int32_t*  rds_ok  = t.li_rds_orderkey_i32.data();
        int32_t*  rds_rd  = t.li_rds_receiptdate.data();

        for (size_t k = 0; k < nn; ++k) {
            size_t row  = static_cast<size_t>(idx[k]);
            rds_sm[k]  = sm_src[row];
            rds_clt[k] = clt_src[row];
            rds_slt[k] = slt_src[row];
            rds_ok[k]  = static_cast<int32_t>(ok_src[row]);
            rds_rd[k]  = rd_src[row];
        }

        // Build AoS packed array for cache-efficient Q12 hot loop.
        // Each 8-byte record holds all per-row filter data in one struct,
        // so a 64-byte cache line covers 8 complete rows instead of 4 separate
        // per-column fetches each covering only part of their cache line.
        t.li_rds_packed.resize(nn);
        LineitemTable::RdsPackedRow* packed = t.li_rds_packed.data();
        for (size_t k = 0; k < nn; ++k) {
            packed[k].shipmode    = rds_sm[k];
            packed[k].flags       = static_cast<uint8_t>((rds_clt[k] & 1u) | ((rds_slt[k] & 1u) << 1));
            packed[k].pad0        = 0;
            packed[k].pad1        = 0;
            packed[k].orderkey_i32 = rds_ok[k];
        }
    }

    // Phase 4: Build scaled-integer filter columns for q6 (and future queries).
    // l_discount_int8: discount * 100 rounded to nearest int, range [0,10]
    // l_quantity_int8: quantity as uint8, range [1,50]
    // l_tax_int8: tax * 100 rounded to nearest int, range [0,8]
    // These must be built AFTER sorting (Phase 2), so they match the sorted order.
    // We convert from the already-sorted l_discount and l_quantity double columns.
    t.l_discount_int8.resize(nn);
    t.l_quantity_int8.resize(nn);
    t.l_extendedprice_int32.resize(nn);
    t.l_tax_int8.resize(nn);
    for (size_t i = 0; i < nn; ++i) {
        // discount is like 0.07, 0.08 — multiply by 100, round to int8
        t.l_discount_int8[i] = static_cast<int8_t>(static_cast<int>(t.l_discount[i] * 100.0 + 0.5));
        // quantity is integer-valued 1-50 in TPC-H
        t.l_quantity_int8[i] = static_cast<uint8_t>(static_cast<int>(t.l_quantity[i] + 0.5));
        // extendedprice * 100 as int32: TPC-H prices have 2 decimal places,
        // max value ~104950 → max*100 = 10495000, fits in int32_t.
        t.l_extendedprice_int32[i] = static_cast<int32_t>(
            static_cast<int64_t>(t.l_extendedprice[i] * 100.0 + 0.5));
        // tax is like 0.02, 0.06 — multiply by 100, round to uint8, range [0,8]
        t.l_tax_int8[i] = static_cast<uint8_t>(static_cast<int>(t.l_tax[i] * 100.0 + 0.5));
    }

    // l_suppkey_i32: int32 cast of suppkey — built after sort so order matches.
    // TPC-H suppkey ≤ 10000*SF fits in int32_t.  Halves memory bandwidth vs int64
    // in hot scan loops (e.g. Q15 revenue aggregation).
    t.l_suppkey_i32.resize(nn);
    for (size_t i = 0; i < nn; ++i) {
        t.l_suppkey_i32[i] = static_cast<int32_t>(t.l_suppkey[i]);
    }

    // l_partkey_i32: int32 cast of partkey — built after sort so order matches.
    // TPC-H partkey ≤ 200000*SF fits in int32_t.  Halves memory bandwidth vs int64
    // in hot scan loops (e.g. Q14 promo revenue scan).
    t.l_partkey_i32.resize(nn);
    for (size_t i = 0; i < nn; ++i) {
        t.l_partkey_i32[i] = static_cast<int32_t>(t.l_partkey[i]);
    }
}

// =============================================================================
// Cross-table acceleration structures
// =============================================================================

static void build_acceleration(Database* db) {
    auto make_direct = [](const std::vector<int64_t>& keys,
                          std::vector<int32_t>& out) {
        int64_t max_k = 0;
        for (int64_t k : keys) if (k > max_k) max_k = k;
        out.assign(static_cast<size_t>(max_k + 1), -1);
        for (int32_t i = 0; i < static_cast<int32_t>(keys.size()); ++i)
            out[static_cast<size_t>(keys[static_cast<size_t>(i)])] = i;
    };

    // All 4 direct-indexed lookups + 4 CSR indexes built fully in parallel
    auto t1 = std::thread([&]{ make_direct(db->orders.o_orderkey,  db->orders_row_by_orderkey); });
    auto t2 = std::thread([&]{ make_direct(db->customer.c_custkey, db->cust_row_by_custkey);    });
    auto t3 = std::thread([&]{ make_direct(db->supplier.s_suppkey, db->supp_row_by_suppkey);    });
    auto t4 = std::thread([&]{ make_direct(db->part.p_partkey,     db->part_row_by_partkey);    });
    auto t5 = std::thread([&]{
        int64_t max_ok = *std::max_element(db->lineitem.l_orderkey.begin(), db->lineitem.l_orderkey.end());
        db->li_by_orderkey = build_csr_from_col(db->lineitem.l_orderkey, max_ok);
        // Build companion clt_rd array: for each CSR slot j, store the
        // l_commitdate_lt_receiptdate flag value inline.  This converts the
        // scattered random access li_clt_rd[row_ids[j]] in the EXISTS probe
        // into a sequential scan of a compact uint8 array.
        const auto& row_ids  = db->li_by_orderkey.row_ids;
        const auto& clt_rd   = db->lineitem.l_commitdate_lt_receiptdate;
        auto& companion      = db->li_by_orderkey_clt_rd;
        size_t m = row_ids.size();
        companion.resize(m);
        for (size_t j = 0; j < m; ++j)
            companion[j] = clt_rd[static_cast<size_t>(row_ids[j])];

        // Build qty companion: li_by_orderkey_qty[j] = l_quantity_int8 for CSR slot j.
        // Enables purely sequential quantity access when iterating orders via CSR,
        // eliminating the random access to l_quantity_int8[row_ids[j]] in Q18.
        {
            auto& qty_comp = db->li_by_orderkey_qty;
            qty_comp.resize(m);
            const uint8_t* qty_src = db->lineitem.l_quantity_int8.data();
            for (size_t j = 0; j < m; ++j)
                qty_comp[j] = qty_src[static_cast<size_t>(row_ids[j])];
        }

        // Build suppnk companion: suppkey → nationkey, then scatter into CSR slots.
        // First build a flat suppkey→nationkey map from the supplier table.
        const auto& supp = db->supplier;
        int64_t max_sk = 0;
        for (int64_t sk : supp.s_suppkey) if (sk > max_sk) max_sk = sk;
        std::vector<uint8_t> sk_to_nk(static_cast<size_t>(max_sk + 1), 0xFF);
        for (int64_t i = 0; i < supp.num_rows; ++i)
            sk_to_nk[static_cast<size_t>(supp.s_suppkey[static_cast<size_t>(i)])] =
                static_cast<uint8_t>(supp.s_nationkey[static_cast<size_t>(i)]);

        // Scatter suppnk and pre-computed revenue into CSR companion arrays.
        const auto& li = db->lineitem;
        auto& suppnk = db->li_by_orderkey_suppnk;
        suppnk.resize(m);
        for (size_t j = 0; j < m; ++j) {
            size_t lr = static_cast<size_t>(row_ids[j]);
            int64_t sk = li.l_suppkey[lr];
            suppnk[j] = (static_cast<size_t>(sk) <= static_cast<size_t>(max_sk))
                        ? sk_to_nk[static_cast<size_t>(sk)] : 0xFF;
        }

        // Build suppkey_i32 companion: li_by_orderkey_suppkey_i32[j] = int32 suppkey for CSR slot j.
        // Converts scattered random reads of l_suppkey[row_ids[j]] (960MB array) in Q21's inner
        // loop into sequential streaming reads of a compact 480MB companion array.
        // Built from the already-sorted l_suppkey_i32 (which is in l_shipdate order) via row_ids.
        {
            auto& sk_comp = db->li_by_orderkey_suppkey_i32;
            sk_comp.resize(m);
            const int32_t* sk_src = db->lineitem.l_suppkey_i32.data();
            for (size_t j = 0; j < m; ++j)
                sk_comp[j] = sk_src[static_cast<size_t>(row_ids[j])];
        }

        // Build ep_i32 companion: li_by_orderkey_ep_i32[j] = l_extendedprice_int32 for CSR slot j.
        // Converts scattered random access to l_extendedprice_int32[row_ids[j]] into
        // sequential streaming reads in Q3's revenue aggregation inner loop.
        {
            auto& ep_comp = db->li_by_orderkey_ep_i32;
            ep_comp.resize(m);
            const int32_t* ep_src = db->lineitem.l_extendedprice_int32.data();
            for (size_t j = 0; j < m; ++j)
                ep_comp[j] = ep_src[static_cast<size_t>(row_ids[j])];
        }

        // Build disc_i8 companion: li_by_orderkey_disc_i8[j] = l_discount_int8 for CSR slot j.
        // Converts scattered random access to l_discount_int8[row_ids[j]] into sequential reads.
        {
            auto& disc_comp = db->li_by_orderkey_disc_i8;
            disc_comp.resize(m);
            const int8_t* disc_src = db->lineitem.l_discount_int8.data();
            for (size_t j = 0; j < m; ++j)
                disc_comp[j] = disc_src[static_cast<size_t>(row_ids[j])];
        }

        // Build shipdate companion: li_by_orderkey_shipdate[j] = l_shipdate for CSR slot j.
        // Converts scattered random access to l_shipdate[row_ids[j]] (used in Q3 binary search)
        // into sequential reads within each order's CSR block.
        {
            auto& sd_comp = db->li_by_orderkey_shipdate;
            sd_comp.resize(m);
            const int32_t* sd_src = db->lineitem.l_shipdate.data();
            for (size_t j = 0; j < m; ++j)
                sd_comp[j] = sd_src[static_cast<size_t>(row_ids[j])];
        }

        // Build precomputed revenue companion: li_by_orderkey_rev_i32[j] = ep_i32 * (100 - disc_i8).
        // Eliminates the multiply in Q3's inner revenue loop and reduces memory bandwidth:
        // 4B vs 5B (4B ep + 1B disc) per slot — same width as the shipdate companion array.
        // Max per-slot value: 10,495,000 * 100 = 1,049,500,000 < INT32_MAX (2,147,483,647).
        {
            PROFILE_SCOPE("build_li_by_orderkey_rev_i32");
            auto& rev_comp = db->li_by_orderkey_rev_i32;
            rev_comp.resize(m);
            const int32_t* ep_src   = db->lineitem.l_extendedprice_int32.data();
            const int8_t*  disc_src = db->lineitem.l_discount_int8.data();
            for (size_t j = 0; j < m; ++j) {
                size_t r = static_cast<size_t>(row_ids[j]);
                rev_comp[j] = static_cast<int32_t>(
                    static_cast<int64_t>(ep_src[r]) * (100LL - static_cast<int64_t>(disc_src[r])));
            }
        }

        // Build returnflag companion: li_by_orderkey_returnflag[j] = l_returnflag for CSR slot j.
        // Converts scattered reads of l_returnflag[row_ids[j]] (random access into 300MB array
        // at SF=50) into sequential streaming reads of a compact companion array.
        // General-purpose: any query filtering by returnflag via the orderkey CSR benefits.
        {
            PROFILE_SCOPE("build_li_by_orderkey_returnflag");
            auto& rf_comp = db->li_by_orderkey_returnflag;
            rf_comp.resize(m);
            const uint8_t* rf_src = db->lineitem.l_returnflag.data();
            for (size_t j = 0; j < m; ++j)
                rf_comp[j] = rf_src[static_cast<size_t>(row_ids[j])];
        }
    });
    auto t6 = std::thread([&]{
        int64_t max_pk = *std::max_element(db->lineitem.l_partkey.begin(), db->lineitem.l_partkey.end());
        db->li_by_partkey = build_csr_from_col(db->lineitem.l_partkey, max_pk);
    });
    auto t7 = std::thread([&]{
        int64_t max_pk = *std::max_element(db->partsupp.ps_partkey.begin(), db->partsupp.ps_partkey.end());
        db->ps_by_partkey = build_csr_from_col(db->partsupp.ps_partkey, max_pk);
    });
    auto t8 = std::thread([&]{
        int64_t max_ck = *std::max_element(db->orders.o_custkey.begin(), db->orders.o_custkey.end());
        db->ord_by_custkey = build_csr_from_col(db->orders.o_custkey, max_ck);
    });
    // part_by_size: general CSR index on p_size (values 1..50 in TPC-H)
    // Allows skipping the full 4M part scan when filtering by p_size
    auto t9 = std::thread([&]{
        PROFILE_SCOPE("build_part_by_size");
        const auto& pt = db->part;
        int32_t max_sz = 0;
        for (int32_t v : pt.p_size) if (v > max_sz) max_sz = v;
        size_t sz = static_cast<size_t>(max_sz + 2);
        CsrIndex& csr = db->part_by_size;
        csr.offsets.assign(sz, 0);
        int32_t n = static_cast<int32_t>(pt.p_size.size());
        csr.row_ids.resize(static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i)
            ++csr.offsets[static_cast<size_t>(pt.p_size[static_cast<size_t>(i)]) + 1];
        for (size_t i = 1; i < sz; ++i)
            csr.offsets[i] += csr.offsets[i - 1];
        std::vector<int32_t> pos(csr.offsets.begin(), csr.offsets.begin() + static_cast<ptrdiff_t>(sz - 1));
        for (int32_t i = 0; i < n; ++i) {
            size_t b = static_cast<size_t>(pt.p_size[static_cast<size_t>(i)]);
            csr.row_ids[static_cast<size_t>(pos[b]++)] = i;
        }
        // Build companion arrays: sequential copies of type_enum, partkey_i32, mfgr
        // in part_by_size CSR order.  Converts random part-table reads in the Q2
        // hot loop into cache-friendly sequential streaming reads.
        size_t total = static_cast<size_t>(n);
        db->psz_type_enum.resize(total);
        db->psz_partkey_i32.resize(total);
        db->psz_brand.resize(total);
        const int32_t* __restrict__ row_ids = csr.row_ids.data();
        for (size_t j = 0; j < total; ++j) {
            size_t pi = static_cast<size_t>(row_ids[j]);
            db->psz_type_enum[j]   = pt.p_type_enum[pi];
            db->psz_partkey_i32[j] = static_cast<int32_t>(pt.p_partkey[pi]);
            db->psz_brand[j]       = pt.p_brand[pi];
        }
    });
    // part_by_brand_container: general CSR index on (p_brand * 256 + p_container)
    // Allows O(matching_parts) lookup by brand+container without full 4M scan.
    auto t10 = std::thread([&]{
        const auto& pt = db->part;
        int32_t n = static_cast<int32_t>(pt.num_rows);
        // Key space: brand in [0,24], container in [0,39] at SF=1.
        // Using brand * 256 + container as composite key (fits in uint16_t).
        // Find max composite key to size the CSR.
        int32_t max_key = 0;
        for (int32_t i = 0; i < n; ++i) {
            int32_t key = static_cast<int32_t>(pt.p_brand[static_cast<size_t>(i)]) * 256
                        + static_cast<int32_t>(pt.p_container[static_cast<size_t>(i)]);
            if (key > max_key) max_key = key;
        }
        size_t sz = static_cast<size_t>(max_key + 2);
        CsrIndex& csr = db->part_by_brand_container;
        csr.offsets.assign(sz, 0);
        csr.row_ids.resize(static_cast<size_t>(n));
        for (int32_t i = 0; i < n; ++i) {
            int32_t key = static_cast<int32_t>(pt.p_brand[static_cast<size_t>(i)]) * 256
                        + static_cast<int32_t>(pt.p_container[static_cast<size_t>(i)]);
            ++csr.offsets[static_cast<size_t>(key) + 1];
        }
        for (size_t i = 1; i < sz; ++i)
            csr.offsets[i] += csr.offsets[i - 1];
        std::vector<int32_t> pos(csr.offsets.begin(), csr.offsets.begin() + static_cast<ptrdiff_t>(sz - 1));
        for (int32_t i = 0; i < n; ++i) {
            int32_t key = static_cast<int32_t>(pt.p_brand[static_cast<size_t>(i)]) * 256
                        + static_cast<int32_t>(pt.p_container[static_cast<size_t>(i)]);
            csr.row_ids[static_cast<size_t>(pos[static_cast<size_t>(key)]++)] = i;
        }
    });
    t1.join(); t2.join(); t3.join(); t4.join();
    t5.join(); t6.join(); t7.join(); t8.join();
    t9.join(); t10.join();

    // ── Pre-compute c_has_order (per-customer boolean) ────────────────────────
    // Denormalized flag: c_has_order[i] = 1 if customer i has >= 1 order.
    // Uses the already-built ord_by_custkey CSR (offsets array only).
    // Eliminates the ad-hoc has_order bitmap construction done in Q22 (and any
    // other NOT EXISTS orders query), replacing it with a simple sequential scan
    // of c_has_order[] and eliminating c_custkey[] reads during that scan.
    //
    // Memory: num_customer bytes (3 MB at SF=20).
    // Build cost: O(num_customer) sequential lookups into the CSR offsets array.
    //   The CSR offsets array is 1.8 MB at SF=20 and is accessed with stride
    //   ~1 (because customer is sorted by custkey), so this is cache-friendly.
    //
    // General-purpose: any query checking NOT EXISTS orders per customer can
    // use c_has_order directly instead of building its own bitmap at query time.
    {
        PROFILE_SCOPE("build_c_has_order");
        CustomerTable& cust      = db->customer;
        const CsrIndex& ord_csr  = db->ord_by_custkey;
        const size_t nc          = static_cast<size_t>(cust.num_rows);
        const size_t oc_sz       = ord_csr.offsets.size();
        cust.c_has_order.resize(nc, 0);
        const int64_t*  __restrict__ ck_ptr  = cust.c_custkey.data();
        const int32_t*  __restrict__ offsets = ord_csr.offsets.data();
        uint8_t*        __restrict__ ho      = cust.c_has_order.data();
        for (size_t i = 0; i < nc; ++i) {
            const int64_t ck = ck_ptr[i];
            const size_t  w  = static_cast<size_t>(ck);
            ho[i] = (w + 1 < oc_sz && offsets[w] < offsets[w + 1]) ? 1u : 0u;
        }
    }

    // ── Build cust_by_phone_cc CSR + companion arrays ─────────────────────────
    // Groups customer rows by their 2-char phone country code (c_phone_cc_u16).
    // Companion arrays cust_by_pcc_bal and cust_by_pcc_ho store c_acctbal and
    // c_has_order in CSR order (no random gather needed during Q22).
    //
    // This allows Q22 to iterate only the ~840K code-matching rows (for 7 codes
    // out of ~25) instead of scanning all 3M, reducing memory access from 54 MB
    // to ~11 MB (row_ids + bal + ho for matching rows only).
    //
    // General-purpose: any query filtering on c_phone country code benefits.
    {
        PROFILE_SCOPE("build_cust_by_phone_cc");
        const CustomerTable& cust = db->customer;
        const size_t nc           = static_cast<size_t>(cust.num_rows);

        // Determine max key value for offset array sizing.
        uint16_t max_key = 0;
        for (size_t i = 0; i < nc; ++i) {
            uint16_t k = cust.c_phone_cc_u16[i];
            if (k > max_key) max_key = k;
        }
        const size_t sz = static_cast<size_t>(max_key) + 2;

        CsrIndex& csr = db->cust_by_phone_cc;
        csr.offsets.assign(sz, 0);
        csr.row_ids.resize(nc);

        // Count occurrences per phone code.
        const uint16_t* __restrict__ cc_arr = cust.c_phone_cc_u16.data();
        for (size_t i = 0; i < nc; ++i)
            ++csr.offsets[static_cast<size_t>(cc_arr[i]) + 1];

        // Prefix sum.
        for (size_t i = 1; i < sz; ++i)
            csr.offsets[i] += csr.offsets[i - 1];

        // Scatter row indices into CSR order.
        std::vector<int32_t> pos(csr.offsets.begin(),
                                 csr.offsets.begin() + static_cast<ptrdiff_t>(sz - 1));
        for (size_t i = 0; i < nc; ++i) {
            const size_t b = static_cast<size_t>(cc_arr[i]);
            csr.row_ids[static_cast<size_t>(pos[b]++)] = static_cast<int32_t>(i);
        }

        // Build companion arrays: c_acctbal and c_has_order in CSR order.
        // After this, the Q22 aggregation loop can iterate:
        //   for j in [offsets[code] .. offsets[code+1]): read bal[j], ho[j]
        // with fully sequential (cache-friendly) access.
        db->cust_by_pcc_bal.resize(nc);
        db->cust_by_pcc_ho.resize(nc);
        const double*   __restrict__ bal_src = cust.c_acctbal.data();
        const uint8_t*  __restrict__ ho_src  = cust.c_has_order.data();
        double*         __restrict__ bal_dst = db->cust_by_pcc_bal.data();
        uint8_t*        __restrict__ ho_dst  = db->cust_by_pcc_ho.data();
        const int32_t*  __restrict__ rids    = csr.row_ids.data();
        for (size_t j = 0; j < nc; ++j) {
            const size_t r = static_cast<size_t>(rids[j]);
            bal_dst[j] = bal_src[r];
            ho_dst[j]  = ho_src[r];
        }
    }

    // ── Build ps_by_suppkey CSR + companion arrays (Q11 fast-path) ───────────
    // Indexes partsupp rows by suppkey so Q11 can iterate only nation-matching
    // supplier rows (~640K at SF=20) instead of all 16M rows (256 MB → ~5 MB).
    // ps_bysuppkey_value_i64[j] = cost_cents * availqty per slot (per-row, not aggregated).
    {
        PROFILE_SCOPE("build_ps_by_suppkey");
        const PartsuppTable& ps = db->partsupp;
        const size_t n = static_cast<size_t>(ps.num_rows);

        // Determine max suppkey for offsets array sizing.
        int32_t max_sk = 0;
        for (size_t i = 0; i < n; ++i) {
            int32_t sk = ps.ps_suppkey_i32[i];
            if (sk > max_sk) max_sk = sk;
        }
        const size_t offsets_sz = static_cast<size_t>(max_sk) + 2;

        CsrIndex& csr = db->ps_by_suppkey;
        csr.offsets.assign(offsets_sz, 0);
        csr.row_ids.resize(n);

        // Count pass: histogram by suppkey
        for (size_t i = 0; i < n; ++i)
            ++csr.offsets[static_cast<size_t>(ps.ps_suppkey_i32[i]) + 1];
        // Prefix-sum to get start offsets
        for (size_t i = 1; i < offsets_sz; ++i)
            csr.offsets[i] += csr.offsets[i - 1];
        // Scatter pass: fill row_ids in suppkey order
        {
            std::vector<int32_t> pos(csr.offsets.begin(),
                                     csr.offsets.begin() + static_cast<ptrdiff_t>(offsets_sz - 1));
            for (size_t i = 0; i < n; ++i) {
                size_t sk = static_cast<size_t>(ps.ps_suppkey_i32[i]);
                csr.row_ids[static_cast<size_t>(pos[sk]++)] =
                    static_cast<int32_t>(i);
            }
        }

        // Build AoS packed slots in CSR slot order for Q11 hot loop.
        // ps_bysuppkey_slots[j] = {ps_partkey_i32[row_ids[j]], cost_cents*availqty}.
        // 8B per slot, 8 rows per 64B cache line.
        db->ps_bysuppkey_slots.resize(n);

        const int32_t* __restrict__ pk_src    = ps.ps_partkey_i32.data();
        const int32_t* __restrict__ cents_src = ps.ps_supplycost_cents.data();
        const int32_t* __restrict__ qty_src   = ps.ps_availqty.data();
        const int32_t* __restrict__ row_ids   = csr.row_ids.data();
        Database::PsBySuppkeySlot* __restrict__ out_slots = db->ps_bysuppkey_slots.data();

        for (size_t j = 0; j < n; ++j) {
            const size_t r = static_cast<size_t>(row_ids[j]);
            out_slots[j] = {pk_src[r], cents_src[r] * qty_src[r]};
        }
    }

    // Build ps_by_partkey companion arrays for Q2 (and any query joining partsupp+supplier via partkey CSR).
    // For each slot j in ps_by_partkey.row_ids, pre-join:
    //   ps_by_pk_suppnk[j] = s_nationkey of the supplier (uint8_t, 0xFF=invalid)
    //   ps_by_pk_sr[j]     = supplier row index in supplier table (-1=invalid)
    //   ps_by_pk_cost[j]   = ps_supplycost (double)
    // This eliminates the scattered random-access chain:
    //   ps_row → ps_suppkey → supp_row_by_suppkey[suppkey] → s_nationkey
    // and replaces it with sequential reads of compact companion arrays.
    {
        PROFILE_SCOPE("build_ps_by_pk_companions");
        const CsrIndex& csr         = db->ps_by_partkey;
        const PartsuppTable& ps     = db->partsupp;
        const SupplierTable& sup    = db->supplier;
        const std::vector<int32_t>& supp_by_sk = db->supp_row_by_suppkey;
        size_t supp_by_sk_sz = supp_by_sk.size();

        const size_t m = csr.row_ids.size();
        db->ps_by_pk_suppnk.resize(m, 0xFF);
        db->ps_by_pk_sr.resize(m, -1);
        db->ps_by_pk_cost.resize(m, 0.0);
        db->ps_by_pk_suppkey_i32.resize(m, 0);
        db->ps_by_pk_packed.resize(m, {0xFF, 0, 0, 0, -1, 0.0});

        const int32_t* __restrict__ row_ids  = csr.row_ids.data();
        const int64_t* __restrict__ suppkeys = ps.ps_suppkey.data();
        const double*  __restrict__ costs    = ps.ps_supplycost.data();
        const int32_t* __restrict__ nkeys    = sup.s_nationkey.data();
        uint8_t*  __restrict__ out_nk   = db->ps_by_pk_suppnk.data();
        int32_t*  __restrict__ out_sr   = db->ps_by_pk_sr.data();
        double*   __restrict__ out_cost = db->ps_by_pk_cost.data();
        int32_t*  __restrict__ out_sk   = db->ps_by_pk_suppkey_i32.data();
        Database::PsByPkPacked* __restrict__ out_packed = db->ps_by_pk_packed.data();

        for (size_t j = 0; j < m; ++j) {
            size_t ps_row   = static_cast<size_t>(row_ids[j]);
            int64_t suppkey = suppkeys[ps_row];
            double  cost    = costs[ps_row];
            out_cost[j] = cost;
            out_sk[j]   = static_cast<int32_t>(suppkey);
            out_packed[j].cost = cost;  // cost always valid
            if (suppkey >= 0 && static_cast<size_t>(suppkey) < supp_by_sk_sz) {
                int32_t sr = supp_by_sk[static_cast<size_t>(suppkey)];
                out_sr[j] = sr;
                if (sr >= 0) {
                    out_nk[j] = static_cast<uint8_t>(nkeys[static_cast<size_t>(sr)]);
                    out_packed[j].suppnk = static_cast<uint8_t>(nkeys[static_cast<size_t>(sr)]);
                    out_packed[j].sr     = sr;
                }
            }
        }
    }

    // Build psz_ps_start/end: pre-joined ps_by_partkey offsets for part_by_size slots.
    // For each slot j in part_by_size CSR (indexed by szi):
    //   psz_ps_start[j] = ps_by_partkey.offsets[partkey]
    //   psz_ps_end[j]   = ps_by_partkey.offsets[partkey + 1]
    // Eliminates random access into the 16MB ps_by_partkey.offsets array for Q2's
    // outer loop (500 random reads replaced by a sequential pre-joined array).
    // General-purpose: any query iterating part_by_size and joining partsupp can use this.
    {
        PROFILE_SCOPE("build_psz_ps_offsets");
        const CsrIndex& ps_csr = db->ps_by_partkey;
        const CsrIndex& sz_csr = db->part_by_size;
        size_t total = sz_csr.row_ids.size();
        db->psz_ps_start.resize(total, 0);
        db->psz_ps_end.resize(total, 0);

        const int32_t* __restrict__ pk_arr     = db->psz_partkey_i32.data();
        int32_t*       __restrict__ out_start  = db->psz_ps_start.data();
        int32_t*       __restrict__ out_end    = db->psz_ps_end.data();
        const int32_t* __restrict__ ps_offsets = ps_csr.offsets.data();
        int32_t ps_off_sz = static_cast<int32_t>(ps_csr.offsets.size());

        for (size_t j = 0; j < total; ++j) {
            int32_t pk = pk_arr[j];
            if (pk > 0 && pk + 1 < ps_off_sz) {
                out_start[j] = ps_offsets[static_cast<size_t>(pk)];
                out_end[j]   = ps_offsets[static_cast<size_t>(pk) + 1];
            }
        }
    }

    // Build dense per-order CSR slice arrays after li_by_orderkey is ready.
    // This replaces the huge sparse offsets[orderkey] lookup (up to 480 MB at
    // SF=20) with a dense sequential array over order rows (120 MB at SF=20),
    // dramatically improving cache behaviour in the Q4 date-range scan.
    {
        const auto& ok_vec  = db->orders.o_orderkey;
        const auto& offsets = db->li_by_orderkey.offsets;
        size_t n = ok_vec.size();
        size_t offsets_sz = offsets.size();

        auto& lo_vec = db->orders.o_li_csr_lo;
        auto& hi_vec = db->orders.o_li_csr_hi;
        lo_vec.resize(n);
        hi_vec.resize(n);

        for (size_t i = 0; i < n; ++i) {
            int64_t okey = ok_vec[i];
            if (okey < 0) {
                lo_vec[i] = 0;
                hi_vec[i] = 0;
            } else {
                size_t ok = static_cast<size_t>(okey);
                lo_vec[i] = (ok + 1 < offsets_sz) ? offsets[ok]     : 0;
                hi_vec[i] = (ok + 1 < offsets_sz) ? offsets[ok + 1] : 0;
            }
        }

        // Build compact per-order lineitem count (uint8_t, fits [1,7] per TPC-H spec).
        // Allows Q18's hot loop to read 5B per order (lo+count) instead of 8B (lo+hi),
        // reducing memory bandwidth by 37.5% for the outer loop at SF=20.
        auto& cnt_vec = db->orders.o_li_count;
        cnt_vec.resize(n);
        for (size_t i = 0; i < n; ++i)
            cnt_vec[i] = static_cast<uint8_t>(hi_vec[i] - lo_vec[i]);

        // Build flat per-order qty array for sequential + SIMD access in Q18.
        // For each order row i (in o_orderdate-sorted order), lay out 8 bytes:
        //   [0..cnt-1] = individual qty values (uint8_t each, 1-50)
        //   [cnt..7] = 0 padding
        // No count byte: horizontal byte-sum of all 8 bytes = correct total.
        // Eliminates 30M cache-scattered reads into 120MB li_by_orderkey_qty.
        // AVX2 _mm256_sad_epu8 processes 4 orders per 32-byte load.
        {
            PROFILE_SCOPE("build_o_li_qty_flat");
            const uint8_t* __restrict__ qty_csr = db->li_by_orderkey_qty.data();
            auto& flat_vec = db->orders.o_li_qty_flat;
            flat_vec.assign(n * 8, 0);  // zero-initialise (padding bytes must be 0)
            uint8_t* __restrict__ flat = flat_vec.data();

            for (size_t i = 0; i < n; ++i) {
                const int32_t lo  = lo_vec[i];
                const uint8_t cnt = cnt_vec[i];
                uint8_t* __restrict__ slot = flat + i * 8;
                // No count byte — qty values at [0..cnt-1], zeros at [cnt..7].
                // Horizontal byte-sum of all 8 bytes gives correct total (padding=0).
                // Also compatible with AVX2 _mm256_sad_epu8 (4 orders per 32-byte load).
                const uint8_t* __restrict__ src = qty_csr + lo;
                // Unrolled copy for cnt=1..7 (TPC-H guarantee)
                if (cnt >= 1) slot[0] = src[0];
                if (cnt >= 2) slot[1] = src[1];
                if (cnt >= 3) slot[2] = src[2];
                if (cnt >= 4) slot[3] = src[3];
                if (cnt >= 5) slot[4] = src[4];
                if (cnt >= 6) slot[5] = src[5];
                if (cnt >= 7) slot[6] = src[6];
                // bytes [cnt..7] remain 0 from assign()
            }
            TRACE_COUNT("build_o_li_qty_flat_orders", static_cast<long long>(n));
        }

        // Build per-order EXISTS(l_commitdate < l_receiptdate) flag array.
        // o_has_clt_rd[i] = 1 iff any lineitem of order i has commitdate < receiptdate.
        // Pre-computing this collapses Q4's inner CSR scan (avg 4 bytes/order) into
        // a single sequential byte load per order, reducing memory traffic ~4x.
        // Also build the packed bitset version for maximum cache efficiency:
        // 3-month window at SF=20 ≈ 375K orders → ~47KB bits → fits in L1/L2 cache.
        {
            PROFILE_SCOPE("build_o_has_clt_rd");
            const uint8_t* __restrict__ clt_data = db->li_by_orderkey_clt_rd.data();

            auto& has_vec = db->orders.o_has_clt_rd;
            has_vec.resize(n, 0);

            for (size_t i = 0; i < n; ++i) {
                int32_t lo = lo_vec[i];
                int32_t hi = hi_vec[i];
                // Scan CSR slice for any nonzero (commitdate < receiptdate) flag.
                // Most orders (avg 4 items) fall in the ≤8 fast path.
                uint8_t found = 0;
                const uint8_t* p = clt_data + lo;
                int32_t len = hi - lo;
                if (len > 0) {
                    if (__builtin_expect(len <= 8, 1)) {
                        for (int32_t t = 0; t < len; ++t) found |= p[t];
                    } else {
                        uint64_t acc64 = 0;
                        const uint8_t* end = p + len;
                        const uint8_t* q = p;
                        for (; q + 8 <= end; q += 8) {
                            uint64_t w; memcpy(&w, q, 8);
                            acc64 |= w;
                            if (__builtin_expect(acc64 != 0, 0)) break;
                        }
                        if (acc64 == 0) for (; q < end; ++q) acc64 |= *q;
                        found = (acc64 != 0) ? 1 : 0;
                    }
                }
                has_vec[i] = found;
            }

            // Build packed bitset: word w = i/64, bit = i%64.
            // o_has_clt_rd_bits[w] |= (1ULL << (i & 63)) when has_vec[i] == 1.
            size_t nwords = (n + 63) / 64;
            auto& bits_vec = db->orders.o_has_clt_rd_bits;
            bits_vec.assign(nwords, 0ULL);
            for (size_t i = 0; i < n; ++i) {
                if (has_vec[i])
                    bits_vec[i >> 6] |= (1ULL << (i & 63));
            }
            TRACE_COUNT("build_o_has_clt_rd_orders", static_cast<long long>(n));
        }

        // Build per-order multi-suppkey flag: o_has_multi_suppkey[i] = 1 if order i
        // has at least two lineitems with different suppkeys.
        // Pre-computes the EXISTS subquery of Q21, eliminating Scan-1 (the inner loop
        // that checks for a second distinct suppkey) by replacing it with a single byte
        // load per order.  Built here after li_by_orderkey_suppkey_i32 is available.
        {
            PROFILE_SCOPE("build_o_has_multi_suppkey");
            const int32_t* __restrict__ sk_csr = db->li_by_orderkey_suppkey_i32.data();
            auto& ms_vec = db->orders.o_has_multi_suppkey;
            ms_vec.resize(n, 0);

            for (size_t i = 0; i < n; ++i) {
                const int32_t lo  = lo_vec[i];
                const int32_t hi  = hi_vec[i];
                if (hi - lo < 2) {
                    ms_vec[i] = 0;
                    continue;
                }
                // Check if any suppkey differs from the first one.
                const int32_t fsk = sk_csr[static_cast<size_t>(lo)];
                uint8_t multi = 0;
                for (int32_t t = lo + 1; t < hi; ++t) {
                    if (sk_csr[static_cast<size_t>(t)] != fsk) {
                        multi = 1;
                        break;
                    }
                }
                ms_vec[i] = multi;
            }
            TRACE_COUNT("build_o_has_multi_suppkey_orders", static_cast<long long>(n));
        }

        // Build per-order unique-late-suppkey:
        //   o_late_unique_suppkey[i] = -1 if no late entries
        //                            =  0 if 2+ distinct late suppkeys
        //                            = sk if exactly one distinct late suppkey = sk
        // Pre-computes Q21's NOT-EXISTS subquery per order, eliminating the ~40M CSR
        // reads in the inner loop at SF=20 and replacing them with 30M int32 reads.
        {
            PROFILE_SCOPE("build_o_late_unique_suppkey");
            const int32_t* __restrict__ sk_csr  = db->li_by_orderkey_suppkey_i32.data();
            const uint8_t* __restrict__ lat_csr = db->li_by_orderkey_clt_rd.data();
            auto& lus_vec = db->orders.o_late_unique_suppkey;
            lus_vec.resize(n);

            for (size_t i = 0; i < n; ++i) {
                const int32_t lo = lo_vec[i];
                const int32_t hi = hi_vec[i];
                int32_t lone_sk = -1;   // -1 = no late entry seen
                bool    multi   = false;
                for (int32_t t = lo; t < hi; ++t) {
                    if (lat_csr[static_cast<size_t>(t)]) {
                        const int32_t sk = sk_csr[static_cast<size_t>(t)];
                        if (lone_sk == -1) {
                            lone_sk = sk;
                        } else if (sk != lone_sk) {
                            multi = true;
                            break;
                        }
                    }
                }
                // Encode: -1 = none, 0 = multiple, sk = unique
                lus_vec[i] = multi ? 0 : lone_sk;
            }
            TRACE_COUNT("build_o_late_unique_suppkey_orders", static_cast<long long>(n));
        }
    }
}

// Build companion arrays for li_by_partkey CSR (Q9 optimisation).
// For each slot j in li_by_partkey.row_ids, pre-join:
//   li_by_pk_suppkey_i32[j] = l_suppkey as int32
//   li_by_pk_year[j]        = year of the order (extracted from o_orderdate via orderkey)
//   li_by_pk_ep[j]          = l_extendedprice (float for compact storage)
//   li_by_pk_disc[j]        = l_discount (float)
//   li_by_pk_qty[j]         = l_quantity (float)
// This converts the scattered random-access reads in Q9's inner loop
// (which must chase: CSR row_id → l_suppkey / l_orderkey → orders table → o_orderdate)
// into sequential streaming reads of compact companion arrays.
// Must be called after build_acceleration() (which builds li_by_partkey and
// orders_row_by_orderkey).
static void build_li_by_pk_companions(Database* db) {
    PROFILE_SCOPE("build_li_by_pk_companions");

    // ── days_to_year: extract year from days-since-epoch ─────────────────
    auto days_to_year = [](int32_t days) -> int16_t {
        int32_t z   = days + 719468;
        int32_t era = (z >= 0 ? z : z - 146096) / 146097;
        int32_t doe = z - era * 146097;
        int32_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        int32_t y   = yoe + era * 400;
        int32_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        int32_t mp  = (5 * doy + 2) / 153;
        int32_t mon = mp < 10 ? mp + 3 : mp - 9;
        if (mon <= 2) y += 1;
        return static_cast<int16_t>(y);
    };

    // ── Build compact orderkey→year array (dense iteration over order rows) ─
    const OrdersTable& ord     = db->orders;
    const size_t ord_map_sz    = db->orders_row_by_orderkey.size();
    std::vector<int16_t> ok_to_year(ord_map_sz, 0);
    {
        const int64_t* ok_ptr   = ord.o_orderkey.data();
        const int32_t* date_ptr = ord.o_orderdate.data();
        int64_t n = ord.num_rows;
        for (int64_t i = 0; i < n; ++i) {
            int64_t ok = ok_ptr[static_cast<size_t>(i)];
            if (ok >= 0 && static_cast<size_t>(ok) < ord_map_sz) {
                ok_to_year[static_cast<size_t>(ok)] =
                    days_to_year(date_ptr[static_cast<size_t>(i)]);
            }
        }
    }

    const CsrIndex&      csr     = db->li_by_partkey;
    const LineitemTable& li      = db->lineitem;
    const size_t         m       = csr.row_ids.size();

    db->li_by_pk_suppkey_i32.resize(m);
    db->li_by_pk_year.resize(m);
    db->li_by_pk_ep.resize(m);
    db->li_by_pk_nation_u16.resize(m);
    db->li_by_pk_disc.resize(m);
    db->li_by_pk_sm_si.resize(m);
    db->li_by_pk_qty.resize(m);
    db->li_by_pk_shipdate_i16.resize(m);
    db->li_by_pk_qty_i8.resize(m);
    db->li_by_pk_cost.resize(m, 0.0f);
    db->li_by_pk_suppnk.resize(m, 0xFFu);
    db->li_by_pk_amount.resize(m, 0.0);
    db->li_by_pk_snk_yr.resize(m, 0xFFFFu);

    const int32_t* __restrict__ row_ids  = csr.row_ids.data();
    const int32_t* __restrict__ sk_src   = li.l_suppkey_i32.data();
    const int64_t* __restrict__ ok_src   = li.l_orderkey.data();
    const int32_t* __restrict__ sd_src   = li.l_shipdate.data();
    const double*  __restrict__ ep_src   = li.l_extendedprice.data();
    const double*  __restrict__ disc_src = li.l_discount.data();
    const double*  __restrict__ qty_src  = li.l_quantity.data();
    const uint8_t* __restrict__ sm_src   = li.l_shipmode.data();
    const uint8_t* __restrict__ si_src   = li.l_shipinstruct.data();
    const uint16_t* __restrict__ nk16_src = li.li_suppnk_custnk_u16.data();
    int32_t* __restrict__ out_sk   = db->li_by_pk_suppkey_i32.data();
    int16_t* __restrict__ out_yr   = db->li_by_pk_year.data();
    float*   __restrict__ out_ep   = db->li_by_pk_ep.data();
    float*   __restrict__ out_disc = db->li_by_pk_disc.data();
    float*   __restrict__ out_qty  = db->li_by_pk_qty.data();
    int16_t* __restrict__ out_sd16 = db->li_by_pk_shipdate_i16.data();
    uint8_t* __restrict__ out_qty8 = db->li_by_pk_qty_i8.data();
    const int16_t* __restrict__ yr_map  = ok_to_year.data();
    const size_t ok_map_sz = ord_map_sz;
    uint16_t* __restrict__ out_nk16 = db->li_by_pk_nation_u16.data();
    float*   __restrict__ out_cost   = db->li_by_pk_cost.data();
    uint8_t* __restrict__ out_suppnk = db->li_by_pk_suppnk.data();
    double*   __restrict__ out_amount = db->li_by_pk_amount.data();
    uint8_t* __restrict__ out_sm_si  = db->li_by_pk_sm_si.data();
    uint16_t* __restrict__ out_snk_yr = db->li_by_pk_snk_yr.data();

    // ── Build ps (partkey,suppkey)→cost lookup using ps_by_partkey CSR ─────
    const CsrIndex&       ps_csr   = db->ps_by_partkey;
    const int32_t* __restrict__ ps_off   = ps_csr.offsets.data();
    const int32_t* __restrict__ ps_sk    = db->ps_by_pk_suppkey_i32.data();
    const double*  __restrict__ ps_cost_src = db->ps_by_pk_cost.data();
    const size_t   ps_off_sz              = ps_csr.offsets.size();

    // Build suppkey_to_suppnk lookup from supplier table
    const SupplierTable& supp = db->supplier;
    const size_t supp_row_sz  = db->supp_row_by_suppkey.size();
    std::vector<uint8_t> suppkey_to_nk(supp_row_sz, 0xFFu);
    {
        PROFILE_SCOPE("build_li_by_pk_companions_suppnk_map");
        for (int64_t i = 0; i < supp.num_rows; ++i) {
            int64_t sk = supp.s_suppkey[static_cast<size_t>(i)];
            int32_t nk = supp.s_nationkey[static_cast<size_t>(i)];
            if (sk >= 0 && static_cast<size_t>(sk) < supp_row_sz && nk >= 0 && nk < 25)
                suppkey_to_nk[static_cast<size_t>(sk)] = static_cast<uint8_t>(nk);
        }
    }
    const uint8_t* __restrict__ sk_to_nk    = suppkey_to_nk.data();
    const size_t                sk_to_nk_sz = supp_row_sz;

    // Iterate partkey-by-partkey so we can hoist the PS slice per partkey.
    // This converts the per-slot PS linear scan from random to sequential pattern.
    const size_t li_off_sz = csr.offsets.size();
    const size_t max_pk    = li_off_sz > 0 ? li_off_sz - 1 : 0;

    {
        PROFILE_SCOPE("build_li_by_pk_companions_main_loop");
        for (size_t pk = 0; pk < max_pk; ++pk) {
            const int32_t li_start = csr.offsets[pk];
            const int32_t li_end   = csr.offsets[pk + 1];
            if (li_start == li_end) continue;

            // PS slice for this partkey (typically 4 entries in TPC-H)
            int32_t ps_start = 0, ps_end = 0;
            if (pk + 1 < ps_off_sz) {
                ps_start = ps_off[pk];
                ps_end   = ps_off[pk + 1];
            }

            for (int32_t j = li_start; j < li_end; ++j) {
                const size_t jj = static_cast<size_t>(j);
                const size_t lr = static_cast<size_t>(row_ids[jj]);
                const int64_t ok      = ok_src[lr];
                const int32_t suppkey = sk_src[lr];

                out_sk[jj]   = suppkey;
                out_yr[jj]   = (ok >= 0 && static_cast<size_t>(ok) < ok_map_sz)
                               ? yr_map[static_cast<size_t>(ok)] : 0;
                out_ep[jj]   = static_cast<float>(ep_src[lr]);
                out_disc[jj] = static_cast<float>(disc_src[lr]);
                out_nk16[jj] = nk16_src[lr];
                // Pack shipmode (low 4 bits) and shipinstruct (high 4 bits) into one byte
                out_sm_si[jj] = static_cast<uint8_t>((sm_src[lr] & 0x0Fu) | ((si_src[lr] & 0x0Fu) << 4));
                out_qty[jj]  = static_cast<float>(qty_src[lr]);
                // New sequential companion arrays for Q20 shipdate-range + qty accumulation
                const int32_t raw_sd = sd_src[lr];
                out_sd16[jj] = static_cast<int16_t>(raw_sd - Database::LI_PK_DATE_BASE);
                out_qty8[jj] = static_cast<uint8_t>(static_cast<int32_t>(qty_src[lr]));

                // Pre-join ps_supplycost: scan PS slice (≤4 entries) for matching suppkey
                float cost = 0.0f;
                for (int32_t pi = ps_start; pi < ps_end; ++pi) {
                    if (ps_sk[static_cast<size_t>(pi)] == suppkey) {
                        cost = static_cast<float>(ps_cost_src[static_cast<size_t>(pi)]);
                        break;
                    }
                }
                out_cost[jj] = cost;

                // Pre-join supplier nationkey
                uint8_t snk = 0xFFu;
                if (suppkey >= 0 && static_cast<size_t>(suppkey) < sk_to_nk_sz)
                    snk = sk_to_nk[static_cast<size_t>(suppkey)];
                out_suppnk[jj] = snk;

                // Pre-compute profit amount: ep*(1-disc) - cost*qty
                // cost (float) was already computed above from ps lookup.
                // Use original double sources for full precision.
                const double ep_d   = ep_src[lr];
                const double disc_d = disc_src[lr];
                const double qty_d  = qty_src[lr];
                const double cost_d = static_cast<double>(cost);  // already found above
                out_amount[jj] = ep_d * (1.0 - disc_d) - cost_d * qty_d;

                // Packed (suppnk<<8)|yr_idx for fast Q9 combined validity filter
                static constexpr int16_t BASE_YEAR_CONST = 1992;
                const int16_t raw_yr = out_yr[jj];
                const int32_t yr_idx = raw_yr - BASE_YEAR_CONST;
                const uint8_t yr_u8  = (yr_idx >= 0 && yr_idx < 10)
                                       ? static_cast<uint8_t>(yr_idx) : 0xFFu;
                out_snk_yr[jj] = static_cast<uint16_t>(
                    (static_cast<uint16_t>(snk) << 8) | static_cast<uint16_t>(yr_u8));
            }
        }
    }

    TRACE_COUNT("build_li_by_pk_companions_slots", static_cast<long long>(m));
}

} // anonymous namespace

// Pre-join nation name into customer table (after both are loaded).
// Stored as a general-purpose denormalised column: any query outputting
// nation name alongside customer data avoids a per-row lookup.
static void build_customer_nation_names(Database* db) {
    const NationTable&   nat  = db->nation;
    CustomerTable&       cust = db->customer;
    const size_t nc = static_cast<size_t>(cust.num_rows);
    cust.c_nation_name.resize(nc);

    // Build nation key → name mapping (only 25 entries)
    std::array<const std::string*, 25> nmap = {};
    for (int32_t i = 0; i < nat.num_rows; ++i) {
        int32_t nk = nat.n_nationkey[static_cast<size_t>(i)];
        if (nk >= 0 && nk < 25)
            nmap[static_cast<size_t>(nk)] = &nat.n_name[static_cast<size_t>(i)];
    }

    const uint8_t* nk_u8 = cust.c_nationkey_u8.data();
    for (size_t i = 0; i < nc; ++i) {
        uint8_t nk = nk_u8[i];
        cust.c_nation_name[i] = (nk < 25 && nmap[nk]) ? *nmap[nk] : std::string{};
    }
}

// Build pre-joined customer nationkey per order row.
// o_cust_nationkey_u8[i] = c_nationkey of the customer for order row i (0xFF if unknown).
// Enables Q5 (and Q7/Q8) to read customer nationkey with sequential memory access
// instead of a random lookup into the customer table via custkey indirection.
// General-purpose: any query needing c_nationkey per order row can use this.
static void build_order_cust_nationkey(Database* db) {
    PROFILE_SCOPE("build_order_cust_nationkey");
    const OrdersTable&   ord  = db->orders;
    const CustomerTable& cust = db->customer;

    // Build custkey → nationkey_u8 lookup (dense, size = max_custkey+1)
    int64_t max_ck = cust.c_custkey.empty() ? 0 : cust.c_custkey.back();
    std::vector<uint8_t> ck_to_nk(static_cast<size_t>(max_ck + 1), 0xFF);
    {
        const uint8_t* nk_u8 = cust.c_nationkey_u8.data();
        size_t nc = static_cast<size_t>(cust.num_rows);
        for (size_t i = 0; i < nc; ++i) {
            int64_t ck = cust.c_custkey[i];
            if (ck >= 0 && ck <= max_ck)
                ck_to_nk[static_cast<size_t>(ck)] = nk_u8[i];
        }
    }

    // Scatter per-order
    size_t no = static_cast<size_t>(ord.num_rows);
    auto& out = db->orders.o_cust_nationkey_u8;
    out.resize(no);
    const int32_t* ck_i32 = ord.o_custkey_i32.data();
    size_t ck_to_nk_sz = ck_to_nk.size();
    for (size_t i = 0; i < no; ++i) {
        uint32_t ck = static_cast<uint32_t>(ck_i32[i]);
        out[i] = (ck < ck_to_nk_sz) ? ck_to_nk[ck] : static_cast<uint8_t>(0xFF);
    }
    TRACE_COUNT("build_order_cust_nationkey_rows", static_cast<long long>(no));

}

// Build li_cust_nationkey_u8 and li_suppkey_nationkey_u8.
//
// li_cust_nationkey_u8[i]  = c_nationkey for the customer of the order containing
//                            lineitem row i. Stored as uint8_t (0xFF = unknown).
//                            Eliminates the two-level indirection in Q7/Q8 hot loops.
//
// li_suppkey_nationkey_u8[i] = s_nationkey for the supplier of lineitem row i.
//                              Stored as uint8_t (0xFF = unknown).
//                              Eliminates the suppkey_pair lookup in Q7 hot loop.
//
// General-purpose: any query needing customer/supplier nationkey per lineitem row
// can use these instead of two-step indirection.
//
// Dependencies: must be called after build_acceleration() (which builds
// orders_row_by_orderkey) and build_order_cust_nationkey() (which builds
// o_cust_nationkey_u8).
static void build_li_cust_suppnk(Database* db) {
    PROFILE_SCOPE("build_li_cust_suppnk");

    const LineitemTable& li  = db->lineitem;
    const size_t n           = static_cast<size_t>(li.num_rows);

    // ── li_cust_nationkey_u8 ──────────────────────────────────────────────────
    {
        PROFILE_SCOPE("build_li_cust_nationkey_u8");
        const int32_t* orbok     = db->orders_row_by_orderkey.data();
        const size_t   orbok_sz  = db->orders_row_by_orderkey.size();
        const uint8_t* ocnk      = db->orders.o_cust_nationkey_u8.data();
        const size_t   ord_n     = static_cast<size_t>(db->orders.num_rows);
        const int64_t* ok_arr    = li.l_orderkey.data();

        auto& out = db->lineitem.li_cust_nationkey_u8;
        out.resize(n, 0xFF);

        for (size_t i = 0; i < n; ++i) {
            uint64_t ok = static_cast<uint64_t>(ok_arr[i]);
            if (__builtin_expect(ok < orbok_sz, 1)) {
                int32_t ord_row = orbok[ok];
                if (__builtin_expect(ord_row >= 0 &&
                        static_cast<size_t>(ord_row) < ord_n, 1))
                    out[i] = ocnk[static_cast<size_t>(ord_row)];
            }
        }
        TRACE_COUNT("build_li_cust_nationkey_u8_rows", static_cast<long long>(n));
    }

    // ── li_suppkey_nationkey_u8 ───────────────────────────────────────────────
    {
        PROFILE_SCOPE("build_li_suppkey_nationkey_u8");
        const SupplierTable& supp = db->supplier;
        int64_t max_sk = 0;
        for (int64_t sk : supp.s_suppkey) if (sk > max_sk) max_sk = sk;
        std::vector<uint8_t> sk_to_nk(static_cast<size_t>(max_sk + 1), 0xFF);
        for (int64_t i = 0; i < supp.num_rows; ++i)
            sk_to_nk[static_cast<size_t>(supp.s_suppkey[static_cast<size_t>(i)])] =
                static_cast<uint8_t>(supp.s_nationkey[static_cast<size_t>(i)]);

        const int32_t* sk_i32 = li.l_suppkey_i32.data();
        auto& out = db->lineitem.li_suppkey_nationkey_u8;
        out.resize(n, 0xFF);
        const size_t sk_map_sz = sk_to_nk.size();
        for (size_t i = 0; i < n; ++i) {
            uint32_t sk = static_cast<uint32_t>(sk_i32[i]);
            out[i] = (sk < sk_map_sz) ? sk_to_nk[sk] : 0xFF;
        }
        TRACE_COUNT("build_li_suppkey_nationkey_u8_rows", static_cast<long long>(n));
    }

    // ── li_suppnk_custnk_u16 ─────────────────────────────────────────────────
    // Packed uint16_t: high byte = suppnk, low byte = custnk.
    // Halves cache-line fetches for two-key nation filter in Q7/Q8 hot loop.
    {
        PROFILE_SCOPE("build_li_suppnk_custnk_u16");
        const uint8_t* suppnk_arr = db->lineitem.li_suppkey_nationkey_u8.data();
        const uint8_t* custnk_arr = db->lineitem.li_cust_nationkey_u8.data();
        auto& out = db->lineitem.li_suppnk_custnk_u16;
        out.resize(n);
        for (size_t i = 0; i < n; ++i) {
            out[i] = static_cast<uint16_t>(
                (static_cast<uint16_t>(suppnk_arr[i]) << 8) |
                 static_cast<uint16_t>(custnk_arr[i]));
        }
        TRACE_COUNT("build_li_suppnk_custnk_u16_rows", static_cast<long long>(n));
    }

    // ── li_rds_packed flags bit 2: prio_is_high ───────────────────────────────
    // Pre-join o_orderpriority into li_rds_packed.flags bit 2 to eliminate the
    // orders_row_by_orderkey random-access join in Q12's hot loop.
    // After this phase, Q12 can compute high/low counts with a pure sequential scan:
    //   prio_is_high = (flags >> 2) & 1
    // Requires: build_acceleration() (orders_row_by_orderkey), load_orders() (o_orderpriority).
    {
        PROFILE_SCOPE("build_li_rds_packed_prio");

        const OrdersTable& ord = db->orders;
        const int32_t* orbok   = db->orders_row_by_orderkey.data();
        const size_t   orbok_sz = db->orders_row_by_orderkey.size();
        const uint8_t* oprio   = ord.o_orderpriority.data();
        const size_t   ord_n   = static_cast<size_t>(ord.num_rows);

        // Determine which priority enum codes correspond to '1-URGENT' and '2-HIGH'.
        // Build a 256-entry lookup: prio_high_lut[code] = 1 if high priority, else 0.
        uint8_t prio_high_lut[256] = {};
        for (size_t pi = 0; pi < ord.orderpriority_names.size(); ++pi) {
            const std::string& name = ord.orderpriority_names[pi];
            if (name == "1-URGENT" || name == "2-HIGH")
                prio_high_lut[static_cast<uint8_t>(pi)] = 1u;
        }

        // Walk the rdate-sorted packed array and set bit 2 of flags.
        LineitemTable::RdsPackedRow* packed = db->lineitem.li_rds_packed.data();
        const size_t rds_n = db->lineitem.li_rds_packed.size();

        for (size_t k = 0; k < rds_n; ++k) {
            int32_t ok = packed[k].orderkey_i32;
            uint32_t ok_u = static_cast<uint32_t>(ok);
            uint8_t pih = 0;
            if (__builtin_expect(ok_u < orbok_sz, 1)) {
                int32_t ord_row = orbok[ok_u];
                if (__builtin_expect(ord_row >= 0 &&
                        static_cast<size_t>(ord_row) < ord_n, 1))
                    pih = prio_high_lut[oprio[static_cast<size_t>(ord_row)]];
            }
            // Set bit 2: (pih << 2) without disturbing bits 0 and 1.
            packed[k].flags = static_cast<uint8_t>(packed[k].flags | (pih << 2));
        }
        TRACE_COUNT("build_li_rds_packed_prio_rows", static_cast<long long>(rds_n));
    }

    // ── li_rds_sm_flags: compact 2-byte array for Q12 hot scan ───────────────
    // Packs (shipmode, flags) into uint16_t = (sm << 8) | flags.
    // Built AFTER the prio phase so that bit 2 of flags is already set.
    // At 2 bytes/row this is 4× more compact than li_rds_packed (8 bytes/row),
    // enabling 32 rows per cache line and reducing bandwidth ~48MB → ~12MB for
    // a typical 1-year receiptdate range at SF=20.
    {
        PROFILE_SCOPE("build_li_rds_sm_flags");
        const LineitemTable::RdsPackedRow* packed = db->lineitem.li_rds_packed.data();
        const size_t rds_n2 = db->lineitem.li_rds_packed.size();
        auto& out = db->lineitem.li_rds_sm_flags;
        out.resize(rds_n2);
        for (size_t k = 0; k < rds_n2; ++k) {
            out[k] = static_cast<uint16_t>(
                (static_cast<uint16_t>(packed[k].shipmode) << 8) |
                 static_cast<uint16_t>(packed[k].flags));
        }
        TRACE_COUNT("build_li_rds_sm_flags_rows", static_cast<long long>(rds_n2));
    }
}

// Build li_is_promo_u8 and li_q14_packed for Q14 acceleration.
//
// li_is_promo_u8[i]: 1 if l_partkey[i] maps to a part with p_type LIKE 'PROMO%', else 0.
//   Stored in l_shipdate-sorted order (same as all other lineitem columns).
//   Eliminates random-access join in Q14 hot scan: partkey → part_row → type_enum → promo check.
//
// li_q14_packed[i]: AoS record {ep_int32, dis_int8, is_promo, pad0, pad1} (8 bytes).
//   Packs all three Q14 hot-loop data items into one record for optimal cache locality.
//
// Dependencies: build_acceleration() (part_row_by_partkey), load_part() (p_type_enum),
//               load_lineitem() (l_partkey_i32, l_extendedprice_int32, l_discount_int8).
static void build_li_q14_promo(Database* db) {
    PROFILE_SCOPE("build_li_q14_promo");

    const LineitemTable& li   = db->lineitem;
    const PartTable&     part = db->part;
    const size_t n            = static_cast<size_t>(li.num_rows);

    // Step A: Build promo_enum_bits — 256-bit bitset of promo type enum IDs.
    // O(num_types) ≈ O(150), negligible cost.
    uint64_t promo_enum_bits[4] = {0, 0, 0, 0};
    {
        const std::vector<std::string>& tnames = part.type_names;
        const size_t num_types = tnames.size();
        for (size_t e = 0; e < num_types && e < 256; ++e) {
            const std::string& s = tnames[e];
            if (s.size() >= 5 &&
                s[0]=='P' && s[1]=='R' && s[2]=='O' && s[3]=='M' && s[4]=='O') {
                promo_enum_bits[e >> 6] |= (uint64_t(1) << (e & 63));
            }
        }
    }
    TRACE_COUNT("build_li_q14_promo_type_enums",
                static_cast<long long>(part.type_names.size()));

    // Step B: Build promo_by_partkey flat array — direct-indexed by partkey.
    // part_row_by_partkey maps partkey→row; p_type_enum[row] gives the enum code.
    // Reading part_row_by_partkey sequentially (4M×4B=16MB) and p_type_enum randomly
    // (4M×1B=4MB, fits L3) is cheaper than doing it per-lineitem (120M times).
    const std::vector<int32_t>& by_pk    = db->part_row_by_partkey;
    const size_t                by_pk_sz = by_pk.size();
    const uint8_t*              te       = part.p_type_enum.data();

    std::vector<uint8_t> promo_by_pk(by_pk_sz, 0);
    for (size_t pk = 0; pk < by_pk_sz; ++pk) {
        int32_t row = by_pk[pk];
        if (__builtin_expect(row < 0, 0)) continue;
        uint8_t e = te[static_cast<size_t>(row)];
        promo_by_pk[pk] = static_cast<uint8_t>((promo_enum_bits[e >> 6] >> (e & 63)) & 1u);
    }
    TRACE_COUNT("build_li_q14_promo_by_pk_sz", static_cast<long long>(by_pk_sz));

    // Step C: Build li_is_promo_u8 — per-lineitem promo flag (sequential for query).
    // Reads l_partkey_i32 sequentially (120M × 4B = 480MB), accesses promo_by_pk
    // randomly (4MB, L3-resident), writes li_is_promo_u8 sequentially (120MB).
    auto& out_promo = db->lineitem.li_is_promo_u8;
    out_promo.resize(n, 0);
    {
        const int32_t* pk_data = li.l_partkey_i32.data();
        for (size_t i = 0; i < n; ++i) {
            uint32_t pk = static_cast<uint32_t>(pk_data[i]);
            out_promo[i] = (pk < by_pk_sz) ? promo_by_pk[pk] : 0;
        }
    }
    TRACE_COUNT("build_li_q14_is_promo_rows", static_cast<long long>(n));
}

// =============================================================================
// Public interface
// =============================================================================

Database* build(ParquetTables* tables) {
    auto* db = new Database();

    // ── Parallel table loading ───────────────────────────────────────────────
    // All 8 tables are independent during load (no cross-table deps).
    // Group by estimated wall-clock cost to balance 4 threads:
    //   Thread A: lineitem  (~120M rows at SF=20)
    //   Thread B: orders    (~30M rows) + region + nation
    //   Thread C: partsupp  (~16M rows) + supplier
    //   Thread D: customer  (~3M rows)  + part   (~4M rows)
    std::exception_ptr ex_a, ex_b, ex_c, ex_d;
    auto thread_a = std::thread([&]() noexcept {
        try {
            load_lineitem(tables->lineitem.get(), db->lineitem);
        }
        catch (...) { ex_a = std::current_exception(); }
    });
    auto thread_b = std::thread([&]() noexcept {
        try {
            load_region(tables->region.get(), db->region);
            load_nation(tables->nation.get(), db->nation);
            load_orders(tables->orders.get(), db->orders);
        } catch (...) { ex_b = std::current_exception(); }
    });
    auto thread_c = std::thread([&]() noexcept {
        try {
            load_supplier(tables->supplier.get(), db->supplier);
            load_partsupp(tables->partsupp.get(), db->partsupp);
        } catch (...) { ex_c = std::current_exception(); }
    });
    auto thread_d = std::thread([&]() noexcept {
        try {
            load_customer(tables->customer.get(), db->customer);
            load_part(tables->part.get(), db->part);
        } catch (...) { ex_d = std::current_exception(); }
    });
    thread_a.join(); thread_b.join(); thread_c.join(); thread_d.join();
    if (ex_a) std::rethrow_exception(ex_a);
    if (ex_b) std::rethrow_exception(ex_b);
    if (ex_c) std::rethrow_exception(ex_c);
    if (ex_d) std::rethrow_exception(ex_d);

    build_acceleration(db);
    build_customer_nation_names(db);
    build_order_cust_nationkey(db);
    build_li_cust_suppnk(db);
    build_li_by_pk_companions(db);
    build_li_q14_promo(db);

    return db;
}

void destroy_database(Database* db) {
    delete db;
}