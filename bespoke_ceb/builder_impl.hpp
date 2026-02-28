#pragma once

#include "loader_impl.hpp"

#include <array>
#include <atomic>
#include <cctype>
#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

struct StringPool {
    static constexpr size_t kShardCount = 64;

    struct Shard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, int32_t> index;
    };

    std::vector<const std::string*> values;
    std::vector<std::string> lower_values;
    std::atomic<int32_t> next_id{0};
    std::array<Shard, kShardCount> shards;
    int32_t intern(const std::string& value) {
        const size_t shard_index = std::hash<std::string>{}(value) % kShardCount;
        auto& shard = shards[shard_index];
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            auto it = shard.index.find(value);
            if (it != shard.index.end()) {
                return it->second;
            }
            const int32_t id = next_id.fetch_add(1, std::memory_order_relaxed);
            shard.index.emplace(value, id);
            return id;
        }
    }

    const std::string& get(int32_t id) const {
        return *values[static_cast<size_t>(id)];
    }

    const std::string& get_lower(int32_t id) const {
        return lower_values[static_cast<size_t>(id)];
    }

    bool try_get_id(const std::string& value, int32_t& out_id) const {
        const size_t shard_index = std::hash<std::string>{}(value) % kShardCount;
        auto& shard = shards[shard_index];
        std::lock_guard<std::mutex> lock(shard.mutex);
        auto it = shard.index.find(value);
        if (it == shard.index.end()) {
            return false;
        }
        out_id = it->second;
        return true;
    }

    static std::string to_lower_copy(const std::string& value) {
        std::string out;
        out.resize(value.size());
        for (size_t i = 0; i < value.size(); ++i) {
            out[i] = static_cast<char>(std::tolower(static_cast<unsigned char>(value[i])));
        }
        return out;
    }

    void finalize() {
        const auto total = static_cast<size_t>(next_id.load(std::memory_order_relaxed));
        values.clear();
        values.resize(total);
        for (auto& shard : shards) {
            std::lock_guard<std::mutex> lock(shard.mutex);
            for (const auto& entry : shard.index) {
                values[static_cast<size_t>(entry.second)] = &entry.first;
            }
        }
        lower_values.clear();
        lower_values.resize(total);
        for (size_t i = 0; i < total; ++i) {
            lower_values[i] = to_lower_copy(*values[i]);
        }
    }
};

enum class ColumnType {
    Int32,
    Int64,
    Float,
    Double,
    Bool,
    String
};

struct ColumnData {
    ColumnType type{};
    bool has_nulls = false;
    int32_t i32_min = 0;
    int32_t i32_max = -1;
    std::vector<int32_t> i32;
    std::vector<int64_t> i64;
    std::vector<float> f32;
    std::vector<double> f64;
    std::vector<uint8_t> b8;
    std::vector<int32_t> str_ids;
    std::vector<uint8_t> is_null;
};

struct TableData {
    int64_t row_count{};
    std::unordered_map<std::string, ColumnData> columns;
    bool sorted_by_movie_id = false;
    bool sorted_by_keyword_id = false;
    bool sorted_by_info_type_id = false;
    bool sorted_by_role_id = false;
};

struct Database {
    StringPool string_pool;
    std::unordered_map<std::string, TableData> tables;
};


Database* build(ParquetTables*);
