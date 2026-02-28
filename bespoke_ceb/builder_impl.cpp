#include "builder_impl.hpp"

#include <arrow/api.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <mutex>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

struct LocalStringPool {
    std::vector<std::string> values;
    std::unordered_map<std::string, int32_t> index;

    int32_t intern(const std::string& value) {
        auto it = index.find(value);
        if (it != index.end()) {
            return it->second;
        }
        const int32_t id = static_cast<int32_t>(values.size());
        values.push_back(value);
        index.emplace(values.back(), id);
        return id;
    }
};

ColumnData build_column(const std::shared_ptr<arrow::ChunkedArray>& column,
                        LocalStringPool& pool) {
    ColumnData data;
    const auto type_id = column->type()->id();
    const int64_t total_length = column->length();
    data.is_null.reserve(static_cast<size_t>(total_length));
    bool has_nulls = false;

    auto append_null = [&data, &has_nulls]() {
        data.is_null.push_back(1);
        has_nulls = true;
    };

    auto append_valid = [&data]() {
        data.is_null.push_back(0);
    };

    switch (type_id) {
        case arrow::Type::INT32: {
            data.type = ColumnType::Int32;
            data.i32.reserve(static_cast<size_t>(total_length));
            int32_t min_val = std::numeric_limits<int32_t>::max();
            int32_t max_val = std::numeric_limits<int32_t>::min();
            bool has_value = false;
            for (const auto& chunk : column->chunks()) {
                auto arr = std::static_pointer_cast<arrow::Int32Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    if (arr->IsNull(i)) {
                        data.i32.push_back(0);
                        append_null();
                    } else {
                        const int32_t value = arr->Value(i);
                        data.i32.push_back(value);
                        append_valid();
                        if (!has_value) {
                            min_val = value;
                            max_val = value;
                            has_value = true;
                        } else {
                            min_val = std::min(min_val, value);
                            max_val = std::max(max_val, value);
                        }
                    }
                }
            }
            if (has_value) {
                data.i32_min = min_val;
                data.i32_max = max_val;
            }
            break;
        }
        case arrow::Type::INT64: {
            data.type = ColumnType::Int64;
            data.i64.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                auto arr = std::static_pointer_cast<arrow::Int64Array>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    if (arr->IsNull(i)) {
                        data.i64.push_back(0);
                        append_null();
                    } else {
                        data.i64.push_back(arr->Value(i));
                        append_valid();
                    }
                }
            }
            break;
        }
        case arrow::Type::FLOAT: {
            data.type = ColumnType::Float;
            data.f32.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                auto arr = std::static_pointer_cast<arrow::FloatArray>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    if (arr->IsNull(i)) {
                        data.f32.push_back(0.0f);
                        append_null();
                    } else {
                        data.f32.push_back(arr->Value(i));
                        append_valid();
                    }
                }
            }
            break;
        }
        case arrow::Type::DOUBLE: {
            data.type = ColumnType::Double;
            data.f64.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                auto arr = std::static_pointer_cast<arrow::DoubleArray>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    if (arr->IsNull(i)) {
                        data.f64.push_back(0.0);
                        append_null();
                    } else {
                        data.f64.push_back(arr->Value(i));
                        append_valid();
                    }
                }
            }
            break;
        }
        case arrow::Type::BOOL: {
            data.type = ColumnType::Bool;
            data.b8.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                auto arr = std::static_pointer_cast<arrow::BooleanArray>(chunk);
                for (int64_t i = 0; i < arr->length(); ++i) {
                    if (arr->IsNull(i)) {
                        data.b8.push_back(0);
                        append_null();
                    } else {
                        data.b8.push_back(static_cast<uint8_t>(arr->Value(i)));
                        append_valid();
                    }
                }
            }
            break;
        }
        case arrow::Type::STRING:
        case arrow::Type::LARGE_STRING: {
            data.type = ColumnType::String;
            data.str_ids.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                std::shared_ptr<arrow::Array> raw = chunk;
                if (type_id == arrow::Type::STRING) {
                    auto arr = std::static_pointer_cast<arrow::StringArray>(raw);
                    for (int64_t i = 0; i < arr->length(); ++i) {
                        if (arr->IsNull(i)) {
                            data.str_ids.push_back(-1);
                            append_null();
                        } else {
                            const auto view = arr->GetView(i);
                            data.str_ids.push_back(pool.intern(std::string(view)));
                            append_valid();
                        }
                    }
                } else {
                    auto arr = std::static_pointer_cast<arrow::LargeStringArray>(raw);
                    for (int64_t i = 0; i < arr->length(); ++i) {
                        if (arr->IsNull(i)) {
                            data.str_ids.push_back(-1);
                            append_null();
                        } else {
                            const auto view = arr->GetView(i);
                            data.str_ids.push_back(pool.intern(std::string(view)));
                            append_valid();
                        }
                    }
                }
            }
            break;
        }
        case arrow::Type::DICTIONARY: {
            data.type = ColumnType::String;
            data.str_ids.reserve(static_cast<size_t>(total_length));
            for (const auto& chunk : column->chunks()) {
                auto dict_arr = std::static_pointer_cast<arrow::DictionaryArray>(chunk);
                auto dict = dict_arr->dictionary();
                std::vector<int32_t> dict_ids;
                if (dict->type()->id() == arrow::Type::STRING) {
                    auto dict_values = std::static_pointer_cast<arrow::StringArray>(dict);
                    dict_ids.reserve(static_cast<size_t>(dict_values->length()));
                    for (int64_t i = 0; i < dict_values->length(); ++i) {
                        if (dict_values->IsNull(i)) {
                            dict_ids.push_back(-1);
                        } else {
                            const auto view = dict_values->GetView(i);
                            dict_ids.push_back(pool.intern(std::string(view)));
                        }
                    }
                } else if (dict->type()->id() == arrow::Type::LARGE_STRING) {
                    auto dict_values = std::static_pointer_cast<arrow::LargeStringArray>(dict);
                    dict_ids.reserve(static_cast<size_t>(dict_values->length()));
                    for (int64_t i = 0; i < dict_values->length(); ++i) {
                        if (dict_values->IsNull(i)) {
                            dict_ids.push_back(-1);
                        } else {
                            const auto view = dict_values->GetView(i);
                            dict_ids.push_back(pool.intern(std::string(view)));
                        }
                    }
                } else {
                    throw std::runtime_error("Unsupported dictionary value type");
                }
                for (int64_t i = 0; i < dict_arr->length(); ++i) {
                    if (dict_arr->IsNull(i)) {
                        data.str_ids.push_back(-1);
                        append_null();
                        continue;
                    }
                    const int64_t dict_index = dict_arr->GetValueIndex(i);
                    data.str_ids.push_back(dict_ids[static_cast<size_t>(dict_index)]);
                    append_valid();
                }
            }
            break;
        }
        default:
            throw std::runtime_error("Unsupported column type in build_column");
    }

    data.has_nulls = has_nulls;
    return data;
}

TableData build_table(const ParquetTables::ArrowTable& table, LocalStringPool& pool) {
    if (!table) {
        return TableData{};
    }
    TableData data;
    data.row_count = table->num_rows();
    const auto& schema = table->schema();
    for (int i = 0; i < schema->num_fields(); ++i) {
        const auto& field = schema->field(i);
        const auto& column = table->column(i);
        data.columns.emplace(field->name(), build_column(column, pool));
    }
    return data;
}

template <typename T>
void reorder_vector(std::vector<T>& data, const std::vector<uint32_t>& order) {
    std::vector<T> reordered;
    reordered.resize(order.size());
    for (size_t i = 0; i < order.size(); ++i) {
        reordered[i] = data[order[i]];
    }
    data.swap(reordered);
}

void sort_table_by_i32_column(TableData& table, const std::string& column_name) {
    auto it = table.columns.find(column_name);
    if (it == table.columns.end()) {
        return;
    }
    auto& key_col = it->second;
    if (key_col.type != ColumnType::Int32) {
        return;
    }
    const size_t row_count = static_cast<size_t>(table.row_count);
    std::vector<uint32_t> order(row_count);
    std::iota(order.begin(), order.end(), 0);
    const auto& key_data = key_col.i32;
    std::sort(order.begin(), order.end(),
              [&](uint32_t a, uint32_t b) { return key_data[a] < key_data[b]; });

    for (auto& entry : table.columns) {
        auto& column = entry.second;
        switch (column.type) {
            case ColumnType::Int32:
                reorder_vector(column.i32, order);
                break;
            case ColumnType::Int64:
                reorder_vector(column.i64, order);
                break;
            case ColumnType::Float:
                reorder_vector(column.f32, order);
                break;
            case ColumnType::Double:
                reorder_vector(column.f64, order);
                break;
            case ColumnType::Bool:
                reorder_vector(column.b8, order);
                break;
            case ColumnType::String:
                reorder_vector(column.str_ids, order);
                break;
        }
        reorder_vector(column.is_null, order);
    }
    table.sorted_by_movie_id = true;
}

void sort_table_by_i32_columns(TableData& table, const std::string& primary_name,
                               const std::string& secondary_name) {
    auto primary_it = table.columns.find(primary_name);
    auto secondary_it = table.columns.find(secondary_name);
    if (primary_it == table.columns.end() || secondary_it == table.columns.end()) {
        return;
    }
    auto& primary_col = primary_it->second;
    auto& secondary_col = secondary_it->second;
    if (primary_col.type != ColumnType::Int32 || secondary_col.type != ColumnType::Int32) {
        return;
    }
    const size_t row_count = static_cast<size_t>(table.row_count);
    std::vector<uint32_t> order(row_count);
    std::iota(order.begin(), order.end(), 0);
    const auto& primary_data = primary_col.i32;
    const auto& secondary_data = secondary_col.i32;
    std::sort(order.begin(), order.end(),
              [&](uint32_t a, uint32_t b) {
                  const int32_t pa = primary_data[a];
                  const int32_t pb = primary_data[b];
                  if (pa != pb) {
                      return pa < pb;
                  }
                  return secondary_data[a] < secondary_data[b];
              });

    for (auto& entry : table.columns) {
        auto& column = entry.second;
        switch (column.type) {
            case ColumnType::Int32:
                reorder_vector(column.i32, order);
                break;
            case ColumnType::Int64:
                reorder_vector(column.i64, order);
                break;
            case ColumnType::Float:
                reorder_vector(column.f32, order);
                break;
            case ColumnType::Double:
                reorder_vector(column.f64, order);
                break;
            case ColumnType::Bool:
                reorder_vector(column.b8, order);
                break;
            case ColumnType::String:
                reorder_vector(column.str_ids, order);
                break;
        }
        reorder_vector(column.is_null, order);
    }
    table.sorted_by_movie_id = true;
}

}  // namespace

Database* build(ParquetTables* tables) {
    auto db = new Database{};
    if (!tables) {
        return db;
    }
    struct TableSpec {
        const char* name;
        ParquetTables::ArrowTable table;
    };
    std::vector<TableSpec> specs = {
        {"aka_name", tables->aka_name},
        {"aka_title", tables->aka_title},
        {"cast_info", tables->cast_info},
        {"char_name", tables->char_name},
        {"comp_cast_type", tables->comp_cast_type},
        {"company_name", tables->company_name},
        {"company_type", tables->company_type},
        {"complete_cast", tables->complete_cast},
        {"info_type", tables->info_type},
        {"keyword", tables->keyword},
        {"kind_type", tables->kind_type},
        {"link_type", tables->link_type},
        {"movie_companies", tables->movie_companies},
        {"movie_info", tables->movie_info},
        {"movie_info_idx", tables->movie_info_idx},
        {"movie_keyword", tables->movie_keyword},
        {"movie_link", tables->movie_link},
        {"name", tables->name},
        {"person_info", tables->person_info},
        {"role_type", tables->role_type},
        {"title", tables->title},
    };

    db->tables.reserve(specs.size());

    std::mutex table_mutex;
    std::atomic<size_t> next_index{0};
    const unsigned int hw_threads = std::thread::hardware_concurrency();
    const size_t base_threads = hw_threads == 0 ? 4 : hw_threads;
    const size_t thread_count = std::min<size_t>(specs.size(), std::min<size_t>(base_threads, 16));
    std::vector<std::thread> threads;
    threads.reserve(thread_count);

    for (size_t t = 0; t < thread_count; ++t) {
        threads.emplace_back([&]() {
            while (true) {
                const size_t idx = next_index.fetch_add(1, std::memory_order_relaxed);
                if (idx >= specs.size()) {
                    break;
                }
                const auto& spec = specs[idx];
                LocalStringPool local_pool;
                TableData data = build_table(spec.table, local_pool);
                std::vector<int32_t> local_to_global(local_pool.values.size());
                for (size_t i = 0; i < local_pool.values.size(); ++i) {
                    local_to_global[i] = db->string_pool.intern(local_pool.values[i]);
                }
                for (auto& entry : data.columns) {
                    auto& column = entry.second;
                    if (column.type != ColumnType::String) {
                        continue;
                    }
                    for (auto& id : column.str_ids) {
                        if (id >= 0) {
                            id = local_to_global[static_cast<size_t>(id)];
                        }
                    }
                }
                if (std::strcmp(spec.name, "movie_info") == 0) {
                    sort_table_by_i32_columns(data, "movie_id", "info_type_id");
                    data.sorted_by_info_type_id = true;
                } else if (std::strcmp(spec.name, "cast_info") == 0) {
                    sort_table_by_i32_columns(data, "movie_id", "role_id");
                    data.sorted_by_role_id = true;
                } else if (std::strcmp(spec.name, "movie_companies") == 0) {
                    sort_table_by_i32_column(data, "movie_id");
                } else if (std::strcmp(spec.name, "movie_keyword") == 0) {
                    sort_table_by_i32_column(data, "keyword_id");
                    data.sorted_by_movie_id = false;
                    data.sorted_by_keyword_id = true;
                } else if (std::strcmp(spec.name, "person_info") == 0) {
                    sort_table_by_i32_column(data, "info_type_id");
                    data.sorted_by_movie_id = false;
                    data.sorted_by_info_type_id = true;
                } else if (std::strcmp(spec.name, "name") == 0) {
                    sort_table_by_i32_column(data, "id");
                } else if (std::strcmp(spec.name, "title") == 0) {
                    sort_table_by_i32_column(data, "id");
                }
                std::lock_guard<std::mutex> lock(table_mutex);
                db->tables.emplace(spec.name, std::move(data));
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    db->string_pool.finalize();

    return db;
}
