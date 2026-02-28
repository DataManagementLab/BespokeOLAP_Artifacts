#include "loader_impl.hpp"

#include "loader_utils.hpp"

#include <stdio.h>
#include <unistd.h>


ParquetTables* load(std::string path) {
    auto tables = new ParquetTables{};

    tables->aka_name = ReadParquetTable(path + "aka_name.parquet");
    tables->aka_title = ReadParquetTable(path + "aka_title.parquet");
    tables->cast_info = ReadParquetTable(path + "cast_info.parquet");
    tables->char_name = ReadParquetTable(path + "char_name.parquet");
    tables->comp_cast_type = ReadParquetTable(path + "comp_cast_type.parquet");
    tables->company_name = ReadParquetTable(path + "company_name.parquet");
    tables->company_type = ReadParquetTable(path + "company_type.parquet");
    tables->complete_cast = ReadParquetTable(path + "complete_cast.parquet");
    tables->info_type = ReadParquetTable(path + "info_type.parquet");
    tables->keyword = ReadParquetTable(path + "keyword.parquet");
    tables->kind_type = ReadParquetTable(path + "kind_type.parquet");
    tables->link_type = ReadParquetTable(path + "link_type.parquet");
    tables->movie_companies = ReadParquetTable(path + "movie_companies.parquet");
    tables->movie_info = ReadParquetTable(path + "movie_info.parquet");
    tables->movie_info_idx = ReadParquetTable(path + "movie_info_idx.parquet");
    tables->movie_keyword = ReadParquetTable(path + "movie_keyword.parquet");
    tables->movie_link = ReadParquetTable(path + "movie_link.parquet");
    tables->name = ReadParquetTable(path + "name.parquet");
    tables->person_info = ReadParquetTable(path + "person_info.parquet");
    tables->role_type = ReadParquetTable(path + "role_type.parquet");
    tables->title = ReadParquetTable(path + "title.parquet");

    return tables;
}
