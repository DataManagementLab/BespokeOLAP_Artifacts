#include "parquet_reader.hpp"

// Increment file version to invalidate cache when this file is changed. This is needed because this file is included in the generated code and changes to it should trigger regeneration of all code that includes it.
// FILE_VERSION: 2


#include <stdio.h>
#include <unistd.h>


void destroy_parquet_tables(ParquetTables* tables) {
    delete tables;
}

ParquetTables* load(std::string path) {
    auto tables = new ParquetTables{};

    tables->aka_name_path = path + "aka_name.parquet";
    tables->aka_title_path = path + "aka_title.parquet";
    tables->cast_info_path = path + "cast_info.parquet";
    tables->char_name_path = path + "char_name.parquet";
    tables->comp_cast_type_path = path + "comp_cast_type.parquet";
    tables->company_name_path = path + "company_name.parquet";
    tables->company_type_path = path + "company_type.parquet";
    tables->complete_cast_path = path + "complete_cast.parquet";
    tables->info_type_path = path + "info_type.parquet";
    tables->keyword_path = path + "keyword.parquet";
    tables->kind_type_path = path + "kind_type.parquet";
    tables->link_type_path = path + "link_type.parquet";
    tables->movie_companies_path = path + "movie_companies.parquet";
    tables->movie_info_path = path + "movie_info.parquet";
    tables->movie_info_idx_path = path + "movie_info_idx.parquet";
    tables->movie_keyword_path = path + "movie_keyword.parquet";
    tables->movie_link_path = path + "movie_link.parquet";
    tables->name_path = path + "name.parquet";
    tables->person_info_path = path + "person_info.parquet";
    tables->role_type_path = path + "role_type.parquet";
    tables->title_path = path + "title.parquet";

    return tables;
}
