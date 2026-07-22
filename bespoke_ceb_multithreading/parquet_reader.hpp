#pragma once

// Increment file version to invalidate cache when this file is changed. This is needed because this file is included in the generated code and changes to it should trigger regeneration of all code that includes it.
// FILE_VERSION: 1


#include <string>

struct ParquetTables {
    std::string aka_name_path;
    std::string aka_title_path;
    std::string cast_info_path;
    std::string char_name_path;
    std::string comp_cast_type_path;
    std::string company_name_path;
    std::string company_type_path;
    std::string complete_cast_path;
    std::string info_type_path;
    std::string keyword_path;
    std::string kind_type_path;
    std::string link_type_path;
    std::string movie_companies_path;
    std::string movie_info_path;
    std::string movie_info_idx_path;
    std::string movie_keyword_path;
    std::string movie_link_path;
    std::string name_path;
    std::string person_info_path;
    std::string role_type_path;
    std::string title_path;
};


ParquetTables* load(std::string);
void destroy_parquet_tables(ParquetTables*);
