#pragma once

#include <arrow/table.h>
#include <memory>

struct ParquetTables {
    using ArrowTable = std::shared_ptr<arrow::Table>;

    ArrowTable aka_name;
    ArrowTable aka_title;
    ArrowTable cast_info;
    ArrowTable char_name;
    ArrowTable comp_cast_type;
    ArrowTable company_name;
    ArrowTable company_type;
    ArrowTable complete_cast;
    ArrowTable info_type;
    ArrowTable keyword;
    ArrowTable kind_type;
    ArrowTable link_type;
    ArrowTable movie_companies;
    ArrowTable movie_info;
    ArrowTable movie_info_idx;
    ArrowTable movie_keyword;
    ArrowTable movie_link;
    ArrowTable name;
    ArrowTable person_info;
    ArrowTable role_type;
    ArrowTable title;
};


ParquetTables* load(std::string);
