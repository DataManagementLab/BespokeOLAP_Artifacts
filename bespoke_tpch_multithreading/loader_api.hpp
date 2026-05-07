#pragma once

#include <string>


struct ParquetTables;

ParquetTables* load(std::string);
void destroy_parquet_tables(ParquetTables*);

struct LoaderApi {
    ParquetTables* (*load)(std::string);
    void (*destroy)(ParquetTables*);
};