#pragma once



#include "db_loader.hpp"
#include "query_api.hpp"


std::vector<QueryResult> query(Database*, const std::vector<std::string>& query_lines);