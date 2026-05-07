#pragma once

#include <string>
#include <vector>

#include "args_parser.hpp"
#include "db_loader.hpp"

std::vector<std::vector<std::string>> run_q4(Database* db, const Q4Args& args);