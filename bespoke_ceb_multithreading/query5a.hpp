#pragma once

#include <string>
#include <vector>

#include "parse_utils.hpp"
#include "args_parser.hpp"
#include "db_loader.hpp"

std::vector<std::vector<std::string>> run_q5a(Database* db, const Q5aArgs& args);