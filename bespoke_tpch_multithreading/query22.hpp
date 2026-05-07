#pragma once

#include <string>
#include <vector>

#include "args_parser.hpp"
#include "db_loader.hpp"

std::vector<std::vector<std::string>> run_q22(Database* db, const Q22Args& args);