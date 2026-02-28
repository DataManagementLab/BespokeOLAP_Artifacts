#pragma once

#include <cstdint>

#include "args_parser.hpp"
#include "builder_impl.hpp"

void prepare_q5a(const Database& db);
int64_t run_q5a(const Database& db, const Q5aArgs& args);