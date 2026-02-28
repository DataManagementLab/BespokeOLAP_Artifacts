#pragma once

#include <cstdint>

#include "args_parser.hpp"
#include "builder_impl.hpp"

int64_t run_q3a(const Database& db, const Q3aArgs& args);