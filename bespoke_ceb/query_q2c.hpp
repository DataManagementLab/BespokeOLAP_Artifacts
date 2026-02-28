#pragma once

#include <cstdint>

#include "args_parser.hpp"
#include "builder_impl.hpp"

int64_t run_q2c(const Database& db, const Q2cArgs& args);