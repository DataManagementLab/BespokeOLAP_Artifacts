#pragma once

#include <cstdint>
#include <vector>

#include "args_parser.hpp"
#include "builder_impl.hpp"

struct Q9aResultRow {
    int32_t movie_info_id{};
    int32_t person_info_id{};
    int64_t count{};
};

std::vector<Q9aResultRow> run_q9a(const Database& db, const Q9aArgs& args);