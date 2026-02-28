#pragma once

#include <cstdint>
#include <vector>

#include "args_parser.hpp"
#include "builder_impl.hpp"

struct Q9bResultRow {
    int32_t movie_info_id{};
    int32_t person_name_id{};
    int64_t count{};
};

std::vector<Q9bResultRow> run_q9b(const Database& db, const Q9bArgs& args);