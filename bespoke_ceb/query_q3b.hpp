#pragma once

#include <cstdint>
#include <vector>

#include "args_parser.hpp"
#include "builder_impl.hpp"

struct Q3bResultRow {
    int32_t title_id{};
    int32_t person_name_id{};
    int32_t company_name_id{};
    int64_t count{};
};

std::vector<Q3bResultRow> run_q3b(const Database& db, const Q3bArgs& args);