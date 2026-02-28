#pragma once

#include <cstdint>
#include <vector>

#include "args_parser.hpp"
#include "builder_impl.hpp"

struct Q11bResultRow {
    int32_t gender_id{};
    int32_t role_id{};
    int32_t company_name_id{};
    int64_t count{};
};

std::vector<Q11bResultRow> run_q11b(const Database& db, const Q11bArgs& args);