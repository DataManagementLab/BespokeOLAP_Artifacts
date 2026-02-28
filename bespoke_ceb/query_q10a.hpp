#pragma once

#include <cstdint>
#include <vector>

#include "args_parser.hpp"
#include "builder_impl.hpp"

struct Q10aResultRow {
    int32_t person_name_id{};
    int32_t movie_info_id{};
    bool has_year{};
    int32_t min_year{};
    int32_t max_year{};
};

std::vector<Q10aResultRow> run_q10a(const Database& db, const Q10aArgs& args);