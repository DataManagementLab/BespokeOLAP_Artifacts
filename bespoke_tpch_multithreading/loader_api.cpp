#include "loader_api.hpp"

#include "utils/plugin_base.hpp"


static const LoaderApi LOADER = {
    .load = &load,
    .destroy = &destroy_parquet_tables,
};

extern "C" __attribute__((visibility("default")))
const void*
plugin_query() {
    return &LOADER;
}