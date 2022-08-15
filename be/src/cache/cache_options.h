// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <vector>
#include <string>

namespace starrocks {

struct DirSpace {
    std::string path;
    size_t size;
};

struct CacheOptions {
    size_t mem_space_size;
    std::vector<DirSpace> disk_spaces;
};

} // namespace starrocks
