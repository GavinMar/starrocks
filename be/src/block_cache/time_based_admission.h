// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <atomic>
#include <fcntl.h>
#include <mutex>
#include <shared_mutex>

#include <butil/fast_rand.h>
#include <butil/time.h>

#include "block_cache/cache_options.h"
#include "common/logging.h"
#include "common/status.h"

namespace starrocks {

class TimeBasedAdmission {
public:
    void init(const CacheOptions& options);

    void record_write(int64_t latency_us);
    void record_read(int64_t latency_us);

    bool check_write();
    bool check_read();

    const int64_t kMaxWriteLatencyRatio = 10;
    const int64_t kMaxReadLatencyRatio = 10;
    const int64_t kSkipWriteFactor = 1;
    const int64_t kSkipReadFactor = 1;
    const size_t kMaxCheckRetries = 5;
    const size_t kIOAlignUnit = 4096; 
    const size_t kIOTestCount = 100;
    const size_t kMaxIOTestFileSize = 10L * 1024 * 1024 * 1024;

private:
    void _detect_base_latency(const CacheOptions& options);
    int64_t _calc_central_avg(std::vector<int64_t>& elems, size_t choose_count);

    std::atomic<bool> _initialized = false;
    std::atomic<int64_t> _base_write_latency_us = 0;
    std::atomic<int64_t> _base_read_latency_us = 0;
    std::atomic<int64_t> _skip_writes = 0;
    std::atomic<int64_t> _skip_reads = 0;

    Status _status;
};

} // namespace starrocks
