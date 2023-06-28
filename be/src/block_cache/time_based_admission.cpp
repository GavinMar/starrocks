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

#include <numeric>

#include "block_cache/time_based_admission.h"

namespace starrocks {

void TimeBasedAdmission::init(const CacheOptions& options) {
    _detect_base_latency(options);
    _initialized = true;
}

void TimeBasedAdmission::record_write(int64_t latency_us) {
    if (!_initialized.load(std::memory_order_relaxed) || !_status.ok()) {
        return;
    }

    int64_t write_latency_ratio = latency_us / _base_write_latency_us.load(std::memory_order_relaxed);
    if (write_latency_ratio > kMaxWriteLatencyRatio) {
        _skip_writes.fetch_add(write_latency_ratio * kSkipWriteFactor, std::memory_order_relaxed);
    }
}

void TimeBasedAdmission::record_read(int64_t latency_us) {
    if (!_initialized.load(std::memory_order_relaxed) || !_status.ok()) {
        return;
    }

    int64_t read_latency_ratio = latency_us / _base_read_latency_us.load(std::memory_order_relaxed);
    if (read_latency_ratio > kMaxReadLatencyRatio) {
        _skip_reads.fetch_add(read_latency_ratio * kSkipReadFactor, std::memory_order_relaxed);
        // We also add the skip writes to prevent it from affecting read, because read is always more important.
        _skip_writes.fetch_add(read_latency_ratio * kSkipWriteFactor, std::memory_order_relaxed);
    }
}

bool TimeBasedAdmission::check_write() {
    // If not ready or some error ocurrs, we skip the admisson check.
    if (!_initialized.load(std::memory_order_relaxed) || !_status.ok()) {
        return true;
    }

    size_t max_retries = kMaxCheckRetries;
    int64_t old_skip = _skip_writes.load(std::memory_order_relaxed);
    while (max_retries-- > 0) {
        if (old_skip <= 0) {
            return true;
        }
        if (_skip_writes.compare_exchange_weak(old_skip, old_skip - 1, std::memory_order_acquire, std::memory_order_relaxed)) {
            return false;
        }
    }
    return false;
}

bool TimeBasedAdmission::check_read() {
    if (!_initialized.load(std::memory_order_relaxed) || !_status.ok()) {
        return true;
    }

    size_t max_retries = kMaxCheckRetries;
    int64_t old_skip = _skip_reads.load(std::memory_order_relaxed);
    while (max_retries-- > 0) {
        if (old_skip <= 0) {
            return true;
        }
        if (_skip_reads.compare_exchange_weak(old_skip, old_skip - 1, std::memory_order_acquire, std::memory_order_relaxed)) {
            return false;
        }
    }
    return false;
}

void TimeBasedAdmission::_detect_base_latency(const CacheOptions& options) {
    auto& disks = options.disk_spaces;
    const size_t size = options.block_size; 
    void* buffer = nullptr;
    if (posix_memalign(&buffer, kIOAlignUnit, size) != 0) {
        LOG(ERROR) << "fail to align memorydata buffer" << ", reason: " << std::strerror(errno);
        _status = Status::MemoryAllocFailed("fail to align memorydata buffer");
        return;
    }

    std::vector<int64_t> write_latencies; 
    std::vector<int64_t> read_latencies; 
    const int oflag = O_RDWR | O_CREAT | O_DIRECT;
    for (auto& disk : disks) {
        const std::string file_path = disk.path + "/test_file.temp";
        int fd = ::open(file_path.c_str(), oflag, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP);
        if (fd < 0) {
            LOG(ERROR) << "fail to open block cache test file: " << file_path << ", reason: "
                       << std::strerror(errno);
            _status = Status::IOError("fail to open block cache test file");
            free(buffer);
            return;
        }

        std::vector<int64_t> disk_write_latencies; 
        std::vector<int64_t> disk_read_latencies; 
        size_t max_file_size = std::min(disk.size, kMaxIOTestFileSize);
        for (size_t i = 0; i < kIOTestCount; ++i) {
            size_t offset = butil::fast_rand_less_than(max_file_size - size);
            offset = offset / kIOAlignUnit * kIOAlignUnit;
            memset(buffer, '0' + i, size);
            int64_t start_us = butil::monotonic_time_us();
            int ret = ::pwrite(fd, buffer, size, offset);
            if (ret < 0) {
                LOG(ERROR) << "fail to write block cache test file: " << file_path << ", reason: "
                           << std::strerror(errno);
                continue;
            }
            int64_t end_us = butil::monotonic_time_us();
            disk_write_latencies.push_back(end_us - start_us);
            LOG(INFO) << "test write, offset: " << offset << ", size: " << size << ", latency_us: " << end_us - start_us;

            start_us = butil::monotonic_time_us();
            ret = ::pread(fd, buffer, size, offset);
            if (ret < 0) {
                LOG(ERROR) << "fail to read block cache test file: " << file_path << ", reason: "
                           << std::strerror(errno);
                continue;
            }
            end_us = butil::monotonic_time_us();
            disk_read_latencies.push_back(end_us - start_us);
            LOG(INFO) << "test read, offset: " << offset << ", size: " << size << ", latency_us: " << end_us - start_us;
        }

        size_t choose_count = kIOTestCount / 2;
        int64_t avg_write_latency = _calc_central_avg(disk_write_latencies, choose_count);
        int64_t avg_read_latency = _calc_central_avg(disk_read_latencies, choose_count);
        if (avg_write_latency > 0) {
            write_latencies.push_back(avg_write_latency);
        }
        if (avg_read_latency > 0) {
            read_latencies.push_back(avg_read_latency);
        }
    }
    free(buffer);

    if (!write_latencies.empty()) {
        _base_write_latency_us = *std::max_element(write_latencies.begin(), write_latencies.end());
    }
    if (!read_latencies.empty()) {
        _base_read_latency_us = *std::max_element(read_latencies.begin(), read_latencies.end());
    }
    LOG(INFO) << "finish detect disk base latency, base_write_latency_us: " << _base_write_latency_us
              << ", base_read_latency_us: " << _base_read_latency_us;
}

int64_t TimeBasedAdmission::_calc_central_avg(std::vector<int64_t>& elems, size_t choose_count) {
    if (elems.size() < choose_count) {
        return 0;
    }

    std::sort(elems.begin(), elems.end());
    size_t discard_count = elems.size() - choose_count;
    auto start_iter = elems.begin() + discard_count / 2;
    auto end_iter = start_iter + choose_count;
    return std::accumulate(start_iter, end_iter, 0) / choose_count;
}

} // namespace starrocks
