// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "common/status.h"
#include "cache/kv_cache.h"

namespace starrocks {

class BlockCache {
public:
    typedef std::string CacheKey;

    ~BlockCache();

    // Return a singleton block cache instance
    static BlockCache* instance();

    // Init the block cache instance
    Status init(const CacheOptions& options);

    // Set block size as the cache unit
    Status set_block_size(size_t block_size);

    // Write data to cache, the offset must be aligned by block size
    Status write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer,
                       size_t ttl_seconds = 0);

    // Read data from cache, it returns the data size if successful; otherwise the error status
    // will be returned. The offset and size must be aligned by block size.
    StatusOr<size_t> read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer);

    // Remove data from cache. The offset and size must be aligned by block size
    Status remove_cache(const CacheKey& cache_key, off_t offset, size_t size);

private:
    BlockCache();

    size_t _block_size = 0;
    KvCache* _kv_cache = nullptr;
};

} // namespace starrocks
