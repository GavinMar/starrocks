// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "cache/block_cache.h"

#include <fmt/format.h>

#include "common/config.h"
#include "common/statusor.h"
#include "common/logging.h"
#include "gutil/strings/substitute.h"
#include "cache/fb_cachelib.h"

//#include "util/hash_util.hpp"
#include "formats/orc/apache-orc/c++/src/Murmur3.hh"

namespace starrocks {

BlockCache::BlockCache() {
    _kv_cache = new FbCacheLib();
}

BlockCache::~BlockCache() {
    if (_kv_cache) {
        _kv_cache->destroy();
        delete _kv_cache;
        _kv_cache = nullptr;
    }
}

BlockCache* BlockCache::instance() {
    static BlockCache cache;
    return &cache;
}

Status BlockCache::init(const CacheOptions& options) {
    return _kv_cache->init(options);
}

Status BlockCache::set_block_size(size_t block_size) {
    // TODO: check block size limit
    _block_size = block_size;
    return Status::OK();
}

Status BlockCache::write_cache(const CacheKey& cache_key, off_t offset, size_t size, const char* buffer,
                               size_t ttl_seconds) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "write block key: " << cache_key << " with invalid args, offset: " << offset;
    }
    if (!buffer) {
        return Status::InvalidArgument("invalid data buffer");
    }
    if (size == 0) {
        return Status::OK();
    }
        
    size_t start_block_index = offset / _block_size;
    size_t end_block_index = (offset + size - 1) / _block_size + 1;
    off_t off_in_buf = 0;
    for (size_t index = start_block_index; index < end_block_index; ++index) {
        std::string block_key = fmt::format("{}/{}", cache_key, index);
        const char* block_buf = buffer + off_in_buf;
        const size_t block_size = std::min(size - off_in_buf, _block_size);
        //uint64_t hash_value = orc::Murmur3::hash64((const uint8_t*)block_buf, block_size);
        RETURN_IF_ERROR(_kv_cache->write_cache(block_key, block_buf, block_size, ttl_seconds));
        off_in_buf += block_size;
    }

    return Status::OK();
}

StatusOr<size_t> BlockCache::read_cache(const CacheKey& cache_key, off_t offset, size_t size, char* buffer) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "read block key: " << cache_key << " with invalid offset: "
                     << offset << ", size: " << size;
        return Status::InvalidArgument(strings::Substitute("offset and size must be aligned by block size $0",
                                                            _block_size));
    }
    if (!buffer) {
        return Status::InvalidArgument("invalid data buffer");
    }
    if (size == 0) {
        return 0;
    }

    size_t start_block_index = offset / _block_size;
    size_t end_block_index = (offset + size - 1) / _block_size + 1;
    off_t off_in_buf = 0;
    size_t read_size = 0;
    for (size_t index = start_block_index; index < end_block_index; ++index) {
        std::string block_key = fmt::format("{}/{}", cache_key, index);
        char* block_buf = buffer + off_in_buf;
        auto res = _kv_cache->read_cache(block_key, block_buf);
        RETURN_IF_ERROR(res);
        //uint64_t hash_value = orc::Murmur3::hash64((const uint8_t*)block_buf, res.value());
        read_size += res.value();
        off_in_buf += _block_size;
    }

    return read_size;
}

Status BlockCache::read_cache_zero_copy(const CacheKey& cache_key, off_t offset, size_t size, const char** buf) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "read block key: " << cache_key << " with invalid offset: "
                     << offset << ", size: " << size;
        return Status::InvalidArgument(strings::Substitute("offset and size must be aligned by block size $0",
                                                            _block_size));
    }
    if (!buf) {
        return Status::InvalidArgument("invalid data buffer");
    }
    if (size == 0) {
        return Status::OK();
    }

    size_t index = offset / _block_size;
    std::string block_key = fmt::format("{}/{}", cache_key, index);
    return _kv_cache->read_cache_zero_copy(block_key, buf);
}

Status BlockCache::remove_cache(const CacheKey& cache_key, off_t offset, size_t size) {
    if (offset % _block_size != 0) {
        LOG(WARNING) << "remove block key: " << cache_key << " with invalid args, offset: "
                     << offset << ", size: " << size;
        return Status::InvalidArgument(strings::Substitute("offset and size must be aligned by block size $0",
                                                            _block_size));
    }
    if (size == 0) {
        return Status::OK();
    }

    size_t start_block_index = offset / _block_size;
    size_t end_block_index = (offset + size - 1) / _block_size + 1;
    for (size_t index = start_block_index; index < end_block_index; ++index) {
        std::string block_key = fmt::format("{}/{}", cache_key, index);
        RETURN_IF_ERROR(_kv_cache->remove_cache(block_key));
    }
    return Status::OK();
}

} // namespace starrocks
