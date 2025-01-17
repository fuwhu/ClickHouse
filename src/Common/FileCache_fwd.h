#pragma once
#include <memory>

namespace DB
{

static constexpr size_t FILE_CACHE_DEFAULT_MAX_SIZE = 1024 * 1024 * 1024; // 1GB
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE = 100 * 1024 * 1024;
static constexpr int REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS = 1024 * 1024;
static constexpr int FILE_CACHE_CLEANER_DEFAULT_INTERVAL_SECOND = 10 * 60; // 10 min
static constexpr int FILE_CACHE_CLEANER_DEFAULT_MAX_SEGMENT_PER_ROUND = 1000;
static constexpr int FILE_CACHE_CLEANER_DEFAULT_TTL = 3600; // 1hour
static constexpr double FILE_CACHE_START_ASYNC_CLEAN_RATIO = 1;

class IFileCache;
using FileCachePtr = std::shared_ptr<IFileCache>;

}
