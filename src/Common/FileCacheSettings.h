#pragma once

#include <string>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/FileCache_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
}

struct FileCacheSettings
{
    std::string base_path;

    size_t max_size = FILE_CACHE_DEFAULT_MAX_SIZE;
    double async_clean_ratio = FILE_CACHE_START_ASYNC_CLEAN_RATIO;
    size_t max_element_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_ELEMENTS;
    size_t max_file_segment_size = REMOTE_FS_OBJECTS_CACHE_DEFAULT_MAX_FILE_SEGMENT_SIZE;
    int clean_interval_seconds = FILE_CACHE_CLEANER_DEFAULT_INTERVAL_SECOND;
    int min_clean_interval_seconds = FILE_CACHE_CLEANER_DEFAULT_INTERVAL_SECOND;
    int max_clean_segment_per_turn = FILE_CACHE_CLEANER_DEFAULT_MAX_SEGMENT_PER_ROUND;
    int clean_ttl = FILE_CACHE_CLEANER_DEFAULT_TTL;

    void loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string &  config_prefix);
};

}
