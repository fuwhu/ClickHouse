#include "FileCacheSettings.h"

#include <filesystem>
#include <sys/statfs.h>
#include <Common/Exception.h>


namespace DB
{

void FileCacheSettings::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (!config.has(config_prefix + ".data_cache_path") || !config.has(config_prefix + ".metadata_path"))
    {
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "{}.data_cache_path or {}.metadata_path must not be empty", config_prefix, config_prefix);
    }

    base_path = config.getString(config_prefix + ".data_cache_path");
    auto metadata_path = config.getString(config_prefix + ".metadata_path");

    if (metadata_path == base_path)
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Metadata path and cache base path must be different: {}", metadata_path);

    if (config.has(config_prefix + ".data_cache_max_size"))
        max_size = config.getUInt64(config_prefix + ".data_cache_max_size");
    else
    {
        if (config.has(config_prefix + ".data_cache_disk_max_ratio"))
        {
            if (!std::filesystem::exists(base_path))
                std::filesystem::create_directories(base_path);
            struct statfs buf;
            statfs(base_path.c_str(), &buf);
            size_t total_size = buf.f_bsize * buf.f_blocks;
            max_size = total_size * config.getDouble(config_prefix + ".data_cache_disk_max_ratio");
        }
    }

    if (config.has(config_prefix + ".data_cache_max_elements"))
        max_element_size = config.getUInt64(config_prefix + ".data_cache_max_elements");
    else
    {
        if (config.has(config_prefix + ".avg_file_segment_size"))
            max_element_size = max_size / config.getUInt64(config_prefix + ".avg_file_segment_size");
    }

    if (config.has(config_prefix + ".max_file_segment_size"))
        max_file_segment_size = config.getUInt64(config_prefix + ".max_file_segment_size");

    if (config.has(config_prefix + ".max_clean_segment_per_turn"))
        max_clean_segment_per_turn = config.getInt(config_prefix + ".max_clean_segment_per_turn");

    if (config.has(config_prefix + ".min_clean_interval_seconds"))
        min_clean_interval_seconds = config.getInt(config_prefix + ".min_clean_interval_seconds");

    if (config.has(config_prefix + ".clean_interval_seconds"))
        clean_interval_seconds = config.getInt(config_prefix + ".clean_interval_seconds");
    if (min_clean_interval_seconds > clean_interval_seconds)
        min_clean_interval_seconds = clean_interval_seconds;

    if (config.has(config_prefix + ".clean_ttl"))
        clean_ttl = config.getInt(config_prefix + ".clean_ttl");

    if (config.has(config_prefix + ".async_clean_ratio"))
        async_clean_ratio = config.getDouble(config_prefix + ".async_clean_ratio");
}

}
