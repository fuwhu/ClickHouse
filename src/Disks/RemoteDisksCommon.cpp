#include <Disks/RemoteDisksCommon.h>
#include <Common/getRandomASCIIString.h>
#include <Common/FileCacheFactory.h>
#include <Common/FileCache.h>

namespace DB
{

namespace ErrorCodes
{extern const int BAD_ARGUMENTS;
}

std::shared_ptr<DiskCacheWrapper> wrapWithCache(
    std::shared_ptr<IDisk> disk, String cache_name, String cache_path, String metadata_path)
{
    if (metadata_path == cache_path)
        throw Exception("Metadata and cache paths should be different: " + metadata_path, ErrorCodes::BAD_ARGUMENTS);

    auto cache_disk = std::make_shared<DiskLocal>(cache_name, cache_path, 0);
    auto cache_file_predicate = [] (const String & path)
    {
        return path.ends_with("idx") // index files.
                || path.ends_with("mrk") || path.ends_with("mrk2") || path.ends_with("mrk3") /// mark files.
                || path.ends_with("txt") || path.ends_with("dat");
    };

    return std::make_shared<DiskCacheWrapper>(disk, cache_disk, cache_file_predicate);
}

static String getDiskMetadataPath(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    return config.getString(config_prefix + ".metadata_path", context->getPath() + "disks/" + name + "/");
}

std::pair<String, DiskPtr> prepareForLocalMetadata(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    /// where the metadata files are stored locally
    auto metadata_path = getDiskMetadataPath(name, config, config_prefix, context);
    fs::create_directories(metadata_path);
    auto metadata_disk = std::make_shared<DiskLocal>(name + "-metadata", metadata_path, 0);
    return std::make_pair(metadata_path, metadata_disk);
}


FileCachePtr getCachePtrForDisk(
    const String & name,
    const Poco::Util::AbstractConfiguration & config,
    const String & config_prefix,
    ContextPtr context)
{
    bool data_cache_enabled = context->getSettings().get(name + "_data_cache_enabled").get<bool>();
    if (!data_cache_enabled)
        return nullptr;

    FileCacheSettings settings;
    settings.loadFromConfig(config, config_prefix);

    auto cache = FileCacheFactory::instance().getOrCreate(settings);
    cache->initialize();
    return cache;
}

}
