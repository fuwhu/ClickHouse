#include "FileCacheFactory.h"
#include "FileCache.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

FileCacheFactory & FileCacheFactory::instance()
{
    static FileCacheFactory ret;
    return ret;
}

FileCachePtr FileCacheFactory::getImpl(const std::string & cache_base_path, std::lock_guard<std::mutex> &)
{
    auto it = caches.find(cache_base_path);
    if (it == caches.end())
        return nullptr;
    return it->second;
}

FileCachePtr FileCacheFactory::getOrCreate(const FileCacheSettings & settings)
{
    std::lock_guard lock(mutex);
    auto cache = getImpl(settings.base_path, lock);
    if (cache)
    {
        if (cache->capacity() != settings.max_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cache with path `{}` already exists, but has different max size", settings.base_path);
        return cache;
    }

    cache = std::make_shared<LRUFileCache>(settings);
    caches.emplace(settings.base_path, cache);
    return cache;
}

}
