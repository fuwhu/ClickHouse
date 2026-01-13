#pragma once

#include <memory>
#include <string>

#include <config.h>

#if USE_AWS_S3

#include <IO/S3/Client.h>
#include <Common/logger_useful.h>

namespace DB
{

class BossClient
{
public:
    /// Initialize client - throws exception on failure
    BossClient(
        const std::string & endpoint,
        const std::string & access_key,
        const std::string & secret_key,
        const std::string & region,
        const int & boss_max_redirects,
        const int & boss_retry_attempts);

    /// Upload content to Boss storage
    /// Returns the ETag of the uploaded object
    String upload(const std::string & key, const std::string & content);

    /// Download content from Boss storage
    /// Returns a pair of {content, etag}
    std::pair<String, String> download(const std::string & key);

    /// Object metadata structure
    struct ObjectMetadata
    {
        String key;
        std::chrono::system_clock::time_point last_modified;
        size_t size;
    };

    /// List objects with metadata (including last_modified time)
    /// Returns a vector of objects with their metadata
    std::vector<ObjectMetadata> listObjectsWithMetadata();

    /// Delete multiple objects from Boss storage
    /// Returns the number of successfully deleted objects
    size_t deleteObjects(const std::vector<String> & keys);

private:
    std::shared_ptr<const S3::Client> client;
    LoggerPtr log;
    std::unique_ptr<S3::URI> uri;
    String prefix;
    String buildFullKey(const String & key) const;
    String removeUriKey(const String & key) const;
};

using BossClientPtr = std::shared_ptr<BossClient>;

}

#endif
