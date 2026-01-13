#include <IO/Boss/BossClient.h>

#if USE_AWS_S3

#include <Common/RemoteHostFilter.h>
#include <Common/Exception.h>
#include <IO/HTTPHeaderEntries.h>
#include <IO/ReadBufferFromS3.h>
#include <IO/ReadHelpers.h>
#include <IO/S3Common.h>
#include <IO/S3RequestSettings.h>
#include <IO/S3/Requests.h>
#include <IO/WriteBufferFromS3.h>

#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ObjectIdentifier.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
}

namespace S3RequestSetting
{
    extern const S3RequestSettingsUInt64 max_single_read_retries;
    extern const S3RequestSettingsUInt64 max_unexpected_write_error_retries;
}

BossClient::BossClient(const String & endpoint,
                       const String & access_key,
                       const String & secret_key,
                       const String & region,
                       const int & boss_max_redirects,
                       const int & boss_retry_attempts)
    : log(getLogger("BossClient"))
{
    LOG_DEBUG(log, "Initializing BossClient with endpoint: {}", endpoint);

    try
    {
        // Parse the endpoint URI to extract bucket and path
        uri = std::make_unique<S3::URI>(endpoint);
        
        LOG_DEBUG(log, "Parsed URI - bucket: '{}', key: '{}', endpoint: '{}'", 
                 uri->bucket, uri->key, uri->endpoint);

        // Use uri->key as the prefix for listing objects
        prefix = uri->key;
        if (!prefix.empty() && prefix.back() != '/')
            prefix += '/';

        LOG_DEBUG(log, "Prefix: {}", prefix);

        RemoteHostFilter remote_host_filter;
        bool slow_all_threads_after_network_error = true;
        bool enable_logging = false;

        S3::PocoHTTPClientConfiguration config =
            S3::ClientFactory::instance().createClientConfiguration(
                region,
                remote_host_filter,
                boss_max_redirects,
                boss_retry_attempts,
                slow_all_threads_after_network_error,
                enable_logging,
                /*for_disk_s3*/ false,
                {},
                {},
                "http");

        // Use the endpoint without the path (just scheme + host)
        config.endpointOverride = uri->endpoint;

        S3::ClientSettings settings{
            .use_virtual_addressing = false,
            .disable_checksum = false,
        };

        S3::ServerSideEncryptionKMSConfig sse_kms_config;
        HTTPHeaderEntries headers;

        client = S3::ClientFactory::instance().create(
            config,
            settings,
            access_key,
            secret_key,
            "",
            sse_kms_config,
            headers,
            S3::CredentialsConfiguration{}
        );

        LOG_INFO(log, "BossClient initialized successfully. Bucket: {}, Key: {}", 
                uri->bucket, uri->key);
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Failed to initialize Boss client: code {} msg {}", e.code(), e.message());
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to initialize Boss client: {}",
            e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Failed to initialize Boss client: {}", e.what());
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to initialize Boss client: {}",
            e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "Failed to initialize Boss client with unknown error");
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to initialize Boss client with unknown error");
    }
}

String BossClient::upload(const String & key, const String & content)
{
    String full_key = buildFullKey(key);
    LOG_DEBUG(log, "Uploading to Boss, bucket: {}, key: {}, full key: {}, content size: {}", 
             uri->bucket, key, full_key, content.size());

    try
    {
        S3::S3RequestSettings request_settings;
        request_settings[S3RequestSetting::max_unexpected_write_error_retries] = 1;

        WriteBufferFromS3 buffer(
            client,
            uri->bucket,
            full_key,
            DBMS_DEFAULT_BUFFER_SIZE,
            request_settings,
            {});

        buffer.write(content.data(), content.size());
        buffer.finalize();

        String etag = buffer.getETag();
        LOG_DEBUG(log, "Upload successful, etag: {}", etag);

        return etag;
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Upload failed for bucket {} key {} (full key {}): code {} msg {}", 
                 uri->bucket, key, full_key, e.code(), e.message());
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to upload to Boss (bucket={}, key={}): {}", 
                       uri->bucket, key, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Upload failed for bucket {} key {} (full key {}): {}", 
                 uri->bucket, key, full_key, e.what());
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to upload to Boss (bucket={}, key={}): {}", 
                       uri->bucket, key, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "Upload failed for bucket {} key {} (full key {}) with unknown error", 
                 uri->bucket, key, full_key);
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to upload to Boss (bucket={}, key={}) with unknown error", 
                       uri->bucket, key);
    }
}

std::pair<String, String> BossClient::download(const String & key)
{
    String full_key = buildFullKey(key);
    LOG_DEBUG(log, "Downloading from Boss, bucket: {}, key: {}, full key: {}", 
             uri->bucket, key, full_key);

    try
    {
        S3::S3RequestSettings request_settings;
        request_settings[S3RequestSetting::max_single_read_retries] = 1;

        ReadSettings read_settings;
        String version_id;

        ReadBufferFromS3 buffer(
            client,
            uri->bucket,
            full_key,
            version_id,
            request_settings,
            read_settings);

        String content;
        readStringUntilEOF(content, buffer);

        String etag = buffer.getETag();
        LOG_DEBUG(log, "Download successful, etag: {}, content size: {}", etag, content.size());

        return {content, etag};
    }
    catch (const Exception & e)
    {
        LOG_ERROR(log, "Download failed for bucket {} key {} (full key {}): code {} msg {}", 
                 uri->bucket, key, full_key, e.code(), e.message());
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to download from Boss (bucket={}, key={}): {}", 
                       uri->bucket, key, e.message());
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "Download failed for bucket {} key {} (full key {}): {}", 
                 uri->bucket, key, full_key, e.what());
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to download from Boss (bucket={}, key={}): {}", 
                       uri->bucket, key, e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "Download failed for bucket {} key {} (full key {}) with unknown error", 
                 uri->bucket, key, full_key);
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR, 
                       "Failed to download from Boss (bucket={}, key={}) with unknown error", 
                       uri->bucket, key);
    }
}

std::vector<BossClient::ObjectMetadata> BossClient::listObjectsWithMetadata()
{       
    LOG_DEBUG(log, "Listing objects with metadata from Boss, bucket: {}, prefix: {}", 
             uri->bucket, prefix);

    try
    {
        std::vector<ObjectMetadata> objects;

        S3::ListObjectsV2Request request;
        request.SetBucket(uri->bucket);
        if (!prefix.empty())
            request.SetPrefix(prefix);

        bool is_truncated = true;
        while (is_truncated)
        {
            auto outcome = client->ListObjectsV2(request);

            if (!outcome.IsSuccess())
            {
                const auto & error = outcome.GetError();
                LOG_ERROR(log, "ListObjectsV2 failed: {}", error.GetMessage());
                throw Exception(
                    ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                    "Failed to list objects with metadata from Boss (bucket={}, prefix={}): {}",
                    uri->bucket,
                    prefix,
                    error.GetMessage());
            }

            const auto & result = outcome.GetResult();
            const auto & contents = result.GetContents();

            for (const auto & object : contents)
            {
                ObjectMetadata metadata;
                metadata.key = removeUriKey(object.GetKey());
                metadata.size = object.GetSize();

                /// Convert AWS DateTime to std::chrono::system_clock::time_point
                const auto & last_mod = object.GetLastModified();
                auto millis = last_mod.Millis();
                metadata.last_modified = std::chrono::system_clock::time_point(
                    std::chrono::milliseconds(millis));

                objects.push_back(metadata);
            }

            is_truncated = result.GetIsTruncated();
            if (is_truncated)
            {
                request.SetContinuationToken(result.GetNextContinuationToken());
            }
        }

        LOG_DEBUG(log, "Listed {} objects with metadata (bucket: {}, prefix: {})", 
                 objects.size(), uri->bucket, prefix);
        return objects;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "ListObjectsWithMetadata failed for bucket {} prefix {}: {}", 
                 uri->bucket, prefix, e.what());
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to list objects with metadata from Boss (bucket={}, prefix={}): {}",
            uri->bucket,
            prefix.empty() ? "<empty>" : prefix,
            e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "ListObjectsWithMetadata failed for bucket {} prefix {} with unknown error", 
                 uri->bucket, prefix);
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to list objects with metadata from Boss (bucket={}, prefix={}) with unknown error",
            uri->bucket,
            prefix);
    }
}

size_t BossClient::deleteObjects(const std::vector<String> & keys)
{
    if (keys.empty())
    {
        LOG_DEBUG(log, "No objects to delete");
        return 0;
    }

    LOG_DEBUG(log, "Deleting {} objects from Boss, bucket: {}", keys.size(), uri->bucket);

    try
    {
        S3::DeleteObjectsRequest request;
        request.SetBucket(uri->bucket);

        Aws::S3::Model::Delete delete_objects;
        for (const auto & key : keys)
        {
            String full_key = buildFullKey(key);
            Aws::S3::Model::ObjectIdentifier object;
            object.SetKey(full_key);
            delete_objects.AddObjects(object);
        }

        request.SetDelete(delete_objects);

        auto outcome = client->DeleteObjects(request);

        if (!outcome.IsSuccess())
        {
            const auto & error = outcome.GetError();
            LOG_ERROR(log, "DeleteObjects failed: {}", error.GetMessage());
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "Failed to delete objects from Boss (bucket={}): {}",
                uri->bucket,
                error.GetMessage());
        }

        const auto & result = outcome.GetResult();
        const auto & deleted = result.GetDeleted();
        const auto & errors = result.GetErrors();

        size_t deleted_count = deleted.size();

        if (!errors.empty())
        {
            LOG_WARNING(log, "Some objects failed to delete: {}/{}", errors.size(), keys.size());
            for (const auto & error : errors)
            {
                LOG_WARNING(log, "Failed to delete {}: {}", error.GetKey(), error.GetMessage());
            }
        }

        LOG_DEBUG(log, "Successfully deleted {}/{} objects from bucket {}", 
                 deleted_count, keys.size(), uri->bucket);
        return deleted_count;
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        LOG_ERROR(log, "DeleteObjects failed: {}", e.what());
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to delete objects from Boss (bucket={}): {}",
            uri->bucket,
            e.what());
    }
    catch (...)
    {
        LOG_ERROR(log, "DeleteObjects failed with unknown error");
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Failed to delete objects from Boss (bucket={}) with unknown error",
            uri->bucket);
    }
}

String BossClient::buildFullKey(const String & key) const
{
    return prefix.empty() ? key : prefix + key;
}

String BossClient::removeUriKey(const String & key) const
{
    return prefix.empty() ? key : key.substr(prefix.size());
}

}

#endif // USE_AWS_S3
