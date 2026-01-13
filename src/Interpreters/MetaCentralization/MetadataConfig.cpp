#include <Interpreters/MetaCentralization/MetadataConfig.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

MetadataCentralizationConfig MetadataCentralizationConfig::fromConfig(const Poco::Util::AbstractConfiguration & config)
{
    MetadataCentralizationConfig cfg;

    cfg.enabled = config.getBool("metadata_centralization.enable", false);

    if (!cfg.enabled)
        return cfg;

    cfg.enable_ddl = config.getBool("metadata_centralization.enable_ddl", true);

    cfg.endpoint = config.getString("metadata_centralization.endpoint", "");
    cfg.access_key_id = config.getString("metadata_centralization.access_key_id", "");
    cfg.secret_access_key = config.getString("metadata_centralization.secret_access_key", "");
    cfg.region = config.getString("metadata_centralization.region", "");

    cfg.lock_path = config.getString("metadata_centralization.lock_path", "/clickhouse/metadata_centralization/locks");
    cfg.lock_key = config.getString("metadata_centralization.lock_key", "meta_centralized_");
    cfg.dist_lock_timeout_ms = std::chrono::milliseconds(config.getUInt("metadata_centralization.dist_lock_timeout_ms", 30000));
    cfg.local_lock_timeout_ms = std::chrono::milliseconds(config.getUInt("metadata_centralization.local_lock_timeout_ms", 30000));
    cfg.sync_interval_seconds = config.getInt("metadata_centralization.sync_interval_seconds", 60);
    cfg.recovery_interval_seconds = config.getInt("metadata_centralization.recovery_interval_seconds", 60);
    cfg.cleanup_markers_path
        = config.getString("metadata_centralization.cleanup_markers_path", "/clickhouse/metadata_centralization/cleanup_markers");
    cfg.cleanup_interval_seconds = config.getInt("metadata_centralization.cleanup_interval_seconds", 3600);
    cfg.cleanup_retention_days = config.getInt("metadata_centralization.cleanup_retention_days", 3);
    cfg.cluster_name = config.getString("metadata_centralization.cluster_name", "");
    cfg.distributed_ddl_task_timeout = config.getInt("metadata_centralization.distributed_ddl_task_timeout", 20);
    cfg.boss_max_redirects = config.getInt("metadata_centralization.boss_max_redirects", 10);
    cfg.boss_retry_attempts = config.getInt("metadata_centralization.boss_retry_attempts", 3);

    return cfg;
}

void MetadataCentralizationConfig::validate() const
{
    if (!enabled)
        return;

    if (endpoint.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.endpoint is required when metadata centralization is enabled");
    }

    if (access_key_id.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.access_key_id is required when metadata centralization is enabled");
    }

    if (secret_access_key.empty())
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.secret_access_key is required when metadata centralization is enabled");
    }

    if (lock_path.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.lock_path cannot be empty");
    }

    if (lock_key.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.lock_key cannot be empty");
    }

    if (dist_lock_timeout_ms.count() <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.dist_lock_timeout_ms must be positive");
    }

    if (local_lock_timeout_ms.count() <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.local_lock_timeout_ms must be positive");
    }

    if (sync_interval_seconds < 10)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.sync_interval_seconds must be greater than or equal to 10 seconds");
    }

    if (recovery_interval_seconds < 10)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.recovery_interval_seconds must be greater than or equal to 10 seconds");
    }

    if (cleanup_markers_path.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.cleanup_markers_path cannot be empty");
    }

    if (cleanup_interval_seconds < 300)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.cleanup_interval_seconds must be greater than or equal to 300 sseconds");
    }

    if (cleanup_retention_days <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.cleanup_retention_days must be positive");
    }

    if (cluster_name.empty())
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.cluster_name cannot be empty");
    }

    if (distributed_ddl_task_timeout <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.distributed_ddl_task_timeout must be positive");
    }

    if (boss_max_redirects <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.boss_max_redirects must be positive");
    }

    if (boss_retry_attempts <= 0)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "metadata_centralization.boss_retry_attempts must be positive");
    }
}

}
