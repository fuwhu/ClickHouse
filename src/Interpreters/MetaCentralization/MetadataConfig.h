#pragma once

#include <base/types.h>
#include <chrono>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace DB
{

/// Configuration for metadata centralization feature.
struct MetadataCentralizationConfig
{
    bool enabled = false;               /// Enable metadata centralization feature

    bool enable_ddl = true;             /// Allow centralized DDL operations

    String endpoint;                    /// Boss service endpoint URL

    String access_key_id;               /// Boss access key
    String secret_access_key;           /// Boss secret key

    String region = "uat";        /// Boss region

    String lock_path = "/clickhouse/metadata_centralization/locks";  /// ZooKeeper lock path

    String lock_key = "meta_centralized_";  /// ZooKeeper lock key

    std::chrono::milliseconds dist_lock_timeout_ms{30000};  /// Distributed lock timeout

    std::chrono::milliseconds local_lock_timeout_ms{30000}; /// Local lock timeout

    Int32 sync_interval_seconds = 60;   /// Background sync interval in seconds

    Int32 recovery_interval_seconds = 60;   /// Background recover boss available interval in seconds

    String cleanup_markers_path = "/clickhouse/metadata_centralization/cleanup_markers";  /// ZooKeeper cleanup markers path

    Int32 cleanup_interval_seconds = 3600;  /// History cleanup check interval in seconds (default 1 hour)

    Int32 cleanup_retention_days = 3;    /// Number of days to retain history files (default 3 days)

    String cluster_name;                /// Cluster name for distributed notifications

    Int32 distributed_ddl_task_timeout = 20;    /// Centralization Distributed DDL task timeout

    Int32 boss_max_redirects = 10;               /// Boss max redirects

    Int32 boss_retry_attempts = 3;           /// Boss retry attempts

    bool generate_local_manifest_from_local_metadata = false;  /// Allow generation of local manifest from local metadata if local manifest doesn't exist

    bool initialize_centralized_metadata = false;    /// Allow initialization of centralized manifest

    /// Parse configuration from Poco config
    static MetadataCentralizationConfig fromConfig(const Poco::Util::AbstractConfiguration & config);

    /// Validate configuration parameters
    void validate() const;

    /// Check if DDL operations are allowed
    bool isDDLAllowed() const { return enable_ddl; }
};

}
