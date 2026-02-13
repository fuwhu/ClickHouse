#pragma once

#include <shared_mutex>
#include <unordered_map>

#include <Common/logger_useful.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>

namespace DB
{

/// In-memory cache for manifest data with thread-safe access
class ManifestCache
{
public:
    ManifestCache();

    /// Load manifest into cache
    void load(const ManifestPtr & manifest);

    void updateDatabase(const Database & db, bool is_create = false);

    void updateTable(const Table & table, bool is_create = false);

    void removeDatabase(const String & uuid);

    void removeTable(const String & uuid);

    String findDatabaseKey(const String & uuid) const;

    String findTableKey(const String & uuid) const;

    /// Get all cached tables (UUID -> key mapping)
    std::unordered_map<String, String> getAllTables() const;

    /// Get all cached databases (UUID -> key mapping)
    std::unordered_map<String, String> getAllDatabases() const;

    void updateEtag(const String & etag);

    void updateVersion(UInt64 version);

    void updateLastModified(const String & last_modified);

    String getEtag() const;

    /// Check if cache has been loaded
    bool isLoaded() const;

    /// Clone manifest without etag
    ManifestPtr cloneManifestWithoutEtag() const;


private:
    mutable std::shared_mutex cache_mutex;  /// Protects all members below

    std::unordered_map<String, String> database_key_map;  /// db_uuid -> db_key
    std::unordered_map<String, String> table_key_map;     /// tb_uuid -> tb_key

    std::unordered_map<String, String> database_name_map; /// db_uuid -> db_name
    std::unordered_map<String, String> table_name_map;    /// tb_uuid -> tb_name

    ManifestPtr manifest;

    bool is_loaded = false;

    LoggerPtr log;

    void updateMappings(const ManifestPtr & manifest);
};

using ManifestCachePtr = std::unique_ptr<ManifestCache>;

}
