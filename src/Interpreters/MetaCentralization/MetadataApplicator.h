#pragma once

#include <memory>

#include <base/types.h>
#include <base/UUID.h>
#include <Common/logger_useful.h>
#include "Interpreters/DatabaseCatalog.h"
#include <Databases/LoadingStrictnessLevel.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class TableDetector;
using TableDetectorPtr = std::unique_ptr<TableDetector>;

class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

class IStorage;
using StoragePtr = std::shared_ptr<IStorage>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

struct Database;
struct Table;
struct StorageInMemoryMetadata;

class ASTCreateQuery;
class IAST;
using ASTPtr = std::shared_ptr<IAST>;

class MetadataCentralizationManager;

/// Applies metadata changes from Boss storage to local database catalog
class MetadataApplicator : public WithMutableContext
{
public:
    MetadataApplicator(MetadataCentralizationManager * manager_, ContextMutablePtr context);
    ~MetadataApplicator();

    /// Apply database changes (create or update)
    void applyDatabaseChanges(const Database & db, const String & db_sql);

    /// Apply table changes (create, update, or attach)
    void applyTableChanges(const Table & table, const String & database_name, const String & table_sql);

    /// Drop table from local database catalog
    void dropTable(const String & database_name, const String & table_name, const String & table_uuid, bool drop_sync = false) const;

    /// Drop database from local database catalog
    void dropDatabase(const String & database_name, const String & database_uuid, bool drop_sync = false) const;

    static UUID parseUUIDFromString(const String & uuid_str);

private:
    MetadataCentralizationManager * manager;
    TableDetectorPtr table_detector;
    LoggerPtr log;

    void createDatabase(const Database & db, const ASTCreateQuery & create_query, const String & db_sql) const;

    void updateDatabase(const Database & db) const;

    void updateTable(const Table & table, const String & database_name, const UUID & table_uuid, ASTCreateQuery & create_query) const;

    void dropTableImpl(const String & database_name, const String & table_name, bool drop_sync) const;

    void dropDatabaseImpl(const String & database_name, bool drop_sync, std::vector<std::pair<String, UUID>> & tables) const;

    void updateZooKeeperMetadataForReplicatedTable(
        const StoragePtr & storage,
        const StorageInMemoryMetadata & new_metadata) const;

    ASTPtr parseDatabaseSQL(const String & db_sql, const String & database_name) const;

    ASTPtr parseTableSQL(const String & table_sql, const String & database_name, const String & table_name) const;

    std::pair<DatabasePtr, String> getDatabaseAndDataPath(const ASTCreateQuery & create_query) const;

    StoragePtr createStorage(
        ASTCreateQuery & create_query,
        const String & data_path,
        LoadingStrictnessLevel mode) const;

    /// Table scenario detection result
    struct TableScenario
    {
        bool should_attach;    /// Whether table should be attached (vs created)
        bool data_exists;      /// Whether table data exists on disk
        bool replica_in_zk;    /// Whether replica metadata exists in ZooKeeper
        bool is_replicated;    /// Whether table is a replicated table
    };

    TableScenario detectTableScenario(const ASTCreateQuery & create_query, const DatabaseCatalog & catalog) const;

    void writeDatabaseMetadataFile(
        DiskPtr disk,
        const String & database_name,
        const String & sql_content) const;

    void createAndRegisterTable(
        const Table & table,
        ASTPtr ast,
        LoadingStrictnessLevel mode) const;
};

using MetadataApplicatorPtr = std::unique_ptr<MetadataApplicator>;

}
