#pragma once

#include <memory>

#include <base/types.h>
#include <base/UUID.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <Interpreters/StorageID.h>

namespace DB
{

class ASTCreateQuery;
class AlterCommands;
class IDatabase;
using DatabasePtr = std::shared_ptr<IDatabase>;

/// Centralized DDL executor for metadata centralization.
/// Coordinates DDL operations with remote Boss storage and cluster synchronization.
class DDLCentralized : public WithContext
{
public:
    explicit DDLCentralized(ContextPtr context_);

    ~DDLCentralized();

    void executeCreateDatabase(ASTCreateQuery & create_query);

    void executeCreateTable(ASTCreateQuery & create_query);

    void executeAlterTable(const String & database_name, const String & table_name, AlterCommands & alter_commands);

    void executeDropTable(const String & database_name, const String & table_name, bool if_exists, bool drop_sync);

    void executeDropDatabase(const String & database_name, bool if_exists, bool drop_sync);

private:
    MetadataCentralizationManagerPtr manager;
    LoggerPtr log;

    String getDatabaseBossKey(const String & database, UInt32 version) const;

    String getTableBossKey(const String & database_uuid, const String & table, UInt32 version) const;

    void checkBossAvailableFlag(const String & operation_description) const;

    /// Synchronize metadata from remote Boss before operation
    void syncBeforeOperation(const String & operation_description);

    void validateDatabaseCreation(const String & database_name, bool if_not_exists);

    void validateDatabaseLimit();

    UUID prepareDatabase(ASTCreateQuery & create, const String & database_name);

    UUID prepareTable(ASTCreateQuery & create, const String & database_name, const String & table_name);

    String generateSQL(ASTCreateQuery & create);

    String applyAlterCommands(
        const String & database_name,
        const String & table_name,
        UUID table_uuid,
        AlterCommands & alter_commands);

    void submitDatabaseToBoss(
        const String & database_name,
        const UUID & database_uuid,
        const String & database_sql);

    void submitTableToBoss(
        const String & database_name,
        const String & table_name,
        const UUID & database_uuid,
        const UUID & table_uuid,
        const String & table_sql);

    void syncMetadataOfAllNodes(bool drop_sync = false);

    String getCurrentTimestamp() const;

    /// Modify manifest and upload to remote Boss
    template <typename Func>
    void modifyManifest(const String & operation_name, Func && modifier, ManifestPtr & manifest);

    std::reference_wrapper<Database> findDatabaseOrThrow(ManifestPtr & manifest, const UUID & database_uuid, const String & database_name);

    void validateTableCreation(
        const DatabasePtr & database,
        const String & database_name,
        const String & table_name,
        const ASTCreateQuery & create) const;

    void validateTableDataPath(
        const String & data_path,
        const ASTCreateQuery & create) const;

    void validateTableLimit(const ASTCreateQuery & create) const;

    void validateVirtualColumns(ASTCreateQuery & create);

    struct DropDatabaseInfo
    {
        UUID database_uuid;
        std::vector<std::pair<String, UUID>> tables; // table_name, table_uuid pairs
    };

    struct TableUploadInfo
    {
        UUID database_uuid;
        UUID table_uuid;
        String table_sql;
    };

    TableUploadInfo prepareTableForUpload(
        const String & database_name,
        const String & table_name,
        ASTCreateQuery & create);

    struct DropTableInfo
    {
        UUID table_uuid;
        UUID database_uuid;
        StorageID table_id;
    };

    DropTableInfo validateAndPrepareDropTable(
        const String & database_name,
        const String & table_name,
        bool if_exists);

    DropDatabaseInfo validateAndPrepareDropDatabase(
        const String & database_name,
        bool if_exists);
};

using DDLCentralizedPtr = std::unique_ptr<DDLCentralized>;

}
