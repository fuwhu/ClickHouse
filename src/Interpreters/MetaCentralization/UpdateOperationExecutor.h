#pragma once

#include <unordered_set>
#include <vector>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MetadataCentralizationManager;

class ManifestCache;
using ManifestCachePtr = std::unique_ptr<ManifestCache>;

class MetadataApplicator;
using MetadataApplicatorPtr = std::unique_ptr<MetadataApplicator>;

struct Manifest;
using ManifestPtr = std::shared_ptr<Manifest>;

struct Database;
struct Table;

/// Executes metadata update operations by comparing Boss manifest with local cache
class UpdateOperationExecutor : public WithContext
{
public:
    enum class OperationType
    {
        CREATE_DATABASE,
        UPDATE_TABLE,
        DROP_TABLE,
        DROP_DATABASE,
        CREATE_TABLE,
    };

    struct Operation
    {
        OperationType type;
        String database_uuid;
        String database_name;
        String table_uuid;
        String table_name;
        String new_key;
        UInt32 new_version;
        String new_last_modified;
        String old_key;
        String sql;
        bool drop_sync = false;

        const Database * db_ptr = nullptr;
        const Table * table_ptr = nullptr;
    };

    struct Result
    {
        bool success = false;
        Operation operation;
        String error_message;
    };

    UpdateOperationExecutor(
        MetadataCentralizationManager * manager_,
        const MetadataApplicatorPtr & applicator_,
        ContextPtr context_);

    std::vector<Operation> planUpdates(const ManifestPtr & boss_manifest, bool drop_sync = false);

    Result executeUpdate(const Operation & op);

    void applyCacheUpdates(const std::vector<Result> & results);

private:
    MetadataCentralizationManager * manager;
    const MetadataApplicatorPtr & applicator;
    LoggerPtr log;

    std::vector<Operation> planDatabaseOperations(const ManifestPtr & boss_manifest);

    std::vector<Operation> planTableOperations(const ManifestPtr & boss_manifest);

    std::vector<Operation> planDropOperations(
        const std::unordered_set<String> & boss_db_uuids,
        const std::unordered_set<String> & boss_table_uuids,
        bool drop_sync);

    String downloadDatabaseSQL(const String & key) const;

    String downloadTableSQL(const String & key) const;
};

using UpdateOperationExecutorPtr = std::unique_ptr<UpdateOperationExecutor>;

}
