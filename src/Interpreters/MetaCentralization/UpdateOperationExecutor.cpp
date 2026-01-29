#include <Interpreters/MetaCentralization/UpdateOperationExecutor.h>

#include <Common/Exception.h>
#include <Databases/IDatabase.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/MetaCentralization/ManifestCache.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/MetadataApplicator.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <IO/Boss/BossClient.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
    extern const int TOO_MANY_TABLES;
}

UpdateOperationExecutor::UpdateOperationExecutor(
    MetadataCentralizationManager * manager_,
    const MetadataApplicatorPtr & applicator_,
    ContextPtr context_)
    : WithContext(context_)
    , manager(manager_)
    , applicator(applicator_)
    , log(getLogger("UpdateOperationExecutor"))
{
}

std::vector<UpdateOperationExecutor::Operation> UpdateOperationExecutor::planUpdates(
    const ManifestPtr & boss_manifest, bool drop_sync)
{
    LOG_DEBUG(log, "Planning updates with drop_sync={}", drop_sync);

    std::unordered_set<String> boss_db_uuids;
    std::unordered_set<String> boss_table_uuids;

    for (const auto & boss_db : boss_manifest->databases) 
    {
        boss_db_uuids.insert(boss_db.uuid);
        for (const auto & boss_table : boss_db.tables)
            boss_table_uuids.insert(boss_table.uuid);
    }

    auto drop_db_table_ops = planDropOperations(boss_db_uuids, boss_table_uuids, drop_sync);
    auto database_ops = planDatabaseOperations(boss_manifest);
    auto table_ops = planTableOperations(boss_manifest);

    /// Order: drop database, drop table, create database, update table, create table.
    std::vector<Operation> operations;
    operations.reserve(drop_db_table_ops.size() + database_ops.size() + table_ops.size());

    operations.insert(operations.end(), drop_db_table_ops.begin(), drop_db_table_ops.end());
    operations.insert(operations.end(), database_ops.begin(), database_ops.end());
    operations.insert(operations.end(), table_ops.begin(), table_ops.end());

    return operations;
}

std::vector<UpdateOperationExecutor::Operation> UpdateOperationExecutor::planDatabaseOperations(const ManifestPtr & boss_manifest)
{
    std::vector<Operation> create_db_ops;

    for (const auto & boss_db : boss_manifest->databases)
    {
        String cached_db_key = manager->getManifestCache()->findDatabaseKey(boss_db.uuid);
        bool db_exists_in_cache = !cached_db_key.empty();

        if (!db_exists_in_cache)
        {
            Operation op;
            op.type = OperationType::CREATE_DATABASE;
            op.database_uuid = boss_db.uuid;
            op.database_name = boss_db.name;
            op.new_key = boss_db.key;
            op.db_ptr = &boss_db;
            op.sql = downloadDatabaseSQL(boss_db.key);
            create_db_ops.push_back(op);
        }
    }

    return create_db_ops;
}

std::vector<UpdateOperationExecutor::Operation> UpdateOperationExecutor::planTableOperations(const ManifestPtr & boss_manifest)
{
    std::vector<Operation> update_table_ops;
    std::vector<Operation> create_table_ops;

    for (const auto & boss_db : boss_manifest->databases)
    {
        for (const auto & boss_table : boss_db.tables)
        {
            String cached_table_key = manager->getManifestCache()->findTableKey(boss_table.uuid);
            bool table_exists_in_cache = !cached_table_key.empty();

            if (table_exists_in_cache)
            {
                if (cached_table_key != boss_table.key)
                {
                    Operation op;
                    op.type = OperationType::UPDATE_TABLE;
                    op.database_uuid = boss_db.uuid;
                    op.database_name = boss_db.name;
                    op.table_uuid = boss_table.uuid;
                    op.table_name = boss_table.name;
                    op.new_key = boss_table.key;
                    op.old_key = cached_table_key;
                    op.table_ptr = &boss_table;
                    op.sql = downloadTableSQL(boss_table.key);
                    update_table_ops.push_back(op);
                }
            }
            else
            {
                Operation op;
                op.type = OperationType::CREATE_TABLE;
                op.database_uuid = boss_db.uuid;
                op.database_name = boss_db.name;
                op.table_uuid = boss_table.uuid;
                op.table_name = boss_table.name;
                op.new_key = boss_table.key;
                op.table_ptr = &boss_table;
                op.sql = downloadTableSQL(boss_table.key);
                create_table_ops.push_back(op);
            }
        }
    }

    std::vector<Operation> operations;
    operations.insert(operations.end(), update_table_ops.begin(), update_table_ops.end());
    operations.insert(operations.end(), create_table_ops.begin(), create_table_ops.end());

    return operations;
}

std::vector<UpdateOperationExecutor::Operation> UpdateOperationExecutor::planDropOperations(
    const std::unordered_set<String> & boss_db_uuids,
    const std::unordered_set<String> & boss_table_uuids,
    bool drop_sync)
{
    std::vector<Operation> drop_database_ops;
    std::vector<Operation> drop_table_ops;

    // First, plan database drops
    auto cached_databases = manager->getManifestCache()->getAllDatabases();
    for (const auto & [db_uuid, db_key] : cached_databases)
    {
        if (boss_db_uuids.find(db_uuid) == boss_db_uuids.end())
        {
            Operation op;
            op.type = OperationType::DROP_DATABASE;
            op.database_uuid = db_uuid;
            op.drop_sync = drop_sync;

            auto & catalog = DatabaseCatalog::instance();
            UUID uuid = MetadataApplicator::parseUUIDFromString(db_uuid);

            auto databases = catalog.getDatabases();
            for (const auto & [db_name, db_ptr] : databases)
            {
                if (!db_ptr)
                    continue;

                if (manager->isSystemDatabase(db_ptr->getDatabaseName()))
                    continue;

                if (db_ptr->getUUID() == uuid)
                {
                    op.database_name = db_name;

                    UInt32 table_count = 0;
                    for (auto table_it = db_ptr->getTablesIterator(getContext()); table_it->isValid(); table_it->next())
                    {
                        if (!table_it->table())
                            table_count++;
                    }
                    LOG_DEBUG(log, "Database {} has {} tables to drop", db_name, table_count);

                    break;
                }
            }

            if (op.database_name.empty())
            {
                LOG_WARNING(log, "Database with UUID {} exists in manager->getManifestCache() but cannot find in DatabaseCatalog, skipping drop", db_uuid);
                continue;
            }

            drop_database_ops.push_back(op);
            LOG_DEBUG(log, "Detected database to drop: {} (UUID: {})", op.database_name, db_uuid);
        }
    }

    // Then, plan table drops (only for tables in databases that are not being dropped)
    auto cached_tables = manager->getManifestCache()->getAllTables();
    for (const auto & [table_uuid, table_key] : cached_tables)
    {
        if (boss_table_uuids.find(table_uuid) == boss_table_uuids.end())
        {
            /// Skip tables in databases that are being dropped
            auto slash_pos = table_key.find('/');
            if (slash_pos != String::npos)
            {
                String db_uuid_str = table_key.substr(0, slash_pos);

                bool db_will_be_dropped = false;
                for (const auto & drop_db_op : drop_database_ops)
                {
                    if (drop_db_op.database_uuid == db_uuid_str)
                    {
                        db_will_be_dropped = true;
                        LOG_DEBUG(log, "Table with UUID {} exists in manager->getManifestCache() but its database {} is being dropped, skipping drop", table_uuid, db_uuid_str);
                        break;
                    }
                }

                if (db_will_be_dropped)
                    continue;
            }

            Operation op;
            op.type = OperationType::DROP_TABLE;
            op.table_uuid = table_uuid;
            op.drop_sync = drop_sync;

            auto & catalog = DatabaseCatalog::instance();
            UUID uuid = MetadataApplicator::parseUUIDFromString(table_uuid);

            auto databases = catalog.getDatabases();
            for (const auto & [db_name, db_ptr] : databases)
            {
                if (!db_ptr)
                    continue;
                
                if (manager->isSystemDatabase(db_ptr->getDatabaseName()))
                    continue;

                for (auto table_it = db_ptr->getTablesIterator(getContext()); table_it->isValid(); table_it->next())
                {
                    auto storage = table_it->table();
                    if (storage && storage->getStorageID().uuid == uuid)
                    {
                        op.database_name = db_name;
                        op.table_name = table_it->name();
                        op.database_uuid = toString(db_ptr->getUUID());
                        break;
                    }
                }
                if (!op.table_name.empty())
                    break;
            }

            if (op.table_name.empty())
            {
                LOG_WARNING(log, "Table with UUID {} exists in manager->getManifestCache() but cannot find in DatabaseCatalog, skipping drop", table_uuid);
                continue;
            }

            drop_table_ops.push_back(op);
            LOG_DEBUG(log, "Detected table to drop: {}.{} (UUID: {})", op.database_name, op.table_name, table_uuid);
        }
    }

    // Combine operations
    std::vector<Operation> operations;
    operations.insert(operations.end(), drop_database_ops.begin(), drop_database_ops.end());
    operations.insert(operations.end(), drop_table_ops.begin(), drop_table_ops.end());

    return operations;
}

UpdateOperationExecutor::Result UpdateOperationExecutor::executeUpdate(const Operation & op)
{
    Result result;
    result.operation = op;
    result.success = false;

    try
    {
        switch (op.type)
        {
            case OperationType::DROP_DATABASE:
                LOG_INFO(log, "Dropping database {} (UUID: {}) with drop_sync={}",
                         op.database_name, op.database_uuid, op.drop_sync);
                applicator->dropDatabase(op.database_name, op.database_uuid, op.drop_sync);
                result.success = true;
                break;

            case OperationType::DROP_TABLE:
                LOG_INFO(log, "Dropping table {}.{} (UUID: {}) with drop_sync={}",
                         op.database_name, op.table_name, op.table_uuid, op.drop_sync);
                applicator->dropTable(op.database_name, op.table_name, op.table_uuid, op.drop_sync);
                result.success = true;
                break;
            
            case OperationType::CREATE_DATABASE:
                LOG_INFO(log, "Creating new database {}", op.database_name);
                applicator->applyDatabaseChanges(*op.db_ptr, op.sql);
                result.success = true;
                break;
            
            case OperationType::UPDATE_TABLE:
                LOG_INFO(log, "Updating table {}.{}: key {} -> {}",
                         op.database_name, op.table_name, op.old_key, op.new_key);
                applicator->applyTableChanges(*op.table_ptr, op.database_name, op.sql);
                result.success = true;
                break;

            case OperationType::CREATE_TABLE:
                LOG_INFO(log, "Creating new table {}.{}", op.database_name, op.table_name);
                applicator->applyTableChanges(*op.table_ptr, op.database_name, op.sql);
                result.success = true;
                break;
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
        result.error_message = getCurrentExceptionMessage(true);
        tryLogCurrentException(log, "Failed to execute update operation");
    }

    return result;
}

void UpdateOperationExecutor::applyCacheUpdates(const std::vector<Result> & results)
{
    for (const auto & result : results)
    {
        if (!result.success)
            continue;

        const auto & op = result.operation;

        switch (op.type)
        {
            case OperationType::CREATE_DATABASE: {
                Database db;
                db.uuid = op.database_uuid;
                db.key = op.new_key;
                db.name = op.database_name;
                manager->getManifestCache()->updateDatabase(db);
                break;
            }

            case OperationType::UPDATE_TABLE:
            case OperationType::CREATE_TABLE: {
                Table table;
                table.uuid = op.table_uuid;
                table.key = op.new_key;
                table.name = op.table_name;
                manager->getManifestCache()->updateTable(table);
                break;
            }

            case OperationType::DROP_TABLE: {
                manager->getManifestCache()->removeTable(op.table_uuid);
                break;
            }

            case OperationType::DROP_DATABASE: {
                manager->getManifestCache()->removeDatabase(op.database_uuid);
                break;
            }
        }
    }
}

String UpdateOperationExecutor::downloadDatabaseSQL(const String & key) const
{
    try
    {
        auto boss_client = manager->getBossClientPtr();
        if (!boss_client)
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "BossClient is null, cannot download database SQL");
        }

        auto [content, etag] = boss_client->download(key);
        return content;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to download database SQL {}", key));
        throw;
    }
}

String UpdateOperationExecutor::downloadTableSQL(const String & key) const
{
    try
    {
        auto boss_client = manager->getBossClientPtr();
        if (!boss_client)
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "BossClient is null, cannot download table SQL");
        }

        auto [content, etag] = boss_client->download(key);
        return content;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to download table SQL {}", key));
        throw;
    }
}

}
