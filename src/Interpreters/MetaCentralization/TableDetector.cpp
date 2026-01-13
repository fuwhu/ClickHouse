#include <Interpreters/MetaCentralization/TableDetector.h>

#include <regex>
#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Common/Exception.h>
#include <Common/Macros.h>
#include <Common/ZooKeeper/ZooKeeper.h>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

TableDetector::TableDetector(ContextPtr context_)
    : WithContext(context_)
    , log(getLogger("TableDetector"))
{
}

bool TableDetector::isTableDataPathExisting(const ASTCreateQuery & create_query, const String & data_path) const
{
    try
    {
        Disks disks_to_check;

        if (create_query.storage->settings)
        {
            const auto & settings_ast = create_query.storage->settings;
            String storage_policy_name;

            for (const auto & change : settings_ast->changes)
            {
                if (change.name == "storage_policy")
                    storage_policy_name = change.value.safeGet<String>();
            }

            if (!storage_policy_name.empty())
            {
                try
                {
                    auto storage_policy = getContext()->getStoragePolicy(storage_policy_name);
                    disks_to_check = storage_policy->getDisks();
                    LOG_DEBUG(
                        log,
                        "Using storage policy '{}' with {} disks to check for table data path",
                        storage_policy_name,
                        disks_to_check.size());
                }
                catch (...)
                {
                    tryLogCurrentException(log, fmt::format("Failed to get storage policy '{}'", storage_policy_name));
                    throw;
                }
            }
        }

        if (disks_to_check.empty())
        {
            disks_to_check.push_back(getContext()->getDatabaseDisk());
            LOG_DEBUG(log, "No specific storage policy/disk found, checking default database disk");
        }

        for (const auto & disk : disks_to_check)
        {
            if (disk->existsDirectory(data_path))
            {
                LOG_INFO(log, "Found existing table data path on disk '{}' at path: {}", disk->getName(), data_path);
                return true;
            }
        }

        LOG_DEBUG(log, "No existing table data path found for path: {}", data_path);
        return false;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error checking if table data path exists");
        throw;
    }
}

bool TableDetector::isReplicaInZooKeeper(const ASTCreateQuery & create_query) const
{
    try
    {
        auto info = extractReplicatedInfo(create_query);

        String replica_path = fs::path(info.zookeeper_path) / "replicas" / info.replica_name;
        
        const auto & zk_name = zkutil::extractZooKeeperName(replica_path);
        
        auto zookeeper = getContext()->getDefaultOrAuxiliaryZooKeeper(zk_name);

        if (!zookeeper)
        {
            tryLogCurrentException(log, fmt::format("ZooKeeper {} is not configured", zk_name));
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ZooKeeper {} is not configured", zk_name);
        }

        const auto & zk_path = zkutil::extractZooKeeperPath(replica_path, true);
        bool exists = zookeeper->exists(zk_path);

        if (exists)
            LOG_INFO(log, "Found existing replica in ZooKeeper at path: {}", replica_path);
        else
            LOG_DEBUG(log, "Replica not found in ZooKeeper at path: {}", replica_path);

        return exists;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error checking if replica exists in ZooKeeper");
        throw;
    }
}

TableDetector::ReplicatedTableInfo TableDetector::extractReplicatedInfo(const ASTCreateQuery & create_query) const
{
    try
    {
        const auto & engine_args = create_query.storage->engine->arguments;
        if (!engine_args || engine_args->children.size() < 2)
        {
            tryLogCurrentException(log, "Replicated table engine has insufficient arguments");
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Replicated table engine has insufficient arguments");
        }

        ReplicatedTableInfo info;

        const auto & zk_path_arg = engine_args->children[0];
        const auto & replica_name_arg = engine_args->children[1];

        if (const auto * literal = zk_path_arg->as<ASTLiteral>())
            info.zookeeper_path = literal->value.safeGet<String>();

        if (const auto * literal = replica_name_arg->as<ASTLiteral>())
            info.replica_name = literal->value.safeGet<String>();

        if (info.zookeeper_path.find("{database}") != String::npos)
            info.zookeeper_path = std::regex_replace(info.zookeeper_path, std::regex("\\{database\\}"), create_query.getDatabase());

        if (info.zookeeper_path.empty() || info.replica_name.empty())
        {
            tryLogCurrentException(log, "Failed to extract zookeeper_path and replica_name from ASTCreateQuery");
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to extract zookeeper_path and replica_name from ASTCreateQuery");
        }

        Macros::MacroExpansionInfo expansion_info;
        expansion_info.expand_special_macros_only = false;
        expansion_info.table_id = StorageID(create_query.getDatabase(), create_query.getTable(), create_query.uuid);

        info.zookeeper_path = getContext()->getMacros()->expand(info.zookeeper_path, expansion_info);
        info.replica_name = getContext()->getMacros()->expand(info.replica_name, expansion_info);

        return info;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error extracting replicated info");
        throw;
    }
}

}
