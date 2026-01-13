#pragma once

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Parsers/ASTCreateQuery.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

/// Helper class for detecting table state and characteristics
class TableDetector : public WithContext
{
public:
    explicit TableDetector(ContextPtr context_);

    bool isTableDataPathExisting(const ASTCreateQuery & create_query, const String & data_path) const;

    bool isReplicaInZooKeeper(const ASTCreateQuery & create_query) const;

private:
    LoggerPtr log;

    struct ReplicatedTableInfo
    {
        String zookeeper_path;
        String replica_name;
    };

    /// Extract zookeeper path and replica name from replicated table ASTCreateQuery
    ReplicatedTableInfo extractReplicatedInfo(const ASTCreateQuery & create_query) const;
};

using TableDetectorPtr = std::unique_ptr<TableDetector>;

}
