#pragma once

#include <Databases/DatabasesCommon.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/Iceberg/IcebergCommon.h>

namespace DB
{
class DatabaseIceberg final : public IDatabase, WithContext
{
public:
    DatabaseIceberg(
        const String & database_name_, const String & metadata_path_, const IcebergCatalogConfig & config_, ContextPtr context_);

    String getEngineName() const override { return "Iceberg"; }

    bool canContainMergeTreeTables() const override { return false; }

    bool canContainDistributedTables() const override { return false; }

    ASTPtr getCreateDatabaseQuery() const override;

    bool empty() const override;

    DatabaseTablesIteratorPtr getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name) const override;

    bool isTableExist(const String & name, ContextPtr local_context) const override;

    StoragePtr tryGetTable(const String & name, ContextPtr local_context) const override;

    void shutdown() override;

    void drop(ContextPtr /*context*/) override;

    String getMetadataPath() const override;

    bool shouldBeEmptyOnDetach() const override { return false; }

protected:
    ASTPtr getCreateTableQueryImpl(const String & name, ContextPtr context, bool throw_on_error) const override;

private:
    void tryInit() const;

    String metadata_path;
    const IcebergCatalogConfig config;

    Poco::Logger * log;
};
}
