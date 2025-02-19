#include <Databases/IDatabase.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <Interpreters/Context.h>
#include <Access/ContextAccess.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Parsers/formatAST.h>


namespace DB
{

NamesAndTypesList StorageSystemDatabases::getNamesAndTypes()
{
    return {
        {"name", std::make_shared<DataTypeString>()},
        {"engine", std::make_shared<DataTypeString>()},
        {"data_path", std::make_shared<DataTypeString>()},
        {"metadata_path", std::make_shared<DataTypeString>()},
        {"uuid", std::make_shared<DataTypeUUID>()},
        {"comment", std::make_shared<DataTypeString>()},
        {"create_query", std::make_shared<DataTypeString>()}
    };
}

NamesAndAliases StorageSystemDatabases::getNamesAndAliases()
{
    return {
        {"database", std::make_shared<DataTypeString>(), "name"}
    };
}

void StorageSystemDatabases::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    const auto access = context->getAccess();
    const bool check_access_for_databases = !access->isGranted(AccessType::SHOW_DATABASES);

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & [database_name, database] : databases)
    {
        if (check_access_for_databases && !access->isGranted(AccessType::SHOW_DATABASES, database_name))
            continue;

        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue; /// We don't want to show the internal database for temporary tables in system.databases

        res_columns[0]->insert(database_name);
        res_columns[1]->insert(database->getEngineName());
        res_columns[2]->insert(context->getPath() + database->getDataPath());
        res_columns[3]->insert(database->getMetadataPath());
        res_columns[4]->insert(database->getUUID());
        res_columns[5]->insert(database->getDatabaseComment());

        if (database->getEngineName() == "Iceberg")
        {
            ASTPtr create_query = database->getCreateDatabaseQuery();
            WriteBufferFromOwnString buf;
            formatAST(*create_query, buf, false, false);
            String res = buf.str();
            res_columns[6]->insert(res);
        }
        else 
        {
            res_columns[6]->insert("");
        }
   }
}

}
