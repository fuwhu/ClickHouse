#include <Interpreters/MetaCentralization/ManifestCache.h>

namespace DB
{

ManifestCache::ManifestCache()
    : manifest(std::make_shared<Manifest>())
    , log(getLogger("ManifestCache"))
{
}

void ManifestCache::load(const ManifestPtr & new_manifest)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    LOG_DEBUG(log, "Loading manifest into cache, databases: {}", new_manifest->databases.size());

    manifest = new_manifest;
    updateMappings(manifest);
    is_loaded = true;

    LOG_DEBUG(log, "Manifest loaded, etag: {}", manifest->etag);
}

void ManifestCache::updateDatabase(const Database & db, bool is_create)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    if (is_create)
        manifest->addDatabase(db);
    else
        manifest->updateDatabase(db.uuid, db);

    database_key_map[db.uuid] = db.key;
    database_name_map[db.uuid] = db.name;
    LOG_DEBUG(log, "Updated database cache: {} ({})", db.name, db.uuid);
}

void ManifestCache::updateTable(const Table & table, bool is_create)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    String db_uuid = table.key.substr(0, table.key.find('/'));

    if (is_create)
        manifest->findDatabase(db_uuid)->get().addTable(table);
    else 
        manifest->findDatabase(db_uuid)->get().updateTable(table.uuid, table);

    manifest->findDatabase(db_uuid)->get().updateTable(table.uuid, table);

    table_key_map[table.uuid] = table.key;
    table_name_map[table.uuid] = table.name;
    LOG_DEBUG(log, "Updated table cache: {} ({})", table.name, table.uuid);
}

void ManifestCache::removeDatabase(const String & uuid)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    manifest->removeDatabase(uuid);

    /// Remove all tables belonging to this database from cache
    /// Table keys are in format: {db_uuid}/{table_name}_{version}.sql
    /// So we can identify tables by checking if their key starts with "{db_uuid}/"
    String db_prefix = uuid + "/";
    std::vector<String> tables_to_remove;

    for (const auto & [table_uuid, table_key] : table_key_map)
    {
        if (table_key.starts_with(db_prefix))
            tables_to_remove.push_back(table_uuid);
    }

    for (const auto & table_uuid : tables_to_remove)
    {
        table_key_map.erase(table_uuid);
        table_name_map.erase(table_uuid);
        LOG_DEBUG(log, "Removed table ({}) from cache due to database removal", table_uuid);
    }

    database_key_map.erase(uuid);
    database_name_map.erase(uuid);
    LOG_DEBUG(log, "Removed database from cache: {}, removed {} tables", uuid, tables_to_remove.size());
}

void ManifestCache::removeTable(const String & uuid)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    auto table_key = table_key_map.find(uuid)->second;
    String db_uuid = table_key.substr(0, table_key.find('/'));
    manifest->findDatabase(db_uuid)->get().removeTable(uuid);

    table_key_map.erase(uuid);
    table_name_map.erase(uuid);
    LOG_DEBUG(log, "Removed table from cache: {}", uuid);
}

String ManifestCache::findDatabaseKey(const String & uuid) const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    auto it = database_key_map.find(uuid);
    return it != database_key_map.end() ? it->second : "";
}

String ManifestCache::findTableKey(const String & uuid) const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    auto it = table_key_map.find(uuid);
    return it != table_key_map.end() ? it->second : "";
}

std::unordered_map<String, String> ManifestCache::getAllTables() const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    return table_key_map;
}

std::unordered_map<String, String> ManifestCache::getAllDatabases() const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    return database_key_map;
}

void ManifestCache::updateEtag(const String & etag)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    manifest->etag = etag;

    LOG_DEBUG(log, "Atomically updated manifest etag: {}", etag);
}

void ManifestCache::updateVersion(UInt64 version)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    manifest->version = version;

    LOG_DEBUG(log, "Atomically updated manifest version: {}", version);
}

void ManifestCache::updateLastModified(const String & last_modified)
{
    std::unique_lock<std::shared_mutex> lock(cache_mutex);

    manifest->last_modified = last_modified;

    LOG_DEBUG(log, "Atomically updated manifest last modified: {}", last_modified);
}

String ManifestCache::getEtag() const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    return manifest->etag;
}

bool ManifestCache::isLoaded() const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    return is_loaded;
}

void ManifestCache::updateMappings(const ManifestPtr & new_manifest)
{
    database_key_map.clear();
    database_name_map.clear();
    table_key_map.clear();
    table_name_map.clear();

    for (const auto & db : new_manifest->databases)
    {
        database_key_map[db.uuid] = db.key;
        database_name_map[db.uuid] = db.name;

        for (const auto & table : db.tables)
        {
            table_key_map[table.uuid] = table.key;
            table_name_map[table.uuid] = table.name;
        }
    }

    LOG_DEBUG(log, "Updated mappings: {} databases, {} tables",
              database_key_map.size(), table_key_map.size());
}

ManifestPtr ManifestCache::cloneManifestWithoutEtag() const
{
    std::shared_lock<std::shared_mutex> lock(cache_mutex);
    auto clone_manifest = std::make_shared<Manifest>(*manifest);
    clone_manifest->etag.clear();
    return clone_manifest;
}

}
