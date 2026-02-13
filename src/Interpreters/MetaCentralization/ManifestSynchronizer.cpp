#include <Interpreters/MetaCentralization/ManifestSynchronizer.h>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <IO/Boss/BossClient.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
    extern const int METADATA_CENTRALIZATION_SERVER_ERROR;
}

ManifestSynchronizer::ManifestSynchronizer(MetadataCentralizationManager * manager_, ContextPtr context_)
    : WithContext(context_)
    , manager(manager_)
    , log(getLogger("ManifestSynchronizer"))
{
}

ManifestPtr ManifestSynchronizer::downloadFromRemote() const
{
    try
    {
        auto boss_client = manager->getBossClientPtr();
        if (!boss_client)
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "BossClient is null, cannot download manifest");
        }

        LOG_DEBUG(log, "Downloading manifest from Boss");

        auto [content, etag] = boss_client->download("manifest.json");

        Manifest manifest = Manifest::fromJSONString(content);
        manifest.etag = etag;

        LOG_DEBUG(log, "Downloaded manifest from Boss, etag: {}, databases: {}", etag, manifest.databases.size());

        return std::make_shared<Manifest>(std::move(manifest));
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to download manifest from Boss");
        throw;
    }
}

String ManifestSynchronizer::uploadToRemote(const ManifestPtr & manifest) const
{
    try
    {
        auto boss_client = manager->getBossClientPtr();
        if (!boss_client)
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "BossClient is null, cannot upload manifest");
        }

        LOG_DEBUG(log, "Uploading manifest to Boss, databases: {}", manifest->databases.size());

        String json_content = manifest->toJSONString();

        String etag = boss_client->upload("manifest.json", json_content);

        LOG_DEBUG(log, "Uploaded manifest to Boss successfully, etag: {}", etag);

        return etag;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to upload manifest to Boss");
        throw;
    }
}

void ManifestSynchronizer::persistToLocal(const ManifestPtr & manifest, const String & etag) const
{
    String local_path = getLocalManifestPath();
    String temp_path = local_path + ".tmp";

    auto db_disk = getContext()->getDatabaseDisk();

    try
    {
        LOG_DEBUG(log, "Persisting manifest to local disk: {}", local_path);

        auto json_obj = manifest->toJSON();
        json_obj->set("etag", etag);

        Manifest manifest_with_etag = Manifest::fromJSON(json_obj);

        manifest_with_etag.serialize(db_disk, temp_path);

        db_disk->replaceFile(temp_path, local_path);

        LOG_DEBUG(log, "Persisted manifest to disk successfully");
    }
    catch (...)
    {
        db_disk->removeFileIfExists(temp_path);
        tryLogCurrentException(log, "Failed to persist manifest to disk");
        throw;
    }
}

ManifestPtr ManifestSynchronizer::loadFromLocal() const
{
    String local_path = getLocalManifestPath();
    auto db_disk = getContext()->getDatabaseDisk();

    if (!db_disk->existsFile(local_path))
        throw Exception(ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR, "local manifest does not exist at path: {}.", local_path);

    try
    {
        LOG_DEBUG(log, "Loading manifest from local disk: {}", local_path);

        Manifest manifest = Manifest::deserialize(db_disk, local_path);

        LOG_DEBUG(log, "Loaded local manifest, etag: {}, databases: {}",
                  manifest.etag, manifest.databases.size());

        return std::make_shared<Manifest>(std::move(manifest));
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to load local manifest");
        throw;
    }
}

bool ManifestSynchronizer::localManifestExists() const
{
    String local_path = getLocalManifestPath();
    auto db_disk = getContext()->getDatabaseDisk();
    return db_disk->existsFile(local_path);
}

String ManifestSynchronizer::getLocalManifestPath() const
{
    return getContext()->getPath() + "metadata/manifest.json";
}

}
