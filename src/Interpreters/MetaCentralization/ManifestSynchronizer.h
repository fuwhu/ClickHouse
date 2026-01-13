#pragma once

#include <memory>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

struct Manifest;
using ManifestPtr = std::shared_ptr<Manifest>;

class MetadataCentralizationManager;

/// Synchronizes manifest between local storage and remote Boss storage
class ManifestSynchronizer : public WithContext
{
public:
    ManifestSynchronizer(MetadataCentralizationManager * manager_, ContextPtr context);

    ManifestPtr downloadFromRemote() const;

    String uploadToRemote(const ManifestPtr & manifest) const;

    void persistToLocal(const ManifestPtr & manifest, const String & etag) const;

    ManifestPtr loadFromLocal() const;

    bool localManifestExists() const;

private:
    MetadataCentralizationManager * manager;
    LoggerPtr log;

    String getLocalManifestPath() const;
};

using ManifestSynchronizerPtr = std::unique_ptr<ManifestSynchronizer>;

}
