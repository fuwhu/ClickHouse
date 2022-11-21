#pragma once

#include <Disks/IDisk.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/copyData.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{
class DataPartsReceive final : public InterserverIOEndpoint
{
public:
    explicit DataPartsReceive(MergeTreeData & data_);

    inline std::string getId(const std::string & table_name) const override { return "DataPartsReceive:" + table_name; }

    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    class TemporaryPartHolder : private boost::noncopyable
    {
    public:
        TemporaryPartHolder(DiskPtr disk_, const String & temp_part_path) : disk(disk_), temp_part(temp_part_path) { }

        ~TemporaryPartHolder();

        bool done{false};

    private:
        DiskPtr disk;
        String temp_part;
    };

    [[nodiscard]] MergeTreeData::MutableDataPartPtr receivePart(const String & part_name, ReadBuffer & in);

    void saveBaseOrProjectionFileToDisk(const String & part_download_path, DiskPtr disk, ReadBuffer & in, ThrottlerPtr throttler);

    MergeTreeData & data;
    Poco::Logger * log;
};
}
