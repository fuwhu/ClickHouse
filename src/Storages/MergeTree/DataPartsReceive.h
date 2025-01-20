#pragma once

#include <Interpreters/InterserverIOHandler.h>

namespace DB
{
class ReadWriteBufferFromHTTP;
class MergeTreeData;

namespace DataPartsReceive
{
class Service final : public InterserverIOEndpoint
{
public:
    explicit Service(MergeTreeData & data_);

    Service(const Service &) = delete;
    Service & operator=(const Service &) = delete;

    std::string getId(const std::string & node_id) const override;
    void processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & out, HTTPServerResponse & response) override;

private:
    MergeTreeData & data;
    LoggerPtr log;
};
}

}
