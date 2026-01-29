#pragma once

#include <functional>
#include <optional>
#include <vector>

#include <Poco/Dynamic/Var.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <base/types.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

namespace DB
{

/// Base class for metadata entities (Database, Table)
struct MetadataEntity
{
    String uuid;              /// Unique identifier for the entity
    String key;               /// Boss storage key
    String name;              /// Entity name
    UInt32 version = 0;       /// Version number
    String last_modified;     /// Last modification timestamp

    void serializeCommonFields(Poco::JSON::Object::Ptr & obj) const;
    void deserializeCommonFields(const Poco::JSON::Object::Ptr & obj);
    void writeCommonFields(WriteBuffer & out) const;
    void readCommonFields(ReadBuffer & in);
};

/// Table metadata entity
struct Table : public MetadataEntity
{
    Poco::JSON::Object::Ptr toJSON() const;
    String toJSONString() const;
    static Table fromJSON(const Poco::JSON::Object::Ptr & obj);
    static Table fromJSONString(const String & json);
    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
};

/// Database metadata entity
struct Database : public MetadataEntity
{
    UInt32 tables_count = 0;  /// Number of tables in this database
    std::vector<Table> tables;

    Poco::JSON::Object::Ptr toJSON() const;
    String toJSONString() const;
    static Database fromJSON(const Poco::JSON::Object::Ptr & obj);
    static Database fromJSONString(const String & json);
    void serialize(WriteBuffer & out) const;
    void deserialize(ReadBuffer & in);
    void addTable(const Table & table);
    void updateTable(const String & table_uuid, const Table & table);
    void removeTable(const String & table_uuid);
    std::optional<std::reference_wrapper<const Table>> findTable(const String & table_uuid) const;
    std::optional<std::reference_wrapper<const Table>> findTableByName(const String & table_name) const;
};

/// Manifest structure containing all database and table metadata
struct Manifest
{
    String ck_version;         /// ClickHouse version
    String last_modified;      /// Last modification timestamp
    String etag;               /// Entity tag for version control
    UInt64 version = 0;        /// Manifest version number
    UInt32 databases_count = 0;
    std::vector<Database> databases;

    Poco::JSON::Object::Ptr toJSON() const;
    String toJSONString() const;
    static Manifest fromJSON(const Poco::JSON::Object::Ptr & obj);
    static Manifest fromJSONString(const String & json);
    void serialize(const DiskPtr & disk, const String & path) const;
    static Manifest deserialize(const DiskPtr & disk, const String & path);
    void addDatabase(const Database & database);
    void updateDatabase(const String & database_uuid, const Database & database);
    void removeDatabase(const String & database_uuid);
    std::optional<std::reference_wrapper<const Database>> findDatabase(const String & database_uuid) const;
    std::optional<std::reference_wrapper<Database>> findDatabase(const String & database_uuid);
    UInt32 getTablesCount() const;
};

using ManifestPtr = std::shared_ptr<Manifest>;

}
