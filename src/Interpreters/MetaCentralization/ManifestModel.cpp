#include <Interpreters/MetaCentralization/ManifestModel.h>

namespace DB
{

void MetadataEntity::serializeCommonFields(Poco::JSON::Object::Ptr & obj) const
{
    obj->set("uuid", uuid);
    obj->set("key", key);
    obj->set("name", name);
    obj->set("version", version);
    obj->set("last_modified", last_modified);
}

void MetadataEntity::deserializeCommonFields(const Poco::JSON::Object::Ptr & obj)
{
    uuid = obj->getValue<String>("uuid");
    key = obj->getValue<String>("key");
    name = obj->getValue<String>("name");
    version = obj->getValue<UInt32>("version");
    last_modified = obj->getValue<String>("last_modified");
}

void MetadataEntity::writeCommonFields(WriteBuffer & out) const
{
    writeString("uuid: ", out);
    writeString(uuid, out);
    writeChar('\n', out);

    writeString("key: ", out);
    writeString(key, out);
    writeChar('\n', out);

    writeString("name: ", out);
    writeString(name, out);
    writeChar('\n', out);

    writeString("version: ", out);
    writeIntText(version, out);
    writeChar('\n', out);

    writeString("last_modified: ", out);
    writeString(last_modified, out);
    writeChar('\n', out);
}

void MetadataEntity::readCommonFields(ReadBuffer & in)
{
    assertString("uuid: ", in);
    readString(uuid, in);
    assertChar('\n', in);

    assertString("key: ", in);
    readString(key, in);
    assertChar('\n', in);

    assertString("name: ", in);
    readString(name, in);
    assertChar('\n', in);

    assertString("version: ", in);
    readIntText(version, in);
    assertChar('\n', in);

    assertString("last_modified: ", in);
    readString(last_modified, in);
    assertChar('\n', in);
}

Poco::JSON::Object::Ptr Table::toJSON() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    serializeCommonFields(obj);
    return obj;
}

String Table::toJSONString() const
{
    Poco::JSON::Object::Ptr obj = toJSON();
    std::ostringstream oss;
    obj->stringify(oss, 2);
    return oss.str();
}

Table Table::fromJSON(const Poco::JSON::Object::Ptr & obj)
{
    Table t;
    t.deserializeCommonFields(obj);
    return t;
}

Table Table::fromJSONString(const String & json)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json);
    const Poco::JSON::Object::Ptr & obj = result.extract<Poco::JSON::Object::Ptr>();
    return fromJSON(obj);
}

void Table::serialize(WriteBuffer & out) const
{
    writeCommonFields(out);
}

void Table::deserialize(ReadBuffer & in)
{
    readCommonFields(in);
}

Poco::JSON::Object::Ptr Database::toJSON() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    serializeCommonFields(obj);

    Poco::JSON::Array::Ptr tables_array = new Poco::JSON::Array();
    for (const auto & table : tables)
    {
        tables_array->add(table.toJSON());
    }
    obj->set("tables_count", tables_array->size());
    obj->set("tables", tables_array);

    return obj;
}

String Database::toJSONString() const
{
    Poco::JSON::Object::Ptr obj = toJSON();
    std::ostringstream oss;
    obj->stringify(oss, 2);
    return oss.str();
}

Database Database::fromJSON(const Poco::JSON::Object::Ptr & obj)
{
    Database d;
    d.deserializeCommonFields(obj);

    if (obj->has("tables"))
    {
        d.tables_count = obj->getValue<UInt32>("tables_count");

        Poco::JSON::Array::Ptr tables_array = obj->getArray("tables");
        for (size_t i = 0; i < tables_array->size(); ++i)
        {
            Poco::JSON::Object::Ptr table_obj = tables_array->getObject(i);
            if (table_obj)
            {
                d.tables.push_back(Table::fromJSON(table_obj));
            }
        }
    }

    return d;
}

Database Database::fromJSONString(const String & json)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json);
    const Poco::JSON::Object::Ptr & obj = result.extract<Poco::JSON::Object::Ptr>();
    return fromJSON(obj);
}

void Database::serialize(WriteBuffer & out) const
{
    writeCommonFields(out);

    writeString("tables_count: ", out);
    writeIntText(tables.size(), out);
    writeChar('\n', out);

    for (const auto & table : tables)
    {
        writeString("--- table ---\n", out);
        table.serialize(out);
    }
}

void Database::deserialize(ReadBuffer & in)
{
    readCommonFields(in);

    assertString("tables_count: ", in);
    readIntText(tables_count, in);
    assertChar('\n', in);

    tables.clear();
    tables.reserve(tables_count);

    for (size_t i = 0; i < tables_count; ++i)
    {
        assertString("--- table ---\n", in);
        Table table;
        table.deserialize(in);
        tables.push_back(std::move(table));
    }
}

void Database::addTable(const Table & table)
{
    tables.push_back(table);
}

void Database::updateTable(const String & table_uuid, const Table & table)
{
    for (auto & t : tables)
    {
        if (t.uuid == table_uuid)
        {
            t = table;
            return;
        }
    }
}

void Database::removeTable(const String & table_uuid)
{
    tables.erase(
        std::remove_if(tables.begin(), tables.end(),
            [&table_uuid](const Table & t) { return t.uuid == table_uuid; }),
        tables.end());
}

std::optional<std::reference_wrapper<const Table>> Database::findTable(const String & table_uuid) const
{
    for (const auto & t : tables)
    {
        if (t.uuid == table_uuid)
            return std::cref(t);
    }
    return std::nullopt;
}

std::optional<std::reference_wrapper<const Table>> Database::findTableByName(const String & table_name) const
{
    for (const auto & t : tables)
    {
        if (t.name == table_name)
            return std::cref(t);
    }
    return std::nullopt;
}

Poco::JSON::Object::Ptr Manifest::toJSON() const
{
    Poco::JSON::Object::Ptr obj = new Poco::JSON::Object();
    obj->set("ck_version", ck_version);
    obj->set("last_modified", last_modified);
    obj->set("version", version);

    if (!etag.empty())
        obj->set("etag", etag);

    obj->set("databases_count", databases.size());

    Poco::JSON::Array::Ptr databases_array = new Poco::JSON::Array();
    for (const auto & database : databases)
    {
        databases_array->add(database.toJSON());
    }
    obj->set("databases", databases_array);

    return obj;
}

String Manifest::toJSONString() const
{
    Poco::JSON::Object::Ptr obj = toJSON();
    std::ostringstream oss;
    obj->stringify(oss, 2);
    return oss.str();
}

Manifest Manifest::fromJSON(const Poco::JSON::Object::Ptr & obj)
{
    Manifest m;
    m.ck_version = obj->getValue<String>("ck_version");
    m.last_modified = obj->getValue<String>("last_modified");

    if (obj->has("version"))
        m.version = obj->getValue<UInt64>("version");

    if (obj->has("etag"))
        m.etag = obj->getValue<String>("etag");

    m.databases_count = obj->getValue<UInt32>("databases_count");

    if (obj->has("databases"))
    {
        Poco::JSON::Array::Ptr databases_array = obj->getArray("databases");
        for (size_t i = 0; i < databases_array->size(); ++i)
        {
            Poco::JSON::Object::Ptr db_obj = databases_array->getObject(i);
            if (db_obj)
            {
                m.databases.push_back(Database::fromJSON(db_obj));
            }
        }
    }

    return m;
}

Manifest Manifest::fromJSONString(const String & json)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var result = parser.parse(json);
    const Poco::JSON::Object::Ptr & obj = result.extract<Poco::JSON::Object::Ptr>();
    return fromJSON(obj);
}

void Manifest::serialize(const DiskPtr & disk, const String & path) const
{
    auto out = disk->writeFile(path);

    auto manifest_json = toJSONString();
    out->write(manifest_json.data(), manifest_json.size());

    out->finalize();
}

Manifest Manifest::deserialize(const DiskPtr & disk, const String & path)
{
    constexpr size_t size_hint = 4096;  /// These files are small.
    auto read_settings = ReadSettings().adjustBufferSize(size_hint);
    auto in = disk->readFile(path, read_settings);

    String manifest_json;
    readStringUntilEOF(manifest_json, *in);

    Manifest m = fromJSONString(manifest_json);

    return m;
}

void Manifest::addDatabase(const Database & database)
{
    databases.push_back(database);
}

void Manifest::updateDatabase(const String & database_uuid, const Database & database)
{
    for (auto & d : databases)
    {
        if (d.uuid == database_uuid)
        {
            d = database;
            return;
        }
    }
}

void Manifest::removeDatabase(const String & database_uuid)
{
    databases.erase(
        std::remove_if(databases.begin(), databases.end(),
            [&database_uuid](const Database & d) { return d.uuid == database_uuid; }),
        databases.end());
}

std::optional<std::reference_wrapper<const Database>> Manifest::findDatabase(const String & database_uuid) const
{
    for (const auto & d : databases)
    {
        if (d.uuid == database_uuid)
            return std::cref(d);
    }
    return std::nullopt;
}

std::optional<std::reference_wrapper<Database>> Manifest::findDatabase(const String & database_uuid)
{
    for (auto & d : databases)
    {
        if (d.uuid == database_uuid)
            return std::ref(d);
    }
    return std::nullopt;
}

UInt32 Manifest::getTablesCount() const
{
    UInt32 count = 0;
    for (const auto & db : databases)
        count += db.tables_count;
    return count;
}

void print(const Table & t, std::ostream & os, int indent)
{
    String sp(indent, ' ');
    os << sp << "Table: " << t.name << " (uuid=" << t.uuid << ")\n";
    os << sp << "  key: " << t.key << "\n";
    os << sp << "  version: " << t.version << "\n";
    os << sp << "  last_modified: " << t.last_modified << "\n";
}

void print(const Database & d, std::ostream & os, int indent)
{
    String sp(indent, ' ');
    os << sp << "Database: " << d.name << " (uuid=" << d.uuid << ")\n";
    os << sp << "  key: " << d.key << "\n";
    os << sp << "  version: " << d.version << "\n";
    os << sp << "  last_modified: " << d.last_modified << "\n";
    os << sp << "  tables_count: " << d.tables.size() << "\n";
    for (const auto & t : d.tables)
        print(t, os, indent + 4);
}

void print(const Manifest & m, std::ostream & os)
{
    os << "ClickHouse Manifest\n";
    os << "  ck_version: " << m.ck_version << "\n";
    os << "  last_modified: " << m.last_modified << "\n";
    os << "  etag: " << m.etag << "\n";
    os << "  databases_count: " << m.databases.size() << "\n";
    for (const auto & db : m.databases)
        print(db, os, 2);
}

}
