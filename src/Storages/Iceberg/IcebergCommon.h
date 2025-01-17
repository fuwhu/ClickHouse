#pragma once

#include <Core/NamesAndTypes.h>
#include <IO/HTTPCommon.h>
#include <roaring.hh>
#include <Disks/RemoteDisksCommon.h>

namespace DB
{
struct PocoJsonSerialize
{
    PocoJsonSerialize() = default;

    PocoJsonSerialize(const PocoJsonSerialize &) = default;

    PocoJsonSerialize & operator=(const PocoJsonSerialize &) = default;

    PocoJsonSerialize(PocoJsonSerialize &&) = default;

    PocoJsonSerialize & operator=(PocoJsonSerialize &&) = default;

    virtual ~PocoJsonSerialize() = default;

    virtual Poco::JSON::Object::Ptr serialize() const = 0;
};

struct PocoJsonDeserialize
{
    PocoJsonDeserialize() = default;

    PocoJsonDeserialize(const PocoJsonDeserialize &) = default;

    PocoJsonDeserialize & operator=(const PocoJsonDeserialize &) = default;

    PocoJsonDeserialize(PocoJsonDeserialize &&) = default;

    PocoJsonDeserialize & operator=(PocoJsonDeserialize &&) = default;

    virtual ~PocoJsonDeserialize() = default;

    virtual void deserialize(const Poco::JSON::Object::Ptr & obj) = 0;

    void deserialize(const String & str);
};

enum class DistributionMode
{
    RANDOM,
    CONSISTENT_HASH
};

struct IcebergCatalogConfig
{
    String iceberg_api_server_uri;
    String iceberg_database;
    String cluster;
    DistributionMode distribution_mode = DistributionMode::CONSISTENT_HASH;
};

using IcebergTables = std::vector<String>;

struct IcebergTableMetadata : public PocoJsonDeserialize
{
    struct PartitionKey
    {
        String name;
    };

    struct SortingKey
    {
        String name;
        int direction; /// 1 - ascending, -1 - descending.
        int nulls_direction; /// 1 - NULLs and NaNs are greater, -1 - less.
            /// To achieve NULLS LAST, set it equal to direction, to achieve NULLS FIRST, set it opposite.
    };

    String database;
    String table;
    Int64 current_snapshot_id;
    Int64 order_id;

    NamesAndTypesList schema;
    std::vector<PartitionKey> partition_keys;
    std::vector<SortingKey> sorting_keys;

    std::optional<Int64> total_records;
    std::optional<Int64> total_files_size;

    void deserialize(const Poco::JSON::Object::Ptr & obj) override;
};

struct IcebergExpression : public PocoJsonSerialize, public PocoJsonDeserialize
{
    struct Node;
    struct Element;
    using NodePtr = std::unique_ptr<Node>;
    using ElementPtr = std::unique_ptr<Element>;

    enum struct Type
    {
        UNKNOWN,
        LEAF,

        NOT,
        AND,
        OR,

        TRUE,
        FALSE,

        IS_NULL,
        NOT_NULL,
        IS_NAN,
        NOT_NAN,
        LT,
        LT_EQ,
        GT,
        GT_EQ,
        EQ,
        NOT_EQ,
        IN,
        NOT_IN,
        STARTS_WITH,
        NOT_STARTS_WITH,
        RANGE,
        HAS_TERM,
        NOT_HAS_TERM,
        LIKE,
        NOT_LIKE,
        ARRAY_CONTAINS,

        /// Aggreators, no use now
        COUNT,
        COUNT_STAR,
        MAX,
        MIN
    };

    static String typeToString(Type type) { return Poco::replace(Poco::toLower(String(magic_enum::enum_name(type))), "_", "-"); }

    static Type stringToType(const String & str)
    {
        return magic_enum::enum_cast<Type>(Poco::replace(Poco::toUpper(str), "-", "_")).value_or(Type::UNKNOWN);
    }

    struct Element
    {
        Type type;

        String reference;

        Field literal;

        Element(Type type_, String reference_, Field literal_) : type(type_), reference(std::move(reference_)), literal(std::move(literal_))
        {
        }
    };

    struct Node
    {
        Type type; /// AND OR NOT TRUE FALSE LEAF

        ElementPtr element; /// LEAF only

        std::vector<NodePtr> children;

        Node(Type type_) : type(type_) { } /// NOLINT
        Node(Type type_, ElementPtr element_) : type(type_), element(std::move(element_)) { }
        Node(Type type_, std::vector<NodePtr> children_) : type(type_), children(std::move(children_)) { }
        Node(Type type_, ElementPtr element_, std::vector<NodePtr> children_)
            : type(type_), element(std::move(element_)), children(std::move(children_))
        {
        }
    };

    IcebergExpression() = default;
    
    IcebergExpression(NodePtr root_) : root(std::move(root_)) { } /// NOLINT

    NodePtr root;

    template <typename... Args>
    static NodePtr newNode(Args &&... args)
    {
        return std::unique_ptr<Node>(new Node(std::forward<Args>(args)...)); /// NOLINT
    }

    template <typename... Args>
    static ElementPtr newElement(Args &&... args)
    {
        return std::unique_ptr<Element>(new Element(std::forward<Args>(args)...)); /// NOLINT
    }

    bool emptyOrTrue() const { return !root || root->type == Type::UNKNOWN || root->type == Type::TRUE; }

    Poco::JSON::Object::Ptr serialize() const override;

    using PocoJsonDeserialize::deserialize;

    void deserialize(const Poco::JSON::Object::Ptr & obj) override;
};

struct IcebergFileStatistics
{
    struct MinMax
    {
        std::vector<Int64> keys;
        std::vector<Field> values;
    };

    MinMax min;
    MinMax max;
    Int64 count;
};

struct IcebergIndexFile
{
    Int64 index_id;
    String index_data;
    Int64 correlated_table_snapshot;
};
struct IcebergTableMetaWithUri
{
    String endpoint;
    String iceberg_database;
    String iceberg_table;
    Int64 snapshot_id;
};

struct IcebergDataFile
{
    String path;
    String format;
    std::vector<IcebergIndexFile> indicies;
    size_t size;
    IcebergFileStatistics statistics;
    std::optional<Int64> sorting_key_id;
};
using IcebergDataFiles = std::vector<IcebergDataFile>;

struct IcebergTableScanResult : public PocoJsonDeserialize
{
    std::vector<IcebergDataFile> files;
    IcebergExpression residual_expression;

    void deserialize(const Poco::JSON::Object::Ptr & obj) override;
};

struct IcebergFileScanResult
{
    bool needed;
    std::shared_ptr<roaring::Roaring> mask;

    IcebergDataFile data_file;
};
using IcebergFileScanResults = std::vector<IcebergFileScanResult>;

IcebergTables listIcebergTables(const String & endpoint, const String & iceberg_database);

IcebergTableMetadata loadIcebergTable(const String & endpoint, const String & iceberg_database, const String & iceberg_table);

IcebergTableScanResult scanIcebergTable(
    const String & endpoint,
    const IcebergTableMetadata & metadata,
    const std::vector<String> & stats_keys,
    const IcebergExpression & filter_expression);

IcebergTableScanResult scanIcebergTable(
    const String & endpoint,
    const String & iceberg_database,
    const String & iceberg_table,
    Int64 snapshot_id,
    const std::vector<String> & stats_keys,
    const IcebergExpression & filter_expression);

IcebergFileScanResults scanIcebergFiles(
    const String & endpoint,
    const String & iceberg_database,
    const String & iceberg_table,
    Int64 snapshot_id,
    IcebergDataFiles & data_files,
    const IcebergExpression & filter_expression);
}
