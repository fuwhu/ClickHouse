#include "IcebergCommon.h"
#include <optional>

#include <base/scope_guard.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/Base64Decoder.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/MemoryStream.h>
#include <Poco/StreamCopier.h>
#include <Common/FieldVisitorToString.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include "Storages/Iceberg/IcebergFileSource.h"
#include <Interpreters/Context.h>

namespace ProfileEvents
{
extern const Event IcebergApiCallElapsedMicroseconds;
extern const Event IcebergApiCalls;
extern const Event IcebergScanIcebergTableTimeCostMicroseconds;
extern const Event IcebergScanIcebergTableCount;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ICEBERG_CATALOG_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_WRITE_TO_OSTREAM;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}

namespace
{
    DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required);

    DataTypePtr convertToInnerType(const String & type_name)
    {
        if (type_name == "boolean")
            return std::make_shared<DataTypeUInt8>();
        else if (type_name == "int")
            return std::make_shared<DataTypeInt32>();
        else if (type_name == "long")
            return std::make_shared<DataTypeInt64>();
        else if (type_name == "float")
            return std::make_shared<DataTypeFloat32>();
        else if (type_name == "double")
            return std::make_shared<DataTypeFloat64>();
        else if (type_name == "date")
            return std::make_shared<DataTypeDate>();
        else if (type_name == "time")
            return std::make_shared<DataTypeInt64>();
        else if (type_name == "timestamp")
            return std::make_shared<DataTypeDateTime64>(6);
        else if (type_name == "timestamptz")
            return std::make_shared<DataTypeDateTime64>(6, "UTC");
        else if (type_name == "string" || type_name == "binary")
            return std::make_shared<DataTypeString>();
        else if (type_name == "uuid")
            return std::make_shared<DataTypeUUID>();
        else if (type_name.starts_with("fixed[") && type_name.ends_with(']'))
        {
            ReadBufferFromString buf(String(type_name.begin() + 6, type_name.end() - 1));
            size_t n;
            readIntText(n, buf);
            return std::make_shared<DataTypeFixedString>(n);
        }
        else if (type_name.starts_with("decimal(") && type_name.ends_with(')'))
        {
            auto s = String(type_name.begin() + 8, type_name.end() - 1);
            ReadBufferFromString buf(s);
            size_t precision;
            size_t scale;
            readIntText(precision, buf);
            skipWhitespaceIfAny(buf);
            assertChar(',', buf);
            skipWhitespaceIfAny(buf);
            tryReadIntText(scale, buf);
            return createDecimal<DataTypeDecimal>(precision, scale);
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported Iceberg type: {}", type_name);
    }

    DataTypePtr convertToInnerType(const Poco::JSON::Object::Ptr & object)
    {
        String type_name = object->getValue<String>("type");
        if (type_name == "list")
        {
            bool element_required = object->getValue<bool>("element-required");
            auto element_type = getFieldType(object, "element", element_required);
            return std::make_shared<DataTypeArray>(element_type);
        }

        if (type_name == "map")
        {
            auto key_type = getFieldType(object, "key", true);
            auto value_required = object->getValue<bool>("value-required");
            auto value_type = getFieldType(object, "value", value_required);
            return std::make_shared<DataTypeMap>(key_type, value_type);
        }

        if (type_name == "struct")
        {
            DataTypes element_types;
            Names element_names;
            auto fields = object->get("fields").extract<Poco::JSON::Array::Ptr>();
            element_types.reserve(fields->size());
            element_names.reserve(fields->size());
            for (size_t i = 0; i != fields->size(); ++i)
            {
                auto field = fields->getObject(static_cast<Int32>(i));
                element_names.push_back(field->getValue<String>("name"));
                auto required = field->getValue<bool>("required");
                element_types.push_back(getFieldType(field, "type", required));
            }

            return std::make_shared<DataTypeTuple>(element_types, element_names);
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported Iceberg type: {}", type_name);
    }

    DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required)
    {
        if (field->isObject(type_key))
            return convertToInnerType(field->getObject(type_key));

        auto type = field->get(type_key);
        if (type.isString())
        {
            const String & type_name = type.extract<String>();
            auto data_type = convertToInnerType(type_name);
            return required ? data_type : makeNullable(data_type);
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected 'type' field: {}", type.toString());
    }

    Poco::Dynamic::Var visitField(const Field & field)
    {
        auto which = field.getType();
        switch (which)
        {
            case Field::Types::UInt64: {
                return field.get<UInt64>();
            }
            case Field::Types::Int64: {
                return field.get<Int64>();
            }
            case Field::Types::Float64: {
                return field.get<Float64>();
            }
            case Field::Types::String: {
                return field.get<String>();
            }
            case Field::Types::Array: {
                Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());
                auto tuple = field.get<Array>();
                for (const auto & child : tuple)
                    arr->add(visitField(child));
                return arr;
            }
            case Field::Types::Tuple: {
                Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());
                auto tuple = field.get<Tuple>();
                for (const auto & child : tuple)
                    arr->add(visitField(child));
                return arr;
            }
            case Field::Types::Decimal32:
            case Field::Types::Decimal64: {
                return applyVisitor(FieldVisitorToString(), field);
            }
            case Field::Types::Bool: {
                return field.get<bool>();
            }
            default:
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported data type: {}", field.getTypeName());
        }
    }

    Field extractField(const Poco::Dynamic::Var & value)
    {
        if (value.isString())
            return value.extract<String>();
        else if (value.isBoolean())
            return value.extract<bool>();
        else if (value.isInteger() && value.isSigned())
            return value.convert<Int64>();
        else if (value.isInteger() && !value.isSigned())
            return value.convert<UInt64>();
        else if (value.isNumeric() && !value.isInteger())
            return value.convert<Float64>();
        else
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported data type");
    }

    String buildInputData(std::unordered_map<String, Poco::Dynamic::Var> params)
    {
        if (params.empty())
            return {};

        Poco::JSON::Object req_json;
        for (const auto & [key, value] : params)
            req_json.set(key, value);

        std::ostringstream oss;
        oss.exceptions(std::ios::failbit);
        Poco::JSON::Stringifier::stringify(req_json, oss);

        return oss.str();
    }

    ConnectionTimeouts timeouts(
        Poco::Timespan(3000000), /// Connection timeout. 3s
        Poco::Timespan(10000000), /// Send timeout. 10s
        Poco::Timespan(10000000) /// Receive timeout. 10s
    );

    constexpr auto max_try_times = 3;
    constexpr auto pool_size = 128;

    template <typename T, typename P>
    T call(const String & endpoint, const String & path, const String & data, P processor)
    {
        Stopwatch watch;

        Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_POST, path, Poco::Net::HTTPRequest::HTTP_1_1);
        request.set("Content-Type", "application/json");
        request.set("Content-Length", std::to_string(data.size()));

        PooledHTTPSessionPtr session;
        Poco::Net::HTTPResponse response;
        auto send_with_retry = [&]() -> std::istream &
        {
            auto try_times = 0;
            while (true)
            {
                ++try_times;
                try
                {
                    session = makePooledHTTPSession(Poco::URI(endpoint), timeouts, pool_size);
                    auto & request_stream = session->sendRequest(request);
                    request_stream.exceptions(std::ios::failbit);

                    if (!data.empty())
                        request_stream.write(data.data(), data.size());

                    return session->receiveResponse(response);
                }
                catch (...)
                {
                    if (try_times >= max_try_times)
                        throw;
                }
            }
        };

        std::istream & response_stream = send_with_retry();

        Poco::JSON::Parser parser;
        if (response.getStatus() != Poco::Net::HTTPResponse::HTTP_OK)
        {
            String exception_message;
            Poco::StreamCopier::copyToString(response_stream, exception_message);

            throw Exception(ErrorCodes::ICEBERG_CATALOG_ERROR, exception_message);
        }

        auto ret = parser.parse(response_stream).extract<Poco::JSON::Object::Ptr>();

        watch.stop();
        ProfileEvents::increment(ProfileEvents::IcebergApiCallElapsedMicroseconds, watch.elapsedMicroseconds());
        ProfileEvents::increment(ProfileEvents::IcebergApiCalls, 1);

        auto ret_code = ret->getValue<int>("code");
        if (ret_code != Poco::Net::HTTPResponse::HTTP_OK)
        {
            String exception_message = ret->getValue<String>("message");
            throw Exception(ErrorCodes::ICEBERG_CATALOG_ERROR, exception_message);
        }

        auto ret_data = ret->getObject("data");

        return processor(ret_data);
    }
}

void PocoJsonDeserialize::deserialize(const String & str)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(str).extract<Poco::JSON::Object::Ptr>();

    deserialize(json);
}

void IcebergTableMetadata::deserialize(const Poco::JSON::Object::Ptr & obj)
{
    this->current_snapshot_id = obj->getValue<Int64>("current-snapshot-id");

    /// parse schema
    auto schemas_obj_arr = obj->getArray("schemas");
    if (schemas_obj_arr->size() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "At least one element in schemas");

    auto fields_obj_arr = schemas_obj_arr->getObject(0)->getArray("fields");
    for (const auto & field_obj : *fields_obj_arr)
    {
        const auto & field_struct = field_obj.extract<Poco::JSON::Object::Ptr>();
        auto name = field_struct->getValue<String>("name");
        auto required = field_struct->getValue<bool>("required");
        auto type = getFieldType(field_struct, "type", required);
        this->schema.push_back({name, type});
    }

    /// parse partition keys
    auto partition_obj_arr = obj->getArray("partition-specs");
    if (partition_obj_arr && partition_obj_arr->size() != 0)
    {
        auto partition_fields_obj_arr = partition_obj_arr->getObject(0)->getArray("fields");
        for (const auto & field_obj : *partition_fields_obj_arr)
        {
            const auto & field_struct = field_obj.extract<Poco::JSON::Object::Ptr>();
            auto name = field_struct->getValue<String>("name");
            auto transform = field_struct->getValue<String>("transform");

            if (transform == "identity")
                this->partition_keys.push_back({name});
        }
    }

    /// parse sorting keys
    auto sort_obj_arr = obj->getArray("sort-orders");
    if (sort_obj_arr && sort_obj_arr->size() != 0)
    {
        for (const auto & sort_order_obj : *sort_obj_arr)
        {
            const auto & sort_order_struct = sort_order_obj.extract<Poco::JSON::Object::Ptr>();
            auto sort_fields_obj_arr = sort_order_struct->getArray("fields");
            this->order_id = sort_order_struct->getValue<Int64>("order-id");
            if (!sort_fields_obj_arr || sort_fields_obj_arr->size() == 0)
                continue;

            for (const auto & field_obj : *sort_fields_obj_arr)
            {
                const auto & field_struct = field_obj.extract<Poco::JSON::Object::Ptr>();
                auto source_id = field_struct->getValue<int>("source-id");
                auto name = this->schema.getNames()[source_id - 1];

                auto direction = field_struct->getValue<String>("direction");
                auto nulls_direction = field_struct->getValue<String>("null-order");
                auto transform = field_struct->getValue<String>("transform");

                if (transform == "identity")
                    this->sorting_keys.push_back({name, direction == "asc" ? 1 : -1, nulls_direction == "nulls-first" ? 1 : -1});
            }
        }

        /// parse summary
        auto snapshots_obj_arr = obj->getArray("snapshots");
        if (snapshots_obj_arr && snapshots_obj_arr->size() != 0)
        {
            auto summary = snapshots_obj_arr->getObject(0)->getObject("summary");
            this->total_records = summary->getValue<Int64>("total-records");
            this->total_files_size = summary->getValue<Int64>("total-files-size");
        }
    }
}

void IcebergTableScanResult::deserialize(const Poco::JSON::Object::Ptr & obj)
{
    auto residual_expr = obj->get("residual-expression");
    if (residual_expr)
    {
        if (residual_expr.isBoolean())
        {
            bool true_or_false = residual_expr.extract<bool>();
            this->residual_expression.root
                = IcebergExpression::newNode(true_or_false ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE);
        }
        else
        {
            this->residual_expression.deserialize(residual_expr.extract<Poco::JSON::Object::Ptr>());
        }
    }

    auto files_obj_arr = obj->getArray("files");
    this->files.reserve(files_obj_arr->size());

    for (const auto & file_obj : *files_obj_arr)
    {
        IcebergDataFile data_file;

        const auto & file_struct = file_obj.extract<Poco::JSON::Object::Ptr>();
        data_file.path = file_struct->getValue<String>("path");
        data_file.format = file_struct->getValue<String>("format");
        data_file.size = file_struct->getValue<size_t>("size");
        auto sorting_key_value = file_struct->get("sort-order-id");
        if (sorting_key_value.isEmpty())
            data_file.sorting_key_id = std::nullopt;
        else
            data_file.sorting_key_id = sorting_key_value.convert<Int64>();

        /// parse indicies
        std::vector<IcebergIndexFile> indices;
        auto indicies_arr = file_struct->getArray("indices");
        if (indicies_arr)
        {
            indices.reserve(indicies_arr->size());
            for (const auto & index_obj : *indicies_arr)
            {
                const auto & index_struct = index_obj.extract<Poco::JSON::Object::Ptr>();
                auto index_id = index_struct->getValue<Int64>("index_id");
                auto index_data = index_struct->getValue<String>("index_data");
                auto correlated_table_snapshot = index_struct->getValue<Int64>("correlated_table_snapshot");

                indices.push_back({index_id, index_data, correlated_table_snapshot});
            }
        }
        data_file.indicies = std::move(indices);

        /// parse statistics
        auto statistics_obj = file_struct->getObject("statistics");
        if (statistics_obj)
        {
            data_file.statistics.count = statistics_obj->getValue<Int64>("count");
            auto min_obj = statistics_obj->getObject("min");
            if (min_obj)
            {
                for (const auto & ele : *min_obj->getArray("keys"))
                    data_file.statistics.min.keys.push_back(ele.extract<Int64>());

                for (const auto & ele : *min_obj->getArray("values"))
                    data_file.statistics.min.values.push_back(extractField(ele));
            }

            auto max_obj = statistics_obj->getObject("max");
            if (max_obj)
            {
                for (const auto & ele : *max_obj->getArray("keys"))
                    data_file.statistics.max.keys.push_back(ele.extract<Int64>());

                for (const auto & ele : *max_obj->getArray("values"))
                    data_file.statistics.max.values.push_back(extractField(ele));
            }
        }

        this->files.push_back(std::move(data_file));
    }
}

Poco::JSON::Object::Ptr IcebergExpression::serialize() const
{
    if (!root)
        return nullptr;

    std::function<void(Poco::JSON::Object::Ptr, const IcebergExpression::NodePtr &)> traverse;

    traverse = [&traverse](Poco::JSON::Object::Ptr obj, const IcebergExpression::NodePtr & node) -> void
    {
        if (node->type == IcebergExpression::Type::LEAF)
        {
            obj->set("type", IcebergExpression::typeToString(node->element->type));
            obj->set("term", node->element->reference);
            if (node->element->type == IcebergExpression::Type::IN || node->element->type == IcebergExpression::Type::NOT_IN)
            {
                obj->set("values", visitField(node->element->literal));
            }
            else if (
                node->element->type != IcebergExpression::Type::IS_NULL && node->element->type != IcebergExpression::Type::NOT_NULL
                && node->element->type != IcebergExpression::Type::IS_NAN && node->element->type != IcebergExpression::Type::NOT_NAN)
            {
                obj->set("value", visitField(node->element->literal));
            }
        }
        else if (node->type == IcebergExpression::Type::TRUE || node->type == IcebergExpression::Type::FALSE)
        {
            obj->set("type", "literal");
            obj->set("value", node->type == IcebergExpression::Type::TRUE);
        }
        else if (node->type == IcebergExpression::Type::NOT)
        {
            assert(node->children.size() == 1);

            obj->set("type", IcebergExpression::typeToString(node->type));
            Poco::JSON::Object::Ptr child_obj(new Poco::JSON::Object());
            traverse(child_obj, node->children[0]);
            obj->set("child", child_obj);
        }
        else if (node->type == IcebergExpression::Type::AND || node->type == IcebergExpression::Type::OR)
        {
            assert(node->children.size() == 2);

            obj->set("type", IcebergExpression::typeToString(node->type));
            Poco::JSON::Object::Ptr left_obj(new Poco::JSON::Object());
            Poco::JSON::Object::Ptr right_obj(new Poco::JSON::Object());
            traverse(left_obj, node->children[0]);
            traverse(right_obj, node->children[1]);

            obj->set("left", left_obj);
            obj->set("right", right_obj);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type: {}", IcebergExpression::typeToString(node->type));
        }
    };

    Poco::JSON::Object::Ptr root_obj(new Poco::JSON::Object());
    traverse(root_obj, root);

    return root_obj;
}

void IcebergExpression::deserialize(const Poco::JSON::Object::Ptr & obj)
{
    /// empty
    if (obj->size() == 0)
        return;

    std::function<void(const Poco::JSON::Object::Ptr &, IcebergExpression::NodePtr &)> traverse;

    traverse = [&traverse](const Poco::JSON::Object::Ptr & json_node, IcebergExpression::NodePtr & node) -> void
    {
        if (!json_node->has("type"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No key 'type' found in json node");

        auto type = json_node->getValue<String>("type");
        if (!node)
            node = newNode(IcebergExpression::stringToType(type));

        switch (node->type)
        {
            case Type::NOT: {
                const auto & child_obj = json_node->getObject("child");
                NodePtr child_node;
                traverse(child_obj, child_node);
                node->children.push_back(std::move(child_node));
            }
            break;
            case Type::AND:
            case Type::OR: {
                const auto & left_obj = json_node->getObject("left");
                const auto & right_obj = json_node->getObject("right");
                NodePtr left_node;
                NodePtr right_node;
                traverse(left_obj, left_node);
                traverse(right_obj, right_node);
                node->children.push_back(std::move(left_node));
                node->children.push_back(std::move(right_node));
            }
            break;
            case Type::IN:
            case Type::NOT_IN: {
                const auto & term = json_node->getValue<String>("term");
                const auto & values_arr = json_node->getArray("values");
                Array array;
                for (const auto & value : *values_arr)
                    array.push_back(extractField(value));

                node->element = IcebergExpression::newElement(node->type, term, std::move(array));
                node->type = IcebergExpression::Type::LEAF;
            }
            break;
            case Type::IS_NULL:
            case Type::NOT_NULL:
            case Type::IS_NAN:
            case Type::NOT_NAN: {
                const auto & term = json_node->getValue<String>("term");
                node->element = IcebergExpression::newElement(node->type, term, Field());
                node->type = IcebergExpression::Type::LEAF;
            }
            break;
            case Type::LT:
            case Type::LT_EQ:
            case Type::GT:
            case Type::GT_EQ:
            case Type::EQ:
            case Type::NOT_EQ:
            case Type::STARTS_WITH:
            case Type::NOT_STARTS_WITH:
            case Type::RANGE:
            case Type::HAS_TERM:
            case Type::NOT_HAS_TERM:
            case Type::LIKE:
            case Type::NOT_LIKE:
            case Type::ARRAY_CONTAINS: {
                const auto & term = json_node->getValue<String>("term");
                const auto & value = json_node->get("value");
                node->element = IcebergExpression::newElement(node->type, term, extractField(value));
                node->type = IcebergExpression::Type::LEAF;
            }
            break;
            default: {
                if (node->type == Type::UNKNOWN && type == "literal")
                {
                    const auto & value = json_node->get("value");
                    node->type = value.convert<bool>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
                }
                else
                {
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type: {}", IcebergExpression::typeToString(node->type));
                }
            }
        }
    };

    traverse(obj, root);
}

IcebergTables listIcebergTables(const String & endpoint, const String & iceberg_database)
{
    static const auto * const api_path = "/iceberg/api/table/list";

    auto data = buildInputData({{"database", iceberg_database}});

    auto processor = [&](const Poco::JSON::Object::Ptr & resp_json) -> IcebergTables
    {
        IcebergTables tables;

        const auto & tables_obj = resp_json->getArray("tables");
        tables.reserve(tables_obj->size());
        for (const auto & table_obj : *tables_obj)
            tables.emplace_back(table_obj.extract<String>());

        return tables;
    };

    try
    {
        return call<IcebergTables>(endpoint, api_path, data, processor);
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::ICEBERG_CATALOG_ERROR,
            "Can not list Iceberg tables for catalog `{}`. error: {}",
            iceberg_database,
            getCurrentExceptionMessage(false));
    }
}

IcebergTableMetadata loadIcebergTable(const String & endpoint, const String & iceberg_database, const String & iceberg_table)
{
    static const auto * const api_path = "/iceberg/api/table/property";

    auto data = buildInputData({{"database", iceberg_database}, {"table", iceberg_table}});

    auto processor = [&](const Poco::JSON::Object::Ptr & resp_json) -> IcebergTableMetadata
    {
        IcebergTableMetadata metadata;
        metadata.deserialize(resp_json);
        metadata.database = iceberg_database;
        metadata.table = iceberg_table;

        return metadata;
    };

    try
    {
        return call<IcebergTableMetadata>(endpoint, api_path, data, processor);
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::ICEBERG_CATALOG_ERROR,
            "Can not get Iceberg table metadata for `{}`.`{}`. error: {}",
            iceberg_database,
            iceberg_table,
            getCurrentExceptionMessage(false));
    }
}

IcebergTableScanResult scanIcebergTable(
    const String & endpoint, const IcebergTableMetadata & metadata, const std::vector<String> & stats_keys, const IcebergExpression & filter_expression)
{
    return scanIcebergTable(endpoint, metadata.database, metadata.table, metadata.current_snapshot_id, stats_keys, filter_expression);
}

IcebergTableScanResult scanIcebergTable(
    const String & endpoint,
    const String & iceberg_database,
    const String & iceberg_table,
    Int64 snapshot_id,
    const std::vector<String> & stats_keys,
    const IcebergExpression & filter_expression)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IcebergScanIcebergTableTimeCostMicroseconds, watch.elapsedMicroseconds()); });
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IcebergScanIcebergTableCount, 1); });

    static const auto * const api_path = "/iceberg/api/filter/table";

    if (snapshot_id == -1)
        return IcebergTableScanResult{};

    Poco::JSON::Array stats_keys_arr;
    for (const String & stats_key : stats_keys)
        stats_keys_arr.add(stats_key);

    Poco::JSON::Object::Ptr filters = filter_expression.serialize();
    auto data = buildInputData(
        {{"database", iceberg_database},
         {"table", iceberg_table},
         {"snapshot-id", snapshot_id},
         {"stats-keys", std::move(stats_keys_arr)},
         {"need-statistics", true},
         {"filtering-expression", std::move(filters)}});

    auto processor = [&](const Poco::JSON::Object::Ptr & resp_json) -> IcebergTableScanResult
    {
        IcebergTableScanResult result;
        result.deserialize(resp_json);

        return result;
    };

    try
    {
        return call<IcebergTableScanResult>(endpoint, api_path, data, processor);
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::ICEBERG_CATALOG_ERROR,
            "Can not scan Iceberg table for `{}`.`{}`. error: {}",
            iceberg_database,
            iceberg_table,
            getCurrentExceptionMessage(false));
    }
}

IcebergFileScanResults scanIcebergFiles(
    const String & endpoint,
    const String & iceberg_database,
    const String & iceberg_table,
    Int64 snapshot_id,
    IcebergDataFiles & data_files,
    const IcebergExpression & filter_expression)
{
    static const auto * const api_path = "/iceberg/api/filter/file";

    IcebergFileScanResults results(data_files.size());
    Poco::JSON::Array::Ptr files(new Poco::JSON::Array());
    for (size_t i = 0; i < data_files.size(); ++i)
    {
        auto & data_file = data_files[i];
        if (filter_expression.emptyOrTrue() || data_file.indicies.empty())
        {
            IcebergFileScanResult result;
            result.data_file = data_file;
            result.needed = true;

            results[i] = std::move(result);
        }
        else
        {
            Poco::JSON::Array::Ptr iceberg_indicies(new Poco::JSON::Array());
            for (const auto & index : data_file.indicies)
            {
                Poco::JSON::Object::Ptr iceberg_index(new Poco::JSON::Object());
                iceberg_index->set("index_data", index.index_data);
                iceberg_index->set("index_id", index.index_id);
                iceberg_index->set("correlated_table_snapshot", index.correlated_table_snapshot);
                iceberg_indicies->add(iceberg_index);
            }  
            Poco::JSON::Object::Ptr file(new Poco::JSON::Object());
            file->set("file-id", i);
            file->set("record-count", data_file.statistics.count);
            file->set("indices", iceberg_indicies);
            files->add(file); 
            
        }
    }
   
    if (files->size() == 0)
        return results;

    Poco::JSON::Object::Ptr filters = filter_expression.serialize();
    auto data = buildInputData(
        {{"database", iceberg_database},
         {"table", iceberg_table},
         {"snapshot-id", snapshot_id},
         {"files", files},
         {"filtering-expression", std::move(filters)}});

    auto processor = [&](const Poco::JSON::Object::Ptr & resp_json) -> void
    {
        auto ret_files_arr = resp_json->getArray("files");
        for (const auto & ret_file_var : *ret_files_arr)
        {
            const auto & ret_file_obj = ret_file_var.extract<Poco::JSON::Object::Ptr>();
            auto file_id = ret_file_obj->getValue<size_t>("file-id");

            IcebergFileScanResult result;
            result.data_file = std::move(data_files[file_id]);

            result.needed = ret_file_obj->getValue<bool>("is-needed");
            if (!result.needed)
            {
                results[file_id] = std::move(result);
                continue;
            }

            auto bitmap_var = ret_file_obj->get("enforced-bitmap");
            if (!bitmap_var.isEmpty())
            {
                const auto & encoded_bitmap = bitmap_var.extract<String>();
                String decoded;
                Poco::MemoryInputStream istr(encoded_bitmap.data(), encoded_bitmap.size());
                Poco::Base64Decoder decoder(istr);
                Poco::StreamCopier::copyToString(decoder, decoded);

                result.mask = std::make_shared<roaring::Roaring>(roaring::Roaring::read(decoded.data()));
            }

            results[file_id] = std::move(result);
        }
    };

    try
    {
        call<void>(endpoint, api_path, data, processor);
        return results;
    }
    catch (...)
    {
        throw Exception(
            ErrorCodes::ICEBERG_CATALOG_ERROR,
            "Can not scan Iceberg files for `{}`.`{}`. error: {}",
            iceberg_database,
            iceberg_table,
            getCurrentExceptionMessage(false));
    }
}
}
