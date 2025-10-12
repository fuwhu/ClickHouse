#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Field.h>
#include <Core/Types.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Poco/StringTokenizer.h>
#include <Common/Exception.h>
#include <Common/re2.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_COLUMN;
extern const int TYPE_MISMATCH;
}

class ConditionOperator
{
public:
    enum class Op
    {
        EQUAL,
        NOT_EQUAL,
        GREATER_OR_EQUAL,
        GREATER,
        LESS,
        LESS_OR_EQUAL,
        LIKE,
        NOT_LIKE,
        IN,
        NOT_IN
    };

    static String getOpName(Op op)
    {
        switch (op)
        {
            case Op::EQUAL:
                return "=";
            case Op::NOT_EQUAL:
                return "!=";
            case Op::GREATER_OR_EQUAL:
                return ">=";
            case Op::GREATER:
                return ">";
            case Op::LESS:
                return "<";
            case Op::LESS_OR_EQUAL:
                return "<=";
            case Op::LIKE:
                return "like";
            case Op::NOT_LIKE:
                return "not like";
            case Op::IN:
                return "in";
            case Op::NOT_IN:
                return "not in";
        }

        std::unreachable();
    }

    virtual bool check(const String &) const = 0;

    virtual ~ConditionOperator() = default;
};

class ConditionOperatorChain
{
public:
    enum class Mode
    {
        AND,
        OR
    };

    virtual ~ConditionOperatorChain() = default;

    virtual void addCondition(const ColumnPtr & cond_keys, const String & op, const String & right) = 0;

    virtual ColumnPtr execute(const IColumn & input, const ColumnArray::Offsets & offsets, size_t input_rows_count) = 0;
};

using ConditionOperatorPtr = std::shared_ptr<const ConditionOperator>;

template <bool negate = false>
class ConditionOperatorIn : public ConditionOperator
{
public:
    static constexpr auto op = negate ? ConditionOperator::Op::NOT_IN : ConditionOperator::Op::IN;

    static constexpr auto split_char = ",";

    static ConditionOperatorPtr init(const String & right_) { return std::make_shared<ConditionOperatorIn>(right_); }

    explicit ConditionOperatorIn(const String & right_)
    {
        Poco::StringTokenizer tokenizer(
            right_, split_char, Poco::StringTokenizer::Options::TOK_IGNORE_EMPTY | Poco::StringTokenizer::Options::TOK_TRIM);

        right.insert(tokenizer.begin(), tokenizer.end());
    }

    inline ALWAYS_INLINE bool check(const String & left) const override
    {
        if constexpr (negate)
        {
            return !right.contains(left);
        }
        return right.contains(left);
    }

private:
    std::unordered_set<String> right;
};

template <typename Comparator>
class ConditionOperatorComparison : public ConditionOperator
{
public:
    static constexpr auto op = Comparator::op;

    static ConditionOperatorPtr init(const String & right_) { return std::make_shared<ConditionOperatorComparison>(right_); }

    explicit ConditionOperatorComparison(const String & right_)
    {
        right_raw = right_;

        if constexpr (
            op == ConditionOperator::Op::LESS || op == ConditionOperator::Op::GREATER || op == ConditionOperator::Op::LESS_OR_EQUAL
            || op == ConditionOperator::Op::GREATER_OR_EQUAL)
        {
            Int64 right_int;
            Float64 right_float;
            ReadBufferFromString buf(right_);
            if (tryReadIntText(right_int, buf))
            {
                right = right_int;
                return;
            }
            else if (tryReadFloatText(right_float, buf))
            {
                right = right_float;
                return;
            }
        }

        right = right_;
    }

    inline ALWAYS_INLINE bool check(const String & left) const override
    {
        if constexpr (
            op == ConditionOperator::Op::LESS || op == ConditionOperator::Op::GREATER || op == ConditionOperator::Op::LESS_OR_EQUAL
            || op == ConditionOperator::Op::GREATER_OR_EQUAL)
        {
            if (!std::holds_alternative<String>(right))
            {
                Int64 left_int;
                Float64 left_float;
                ReadBufferFromString buf(left);
                if (std::holds_alternative<Int64>(right) && tryReadIntText(left_int, buf))
                    return Comparator::check(left_int, std::get<Int64>(right));
                else if (std::holds_alternative<Float64>(right) && tryReadFloatText(left_float, buf))
                    return Comparator::check(left_float, std::get<Float64>(right));
                else
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS, "left({}) and right({}) arguments have different data types", left, right_raw);
            }
        }

        return Comparator::check(left, std::get<String>(right));
    }

private:
    String right_raw;
    std::variant<String, Int64, Float64> right;
};

template <bool negate = false>
struct ComparatorGreater
{
    static constexpr auto op = negate ? ConditionOperator::Op::LESS : ConditionOperator::Op::GREATER;

    template <typename T>
    inline ALWAYS_INLINE static bool check(const T & left, const T & right)
    {
        if constexpr (negate)
            return left < right;
        return left > right;
    }
};

template <bool negate = false>
struct ComparatorEqual
{
    static constexpr auto op = negate ? ConditionOperator::Op::NOT_EQUAL : ConditionOperator::Op::EQUAL;

    template <typename T>
    inline ALWAYS_INLINE static bool check(const T & left, const T & right)
    {
        if constexpr (negate)
            return left != right;
        return left == right;
    }
};

template <bool negate = false>
struct ComparatorGreaterEqual
{
    static constexpr auto op = negate ? ConditionOperator::Op::LESS_OR_EQUAL : ConditionOperator::Op::GREATER_OR_EQUAL;

    template <typename T>
    inline ALWAYS_INLINE static bool check(const T & left, const T & right)
    {
        return (ComparatorGreater<negate>::check(left, right) || ComparatorEqual<false>::check(left, right));
    }
};

template <bool negate = false>
class ConditionOperatorLike : public ConditionOperator
{
public:
    static constexpr auto op = negate ? ConditionOperator::Op::NOT_LIKE : ConditionOperator::Op::LIKE;

    static ConditionOperatorPtr init(const String & right_) { return std::make_shared<ConditionOperatorLike>(right_); }

    explicit ConditionOperatorLike(const String & right_) { right = std::make_unique<re2::RE2>(right_); }

    inline ALWAYS_INLINE bool check(const String & left) const override
    {
        if constexpr (negate)
            return !re2::RE2::FullMatch(left, *right);
        return re2::RE2::FullMatch(left, *right);
    }

private:
    std::unique_ptr<re2::RE2> right;
};

class ConditionOperatorFactory : private boost::noncopyable
{
public:
    static ConditionOperatorFactory & instance()
    {
        static ConditionOperatorFactory factory;

        return factory;
    }

    ConditionOperatorPtr get(const String & op, const String & right)
    {
        auto it = operators.find(op);
        if (it != operators.end())
            return it->second(right);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Input operator error, got '{}', expected one of [>, <, >=, <=, =, !=, in, not in, like, not like]",
            op);
    }

private:
    ConditionOperatorFactory()
    {
#define EMPLACE_CONDITION(cond) operators.emplace(ConditionOperator::getOpName(cond::op), &cond::init)

        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorGreater<false>>);
        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorGreater<true>>);
        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorGreaterEqual<false>>);
        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorGreaterEqual<true>>);
        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorEqual<false>>);
        EMPLACE_CONDITION(ConditionOperatorComparison<ComparatorEqual<true>>);
        EMPLACE_CONDITION(ConditionOperatorIn<false>);
        EMPLACE_CONDITION(ConditionOperatorIn<true>);
        EMPLACE_CONDITION(ConditionOperatorLike<false>);
        EMPLACE_CONDITION(ConditionOperatorLike<true>);

#undef EMPLACE_CONDITION
    }

    std::unordered_map<String, std::function<ConditionOperatorPtr(const String &)>> operators;
};

template <ConditionOperatorChain::Mode mode>
class ConditionOperatorChainImpl : public ConditionOperatorChain, private boost::noncopyable
{
public:
    static constexpr auto split_char = '`';

    void addCondition(const ColumnPtr & cond_keys, const String & op, const String & right) override
    {
        auto cond_op = ConditionOperatorFactory::instance().get(Poco::toLower(Poco::trim(op)), right);

        checkers.push_back({cond_op, cond_keys});
    }

    ColumnPtr execute(const IColumn & input, const ColumnArray::Offsets & offsets, size_t input_rows_count) override
    {
        auto result = ColumnUInt8::create(input.size());
        auto & res_vec = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto offset = offsets[i - 1];
            auto size = offsets[i] - offsets[i - 1];

            for (size_t j = 0; j < size; ++j)
            {
                if constexpr (mode == ConditionOperatorChain::Mode::AND)
                    res_vec[offset + j] = true;
                else
                    res_vec[offset + j] = false;

                for (const auto & [func, col] : checkers)
                {
                    auto idx = col->get64(i);
                    if (idx == 0)
                    {
                        res_vec[offset + j] = false;

                        if constexpr (mode == ConditionOperatorChain::Mode::AND)
                            break;
                        else
                            continue;
                    }

                    String left = getValueAt(input.getDataAt(offset + j), idx);

                    if constexpr (mode == ConditionOperatorChain::Mode::AND)
                    {
                        res_vec[offset + j] &= func->check(left);
                        if (!res_vec[offset + j])
                            break;
                    }
                    else
                    {
                        res_vec[offset + j] |= func->check(left);
                        if (res_vec[offset + j])
                            break;
                    }
                }
            }
        }

        return result;
    }

private:
    static inline ALWAYS_INLINE String getValueAt(const StringRef & s, size_t n)
    {
        const char * end = s.data + s.size;

        const char * pos = s.data;
        const char * token_begin = pos;
        const char * token_end = pos;
        for (size_t i = 0; i < n; ++i)
        {
            if (!pos)
            {
                token_begin = end;
                token_end = end;
                break;
            }

            token_begin = pos;

            pos = reinterpret_cast<const char *>(memchr(pos, split_char, end - pos));

            if (pos)
            {
                token_end = pos;
                ++pos;
            }
            else
            {
                token_end = end;
            }
        }

        return String(token_begin, token_end);
    }


    std::vector<std::tuple<ConditionOperatorPtr, ColumnPtr>> checkers;
};

class FunctionPolarisArgsParse final : public IFunction
{
public:
    static constexpr auto name = "polarisArgsParse";

    static constexpr auto max_conditions_size = 32;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPolarisArgsParse>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 4 || arguments.size() > max_conditions_size + 3)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bad arguments, use function {} as: "
                "polarisArgsParse(fields_keys, fields_values, logical connector, (column value, logic comparator, to be compared value), "
                "(...))",
                getName());
        }

        const auto * type_arr = checkAndGetDataType<DataTypeArray>(arguments[0].get());
        if (!type_arr || !isString(type_arr->getNestedType()))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The first argument of function {} must be a Array(String)", getName());
        }

        const auto * type_map = checkAndGetDataType<DataTypeMap>(arguments[1].get());
        if (!type_map || !isString(type_map->getKeyType()) || !isArray(type_map->getValueType()))
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a Map(String, Array)", getName());
        }

        if (!isString(arguments[2]))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The third argument of function {} must be a String", getName());
        }

        for (size_t i = 3; i < arguments.size(); ++i)
        {
            if (!isTuple(arguments[i]))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The condition arguments of function {} must be Tuple(String, String, String)",
                    getName());

            const auto & data_type_tuple = checkAndGetDataType<DataTypeTuple>(*arguments[i]);
            if (data_type_tuple.getElements().size() != 3)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The condition arguments of function {} must be Tuple(String, String, String)",
                    getName());

            for (const auto & sub_data_type : data_type_tuple.getElements())
            {
                if (!isString(sub_data_type))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The condition arguments of function {} must be Tuple(String, String, String)",
                        getName());
            }
        }

        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        const auto & col_mode = checkAndGetColumnConst<ColumnString>(*arguments[2].column);
        auto mode = Poco::toUpper(Poco::trim(col_mode.getValue<String>()));

        std::unique_ptr<ConditionOperatorChain> chain;
        if (mode == "AND")
            chain = std::make_unique<ConditionOperatorChainImpl<ConditionOperatorChain::Mode::AND>>();
        else if (mode == "OR")
            chain = std::make_unique<ConditionOperatorChainImpl<ConditionOperatorChain::Mode::OR>>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Logical connector only support [AND, OR], got '{}'", mode);

        {
            auto func_index_of = FunctionFactory::instance().get("indexOf", nullptr);

            ColumnsWithTypeAndName args(2);
            args[0] = arguments[0];

            for (size_t i = 3; i < arguments.size(); ++i)
            {
                const auto & col_cond = checkAndGetColumnConst<ColumnTuple>(*arguments[i].column);

                auto cond_tuple = col_cond.getValue<Tuple>();
                auto left = cond_tuple[0].safeGet<String>();
                auto op = cond_tuple[1].safeGet<String>();
                auto right = cond_tuple[2].safeGet<String>();

                auto col_left = DataTypeString().createColumnConst(1, left);
                args[1] = ColumnWithTypeAndName{col_left, std::make_shared<DataTypeString>(), ""};

                ColumnPtr cond_keys = func_index_of->build(args)->execute(args, std::make_shared<DataTypeUInt64>(), input_rows_count, /* dry_run = */ false);

                chain->addCondition(cond_keys, op, right);
            }
        }

        const auto & col_map = checkAndGetColumn<ColumnMap>(*arguments[1].column);

        const auto & offsets = col_map.getNestedColumn().getOffsets();
        const auto & keys_data = col_map.getNestedData().getColumn(0);
        const auto & values_data = col_map.getNestedData().getColumn(1);

        auto col_flags = chain->execute(keys_data, offsets, input_rows_count);

        auto result = result_type->createColumn();
        result->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto offset = offsets[i - 1];
            auto size = offsets[i] - offsets[i - 1];

            UInt64 r1 = 0;
            UInt64 r2 = 0;
            for (size_t j = 0; j < size; ++j)
            {
                if (col_flags->getBool(offset + j))
                {
                    Array arr = values_data[offset + j].safeGet<Array>();
                    r1 += arr[0].safeGet<UInt64>();
                    r2 += arr[1].safeGet<UInt64>();
                }
            }

            result->insert(Tuple{r1, r2});
        }

        return result;
    }
};


REGISTER_FUNCTION(polarisArgsParse)
{
    /// Special cases for polaris
    /// Use function as: polarisArgsParse(fields_keys, fields_values, logical connector, (column value, logic comparator, to be compared value)

    /// fields_keys(Array(String))                                  fields_values(Map(String, Array(UInt64)))
    /// ['page_num', 'click_area', 'sort_filter', 'country']        {'20`supermali`9`england': [3,5], '44`mamamiya`85`russia': [4,27]}

    /// SELECT polarisArgsParse(fields_keys, fields_values, 'and', ('page_num', '=', '20'), ('click_area', '=', 'mamamiya'))
    /// result: [0, 0] -- condition 'page_num = 20 and click_area = mamamiya' match nothing

    /// SELECT polarisArgsParse(fields_keys, fields_values, 'or', ('page_num', '=', '20'), ('click_area', '=', 'mamamiya'))
    /// result: [7, 32] -- condition 'page_num = 20 or click_area = mamamiya' match 2 elements in the fields_values map

    /// SELECT polarisArgsParse(fields_keys, fields_values, 'or', ('page_num', '=', '30'), ('click_area', '=', 'mamamiya'))
    /// result: [4, 27] -- condition 'page_num = 30 or click_area = mamamiya' match 1 elements in the fields_values map
    factory.registerFunction<FunctionPolarisArgsParse>({}, FunctionFactory::Case::Insensitive);
}
}
