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
#include <boost/algorithm/string.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/range/algorithm/find_first_of.hpp>
#include <boost/regex.hpp>
#include <Common/Exception.h>

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
    virtual bool check(const String &) const = 0;

protected:
    virtual ~ConditionOperator() = default;
};

using ConditionOperatorPtr = std::shared_ptr<const ConditionOperator>;
using ConditionOperatorImpl = std::function<ConditionOperatorPtr(const String &)>;

template <bool Not = false>
class ConditionOperatorIn : public ConditionOperator
{
public:
    static constexpr auto op = Not ? "not in" : "in";

    static constexpr auto split_char = ",";

    static ConditionOperatorPtr init(const String & right_) { return std::make_shared<ConditionOperatorIn>(right_); }

    explicit ConditionOperatorIn(const String & right_)
    {
        Strings res;
        boost::split(res, right_, boost::is_any_of(split_char));
        std::for_each(res.begin(), res.end(), [](auto & i) { boost::trim(i); });

        right.insert(res.begin(), res.end());
    }

    inline ALWAYS_INLINE bool check(const String & left) const override
    {
        if constexpr (Not)
        {
            return !right.count(left);
        }
        return right.count(left);
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

    explicit ConditionOperatorComparison(const String & right_) : right(right_) { }

    inline ALWAYS_INLINE bool check(const String & left) const override { return Comparator::check(left, right); }

private:
    String right;
};

struct ComparatorGreater
{
    static constexpr auto op = ">";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left > right; }
};

struct ComparatorGreaterEqual
{
    static constexpr auto op = ">=";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left >= right; }
};

struct ComparatorLess
{
    static constexpr auto op = "<";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left < right; }
};

struct ComparatorLessEqual
{
    static constexpr auto op = "<=";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left <= right; }
};

struct ComparatorEqual
{
    static constexpr auto op = "=";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left == right; }
};

struct ComparatorNotEqual
{
    static constexpr auto op = "!=";

    inline ALWAYS_INLINE static bool check(const String & left, const String & right) { return left != right; }
};

template <bool Not = false>
class ConditionOperatorLike : public ConditionOperator
{
public:
    static constexpr auto op = Not ? "not like" : "like";

    static ConditionOperatorPtr init(const String & right_) { return std::make_shared<ConditionOperatorLike>(right_); }

    explicit ConditionOperatorLike(const String & right_) { right = boost::regex{right_}; }

    inline ALWAYS_INLINE bool check(const String & left) const override
    {
        if constexpr (Not)
        {
            return !boost::regex_match(left, right);
        }
        return boost::regex_match(left, right);
    }

private:
    boost::regex right;
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
        {
            return it->second(right);
        }

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Input operator error, got '{}', expected one of [>, <, >=, <=, =, !=, in, not in, like, not like]",
            op);
    }

private:
    using ConditionOperatorGreater = ConditionOperatorComparison<ComparatorGreater>;
    using ConditionOperatorLess = ConditionOperatorComparison<ComparatorLess>;
    using ConditionOperatorGreaterEqual = ConditionOperatorComparison<ComparatorGreaterEqual>;
    using ConditionOperatorLessEqual = ConditionOperatorComparison<ComparatorLessEqual>;
    using ConditionOperatorEqual = ConditionOperatorComparison<ComparatorEqual>;
    using ConditionOperatorNotEqual = ConditionOperatorComparison<ComparatorNotEqual>;
    using ConditionOperatorNotIn = ConditionOperatorIn<true>;
    using ConditionOperatorNotLike = ConditionOperatorLike<true>;

    ConditionOperatorFactory()
    {
#define EMPLACE_CONDITION(cond) operators.emplace(cond::op, &cond::init);

        EMPLACE_CONDITION(ConditionOperatorIn<>)
        EMPLACE_CONDITION(ConditionOperatorNotIn)
        EMPLACE_CONDITION(ConditionOperatorGreater)
        EMPLACE_CONDITION(ConditionOperatorLess)
        EMPLACE_CONDITION(ConditionOperatorGreaterEqual)
        EMPLACE_CONDITION(ConditionOperatorLessEqual)
        EMPLACE_CONDITION(ConditionOperatorEqual)
        EMPLACE_CONDITION(ConditionOperatorNotEqual)
        EMPLACE_CONDITION(ConditionOperatorLike<>)
        EMPLACE_CONDITION(ConditionOperatorNotLike)

#undef EMPLACE_CONDITION
    }

    std::unordered_map<String, ConditionOperatorImpl> operators;
};

class ConditionPipeline : private boost::noncopyable
{
public:
    static constexpr auto split_char = '`';

    enum class Mode
    {
        AND,
        OR
    };

    explicit ConditionPipeline(String & mode_)
    {
        boost::trim(mode_);
        boost::to_upper(mode_);

        if (mode_ == "AND")
        {
            mode = Mode::AND;
        }
        else if (mode_ == "OR")
        {
            mode = Mode::OR;
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Logical connector only support [AND, OR], got '{}'", mode_);
        }
    }

    void addCondition(const ColumnPtr & cond_keys, String & op, const String & right)
    {
        boost::trim(op);
        boost::to_lower(op);

        auto cond_op = ConditionOperatorFactory::instance().get(op, right);

        checkers.push_back({cond_op, cond_keys});
    }

    ColumnPtr execute(const IColumn & input, const ColumnArray::Offsets & offsets, size_t input_rows_count)
    {
        auto result = ColumnUInt8::create(input.size());
        auto & res_vec = result->getData();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto offset = offsets[i - 1];
            auto size = offsets[i] - offsets[i - 1];

            for (size_t j = 0; j < size; ++j)
            {
                if (mode == Mode::AND)
                {
                    res_vec[offset + j] = true;
                }
                else
                {
                    res_vec[offset + j] = false;
                }

                for (const auto & [func, col] : checkers)
                {
                    auto idx = col->get64(i);
                    String left;
                    if (idx != 0)
                    {
                        left = getValueAt(input.getDataAt(offset + j), idx);
                    }

                    if (mode == Mode::AND)
                    {
                        res_vec[offset + j] &= func->check(left);
                        if (!res_vec[offset + j])
                        {
                            break;
                        }
                    }
                    else
                    {
                        res_vec[offset + j] |= func->check(left);
                        if (res_vec[offset + j])
                        {
                            break;
                        }
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


    Mode mode;
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
            {
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
        const auto * col_mode = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());
        if (!col_mode)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The third argument of function {} must be a const String", getName());
        }
        auto mode = col_mode->getValue<String>();

        ConditionPipeline pipeline(mode);
        {
            auto func_index_of = FunctionFactory::instance().get("indexOf", nullptr);

            ColumnsWithTypeAndName args(2);
            args[0] = arguments[0];

            for (size_t i = 3; i < arguments.size(); ++i)
            {
                const auto * col_cond = checkAndGetColumnConst<ColumnTuple>(arguments[i].column.get());
                if (!col_cond)
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "The condition arguments of function {} must be const Tuple(String, String, String)",
                        getName());
                }

                auto cond_tuple = col_cond->getValue<Tuple>();
                if (cond_tuple.size() != 3)
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_COLUMN,
                        "The condition arguments of function {} must be const Tuple(String, String, String)",
                        getName());
                }

                auto left = cond_tuple[0].safeGet<String>();
                auto op = cond_tuple[1].safeGet<String>();
                auto right = cond_tuple[2].safeGet<String>();

                auto col_left = DataTypeString().createColumnConst(1, left);
                args[1] = ColumnWithTypeAndName{col_left, std::make_shared<DataTypeString>(), ""};

                ColumnPtr cond_keys = func_index_of->build(args)->execute(args, std::make_shared<DataTypeUInt64>(), input_rows_count);

                pipeline.addCondition(cond_keys, op, right);
            }
        }

        const auto * col_map = checkAndGetColumn<ColumnMap>(arguments[1].column.get());
        if (!col_map)
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The second argument of function {} must be a Map(String, Array)", getName());
        }

        const auto & offsets = col_map->getNestedColumn().getOffsets();
        const auto & keys_data = col_map->getNestedData().getColumn(0);
        const auto & values_data = col_map->getNestedData().getColumn(1);

        auto col_flags = pipeline.execute(keys_data, offsets, input_rows_count);

        auto result = result_type->createColumn();
        result->reserve(input_rows_count);

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto offset = offsets[i - 1];
            auto size = offsets[i] - offsets[i - 1];

            std::array<UInt64, 2> ret{0, 0};
            for (size_t j = 0; j < size; ++j)
            {
                if (col_flags->getBool(offset + j))
                {
                    Array arr = values_data[offset + j].safeGet<Array>();
                    ret[0] += arr[0].safeGet<UInt64>();
                    ret[1] += arr[1].safeGet<UInt64>();
                }
            }

            result->insert(Tuple{ret[0], ret[1]});
        }

        return result;
    }
};


void registerFunctionPolarisArgsParse(FunctionFactory & factory)
{
    factory.registerFunction<FunctionPolarisArgsParse>(FunctionFactory::CaseInsensitive);
}
}
