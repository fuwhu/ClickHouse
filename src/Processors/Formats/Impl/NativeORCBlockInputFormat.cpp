#include "NativeORCBlockInputFormat.h"
#include <algorithm>
#include <exception>
#include <memory>
#include <string>
#include <vector>
#include <orc/Reader.hh>
#include "Common/Exception.h"
#include "Common/Stopwatch.h"
#include "Columns/ColumnConst.h"
#include "Columns/IColumn.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/Field.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "IO/WriteBufferFromString.h"
#include "Storages/ColumnsDescription.h"
#include "base/logger_useful.h"
#include "base/types.h"

#if USE_ORC
#    include <Columns/ColumnDecimal.h>
#    include <Columns/ColumnFixedString.h>
#    include <Columns/ColumnMap.h>
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnsCommon.h>
#    include <Columns/ColumnsNumber.h>
#    include <Columns/FilterDescription.h>
#    include <DataTypes/DataTypeArray.h>
#    include <DataTypes/DataTypeDate32.h>
#    include <DataTypes/DataTypeDateTime64.h>
#    include <DataTypes/DataTypeFactory.h>
#    include <DataTypes/DataTypeFixedString.h>
#    include <DataTypes/DataTypeMap.h>
#    include <DataTypes/DataTypeNullable.h>
#    include <DataTypes/DataTypeString.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <DataTypes/DataTypesDecimal.h>
#    include <DataTypes/DataTypesNumber.h>
#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <Formats/insertNullAsDefaultIfNeeded.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/WriteHelpers.h>
#    include <IO/copyData.h>
#    include <Interpreters/ExpressionActions.h>
#    include <Interpreters/castColumn.h>
#    include <Storages/MergeTree/MergeTreeRangeReader.h>
#    include <boost/algorithm/string/case_conv.hpp>
#    include <Common/quoteString.h>
#    include "ArrowBufferedStreams.h"

#    if USE_JEMALLOC
#        include <jemalloc/jemalloc.h>
#    endif

#    if !USE_JEMALLOC
#        include <cstdlib>
#    endif

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_TYPE;
    extern const int VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE;
    extern const int THERE_IS_NO_COLUMN;
    extern const int INCORRECT_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}

class ORCMemoryPool final : public orc::MemoryPool
{
public:
    static ORCMemoryPool & instance()
    {
        static ORCMemoryPool pool;
        return pool;
    }
    char * malloc(uint64_t size) override
    {
        auto * ptr = ::malloc(size);
        /// For nullable columns some of the values will not be initialized.
        // __msan_unpoison(ptr, size);
        return static_cast<char *>(ptr);
    }

    void free(char * p) override { ::free(p); }

private:
    ORCMemoryPool() = default;
};

ORCInputStream::ORCInputStream(SeekableReadBuffer & in_, size_t file_size_) : in(in_), file_size(file_size_), pool(ORCMemoryPool::instance()) {
    buffer = std::make_unique<orc::DataBuffer<char>>(pool);
    buffer->reserve(MAX_BUFFER_SIZE);
}

uint64_t ORCInputStream::getLength() const
{
    return file_size;
}

uint64_t ORCInputStream::getNaturalReadSize() const
{
    return 1 * 1024 * 1024;
}

void ORCInputStream::read(void * buf, uint64_t length, uint64_t offset)
{
    in.readDirect(reinterpret_cast<char *>(buf), offset, length);
}

uint64_t ORCInputStream::readFromRanges(void* buf, uint64_t length, uint64_t offset)
{
    if (!inCurrentRange(offset)) {
        current_range = findIncludingRange(offset);
        seek(offset, false);
        uint64_t bytes_to_fetch = std::min(MAX_BUFFER_SIZE, current_range.length() - (offset - current_range.start));
        auto bytes_read = in.readDirect(buffer->data(), file_position, bytes_to_fetch);
        file_position += bytes_read;
        buffer->resize(bytes_read, false);
        auto bytes_to_copy = std::min(length, buffer->size());
        ::memcpy(buf, buffer->data(), bytes_to_copy);
        buffer_position = bytes_to_copy;
        return buffer_position;
    } else {
        seek(offset, false);
        uint64_t bytes_read;
        if (buffer->empty()) {
            uint64_t bytes_to_fetch = std::min(MAX_BUFFER_SIZE, current_range.length() - (offset - current_range.start));
            bytes_read = in.readDirect(buffer->data(), file_position, bytes_to_fetch);
            file_position += bytes_read;
            buffer->resize(bytes_read, false);
            buffer_position = 0;
        }
        // *buf = buffer->data() + buffer_position;
        bytes_read = std::min(length, buffer->size() - buffer_position);
        ::memcpy(buf, buffer->data() + buffer_position, bytes_read);
        buffer_position += bytes_read;
        return bytes_read;
    }
}

void ORCInputStream::prefetch(uint64_t offset, uint64_t length)
{
    if (!inCurrentRange(offset)) {
        current_range = findIncludingRange(offset);
        seek(offset, false);
        uint64_t bytes_to_fetch = std::min(MAX_BUFFER_SIZE, current_range.length() - (offset - current_range.start));
        auto bytes_read = in.readDirect(buffer->data(), file_position, bytes_to_fetch);
        file_position += bytes_read;
        buffer->resize(bytes_read, false);
        buffer_position = 0;
    } else {
        seek(offset, false);
        if (buffer->empty()) {
            uint64_t bytes_to_fetch = std::min(MAX_BUFFER_SIZE, current_range.length() - (offset - current_range.start));
            auto bytes_read = in.readDirect(buffer->data(), file_position, bytes_to_fetch);
            file_position += bytes_read;
            buffer->resize(bytes_read, false);
            buffer_position = 0;
        } else {
            auto slice_lenth = buffer->size() - buffer_position;
            if (slice_lenth >= length)
                return;
            auto new_buffer_size = std::min(MAX_BUFFER_SIZE, current_range.end - position());
            buffer->reserveWithSliceRetained(MAX_BUFFER_SIZE, buffer_position, slice_lenth);
            if (slice_lenth > new_buffer_size)
                throw Exception("the buffer end should not execeed the end of current range.", ErrorCodes::LOGICAL_ERROR);
            auto bytes_to_fetch = new_buffer_size - slice_lenth;
            auto bytes_read = in.readDirect(buffer->data() + slice_lenth, file_position, bytes_to_fetch);
            file_position += bytes_read;
            buffer->resize(slice_lenth + bytes_read, false);
            buffer_position = 0;
        }
    }
}

void ORCInputStream::seek(const uint64_t & offset, bool seek_in) {
    if (offset < file_position && offset >= bufferStartPosition())
    {
        buffer_position = buffer->size() - (file_position - offset);
        return;
    }
    if (offset > file_size) [[unlikely]]
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);
    if (seek_in) [[unlikely]]
        in.seek(offset);
    file_position = offset;
    // Clear the buffer.
    buffer->resize(0);
    buffer_position = 0;
}

const orc::OffsetRange & ORCInputStream::findIncludingRange(const uint64_t & offset) const {
    for (const auto & range : ranges_to_read) {
        if (offset>=range.start && offset<range.end)
            return range;
    }
    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "the offset is not in ranges.");
}

void ORCInputStream::setOrAddRangesToRead(const std::vector<orc::OffsetRange> & ranges, bool is_set) {
    if (is_set)
        ranges_to_read = ranges;
    else
        ranges_to_read.insert(ranges_to_read.end(), ranges.begin(), ranges.end());
}

std::unique_ptr<orc::InputStream> asORCInputStream(ReadBuffer & in, const FormatSettings & settings)
{
    auto * seekable_in = dynamic_cast<SeekableReadBufferWithSize *>(&in);

    if (seekable_in && settings.seekable_read && seekable_in->getTotalSize())
        return std::make_unique<ORCInputStream>(*seekable_in, *seekable_in->getTotalSize());
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "input should be subclass of SeekableReadBufferWithSize with positive size");
}

static DataTypePtr parseORCType(const orc::Type * orc_type, bool skip_columns_with_unsupported_types, bool & skipped)
{
    assert(orc_type != nullptr);

    const int subtype_count = static_cast<int>(orc_type->getSubtypeCount());
    switch (orc_type->getKind())
    {
        case orc::TypeKind::BOOLEAN:
            return std::make_shared<DataTypeInt8>();
        case orc::TypeKind::BYTE:
            return std::make_shared<DataTypeInt8>();
        case orc::TypeKind::SHORT:
            return std::make_shared<DataTypeInt16>();
        case orc::TypeKind::INT:
            return std::make_shared<DataTypeInt32>();
        case orc::TypeKind::LONG:
            return std::make_shared<DataTypeInt64>();
        case orc::TypeKind::FLOAT:
            return std::make_shared<DataTypeFloat32>();
        case orc::TypeKind::DOUBLE:
            return std::make_shared<DataTypeFloat64>();
        case orc::TypeKind::DATE:
            return std::make_shared<DataTypeDate32>();
        case orc::TypeKind::TIMESTAMP:
            return std::make_shared<DataTypeDateTime64>(9);
        case orc::TypeKind::VARCHAR:
        case orc::TypeKind::BINARY:
        case orc::TypeKind::STRING:
            return std::make_shared<DataTypeString>();
        case orc::TypeKind::CHAR:
            return std::make_shared<DataTypeFixedString>(orc_type->getMaximumLength());
        case orc::TypeKind::DECIMAL: {
            UInt64 precision = orc_type->getPrecision();
            UInt64 scale = orc_type->getScale();
            if (precision == 0)
            {
                // In HIVE 0.11/0.12 precision is set as 0, but means max precision
                return createDecimal<DataTypeDecimal>(38, 6);
            }
            else
                return createDecimal<DataTypeDecimal>(precision, scale);
        }
        case orc::TypeKind::LIST: {
            if (subtype_count != 1)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc List type {}", orc_type->toString());

            DataTypePtr nested_type = parseORCType(orc_type->getSubtype(0), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeArray>(nested_type);
        }
        case orc::TypeKind::MAP: {
            if (subtype_count != 2)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid Orc Map type {}", orc_type->toString());

            DataTypePtr key_type = parseORCType(orc_type->getSubtype(0), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            DataTypePtr value_type = parseORCType(orc_type->getSubtype(1), skip_columns_with_unsupported_types, skipped);
            if (skipped)
                return {};

            return std::make_shared<DataTypeMap>(key_type, value_type);
        }
        case orc::TypeKind::STRUCT: {
            DataTypes nested_types;
            Strings nested_names;
            nested_types.reserve(subtype_count);
            nested_names.reserve(subtype_count);

            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                auto parsed_type = parseORCType(orc_type->getSubtype(i), skip_columns_with_unsupported_types, skipped);
                if (skipped)
                    return {};

                nested_types.push_back(parsed_type);
                nested_names.push_back(orc_type->getFieldName(i));
            }
            return std::make_shared<DataTypeTuple>(nested_types, nested_names);
        }
        default: {
            if (skip_columns_with_unsupported_types)
            {
                skipped = true;
                return {};
            }

            throw Exception(
                ErrorCodes::UNKNOWN_TYPE,
                "Unsupported ORC type '{}'."
                "If you want to skip columns with unsupported types, "
                "you can enable setting input_format_orc_skip_columns_with_unsupported_types_in_schema_inference",
                orc_type->toString());
        }
    }
}

static void getFileReaderAndSchema(
    ReadBuffer & in,
    std::unique_ptr<orc::Reader> & file_reader,
    Block & header,
    const FormatSettings & format_settings,
    std::atomic<int> & is_stopped)
{
    if (is_stopped)
        return;

    orc::ReaderOptions options;
    auto input_stream = asORCInputStream(in, format_settings);
    file_reader = orc::createReader(std::move(input_stream), options, true);
    const auto & schema = file_reader->getType();

    for (size_t i = 0; i < schema.getSubtypeCount(); ++i)
    {
        const std::string & name = schema.getFieldName(i);
        const orc::Type * orc_type = schema.getSubtype(i);

        bool skipped = false;
        DataTypePtr type = parseORCType(orc_type, format_settings.orc.skip_columns_with_unsupported_types_in_schema_inference, skipped);
        if (!skipped)
            header.insert(ColumnWithTypeAndName{type, name});
    }
}

template <class FieldType, class StatisticsType>
Range createRangeFromOrcStatistics(const StatisticsType * stats)
{
    if (stats->hasMinimum() && stats->hasMaximum())
    {
        return Range(static_cast<FieldType>(stats->getMinimum()), true, static_cast<FieldType>(stats->getMaximum()), true);
    }
    else if (stats->hasMinimum())
    {
        return Range::createLeftBounded(static_cast<FieldType>(stats->getMinimum()), true);
    }
    else if (stats->hasMaximum())
    {
        return Range::createRightBounded(static_cast<FieldType>(stats->getMaximum()), true);
    }

    return Range();
}

Range buildRange(const orc::ColumnStatistics * col_stats)
{
    Range r;

    if (!col_stats) [[unlikely]]
        return r;

    if (!col_stats->getNumberOfValues())
    {
        r = Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
        r.has_null = true;
        r.only_null = true;
        return r;
    }

    if (const auto * int_stats = dynamic_cast<const orc::IntegerColumnStatistics *>(col_stats))
    {
        r = createRangeFromOrcStatistics<Int64>(int_stats);
    }
    else if (const auto * double_stats = dynamic_cast<const orc::DoubleColumnStatistics *>(col_stats))
    {
        r = createRangeFromOrcStatistics<Float64>(double_stats);
    }
    else if (const auto * string_stats = dynamic_cast<const orc::StringColumnStatistics *>(col_stats))
    {
        r = createRangeFromOrcStatistics<String>(string_stats);
    }
    else if (const auto * bool_stats = dynamic_cast<const orc::BooleanColumnStatistics *>(col_stats))
    {
        auto false_cnt = bool_stats->getFalseCount();
        auto true_cnt = bool_stats->getTrueCount();
        if (false_cnt && true_cnt)
        {
            r = Range(static_cast<UInt8>(0), true, static_cast<UInt8>(1), true);
        }
        else if (false_cnt)
        {
            r = Range::createLeftBounded(static_cast<UInt8>(0), true);
        }
        else if (true_cnt)
        {
            r = Range::createRightBounded(static_cast<UInt8>(1), true);
        }
    }
    else if (const auto * timestamp_stats = dynamic_cast<const orc::TimestampColumnStatistics *>(col_stats))
    {
        r = createRangeFromOrcStatistics<UInt32>(timestamp_stats);
    }
    else if (const auto * date_stats = dynamic_cast<const orc::DateColumnStatistics *>(col_stats))
    {
        r = createRangeFromOrcStatistics<UInt16>(date_stats);
    }

    if (col_stats->hasNull())
    {
        r.has_null = true;
        r.only_null = false;
    }

    return r;
}

NativeORCBlockInputFormat::NativeORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
    header = getPort().getHeader();
}

void NativeORCBlockInputFormat::prepareFileReaderAndMetadata()
{
    if (file_reader_and_metadata_initialized)
        return;

    getFileReaderAndSchema(*in, file_reader, file_schema, format_settings, is_stopped);
    if (is_stopped)
        return;

    orc_column_to_ch_column = std::make_unique<ORCColumnToCHColumn>(
        format_settings.orc.allow_missing_columns, format_settings.null_as_default, format_settings.orc.case_insensitive_column_matching);

    const bool ignore_case = format_settings.orc.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);

    int index = 1;
    for (size_t i = 0; i < file_schema.columns(); ++i)
    {
        const auto & name = file_schema.getByPosition(i).name;
        auto name_to_check = ignore_case ? boost::to_lower_copy(name) : name;
        if (required_columns.has(name_to_check) || nested_table_names.contains(name_to_check))
        {
            include_indices.push_back(i);
            column_name_to_index.insert({name, index});
        }
        ++index;
    }

    current_stripe_index = 0;
    file_reader_and_metadata_initialized = true;
}

NativeORCBlockInputFormat::StripeReader::Range
NativeORCBlockInputFormat::StripeReader::getRowGroupRange(const OrcStripeInformation & stripe_info, size_t row_group_index)
{
    auto start_row_of_current_row_group
        = stripe_info.first_row_of_stripe + row_group_index * stripe_info.row_group_size;
    auto end_row_of_current_row_group = stripe_info.first_row_of_stripe
        + std::min(stripe_info.num_rows, (row_group_index + 1) * stripe_info.row_group_size);

    return {.start = start_row_of_current_row_group, .end = end_row_of_current_row_group};
}

void NativeORCBlockInputFormat::StripeReader::prepare()
{
    /// if batch_size is more than 10000, read at least 2 row groups
    auto row_group_batch_size
        = batch_size.has_value() ? (batch_size.value() + (stripe_info.row_group_size - 1)) / stripe_info.row_group_size : 1;
    
    auto batch_size_to_read = std::min(row_group_batch_size, stripe_info.row_groups.size());
    
    size_t succssive_size = 1;
    
    for (size_t i = current_row_group_index; i < stripe_info.row_groups.size() - 1; ++i)
    {
        if (stripe_info.row_groups[i + 1] == stripe_info.row_groups[i] + 1)
            succssive_size++;
        else 
            break;
    }

    batch_size_to_read = std::min(batch_size_to_read, succssive_size);

    row_groups_to_read.clear();

    std::move(
        stripe_info.row_groups.begin() + current_row_group_index,
        stripe_info.row_groups.begin() + current_row_group_index + batch_size_to_read,
        std::back_inserter(row_groups_to_read));
    
    current_row_group_index += batch_size_to_read;
    
    /// TODO: if stripe_info.row_groups is not successive, it need to read several times and combine the row batches
    to_read_rows = 0;
    for (auto row_group_index : row_groups_to_read)
    {
        auto row_group_range = getRowGroupRange(stripe_info, row_group_index);
        to_read_rows += std::min(stripe_info.row_group_size, row_group_range.end - row_group_range.start);
    }

    if (only_constant_columns)
        return;

    if (!batch || batch->capacity > to_read_rows)
        batch = row_reader->createRowBatch(to_read_rows);
    else
        batch->resize(to_read_rows);
}

bool NativeORCBlockInputFormat::StripeReader::hasPendingData() const
{
    if (current_row_group_index < stripe_info.row_groups.size() - 1)
        return true;
    else if (current_row_group_index == stripe_info.row_groups.size() - 1)
    {
        auto range = getRowGroupRange(stripe_info, stripe_info.row_groups[current_row_group_index]);
        return current_row != range.end;
    }

    return false;
}

void NativeORCBlockInputFormat::StripeReader::appendConstantColumns(ColumnsWithTypeAndName &res_columns, const size_t &num_rows, UInt64 &add_constant_col_time_cost) const
{
    // add the constant columns into res_columns if exist.
    if (constant_columns_desc && !constant_columns_desc->empty())
    {
        Stopwatch stop_watch;
        for (auto & constant_column_desc : *constant_columns_desc)
        {
            auto internal_column = constant_column_desc.column_type->createColumn();
            internal_column->insert(constant_column_desc.constant_value);
            ColumnPtr const_column = ColumnConst::create(std::move(internal_column), num_rows);
            ColumnWithTypeAndName column_with_type_name{const_column, constant_column_desc.column_type, constant_column_desc.column_name};
            res_columns.emplace_back(column_with_type_name);
        }
        stop_watch.stop();
        add_constant_col_time_cost = stop_watch.elapsedMicroseconds();
    }
}

size_t NativeORCBlockInputFormat::StripeReader::readColumns(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost)
{
    if (current_row_group_index >= stripe_info.row_groups.size())
        return 0;

    prepare();
    row_reader->readData(*batch, row_groups_to_read);
    size_t num_rows = batch->numElements;
    if (num_rows != to_read_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Should read exactly {} rows, bug got {}", to_read_rows, num_rows);

    current_row = row_reader->getRowNumber() + num_rows; // TODO :: update RowReaderImpl::previousRow to keep getRowNumber() returns correct rown number.

    const auto & schema = row_reader->getSelectedType();
    Stopwatch stop_watch;
    res_columns = orc_column_to_ch_column.orcTableToCHColumns(sample_block, &schema, batch.get());
    stop_watch.stop();
    orc_table_to_ch_columns_time_cost = stop_watch.elapsedMicroseconds();

    // add the constant columns into res_columns if exist.
    appendConstantColumns(res_columns, num_rows, add_constant_col_time_cost);

    return num_rows;
}

size_t NativeORCBlockInputFormat::StripeReader::readConstantColumns(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost)
{
    if (!only_constant_columns)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "readConstantColumns should be called only when the `only_constant_columns` is true.");

    if (current_row_group_index >= stripe_info.row_groups.size())
        return 0;

    prepare();
    size_t num_rows = to_read_rows;
    current_row += num_rows;
    // add the constant columns into res_columns if exist.
    appendConstantColumns(res_columns, num_rows, add_constant_col_time_cost);

    // add the columns which exist in `sample_block` and does not exist in the orc file schema, make it as constant column with default value of its data type.
    auto names_and_types = sample_block.getNamesAndTypesList();
    if (!names_and_types.empty())
    {
        for (const auto &name_and_type : names_and_types)
        {
            auto internal_column = name_and_type.type->createColumn();
            internal_column->insertDefault();
            ColumnPtr const_column = ColumnConst::create(std::move(internal_column), num_rows);
            ColumnWithTypeAndName column_with_type_name{const_column, name_and_type.type, name_and_type.name};
            res_columns.emplace_back(std::move(column_with_type_name));
        }
    }

    return num_rows;
}

size_t NativeORCBlockInputFormat::StripeReader::read(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost)
{
    if (only_constant_columns)
        return readConstantColumns(res_columns, add_constant_col_time_cost);
    else
        return readColumns(res_columns, add_constant_col_time_cost);
}

std::unique_ptr<NativeORCBlockInputFormat::StripeReader> NativeORCBlockInputFormat::newStripeReader(
    const OrcStripeInformation & stripe_info,
    const std::list<UInt64> & indices,
    const Block & sample_block,
    bool overwrite_input_ranges,
    OrcConstantColumnsDescriptionPtr & constant_column_values,
    const bool &constant_columns_only)
{
    std::optional<size_t> batch_size = static_cast<size_t>(format_settings.orc.row_batch_size);
    if (ignore_batch_size_limit)
        batch_size = std::nullopt;

    if (constant_columns_only)
        return std::make_unique<NativeORCBlockInputFormat::StripeReader>(stripe_info, batch_size, sample_block, *orc_column_to_ch_column, constant_column_values);

    orc::RowReaderOptions row_reader_options;
    row_reader_options.include(indices);
    row_reader_options.range(stripe_info.offset, stripe_info.length);
    auto row_reader = file_reader->createRowReader(row_reader_options);
    return std::make_unique<NativeORCBlockInputFormat::StripeReader>(
        std::move(row_reader), stripe_info, batch_size, sample_block, *orc_column_to_ch_column, overwrite_input_ranges, constant_column_values);
}

bool NativeORCBlockInputFormat::checkStripeColumnConstantness(UInt64 stripe_index, UInt64 column_index, Field & constant_value)
{
    if (!stripes_statistics)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "stripes statistics is null while checking the column constantness, which could be an bug.");
    const auto & stripe_statistics = (*stripes_statistics)[stripe_index];
    const orc::ColumnStatistics * col_stats = stripe_statistics->getColumnStatistics(column_index);
    auto range = buildRange(col_stats);
    bool is_constant = (range.left == range.right && range.left_included && range.right_included && !range.has_null);
    if (is_constant)
        constant_value = range.left;
    return is_constant;
}

bool NativeORCBlockInputFormat::prepareStripeReader()
{
    assert(file_reader);

    bool has_pending_data = false;
    if (prewhere_stripe_reader)
        has_pending_data = prewhere_stripe_reader->hasPendingData();
    else
        has_pending_data = stripe_reader && stripe_reader->hasPendingData();

    if (has_pending_data)
        return true;

    if (current_stripe_index >= stripes_to_read->size())
        return false;

    /// skip unused row groups
    if (filter_row_group_callback)
    {
        while (current_stripe_index < stripes_to_read->size())
        {
            auto & current_stripe = (*stripes_to_read)[current_stripe_index];
            filter_row_group_callback(current_stripe, getStripeStatistics(current_stripe.stripe_index).get());
            if (current_stripe.row_groups.empty())
                current_stripe_index++;
            else
                break;
        }
        if (current_stripe_index >= stripes_to_read->size())
            return false;
    }

    // Find and remove constant columns from the include indices of orc row readers.
    OrcConstantColumnsDescriptionPtr constant_columns_desc;
    auto remove_constant_column = [&](std::list<UInt64> &include_indices_, Block &sample_block)
    {
        Stopwatch stop_watch;
        constant_columns_desc = std::make_shared<OrcConstantColumnsDescription>();
        for (auto it = include_indices_.begin(); it != include_indices_.end(); )
        {
            const auto &col_id = *it;
            auto & col_with_type_name = file_schema.getByPosition(col_id);
            GetColumnsOptions options{GetColumnsOptions::Kind::Ordinary};
            DataTypePtr column_type;
            if (auto got_col = required_columns.tryGetColumn(options, col_with_type_name.name))
                column_type = got_col->type;
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "constant column {} is not found in the required columns, which could be a bug.", col_with_type_name.name);

            auto found_it = column_name_to_index.find(col_with_type_name.name);
            if (found_it == column_name_to_index.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "column {} is not found in column_name_to_index, which could be a bug.", col_with_type_name.name);
            auto col_index = found_it->second;
            Field constant_value;
            auto is_constant = checkStripeColumnConstantness(current_stripe_index, col_index, constant_value);
            if (is_constant)
            {
                OrcConstantColumnDescription constant_col_desc{col_id, col_with_type_name.name, column_type, constant_value};
                constant_columns_desc->emplace_back(constant_col_desc);
                it = include_indices_.erase(it);
                sample_block.erase(sample_block.getPositionByName(col_with_type_name.name));
            } else
                ++it;
        }
        remove_constant_column_time_cost += stop_watch.elapsedMicroseconds();
    };
    std::list<UInt64> prewhere_include_indices_for_stripe = prewhere_include_indices;
    std::list<UInt64> include_indices_for_stripe = include_indices;
    Block prewhere_header_for_stripe = prewhere_header;
    Block header_for_stripe = header;

    if (prewhere_info)
    {
        remove_constant_column(prewhere_include_indices_for_stripe, prewhere_header_for_stripe);
        if (prewhere_include_indices_for_stripe.empty())
        {
            LOG_DEBUG(&Poco::Logger::get("NativeORCBlockInputFormat"), "prewhere_include_indices_for_stripe is empty, and the size of constant_columns_desc is {}.", constant_columns_desc->size());
            prewhere_stripe_reader = newStripeReader((*stripes_to_read)[current_stripe_index], prewhere_include_indices_for_stripe, prewhere_header_for_stripe, true, constant_columns_desc, true);
        } else
            prewhere_stripe_reader = newStripeReader((*stripes_to_read)[current_stripe_index], prewhere_include_indices_for_stripe, prewhere_header_for_stripe, true, constant_columns_desc, false);
    }

    remove_constant_column(include_indices_for_stripe, header_for_stripe);
    if (include_indices_for_stripe.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("NativeORCBlockInputFormat"), "include_indices_for_stripe is empty, and the size of constant_columns_desc is {}.", constant_columns_desc->size());
        stripe_reader = newStripeReader((*stripes_to_read)[current_stripe_index], include_indices_for_stripe, header_for_stripe, !static_cast<bool>(prewhere_info), constant_columns_desc, true);
    } else
        stripe_reader = newStripeReader((*stripes_to_read)[current_stripe_index], include_indices_for_stripe, header_for_stripe, !static_cast<bool>(prewhere_info), constant_columns_desc, false);

    ++current_stripe_index;
    return true;
}

OrcStripesInformationPtr NativeORCBlockInputFormat::getStripes()
{
    prepareFileReaderAndMetadata();

    auto number_of_stripes = file_reader->getNumberOfStripes();
    auto row_group_size = file_reader->getRowIndexStride();
    uint64_t first_row_of_stripe = 0;

    OrcStripesInformationPtr stripes = std::make_shared<OrcStripesInformation>();
    stripes->reserve(number_of_stripes);
    for (size_t i = 0; i < number_of_stripes; ++i)
    {
        auto stripe_info = file_reader->getStripe(i);
        auto number_of_row_groups = (stripe_info->getNumberOfRows() - 1) / row_group_size + 1;
        std::vector<size_t> row_groups(number_of_row_groups);
        for (size_t j = 0; j < number_of_row_groups; ++j)
            row_groups[j] = j;
        stripes->push_back(OrcStripeInformation(*stripe_info, first_row_of_stripe, row_group_size, row_groups, i));
        first_row_of_stripe += stripe_info->getNumberOfRows();
    }

    return stripes;
}

OrcStripesInformationPtr NativeORCBlockInputFormat::getStripesToRead()
{
    if (!stripes_to_read)
        stripes_to_read = getStripes();
    return stripes_to_read;
}

StripesStatisticsPtr NativeORCBlockInputFormat::getStripesStatistics()
{
    if (stripes_statistics)
        return stripes_statistics;

    prepareFileReaderAndMetadata();
    auto number_of_stripes = file_reader->getNumberOfStripes();
    stripes_statistics = std::make_shared<StripesStatistics>();
    stripes_statistics->reserve(number_of_stripes);

    for (size_t i = 0; i < number_of_stripes; ++i)
    {
        stripes_statistics->emplace_back(file_reader->getStripeStatistics(i, false));
    }

    return stripes_statistics;
}

std::unique_ptr<orc::StripeStatistics> NativeORCBlockInputFormat::getStripeStatistics(size_t stripe_index)
{
    prepareFileReaderAndMetadata();
    return file_reader->getStripeStatistics(stripe_index, true);
}

ColumnStatistics NativeORCBlockInputFormat::getColumnStatistics(uint32_t columnId)
{
    prepareFileReaderAndMetadata();

    return file_reader->getColumnStatistics(columnId);
}

void NativeORCBlockInputFormat::addPrewhere(NamesAndTypesList prewhere_columns_, std::shared_ptr<PrewhereExprInfo> prewhere_info_)
{
    if (!prewhere_info_)
        return;

    prewhere_columns = prewhere_columns_;
    prewhere_info = prewhere_info_;

    if (!file_reader_and_metadata_initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "file_reader must be set before calling addPrewhere.");

    for (size_t i = 0; i < file_schema.columns(); ++i)
    {
        const auto & name = file_schema.getByPosition(i).name;
        if (prewhere_columns.contains(name))
        {
            prewhere_include_indices.push_back(i);
            include_indices.remove(i);
            if (header.has(name))
                header.erase(name);
        }
    }
    prewhere_header.clear();
    for (const auto & [name, type] : prewhere_columns)
        prewhere_header.insert({type->createColumn(), type, name});

    if (header.has(prewhere_info->prewhere_column_name))
        header.erase(prewhere_info->prewhere_column_name);
}

size_t NativeORCBlockInputFormat::executePrewhere(ColumnsWithTypeAndName & pre_res_columns)
{
    filter_holder.reset(nullptr);
    filter = nullptr;

    size_t num_rows = prewhere_stripe_reader->to_read_rows;
    for (auto & col_with_type_name : pre_res_columns)
    {
        if (!col_with_type_name.column)
        {
            auto col = col_with_type_name.type->createColumn();
            col->insertDefault();
            ColumnPtr const_col = ColumnConst::create(std::move(col), num_rows);
            col_with_type_name.column = std::move(const_col);
        }
    }

    Block block;
    for (const auto & column : pre_res_columns)
        block.insert(column);

    if (prewhere_info->alias_actions)
        prewhere_info->alias_actions->execute(block);

    prewhere_info->prewhere_actions->execute(block);

    auto prewhere_column = block.getByName(prewhere_info->prewhere_column_name).column;

    prewhere_column = prewhere_column->convertToFullColumnIfConst();
    FilterDescription filter_description(*prewhere_column);
    filter_holder = filter_description.data_holder ? filter_description.data_holder : prewhere_column;
    filter = typeid_cast<const ColumnUInt8 *>(filter_holder.get());
    if (!filter)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Column filter type should be ColumnUInt8");

    pre_res_columns = block.getColumnsWithTypeAndName();

    return countBytesInFilter(filter->getData());
}

size_t
NativeORCBlockInputFormat::executeFilter(ColumnsWithTypeAndName & pre_res_columns, ColumnsWithTypeAndName & res_columns, size_t num_rows)
{
    if (!filter)
        return num_rows;

    auto bytes_in_filter = countBytesInFilter(filter->getData());
    if (prewhere_info->need_filter || bytes_in_filter < 0.6 * num_rows)
    {
        auto do_filter = [&, this](ColumnsWithTypeAndName & input_columns)
        {
            if (input_columns.empty())
                return;

            for (auto & column : input_columns)
            {
                if (column.column)
                    column.column = column.column->filter(filter->getData(), -1);
            }
        };

        do_filter(pre_res_columns);
        do_filter(res_columns);

        return bytes_in_filter;
    }

    return num_rows;
}

Chunk NativeORCBlockInputFormat::generate()
{
    block_missing_values.clear();

    if (stripes_to_read && stripes_to_read->empty())
        return {};

    prepareFileReaderAndMetadata();

    if (is_stopped)
        return {};

    if (!prepareStripeReader())
        return {};

    size_t num_rows = 0;
    ColumnsWithTypeAndName pre_res_columns;
    if (prewhere_info)
    {
        if (!prewhere_stripe_reader)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "prewhere stripe reader is null while prewhere info exists, which could be a bug.");
        UInt64 tmp_add_constant_col_time_cost = 0;
        num_rows = prewhere_stripe_reader->read(pre_res_columns, tmp_add_constant_col_time_cost);
        add_constant_column_time_cost += tmp_add_constant_col_time_cost;
        file_total_orc_table_to_ch_columns_time_cost += prewhere_stripe_reader->getOrcToCHColumnsTimeCost();    

        if (!num_rows)
            return {};

        if (executePrewhere(pre_res_columns) == 0)
        {
            stripe_reader->current_row_group_index = prewhere_stripe_reader->current_row_group_index;
            auto columns = getPort().getHeader().cloneEmptyColumns();
            return Chunk{std::move(columns), 0};
        }
    }

    /// output columns may be empty if there is only PREWHERE
    /// but it also make sense
    ColumnsWithTypeAndName res_columns;
    UInt64 tmp_add_constant_col_time_cost = 0;
    num_rows = stripe_reader->read(res_columns, tmp_add_constant_col_time_cost);
    add_constant_column_time_cost += tmp_add_constant_col_time_cost;
    file_total_orc_table_to_ch_columns_time_cost += stripe_reader->getOrcToCHColumnsTimeCost();

    if (!num_rows)
        return {};

    num_rows = executeFilter(pre_res_columns, res_columns, num_rows);

    Columns columns;
    const auto & output_header = getPort().getHeader();
    columns.resize(output_header.columns());

    auto fill_res = [&, this](ColumnsWithTypeAndName & input_columns)
    {
        if (input_columns.empty())
            return;

        for (auto & column : input_columns)
        {
            auto name = column.name;
            if (!output_header.has(name))
                continue;

            auto pos = output_header.getPositionByName(name);
            if (!column.column)
            {
                column.column = output_header.getByPosition(pos).column->cloneResized(num_rows);
                if (format_settings.defaults_for_omitted_fields)
                    block_missing_values.setBits(pos, num_rows);
            }
            columns[pos] = column.column;
        }
    };

    fill_res(pre_res_columns);
    fill_res(res_columns);

    return Chunk{std::move(columns), num_rows};
}

void NativeORCBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset(nullptr);
    file_schema.clear();
    orc_column_to_ch_column.reset(nullptr);
    stripe_reader.reset(nullptr);
    prewhere_stripe_reader.reset(nullptr);
    include_indices.clear();
    column_name_to_index.clear();
    prewhere_include_indices.clear();
    block_missing_values.clear();
}

const BlockMissingValues & NativeORCBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

NativeORCSchemaReader::NativeORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList NativeORCSchemaReader::readSchema()
{
    Block header;
    std::unique_ptr<orc::Reader> file_reader;
    std::atomic<int> is_stopped = 0;
    getFileReaderAndSchema(in, file_reader, header, format_settings, is_stopped);

    return header.getNamesAndTypesList();
}


ORCColumnToCHColumn::ORCColumnToCHColumn(bool allow_missing_columns_, bool null_as_default_, bool case_insensitive_matching_)
    : allow_missing_columns(allow_missing_columns_)
    , null_as_default(null_as_default_)
    , case_insensitive_matching(case_insensitive_matching_)
{
}

ColumnsWithTypeAndName
ORCColumnToCHColumn::orcTableToCHColumns(const Block & header, const orc::Type * schema, const orc::ColumnVectorBatch * table)
{
    const auto * struct_batch = dynamic_cast<const orc::StructVectorBatch *>(table);
    if (!struct_batch)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table must be StructVectorBatch but is {}", struct_batch->toString());

    if (schema->getSubtypeCount() != struct_batch->fields.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "ORC table has {} fields but schema has {}", struct_batch->fields.size(), schema->getSubtypeCount());

    size_t field_num = struct_batch->fields.size();
    NameToColumnPtr name_to_column_ptr;
    for (size_t i = 0; i < field_num; ++i)
    {
        auto name = schema->getFieldName(i);
        const auto * field = struct_batch->fields[i];
        if (!field)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ORC table field {} is null", name);

        if (case_insensitive_matching)
            boost::to_lower(name);

        name_to_column_ptr[std::move(name)] = {field, schema->getSubtype(i)};
    }

    return orcColumnsToCHColumns(header, name_to_column_ptr);
}

/// Creates a null bytemap from ORC's not-null bytemap
static ColumnPtr readByteMapFromORCColumn(const orc::ColumnVectorBatch * orc_column)
{
    if (!orc_column->hasNulls)
        return ColumnUInt8::create(orc_column->numElements, 0);

    auto nullmap_column = ColumnUInt8::create();
    PaddedPODArray<UInt8> & bytemap_data = assert_cast<ColumnVector<UInt8> &>(*nullmap_column).getData();
    bytemap_data.resize(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        bytemap_data[i] = 1 - orc_column->notNull[i];
    return nullmap_column;
}


static const orc::ColumnVectorBatch * getNestedORCColumn(const orc::ListVectorBatch * orc_column)
{
    return orc_column->elements.get();
}

template <typename BatchType>
static ColumnPtr readOffsetsFromORCListColumn(const BatchType * orc_column)
{
    auto offsets_column = ColumnUInt64::create();
    ColumnArray::Offsets & offsets_data = assert_cast<ColumnVector<UInt64> &>(*offsets_column).getData();
    offsets_data.reserve_exact(orc_column->numElements);

    for (size_t i = 0; i < orc_column->numElements; ++i)
        offsets_data.push_back(orc_column->offsets[i + 1]);

    return offsets_column;
}

static ColumnWithTypeAndName
readColumnWithBooleanData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_bool_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_type = std::make_shared<DataTypeInt8>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnVector<UInt8> &>(*internal_column).getData();
    column_data.reserve_exact(orc_bool_column->numElements);

    for (size_t i = 0; i < orc_bool_column->numElements; ++i)
        column_data.push_back(static_cast<UInt8>(orc_bool_column->data[i]));

    return {std::move(internal_column), internal_type, column_name};
}

/// Inserts numeric data right into internal column data to reduce an overhead
template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve_exact(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    column_data.insert_assume_reserved(orc_int_column->data.data(), orc_int_column->data.data() + orc_int_column->numElements);

    return {std::move(internal_column), std::move(internal_type), column_name};
}

template <typename NumericType, typename BatchType, typename VectorType = ColumnVector<NumericType>>
static ColumnWithTypeAndName
readColumnWithNumericDataCast(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeNumber<NumericType>>();
    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve_exact(orc_column->numElements);

    const auto * orc_int_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_int_column->numElements; ++i)
        column_data.push_back(static_cast<NumericType>(orc_int_column->data[i]));

    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName
readColumnWithStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    auto internal_type = std::make_shared<DataTypeString>();
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnString &>(*internal_column).getChars();
    PaddedPODArray<UInt64> & column_offsets = assert_cast<ColumnString &>(*internal_column).getOffsets();

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    size_t reserver_size = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
        reserver_size += orc_str_column->length[i] + 1;
    column_chars_t.reserve_exact(reserver_size);
    column_offsets.reserve_exact(orc_str_column->numElements);

    size_t curr_offset = 0;
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        const auto * buf = orc_str_column->data[i];
        if (buf)
        {
            size_t buf_size = orc_str_column->length[i];
            column_chars_t.insert_assume_reserved(buf, buf + buf_size);
            curr_offset += buf_size;
        }

        column_chars_t.push_back(0);
        ++curr_offset;

        column_offsets.push_back(curr_offset);
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName
readColumnWithFixedStringData(const orc::ColumnVectorBatch * orc_column, const orc::Type * orc_type, const String & column_name)
{
    size_t fixed_len = orc_type->getMaximumLength();
    auto internal_type = std::make_shared<DataTypeFixedString>(fixed_len);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<UInt8> & column_chars_t = assert_cast<ColumnFixedString &>(*internal_column).getChars();
    column_chars_t.reserve_exact(orc_column->numElements * fixed_len);

    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);
    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (orc_str_column->data[i])
            column_chars_t.insert_assume_reserved(orc_str_column->data[i], orc_str_column->data[i] + orc_str_column->length[i]);
        else
            column_chars_t.resize_fill(column_chars_t.size() + fixed_len);
    }

    return {std::move(internal_column), std::move(internal_type), column_name};
}


template <typename DecimalType, typename BatchType, typename VectorType = ColumnDecimal<DecimalType>>
static ColumnWithTypeAndName readColumnWithDecimalDataCast(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, DataTypePtr internal_type)
{
    using NativeType = typename DecimalType::NativeType;
    static_assert(std::is_same_v<BatchType, orc::Decimal128VectorBatch> || std::is_same_v<BatchType, orc::Decimal64VectorBatch>);

    auto internal_column = internal_type->createColumn();
    auto & column_data = static_cast<VectorType &>(*internal_column).getData();
    column_data.reserve_exact(orc_column->numElements);

    const auto * orc_decimal_column = dynamic_cast<const BatchType *>(orc_column);
    for (size_t i = 0; i < orc_decimal_column->numElements; ++i)
    {
        DecimalType decimal_value;
        if constexpr (std::is_same_v<BatchType, orc::Decimal128VectorBatch>)
        {
            Int128 int128_value;
            int128_value.items[0] = orc_decimal_column->values[i].getLowBits();
            int128_value.items[1] = orc_decimal_column->values[i].getHighBits();
            decimal_value.value = static_cast<NativeType>(int128_value);
        }
        else
            decimal_value.value = static_cast<NativeType>(orc_decimal_column->values[i]);

        column_data.push_back(std::move(decimal_value));
    }

    return {std::move(internal_column), internal_type, column_name};
}

template <typename ColumnType>
static ColumnWithTypeAndName readColumnWithBigNumberFromBinaryData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, const DataTypePtr & column_type)
{
    const auto * orc_str_column = dynamic_cast<const orc::StringVectorBatch *>(orc_column);

    auto internal_column = column_type->createColumn();
    auto & integer_column = assert_cast<ColumnType &>(*internal_column);
    integer_column.reserve(orc_str_column->numElements);

    for (size_t i = 0; i < orc_str_column->numElements; ++i)
    {
        if (!orc_str_column->data[i]) [[unlikely]]
            integer_column.insertDefault();
        else
        {
            if (sizeof(typename ColumnType::ValueType) != orc_str_column->length[i])
                throw Exception(
                    ErrorCodes::INCORRECT_DATA,
                    "ValueType size {} of column {} is not equal to size of binary data {}",
                    sizeof(typename ColumnType::ValueType),
                    integer_column.getName(),
                    orc_str_column->length[i]);

            integer_column.insertData(orc_str_column->data[i], orc_str_column->length[i]);
        }
    }
    return {std::move(internal_column), column_type, column_name};
}

static ColumnWithTypeAndName readColumnWithDateData(
    const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name, const DataTypePtr & type_hint)
{
    DataTypePtr internal_type;
    bool check_date_range = false;
    /// Make result type Date32 when requested type is actually Date32 or when we use schema inference
    if (!type_hint || (type_hint && isDate32(*type_hint)))
    {
        internal_type = std::make_shared<DataTypeDate32>();
        check_date_range = true;
    }
    else
    {
        internal_type = std::make_shared<DataTypeInt32>();
    }

    const auto * orc_int_column = dynamic_cast<const orc::LongVectorBatch *>(orc_column);
    auto internal_column = internal_type->createColumn();
    PaddedPODArray<Int32> & column_data = assert_cast<ColumnVector<Int32> &>(*internal_column).getData();
    column_data.reserve_exact(orc_int_column->numElements);

    for (size_t i = 0; i < orc_int_column->numElements; ++i)
    {
        Int32 days_num = static_cast<Int32>(orc_int_column->data[i]);
        if (check_date_range && (days_num > DATE_LUT_MAX_EXTEND_DAY_NUM || days_num < -DAYNUM_OFFSET_EPOCH))
            throw Exception(
                ErrorCodes::VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
                "Input value {} of a column \"{}\" exceeds the range of type Date32",
                days_num,
                column_name);

        column_data.push_back(days_num);
    }

    return {std::move(internal_column), internal_type, column_name};
}

static ColumnWithTypeAndName
readColumnWithTimestampData(const orc::ColumnVectorBatch * orc_column, const orc::Type *, const String & column_name)
{
    const auto * orc_ts_column = dynamic_cast<const orc::TimestampVectorBatch *>(orc_column);

    auto internal_type = std::make_shared<DataTypeDateTime64>(9);
    auto internal_column = internal_type->createColumn();
    auto & column_data = assert_cast<ColumnDecimal<DateTime64> &>(*internal_column).getData();
    column_data.reserve_exact(orc_ts_column->numElements);

    constexpr Int64 multiplier = 1e9L;
    for (size_t i = 0; i < orc_ts_column->numElements; ++i)
    {
        Decimal64 decimal64;
        decimal64.value = orc_ts_column->data[i] * multiplier + orc_ts_column->nanoseconds[i];
        column_data.emplace_back(std::move(decimal64));
    }
    return {std::move(internal_column), std::move(internal_type), column_name};
}

static ColumnWithTypeAndName readColumnFromORCColumn(
    const orc::ColumnVectorBatch * orc_column,
    const orc::Type * orc_type,
    const std::string & column_name,
    bool inside_nullable,
    DataTypePtr type_hint = nullptr)
{
    bool skipped = false;

    if (!inside_nullable && (orc_column->hasNulls || (type_hint && type_hint->isNullable()))
        && (orc_type->getKind() != orc::LIST && orc_type->getKind() != orc::MAP && orc_type->getKind() != orc::STRUCT))
    {
        DataTypePtr nested_type_hint;
        if (type_hint)
            nested_type_hint = removeNullable(type_hint);

        auto nested_column = readColumnFromORCColumn(orc_column, orc_type, column_name, true, nested_type_hint);

        auto nullmap_column = readByteMapFromORCColumn(orc_column);
        auto nullable_type = std::make_shared<DataTypeNullable>(std::move(nested_column.type));
        auto nullable_column = ColumnNullable::create(nested_column.column, nullmap_column);
        return {std::move(nullable_column), std::move(nullable_type), column_name};
    }

    switch (orc_type->getKind())
    {
        case orc::STRING:
        case orc::BINARY:
        case orc::VARCHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    /// ORC format outputs big integers as binary column, because there is no fixed binary in ORC.
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    /// ORC doesn't support Decimal256 as separate type. We read and write it as binary data.
                    case TypeIndex::Decimal256:
                        return readColumnWithBigNumberFromBinaryData<ColumnDecimal<Decimal256>>(
                            orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }
            return readColumnWithStringData(orc_column, orc_type, column_name);
        }
        case orc::CHAR: {
            if (type_hint)
            {
                switch (type_hint->getTypeId())
                {
                    case TypeIndex::Int128:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt128:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt128>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::Int256:
                        return readColumnWithBigNumberFromBinaryData<ColumnInt256>(orc_column, orc_type, column_name, type_hint);
                    case TypeIndex::UInt256:
                        return readColumnWithBigNumberFromBinaryData<ColumnUInt256>(orc_column, orc_type, column_name, type_hint);
                    default:;
                }
            }
            return readColumnWithFixedStringData(orc_column, orc_type, column_name);
        }
        case orc::BOOLEAN:
            return readColumnWithBooleanData(orc_column, orc_type, column_name);
        case orc::BYTE:
            return readColumnWithNumericDataCast<Int8, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::SHORT:
            return readColumnWithNumericDataCast<Int16, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::INT: {
            return readColumnWithNumericDataCast<Int32, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        }
        case orc::LONG:
            return readColumnWithNumericData<Int64, orc::LongVectorBatch>(orc_column, orc_type, column_name);
        case orc::FLOAT:
            return readColumnWithNumericDataCast<Float32, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DOUBLE:
            return readColumnWithNumericData<Float64, orc::DoubleVectorBatch>(orc_column, orc_type, column_name);
        case orc::DATE:
            return readColumnWithDateData(orc_column, orc_type, column_name, type_hint);
        case orc::TIMESTAMP:
            return readColumnWithTimestampData(orc_column, orc_type, column_name);
        case orc::DECIMAL: {
            auto interal_type = parseORCType(orc_type, false, skipped);

            auto precision = orc_type->getPrecision();
            if (precision == 0)
                precision = 38;

            if (precision <= DecimalUtils::max_precision<Decimal32>)
                return readColumnWithDecimalDataCast<Decimal32, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            else if (precision <= DecimalUtils::max_precision<Decimal64>)
                return readColumnWithDecimalDataCast<Decimal64, orc::Decimal64VectorBatch>(orc_column, orc_type, column_name, interal_type);
            else if (precision <= DecimalUtils::max_precision<Decimal128>)
                return readColumnWithDecimalDataCast<Decimal128, orc::Decimal128VectorBatch>(
                    orc_column, orc_type, column_name, interal_type);
            else
                throw Exception(
                    ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                    "Decimal precision {} in ORC type {} is out of bound",
                    precision,
                    orc_type->toString());
        }
        case orc::MAP: {
            DataTypePtr key_type_hint;
            DataTypePtr value_type_hint;
            if (type_hint)
            {
                const auto * map_type_hint = typeid_cast<const DataTypeMap *>(type_hint.get());
                if (map_type_hint)
                {
                    key_type_hint = map_type_hint->getKeyType();
                    value_type_hint = map_type_hint->getValueType();
                }
            }

            const auto * orc_map_column = dynamic_cast<const orc::MapVectorBatch *>(orc_column);
            const auto * orc_key_column = orc_map_column->keys.get();
            const auto * orc_value_column = orc_map_column->elements.get();
            const auto * orc_key_type = orc_type->getSubtype(0);
            const auto * orc_value_type = orc_type->getSubtype(1);

            auto key_column = readColumnFromORCColumn(orc_key_column, orc_key_type, "key", false, key_type_hint);
            if (key_type_hint && !key_type_hint->equals(*key_column.type))
            {
                /// Cast key column to target type, because it can happen
                /// that parsed type cannot be ClickHouse Map key type.
                key_column.column = castColumn(key_column, key_type_hint);
                key_column.type = key_type_hint;
            }

            auto value_column = readColumnFromORCColumn(orc_value_column, orc_value_type, "value", false, value_type_hint);
            if (skipped)
                return {};

            if (value_type_hint && !value_type_hint->equals(*value_column.type))
            {
                /// Cast value column to target type, because it can happen
                /// that parsed type cannot be ClickHouse Map value type.
                value_column.column = castColumn(value_column, value_type_hint);
                value_column.type = value_type_hint;
            }

            auto offsets_column = readOffsetsFromORCListColumn(orc_map_column);
            auto map_column = ColumnMap::create(key_column.column, value_column.column, offsets_column);
            auto map_type = std::make_shared<DataTypeMap>(key_column.type, value_column.type);
            return {std::move(map_column), std::move(map_type), column_name};
        }
        case orc::LIST: {
            DataTypePtr nested_type_hint;
            if (type_hint)
            {
                const auto * array_type_hint = typeid_cast<const DataTypeArray *>(type_hint.get());
                if (array_type_hint)
                    nested_type_hint = array_type_hint->getNestedType();
            }

            const auto * orc_list_column = dynamic_cast<const orc::ListVectorBatch *>(orc_column);
            const auto * orc_nested_column = getNestedORCColumn(orc_list_column);
            const auto * orc_nested_type = orc_type->getSubtype(0);
            auto nested_column = readColumnFromORCColumn(orc_nested_column, orc_nested_type, column_name, false, nested_type_hint);

            auto offsets_column = readOffsetsFromORCListColumn(orc_list_column);
            auto array_column = ColumnArray::create(nested_column.column, offsets_column);
            auto array_type = std::make_shared<DataTypeArray>(nested_column.type);
            return {std::move(array_column), std::move(array_type), column_name};
        }
        case orc::STRUCT: {
            Columns tuple_elements;
            DataTypes tuple_types;
            std::vector<String> tuple_names;
            const auto * tuple_type_hint = type_hint ? typeid_cast<const DataTypeTuple *>(type_hint.get()) : nullptr;

            const auto * orc_struct_column = dynamic_cast<const orc::StructVectorBatch *>(orc_column);
            for (size_t i = 0; i < orc_type->getSubtypeCount(); ++i)
            {
                const auto & field_name = orc_type->getFieldName(i);

                DataTypePtr nested_type_hint;
                if (tuple_type_hint)
                {
                    if (tuple_type_hint->haveExplicitNames())
                    {
                        auto pos = tuple_type_hint->getPositionByName(field_name);
                        nested_type_hint = tuple_type_hint->getElement(pos);
                    }
                    else if (i < tuple_type_hint->getElements().size())
                        nested_type_hint = tuple_type_hint->getElement(i);
                }

                const auto * nested_orc_column = orc_struct_column->fields[i];
                const auto * nested_orc_type = orc_type->getSubtype(i);
                auto element = readColumnFromORCColumn(nested_orc_column, nested_orc_type, field_name, false, nested_type_hint);

                tuple_elements.emplace_back(std::move(element.column));
                tuple_types.emplace_back(std::move(element.type));
                tuple_names.emplace_back(std::move(element.name));
            }

            auto tuple_column = ColumnTuple::create(std::move(tuple_elements));
            auto tuple_type = std::make_shared<DataTypeTuple>(std::move(tuple_types), std::move(tuple_names));
            return {std::move(tuple_column), std::move(tuple_type), column_name};
        }
        default:
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE, "Unsupported ORC type {} while reading column {}.", orc_type->toString(), column_name);
    }
}

ColumnsWithTypeAndName ORCColumnToCHColumn::orcColumnsToCHColumns(const Block & header, NameToColumnPtr & name_to_column_ptr)
{
    ColumnsWithTypeAndName columns_list;
    columns_list.reserve(header.columns());

    std::unordered_map<String, std::pair<BlockPtr, std::shared_ptr<NestedColumnExtractHelper>>> nested_tables;
    for (size_t column_i = 0, columns = header.columns(); column_i < columns; ++column_i)
    {
        const ColumnWithTypeAndName & header_column = header.getByPosition(column_i);

        auto search_column_name = header_column.name;
        if (case_insensitive_matching)
            boost::to_lower(search_column_name);

        ColumnWithTypeAndName column;
        if (!name_to_column_ptr.contains(search_column_name))
        {
            bool read_from_nested = false;

            /// Check if it's a column from nested table.
            String nested_table_name = Nested::extractTableName(header_column.name);
            String search_nested_table_name = nested_table_name;
            if (case_insensitive_matching)
                boost::to_lower(search_nested_table_name);
            if (name_to_column_ptr.contains(search_nested_table_name))
            {
                if (!nested_tables.contains(search_nested_table_name))
                {
                    NamesAndTypesList nested_columns;
                    for (const auto & name_and_type : header.getNamesAndTypesList())
                    {
                        if (name_and_type.name.starts_with(nested_table_name + "."))
                            nested_columns.push_back(name_and_type);
                    }
                    auto nested_table_type = Nested::collect(nested_columns).front().type;

                    auto orc_column_with_type = name_to_column_ptr[search_nested_table_name];
                    ColumnsWithTypeAndName cols = {readColumnFromORCColumn(
                        orc_column_with_type.first, orc_column_with_type.second, nested_table_name, false, nested_table_type)};
                    BlockPtr block_ptr = std::make_shared<Block>(cols);
                    auto column_extractor = std::make_shared<NestedColumnExtractHelper>(*block_ptr, case_insensitive_matching);
                    nested_tables[search_nested_table_name] = {block_ptr, column_extractor};
                }

                auto nested_column = nested_tables[search_nested_table_name].second->extractColumn(search_column_name);
                if (nested_column)
                {
                    column = *nested_column;
                    if (case_insensitive_matching)
                        column.name = header_column.name;
                    read_from_nested = true;
                }
            }

            if (!read_from_nested)
            {
                if (!allow_missing_columns)
                    throw Exception{ErrorCodes::THERE_IS_NO_COLUMN, "Column '{}' is not presented in input data.", header_column.name};
                else
                {
                    column.name = header_column.name;
                    column.type = header_column.type;
                    columns_list.push_back(std::move(column));
                    continue;
                }
            }
        }
        else
        {
            auto orc_column_with_type = name_to_column_ptr[search_column_name];
            column = readColumnFromORCColumn(
                orc_column_with_type.first, orc_column_with_type.second, header_column.name, false, header_column.type);
        }

        if (null_as_default)
            insertNullAsDefaultIfNeeded(column, header_column, column_i, nullptr);

        try
        {
            column.column = castColumn(column, header_column.type);
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format(
                "while converting column {} from type {} to type {}",
                backQuote(header_column.name),
                column.type->getName(),
                header_column.type->getName()));
            throw;
        }

        column.type = header_column.type;
        columns_list.push_back(std::move(column));
    }

    return columns_list;
}

void registerInputFormatORC(FormatFactory & factory)
{
    factory.registerInputFormat(
        "ORC",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings) -> InputFormatPtr
        { return std::make_shared<NativeORCBlockInputFormat>(buf, sample, settings); });
    factory.markFormatAsColumnOriented("ORC");
}

void registerORCSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "ORC",
        [](ReadBuffer & buf, const FormatSettings & settings, ContextPtr) -> SchemaReaderPtr
        { return std::make_shared<NativeORCSchemaReader>(buf, settings); });
}
}

#endif
