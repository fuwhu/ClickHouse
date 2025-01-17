#include "ParquetBlockInputFormat.h"
#include <boost/algorithm/string/case_conv.hpp>

#if USE_PARQUET

#    include <DataTypes/NestedUtils.h>
#    include <Formats/FormatFactory.h>
#    include <IO/ReadBufferFromMemory.h>
#    include <IO/copyData.h>
#    include <arrow/api.h>
#    include <arrow/io/api.h>
#    include <arrow/status.h>
#    include <parquet/arrow/reader.h>
#    include <parquet/arrow/schema.h>
#    include <parquet/file_reader.h>
#    include "ArrowBufferedStreams.h"
#    include "ArrowColumnToCHColumn.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int INCORRECT_DATA;
}

#    define THROW_ARROW_NOT_OK(status) \
        do \
        { \
            if (::arrow::Status _s = (status); !_s.ok()) \
            { \
                throw Exception(_s.ToString(), _s.IsOutOfMemory() ? ErrorCodes::CANNOT_ALLOCATE_MEMORY : ErrorCodes::INCORRECT_DATA); \
            } \
        } while (false)

ParquetBlockInputFormat::ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_)
    : IInputFormat(std::move(header_), in_), format_settings(format_settings_)
{
    setRowGroupsToRead(getRowGroups());
    header = getPort().getHeader();
}

Chunk ParquetBlockInputFormat::generate()
{
    block_missing_values.clear();

    if (row_groups_to_read.empty())
        return {};

    if (!metadata)
        prepareMetadata();

    if (is_stopped)
        return {};

    if (!prepareRowGroupReader())
        return {};

    auto batch = batch_reader->Next();
    if (!batch.ok())
        throw ParsingException{"Error while reading Parquet data: " + batch.status().ToString(), ErrorCodes::CANNOT_READ_ALL_DATA};

    size_t num_rows = (*batch)->num_rows();
    if (to_read_rows != num_rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Should read exactly {} rows, bug got {}", to_read_rows, num_rows);

    current_row += num_rows;

    std::shared_ptr<arrow::Table> table = *arrow::Table::FromRecordBatches({*batch});
    /// If defaults_for_omitted_fields is true, calculate the default values from default expression for omitted fields.
    /// Otherwise fill the missing columns with zero values of its type.
    BlockMissingValues * block_missing_values_ptr = format_settings.defaults_for_omitted_fields ? &block_missing_values : nullptr;
    Chunk res = arrow_column_to_ch_column->arrowTableToCHChunk(table, num_rows, block_missing_values_ptr);

    return res;
}

void ParquetBlockInputFormat::resetParser()
{
    IInputFormat::resetParser();

    file_reader.reset();
    batch_reader.reset();
    column_indices.clear();
    column_name_to_index.clear();
    current_row_group_index = 0;
    block_missing_values.clear();
}

const BlockMissingValues & ParquetBlockInputFormat::getMissingValues() const
{
    return block_missing_values;
}

static size_t countIndicesForType(std::shared_ptr<arrow::DataType> type)
{
    if (type->id() == arrow::Type::LIST)
        return countIndicesForType(static_cast<arrow::ListType *>(type.get())->value_type());

    if (type->id() == arrow::Type::STRUCT)
    {
        int indices = 0;
        auto * struct_type = static_cast<arrow::StructType *>(type.get());
        for (int i = 0; i != struct_type->num_fields(); ++i)
            indices += countIndicesForType(struct_type->field(i)->type());
        return indices;
    }

    if (type->id() == arrow::Type::MAP)
    {
        auto * map_type = static_cast<arrow::MapType *>(type.get());
        return countIndicesForType(map_type->key_type()) + countIndicesForType(map_type->item_type());
    }

    return 1;
}

void ParquetBlockInputFormat::prepareMetadata()
{
    if (metadata)
        return;

    arrow_file = asArrowFile(*in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, true);
    if (is_stopped)
        return;

    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    metadata = parquet::ParquetFileReader::Open(arrow_file, reader_properties)->metadata();

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));
    file_schema = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Parquet");

    arrow_column_to_ch_column = std::make_unique<ArrowColumnToCHColumn>(
        getPort().getHeader(),
        "Parquet",
        format_settings.parquet.allow_missing_columns,
        format_settings.null_as_default,
        format_settings.date_time_overflow_behavior,
        format_settings.parquet.case_insensitive_column_matching);

    const bool ignore_case = format_settings.parquet.case_insensitive_column_matching;
    std::unordered_set<String> nested_table_names = Nested::getAllTableNames(getPort().getHeader(), ignore_case);

    int index = 0;
    for (int i = 0; i < schema->num_fields(); ++i)
    {
        /// STRUCT type require the number of indexes equal to the number of
        /// nested elements, so we should recursively
        /// count the number of indices we need for this type.
        int indexes_count = countIndicesForType(schema->field(i)->type());
        const auto & name = schema->field(i)->name();
        if (getPort().getHeader().has(name, ignore_case) || nested_table_names.contains(ignore_case ? boost::to_lower_copy(name) : name))
        {
            for (int j = 0; j != indexes_count; ++j)
            {
                column_indices.push_back(index + j);
                column_name_to_index.insert({name, index + j});
            }
        }
        index += indexes_count;
    }

    current_row_group_index = 0;
    current_row = 0;
}

bool ParquetBlockInputFormat::prepareRowGroupReader()
{
    bool has_pending_data = false;

    if (batch_reader)
    {
        auto & current_row_group = row_groups_to_read[current_row_group_index];
        auto first_row_of_current_row_group = current_row_group.first_row_of_row_group;
        auto end_row_of_current_row_group = current_row_group.first_row_of_row_group + current_row_group.num_rows;
        has_pending_data = current_row > first_row_of_current_row_group && current_row < end_row_of_current_row_group;

        if (has_pending_data)
        {
            to_read_rows
                = std::min(end_row_of_current_row_group - current_row, static_cast<size_t>(format_settings.parquet.row_batch_size));
            return true;
        }
    }

    if (batch_reader)
        ++current_row_group_index;

    if (current_row_group_index >= row_groups_to_read.size())
        return false;

    auto & current_row_group = row_groups_to_read[current_row_group_index];
    current_row = current_row_group.first_row_of_row_group;

    parquet::ArrowReaderProperties arrow_properties;
    arrow_properties.set_use_threads(false);

    if (ignore_batch_size_limit)
    {
        arrow_properties.set_batch_size(current_row_group.num_rows);
        to_read_rows = current_row_group.num_rows;
    }
    else
    {
        arrow_properties.set_batch_size(format_settings.parquet.row_batch_size);
        to_read_rows = std::min(current_row_group.num_rows, static_cast<size_t>(format_settings.parquet.row_batch_size));
    }

    arrow_properties.set_pre_buffer(true);
    auto cache_options = arrow::io::CacheOptions::LazyDefaults();
    cache_options.hole_size_limit = 1 * 1024 * 1024;
    arrow_properties.set_cache_options(cache_options);

    // Workaround for a workaround in the parquet library.
    //
    // From ComputeColumnChunkRange() in contrib/arrow/cpp/src/parquet/file_reader.cc:
    //  > The Parquet MR writer had a bug in 1.2.8 and below where it didn't include the
    //  > dictionary page header size in total_compressed_size and total_uncompressed_size
    //  > (see IMPALA-694). We add padding to compensate.
    //
    // That padding breaks the pre-buffered mode because the padded read ranges may overlap each
    // other, failing an assert. So we disable pre-buffering in this case.
    // That version is >10 years old, so this is not very important.
    if (metadata->writer_version().VersionLt(parquet::ApplicationVersion::PARQUET_816_FIXED_VERSION()))
        arrow_properties.set_pre_buffer(false);

    parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
    parquet::arrow::FileReaderBuilder builder;
    builder.properties(arrow_properties);
    builder.memory_pool(ArrowMemoryPool::instance());
    THROW_ARROW_NOT_OK(builder.Open(arrow_file, reader_properties, metadata));
    THROW_ARROW_NOT_OK(builder.Build(&file_reader));

    THROW_ARROW_NOT_OK(
        file_reader->GetRecordBatchReader({static_cast<int>(current_row_group.row_group_idx)}, column_indices, &batch_reader));

    return true;
}

ParquetRowGroupsInformation ParquetBlockInputFormat::getRowGroups()
{
    if (!metadata)
        prepareMetadata();

    auto number_of_row_groups = static_cast<size_t>(metadata->num_row_groups());
    ParquetRowGroupsInformation row_groups;
    row_groups.reserve(number_of_row_groups);

    uint64_t first_row_of_row_group = 0;
    for (size_t i = 0; i < number_of_row_groups; ++i)
    {
        auto row_group = metadata->RowGroup(i);
        auto num_rows = static_cast<uint64_t>(row_group->num_rows());
        row_groups.push_back(
            ParquetRowGroupInformation{.row_group_idx = i, .num_rows = num_rows, .first_row_of_row_group = first_row_of_row_group});
        first_row_of_row_group += num_rows;
    }

    return row_groups;
}

ParquetRowGroupsMetadata ParquetBlockInputFormat::getRowGroupsMetadata()
{
    if (!metadata)
        prepareMetadata();

    auto number_of_row_groups = static_cast<size_t>(metadata->num_row_groups());
    ParquetRowGroupsMetadata row_groups_metadata;
    row_groups_metadata.reserve(number_of_row_groups);

    for (size_t i = 0; i < number_of_row_groups; ++i)
    {
        row_groups_metadata.push_back(metadata->RowGroup(i));
    }

    return row_groups_metadata;
}

ParquetSchemaReader::ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_)
    : ISchemaReader(in_), format_settings(format_settings_)
{
}

NamesAndTypesList ParquetSchemaReader::readSchema()
{
    if (!metadata)
    {
        std::unique_ptr<parquet::arrow::FileReader> file_reader;
        std::atomic<int> is_stopped = 0;

        auto arrow_file = asArrowFile(in, format_settings, is_stopped, "Parquet", PARQUET_MAGIC_BYTES, true);

        parquet::ReaderProperties reader_properties(ArrowMemoryPool::instance());
        metadata = parquet::ParquetFileReader::Open(arrow_file, reader_properties)->metadata();
    }

    std::shared_ptr<arrow::Schema> schema;
    THROW_ARROW_NOT_OK(parquet::arrow::FromParquetSchema(metadata->schema(), &schema));

    auto header = ArrowColumnToCHColumn::arrowSchemaToCHHeader(*schema, "Parquet");

    return header.getNamesAndTypesList();
}

void registerInputFormatParquet(FormatFactory & factory)
{
    factory.registerInputFormat(
        "Parquet",
        [](ReadBuffer & buf, const Block & sample, const RowInputFormatParams &, const FormatSettings & settings)
        { return std::make_shared<ParquetBlockInputFormat>(buf, sample, settings); });
    factory.markFormatAsColumnOriented("Parquet");
}

void registerParquetSchemaReader(FormatFactory & factory)
{
    factory.registerSchemaReader(
        "Parquet",
        [](ReadBuffer & buf, const FormatSettings & settings, ContextPtr) { return std::make_shared<ParquetSchemaReader>(buf, settings); });
}

}

#else

namespace DB
{
class FormatFactory;
void registerInputFormatParquet(FormatFactory &)
{
}

void registerParquetSchemaReader(FormatFactory &)
{
}
}

#endif
