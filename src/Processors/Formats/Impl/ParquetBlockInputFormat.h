#pragma once
#include "config_formats.h"
#if USE_PARQUET

#    include <Formats/FormatSettings.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <parquet/metadata.h>

namespace parquet::arrow
{
class FileReader;
}

namespace arrow
{
class Buffer;
}

namespace DB
{

class ArrowColumnToCHColumn;

struct ParquetRowGroupInformation
{
    size_t row_group_idx;
    uint64_t num_rows;
    uint64_t first_row_of_row_group;
};

using ParquetRowGroupsInformation = std::vector<ParquetRowGroupInformation>;
using ParquetRowGroupsMetadata = std::vector<std::unique_ptr<parquet::RowGroupMetaData>>;

class ParquetBlockInputFormat : public IInputFormat
{
public:
    ParquetBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    void resetParser() override;

    String getName() const override { return "ParquetBlockInputFormat"; }

    const BlockMissingValues & getMissingValues() const override;

    ParquetRowGroupsInformation getRowGroups();

    ParquetRowGroupsMetadata getRowGroupsMetadata();

    const std::unordered_map<String, int> & getColumnNameToIndexMapping() { return column_name_to_index; }

    void setRowGroupsToRead(const ParquetRowGroupsInformation & row_groups) { row_groups_to_read = row_groups; }

    void ignoreBatchSizeLimit() { ignore_batch_size_limit = true; }

private:
    Chunk generate() override;

    void prepareMetadata();

    bool prepareRowGroupReader();

    void onCancel() override { is_stopped = 1; }

    std::shared_ptr<arrow::io::RandomAccessFile> arrow_file;

    std::unique_ptr<parquet::arrow::FileReader> file_reader;
    std::shared_ptr<arrow::RecordBatchReader> batch_reader;
    // indices of columns to read from Parquet file
    std::vector<int> column_indices;

    std::unique_ptr<ArrowColumnToCHColumn> arrow_column_to_ch_column;
    BlockMissingValues block_missing_values;
    const FormatSettings format_settings;

    Block file_schema;
    Block header;

    std::shared_ptr<parquet::FileMetaData> metadata;

    /// mapping from column name to column index in Parquet file
    /// used for reading statistics
    std::unordered_map<String, int> column_name_to_index;

    ParquetRowGroupsInformation row_groups_to_read;
    size_t current_row_group_index;
    size_t current_row;
    size_t to_read_rows{0};

    /// for in reverse order reading
    /// should read whole row group
    bool ignore_batch_size_limit{false};

    std::atomic<int> is_stopped{0};
};

class ParquetSchemaReader : public ISchemaReader
{
public:
    ParquetSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
    std::shared_ptr<parquet::FileMetaData> metadata;
};

}

#endif
