#pragma once

#include <functional>
#include "DataTypes/Serializations/ISerialization.h"
#include "Storages/ColumnsDescription.h"
#include "base/types.h"
#include "config_formats.h"
#include <cstdint>
#include <memory>
#include <orc/Reader.hh>
#include <Common/config.h>
#include <Storages/MergeTree/KeyCondition.h>

#if USE_ORC
#    include <Columns/ColumnsNumber.h>
#    include <Formats/FormatSettings.h>
#    include <IO/ReadBufferFromString.h>
#    include <Processors/Formats/IInputFormat.h>
#    include <Processors/Formats/ISchemaReader.h>
#    include <orc/OrcFile.hh>

namespace DB
{
struct OrcStripeInformation
{
    uint64_t offset;
    uint64_t length;
    uint64_t num_rows;
    uint64_t first_row_of_stripe;

    uint64_t row_group_size;
    std::vector<size_t> row_groups;
    size_t stripe_index;

    OrcStripeInformation(
        const orc::StripeInformation & stripe, uint64_t first_row_of_stripe_, uint64_t row_group_size_, std::vector<size_t> row_groups_, size_t stripe_index_)
        : offset(stripe.getOffset())
        , length(stripe.getLength())
        , num_rows(stripe.getNumberOfRows())
        , first_row_of_stripe(first_row_of_stripe_)
        , row_group_size(row_group_size_)
        , row_groups(row_groups_)
        , stripe_index(stripe_index_)
    {
    }
};

struct OrcConstantColumnDescription
{
    UInt64 column_id;
    String column_name;
    DataTypePtr column_type;
    Field constant_value;
};
using OrcConstantColumnsDescription = std::vector<OrcConstantColumnDescription>;
using OrcConstantColumnsDescriptionPtr = std::shared_ptr<OrcConstantColumnsDescription>;

using OrcStripesInformation = std::vector<OrcStripeInformation>;
using OrcStripesInformationPtr = std::shared_ptr<OrcStripesInformation>;
using StripesStatistics = std::vector<std::unique_ptr<orc::StripeStatistics>>;
using StripesStatisticsPtr = std::shared_ptr<StripesStatistics>;
using ColumnStatistics = std::unique_ptr<orc::ColumnStatistics>;

struct PrewhereExprInfo;

template <class FieldType, class StatisticsType>
Range createRangeFromOrcStatistics(const StatisticsType * stats);

Range buildRange(const orc::ColumnStatistics * col_stats);

class ORCInputStream : public orc::InputStream
{
public:
    static const uint64_t MAX_BUFFER_SIZE = 8 * 1024 * 1024; // TODO : make it configurable.
    ORCInputStream(SeekableReadBuffer & in_, size_t file_size_);

    uint64_t getLength() const override;
    uint64_t getNaturalReadSize() const override;
    void read(void * buf, uint64_t length, uint64_t offset) override;
    uint64_t readFromRanges(void* buf, uint64_t length, uint64_t offset) override;
    void prefetch(uint64_t offset, uint64_t length) override;
    const std::string & getName() const override { return name; }
    void setOrAddRangesToRead(const std::vector<orc::OffsetRange> & ranges, bool is_set) override;

private :
    SeekableReadBuffer & in;
    size_t file_size;
    std::string name = "ORCInputStream";
    std::vector<orc::OffsetRange> ranges_to_read;
    orc::OffsetRange current_range{0, 0};
    uint64_t buffer_position = 0;
    uint64_t file_position = 0;
    orc::MemoryPool& pool;
    std::unique_ptr<orc::DataBuffer<char>> buffer;

    void seek(const uint64_t & offset, bool seek_in);
    uint64_t position() const { return file_position - (buffer->size() - buffer_position);}
    uint64_t bufferEndPosition() const { return file_position; }
    uint64_t bufferStartPosition() const { return file_position - buffer->size(); }
    bool inCurrentRange(const uint64_t & offset) const { return offset>=current_range.start && offset<current_range.end; }
    const orc::OffsetRange & findIncludingRange(const uint64_t & offset) const;
};

class ORCInputStreamFromString : public ReadBufferFromOwnString, public ORCInputStream
{
public:
    template <typename S>
    ORCInputStreamFromString(S && s_, size_t file_size_)
        : ReadBufferFromOwnString(std::forward<S>(s_)), ORCInputStream(dynamic_cast<SeekableReadBuffer &>(*this), file_size_)
    {
    }
};

std::unique_ptr<orc::InputStream> asORCInputStream(ReadBuffer & in, const FormatSettings & settings);

class ORCColumnToCHColumn;
class NativeORCBlockInputFormat : public IInputFormat
{
public:
    NativeORCBlockInputFormat(ReadBuffer & in_, Block header_, const FormatSettings & format_settings_);

    String getName() const override { return "NativeORCBlockInputFormat"; }

    void resetParser() override;

    const std::unordered_map<String, int> & getColumnNameToIndexMapping() { return column_name_to_index; }

    const BlockMissingValues & getMissingValues() const override;

    StripesStatisticsPtr getStripesStatistics();

    std::unique_ptr<orc::StripeStatistics> getStripeStatistics(size_t stripe_index);

    ColumnStatistics getColumnStatistics(uint32_t columnId);

    void setStripesToRead(const OrcStripesInformationPtr & stripes_information) { stripes_to_read = stripes_information; }

    void setStripesStatistics(const StripesStatisticsPtr & stripes_information_) { stripes_statistics = stripes_information_; }

    void setRequiredColumns(ColumnsDescription &columns_desc) { required_columns = columns_desc; }

    OrcStripesInformationPtr getStripesToRead();

    void addPrewhere(NamesAndTypesList prewhere_columns_, std::shared_ptr<PrewhereExprInfo> prewhere_info_);

    void ignoreBatchSizeLimit() { ignore_batch_size_limit = true; }

    void registerFilterRowGroupCallback(const std::function<void(OrcStripeInformation & stripe, orc::StripeStatistics * stripe_statistics)> & call_back_) 
    {
        filter_row_group_callback = call_back_;
    }

    UInt64 getReadersOrcToCHColumnsTimeCost() const
    {
        return file_total_orc_table_to_ch_columns_time_cost;
    }

    void prepareFileReaderAndMetadata();

    bool checkStripeColumnConstantness(UInt64 stripe_index, UInt64 column_index, Field & constant_value);

    UInt64 remove_constant_column_time_cost = 0;
    UInt64 add_constant_column_time_cost = 0;

protected:
    OrcStripesInformationPtr getStripes();

    Chunk generate() override;

    void onCancel() override { is_stopped = 1; }

private:
    struct StripeReader
    {
        std::unique_ptr<orc::RowReader> row_reader;
        OrcStripeInformation stripe_info;
        std::optional<size_t> batch_size;
        Block sample_block;
        ORCColumnToCHColumn & orc_column_to_ch_column;
        size_t current_row_group_index;
        size_t current_row;
        std::vector<size_t> row_groups_to_read;
        std::unique_ptr<orc::ColumnVectorBatch> batch;
        size_t to_read_rows;
        UInt64 orc_table_to_ch_columns_time_cost; // orc to clickhouse columns time cost
        OrcConstantColumnsDescriptionPtr constant_columns_desc; // the values of constant columns in current stripe.
        bool only_constant_columns = false;

        StripeReader(
            std::unique_ptr<orc::RowReader> row_reader_,
            const OrcStripeInformation & stripe_info_,
            std::optional<size_t> batch_size_,
            const Block & sample_block_,
            ORCColumnToCHColumn & orc_column_to_ch_column_,
            bool overwrite_input_ranges,
            OrcConstantColumnsDescriptionPtr & constant_columns_desc_)
            : row_reader(std::move(row_reader_))
            , stripe_info(stripe_info_)
            , batch_size(batch_size_)
            , sample_block(sample_block_)
            , orc_column_to_ch_column(orc_column_to_ch_column_)
            , current_row_group_index(0)
            , current_row(stripe_info_.first_row_of_stripe)
            , orc_table_to_ch_columns_time_cost(0)
            , constant_columns_desc(constant_columns_desc_)
        {
            row_reader->prepareStripeRanges(stripe_info.stripe_index, nullptr, overwrite_input_ranges);
        }

        StripeReader(
            const OrcStripeInformation & stripe_info_,
            std::optional<size_t> batch_size_,
            const Block & sample_block_,
            ORCColumnToCHColumn & orc_column_to_ch_column_,
            OrcConstantColumnsDescriptionPtr & constant_columns_desc_)
            : stripe_info(stripe_info_)
            , batch_size(batch_size_)
            , sample_block(sample_block_)
            , orc_column_to_ch_column(orc_column_to_ch_column_)
            , current_row_group_index(0)
            , current_row(stripe_info_.first_row_of_stripe)
            , orc_table_to_ch_columns_time_cost(0)
            , constant_columns_desc(constant_columns_desc_)
            , only_constant_columns(true) {}

        void prepare();

        size_t read(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost);

        size_t currentRowNumber() const { return current_row; }

        bool hasPendingData() const;

        struct Range
        {
            size_t start;
            size_t end;
        };

        static Range getRowGroupRange(const OrcStripeInformation & stripe_info, size_t current_row_group_index);

        UInt64 getOrcToCHColumnsTimeCost() const
        {
            return orc_table_to_ch_columns_time_cost;
        }

        private:

        size_t readColumns(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost);

        size_t readConstantColumns(ColumnsWithTypeAndName &res_columns, UInt64 &add_constant_col_time_cost);

        void appendConstantColumns(ColumnsWithTypeAndName &res_columns, const size_t &num_rows, UInt64 &add_constant_col_time_cost) const;
    };

    UInt64 file_total_orc_table_to_ch_columns_time_cost = 0;

    std::unique_ptr<StripeReader> stripe_reader;
    std::unique_ptr<StripeReader> prewhere_stripe_reader;

    std::unique_ptr<StripeReader>
    newStripeReader(const OrcStripeInformation & stripe_info, const std::list<UInt64> & indices, const Block & sample_block, bool overwrite_input_ranges, OrcConstantColumnsDescriptionPtr & constant_column_values, const bool &constant_columns_only);

    bool prepareStripeReader();

    size_t executePrewhere(ColumnsWithTypeAndName & pre_res_columns);
    size_t executeFilter(ColumnsWithTypeAndName & pre_res_columns, ColumnsWithTypeAndName & res_columns, size_t num_rows);

    ColumnsDescription required_columns;

    Block file_schema;
    Block header;

    std::unique_ptr<orc::Reader> file_reader;
    std::unique_ptr<ORCColumnToCHColumn> orc_column_to_ch_column;

    bool file_reader_and_metadata_initialized = false;

    std::shared_ptr<PrewhereExprInfo> prewhere_info;
    NamesAndTypesList prewhere_columns;
    Block prewhere_header;

    ColumnPtr filter_holder;
    const ColumnUInt8 * filter{nullptr};

    /// indices of columns to read from ORC file
    std::list<UInt64> include_indices;
    std::list<UInt64> prewhere_include_indices;

    BlockMissingValues block_missing_values;

    const FormatSettings format_settings;

    std::atomic<int> is_stopped{0};

    /// mapping from column name to column index in ORC file
    /// used for reading statistics
    std::unordered_map<String, int> column_name_to_index;
    std::function<void(OrcStripeInformation & stripe, orc::StripeStatistics * stripe_statistics)> filter_row_group_callback;

    OrcStripesInformationPtr stripes_to_read;
    StripesStatisticsPtr stripes_statistics;
    size_t current_stripe_index = 0;

    /// for in reverse order reading
    /// should read whole stripe or row group
    bool ignore_batch_size_limit{false};
};

class NativeORCSchemaReader : public ISchemaReader
{
public:
    NativeORCSchemaReader(ReadBuffer & in_, const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

class ORCColumnToCHColumn
{
public:
    using ORCColumnPtr = const orc::ColumnVectorBatch *;
    using ORCTypePtr = const orc::Type *;
    using ORCColumnWithType = std::pair<ORCColumnPtr, ORCTypePtr>;
    using NameToColumnPtr = std::unordered_map<std::string, ORCColumnWithType>;

    ORCColumnToCHColumn(bool allow_missing_columns_, bool null_as_default_, bool case_insensitive_matching_ = false);

    ColumnsWithTypeAndName orcTableToCHColumns(const Block & header, const orc::Type * schema, const orc::ColumnVectorBatch * table);

    ColumnsWithTypeAndName orcColumnsToCHColumns(const Block & header, NameToColumnPtr & name_to_column_ptr);

private:
    bool allow_missing_columns;
    bool null_as_default;
    bool case_insensitive_matching;
};
}
#endif
