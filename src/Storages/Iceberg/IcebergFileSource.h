#pragma once

#include <memory>
#include "IcebergCommon.h"
#include "Interpreters/Context_fwd.h"

#include <Formats/FormatFactory.h>
#include <Interpreters/Context.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/StorageSnapshot.h>
#include "Common/Exception.h"
#include "Common/Stopwatch.h"
#include <Disks/IO/IReadBufferFromRemote.h>
#include "Common/RecycleThreadPoolThread.h"

namespace DB
{
struct IcebergFile;
using IcebergFilePtr = std::unique_ptr<IcebergFile>;
using StreamFiles = std::vector<IcebergFilePtr>;
using StreamFilesPtr = std::unique_ptr<std::vector<IcebergFilePtr>>;

struct PrewhereExprInfo;

enum class IcebergFileFormat
{
    ORC,
    PARQUET,
    UNKNOWN
};

enum class FilterStage 
{
    FILTER_WITH_STRIPE_STATISTICS,
    FILTER_WITH_STRIPE_ENFORCED_BITMAP,
    FILTER_WITH_ROW_INDEX
};

enum class ReadType
{
    /// By default, read will use MergeTreeReadPool and return pipe with num_streams outputs.
    /// If num_streams == 1, will read without pool, in order specified in parts.
    Default,
    /// Read in sorting key order.
    /// Returned pipe will have the number of ports equals to parts.size().
    /// Parameter num_streams_ is ignored in this case.
    /// User should add MergingSorted itself if needed.
    InOrder,
    /// The same as InOrder, but in reverse order.
    /// For every part, read ranges and granules from end to begin. Also add ReverseTransform.
    InReverseOrder,
};

struct IcebergFile
{
    IcebergFileScanResult scan_result;
    FormatSettings format_settings;

    /// prewhere
    std::shared_ptr<PrewhereExprInfo> prewhere_info;
    NamesAndTypesList prewhere_columns;

    Block sample_block;
    ColumnsDescription columns_description;
    size_t max_block_size;

    std::shared_ptr<IReadBufferFromRemote> read_buf;
    InputFormatPtr input_format;

    std::shared_ptr<KeyCondition> key_condition; 

    FileCachePtr cache;

    virtual ~IcebergFile() = default;

    virtual void applyFilters(FilterStage stage, UInt64 & apply_filters_time_cost_us) = 0;

    virtual size_t totalSplitsSize() const = 0;

    virtual UInt64 getFileLength() 
    {
        return scan_result.data_file.size;
    }

    virtual void reverseSplits() = 0;

    virtual void prepare() { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "prepare() is not implemented for Iceberg file of format {}.", getFormatName()); }

    virtual void prepare(const ContextPtr & /*context*/, const ReadType & /*read_type*/) { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "prepare(const ContextPtr &) is not implemented for Iceberg file of format {}.", getFormatName()); }

    virtual void initializeSplits() { throw Exception("initializeSplits is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool splitsInitialized() { throw Exception("splitsInitialized is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    virtual void loadSplitsStatistics() { throw Exception("loadSplitsStatistics is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool splitsStatisticsLoaded() { throw Exception("splitsStatisticsLoaded is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    virtual bool filteredWithSplitsStatistics() { throw Exception("filteredWithSplitsStatistics is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    virtual String getFormatName() const = 0;

    static IcebergFilePtr newFile(const String & format);

    virtual void getFileSortingKeyRanges(std::vector<SortColumnDescription> & /*order_key_description*/, int /*direction*/) { throw Exception("getFileSortingKeyRanges is not implemented for the file of format {} " + getFormatName() + ".", ErrorCodes::NOT_IMPLEMENTED); }

    void initializeInputFormat(const ContextPtr & context_);

    bool inputFormatInitialized() const;

    bool readBufferInitialized() const;

    void createReadBufferForFile(const ContextPtr & context_);

    void createInputFormat(const ContextPtr & context_);
};

#if USE_ORC
struct IcebergORCFile final : public IcebergFile
{
    void applyFilters(FilterStage stage, UInt64 & apply_filters_time_cost_us) override;

    void filterOrcStripes(
        const std::unordered_map<String, int> & column_name_to_index,
        FilterStage stage);

    size_t totalSplitsSize() const override { return stripes_to_read->size(); }

    void reverseSplits() override;

    void prepare(const ContextPtr & context, const ReadType & read_type) override;

    void initializeSplits() override;

    bool splitsInitialized() override
    {
        return static_cast<bool>(stripes_to_read);
    }

    void loadSplitsStatistics() override;

    bool splitsStatisticsLoaded() override {
        return static_cast<bool>(stripes_statistics);
    }

    bool filteredWithSplitsStatistics() override { return filtered_with_stripe_statistics; }

    String getFormatName() const override { return "ORC"; }

    void getFileSortingKeyRanges(std::vector<SortColumnDescription> & order_key_description, int direction) override;

    NativeORCBlockInputFormat * getORCInputFormat();

    OrcStripesInformationPtr stripes_to_read;
    StripesStatisticsPtr stripes_statistics;
    bool filtered_with_stripe_statistics = false;
};
#endif

#if USE_PARQUET
struct IcebergParquetFile final : public IcebergFile
{
    void applyFilters(FilterStage stage, UInt64 & apply_filters_time_cost_us) override;

    void filterParquetRowGroups(
        const ParquetRowGroupsInformation & row_groups,
        const ParquetRowGroupsMetadata & metadata,
        const std::unordered_map<String, int> & column_name_to_index);

    size_t totalSplitsSize() const override { return row_groups_to_read.size(); }

    void reverseSplits() override;

    void prepare() override;

    String getFormatName() const override { return "Parquet"; }

    ParquetRowGroupsInformation row_groups_to_read;
};
#endif

class IcebergFileSource final : public SourceWithProgress, WithContext
{
public:
    using FileComparator = std::function<bool(const IcebergFilePtr & lhs, const IcebergFilePtr & rhs)>;
    using FileOverlapChecker = std::function<bool(const IcebergFilePtr & lhs, const IcebergFilePtr & rhs)>;

    IcebergFileSource(
        ContextPtr context_,
        Block header,
        const ColumnsDescription & columns_description_,
        std::unique_ptr<std::vector<IcebergFilePtr>> iceberg_files_,
        ReadType read_type_,
        bool need_file_column_,
        IcebergExpression & iceberg_expression_,
        IcebergTableMetaWithUri & table_meta_,
        std::shared_ptr<KeyCondition> key_condition_);

    ~IcebergFileSource() override;

    String getName() const override { return "IcebergFileSource(" + String(magic_enum::enum_name(read_type)) + ")"; }

    static IcebergFilePtr createIcebergFile(
        IcebergFileScanResult scan_result,
        const ContextPtr & local_context,
        SelectQueryInfo & query_info,
        const StorageSnapshotPtr & storage_snapshot,
        ColumnsDescription columns_description_,
        size_t max_block_size,
        std::shared_ptr<KeyCondition> key_condition_,
        UInt64 & apply_filters_time_cost_us,
        bool spread_splits,
        FileCachePtr cache);

    static std::vector<IcebergFilePtr>
    splitIcebergFile(IcebergFilePtr iceberg_file, size_t request_splits, const ContextPtr & local_context);

    static Pipe spreadFilesAmongStreams(
        std::unique_ptr<std::vector<IcebergFilePtr>> files,
        const ContextPtr & local_context,
        Block source_block,
        ColumnsDescription columns_description,
        bool need_file_column,
        unsigned num_streams,
        IcebergExpression & filter_expression,
        IcebergTableMetaWithUri & table_meta,
        std::shared_ptr<KeyCondition> key_condition);

    static Pipe spreadSplitsAmongStreams(
        std::unique_ptr<std::vector<IcebergFilePtr>> files,
        const ContextPtr & local_context,
        Block source_block,
        ColumnsDescription columns_description,
        bool need_file_column,
        unsigned num_streams,
        IcebergExpression & filter_expression,
        IcebergTableMetaWithUri & table_meta,
        std::shared_ptr<KeyCondition> & key_condition);

    static Pipe spreadFilesOrSplitsAmongStreamsWithOrder(
        std::unique_ptr<std::vector<IcebergFilePtr>> files,
        const ContextPtr & local_context,
        InputOrderInfoPtr input_order_info,
        ReadType read_type,
        int direction,
        Block source_block,
        ColumnsDescription columns_description,
        bool need_file_column,
        unsigned num_streams,
        IcebergExpression & filter_expression,
        IcebergTableMetaWithUri & table_meta,
        std::shared_ptr<KeyCondition> key_condition,
        bool spread_splits);

protected:
    Chunk generate() override;

    void onCancel() override;

private:
    void initialize();

    bool prepareReader();

    void preInitFile(const ContextPtr & local_context);

    static Pipe spreadFilesAmongStreamsWithOrder(
        std::unique_ptr<std::vector<IcebergFilePtr>> & files,
        FileComparator & comparator,
        FileOverlapChecker & overlap_checker,
        ReadType & read_type,
        const ContextPtr & local_context,
        ColumnsDescription & columns_description,
        IcebergTableMetaWithUri & table_meta,
        Block & source_block,
        std::shared_ptr<KeyCondition> & key_condition,
        bool & need_file_column,
        unsigned & num_streams,
        IcebergExpression & filter_expression);

    static Pipe spreadSplitsAmongStreamsWithOrder(
        std::unique_ptr<std::vector<IcebergFilePtr>> & files,
        std::vector<SortColumnDescription> & order_key_description,
        FileComparator & comparator,
        FileOverlapChecker & overlap_checker,
        ReadType & read_type,
        const ContextPtr & local_context,
        ColumnsDescription & columns_description,
        IcebergTableMetaWithUri & table_meta,
        Block & source_block,
        std::shared_ptr<KeyCondition> & key_condition,
        bool & need_file_column,
        unsigned & num_streams,
        IcebergExpression & filter_expression,
        int & direction);

    ColumnsDescription columns_description;
    std::unique_ptr<std::vector<IcebergFilePtr>> iceberg_files;
    ReadType read_type;
    bool need_file_column;

    std::vector<IcebergFilePtr>::iterator file_iterator;
    UInt32 file_index = 0;
    std::vector<IcebergFilePtr>::iterator current_file_iterator;
    IcebergFile * current_file = nullptr;
    UInt32 file_index_to_open = 0;
    size_t max_pre_init_batch_size = 0;

    std::unique_ptr<QueryPipeline> pipeline;
    std::unique_ptr<PullingPipelineExecutor> reader;
    /// onCancel and generate can be called concurrently.
    std::mutex reader_mutex;

    IcebergExpression expr;
    IcebergTableMetaWithUri table_meta;
    std::shared_ptr<KeyCondition> key_condition;

    bool data_generate_started = false;
    bool can_lazy_init = true;
    Int32 metrics_index_in_context = -1;
    UInt64 file_iceberg_file_source_read_time_cost_us = 0;

    std::unique_ptr<ThreadPool> input_format_preinit_pool;

    static std::unique_ptr<RecycleThreadPoolThread<ThreadFromGlobalPool>> static_recycle_thread;
    static std::mutex static_thread_mutex;

    UInt64 remove_constantness_time_cost = 0;
    UInt64 add_constant_columns_time_cost = 0;

};

}
