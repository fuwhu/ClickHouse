#include "IcebergFileSource.h"
#include "Core/Types.h"
#include "ReadBufferFromHDFS.h"
#include <algorithm>
#include <functional>
#include <memory>
#include "Storages/MergeTree/KeyCondition.h"
#include "base/scope_guard.h"
#include <base/BorrowedObjectPool.h>
#include <Disks/IO/CachedReadBufferFromRemoteFS.h>

#include <filesystem>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <IO/Operators.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <IO/parseDateTimeBestEffort.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTSelectQuery.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/NativeORCBlockInputFormat.h>
#include <Processors/Formats/Impl/ORCBlockInputFormat.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Processors/Transforms/ReverseTransform.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/MergeTreeRangeReader.h>
#include <arrow/filesystem/hdfs.h>
#include <re2/re2.h>
#include <Poco/StringTokenizer.h>
#include "Common/CurrentMetrics.h"
#include "Common/CurrentThread.h"
#include "Common/Stopwatch.h"
#include "Common/ThreadPool.h"
#include "Common/tests/gtest_global_context.h"
#include <Common/ShellCommand.h>
#include "Common/RecycleThreadPoolThread.h"

namespace CurrentMetrics
{
    extern const Metric IcebergFileSourceRecyclePoolSize;
}

namespace ProfileEvents
{
extern const Event IcebergScanFileTimeCostMicroseconds;
extern const Event IcebergScanFileCount;
extern const Event IcebergReadBufferInitCostMicroseconds;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int UNKNOWN_EXCEPTION;
}

#if USE_LIBHDFS
using HadoopFileSystem = std::shared_ptr<arrow::fs::HadoopFileSystem>;

struct HadoopFileSystemWithInfo
{
    static constexpr auto MIN_RELAX_LIFETIME_SECONDS = 60L;

    bool valid() const
    {
        auto free = expired_time - time(nullptr);
        return free > MIN_RELAX_LIFETIME_SECONDS;
    }

    HadoopFileSystem fs;
    time_t expired_time;
};

static std::unordered_map<String, HadoopFileSystemWithInfo> hdfs_file_system_cache;
static std::mutex hdfs_init_mutex;
static std::atomic<time_t> kerberos_auth_expired_time;

static void checkKerberosAuth(const Poco::Util::AbstractConfiguration & config)
{
    String hadoop_kerberos_keytab = config.getString("hdfs.hadoop_kerberos_keytab");
    String hadoop_kerberos_principal = config.getString("hdfs.hadoop_kerberos_principal");
    String hadoop_security_kerberos_ticket_cache_path = config.getString("hdfs.hadoop_security_kerberos_ticket_cache_path");

    if (hadoop_kerberos_keytab.empty() || hadoop_kerberos_principal.empty() || hadoop_security_kerberos_ticket_cache_path.empty())
        throw Exception("Not enough parameters to check kerberos authentication", ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

    auto now = time(nullptr);
    if (kerberos_auth_expired_time.load() > now)
        return;

    String cache_name = (String(" -c \"") + hadoop_security_kerberos_ticket_cache_path + "\"");

    /// Use klist to check kerberos auth expiring time first
    String hadoop_kerberos_klist_command = config.getString("hadoop_kerberos_klist_command", "klist");
    String hadoop_kerberos_kinit_command = config.getString("hadoop_kerberos_kinit_command", "kinit");

    WriteBufferFromOwnString ss;

    auto run_klist = [&]() -> bool
    {
        ss.restart();
        ss << hadoop_kerberos_klist_command << cache_name;
        auto klist_cmd = ss.str();

        LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "running klist: {}", klist_cmd);
        auto command = ShellCommand::execute(klist_cmd);
        WriteBufferFromOwnString write_buf;
        copyData(command->out, write_buf);

        auto status = command->tryWait();
        if (status)
            throw Exception("klist failure: " + klist_cmd, ErrorCodes::BAD_ARGUMENTS);

        Poco::StringTokenizer lines(write_buf.str(), "\n", Poco::StringTokenizer::TOK_IGNORE_EMPTY);
        Poco::StringTokenizer tokenizer(lines[3], " ", Poco::StringTokenizer::TOK_IGNORE_EMPTY);

        String expired_time_str = tokenizer[2] + " " + tokenizer[3];
        ReadBufferFromString read_buf(expired_time_str);
        time_t expired;
        if (tryParseDateTimeBestEffortUS(expired, read_buf, DateLUT::instance(), DateLUT::instance("UTC")))
        {
            kerberos_auth_expired_time.store(expired);
            if (expired > now)
            {
                LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "NowTime: {}, ExpiredTime: {}, no need to run kinit", now, expired);
                return true;
            }
        }
        else
        {
            LOG_WARNING(&Poco::Logger::get("HDFSClient"), "Can not determine kerberos auth expired time");
        }

        return false;
    };

    auto run_kinit = [&]() -> void
    {
        ss.restart();
        ss << hadoop_kerberos_kinit_command << cache_name << " -R -t \"" << hadoop_kerberos_keytab << "\" -k " << hadoop_kerberos_principal
           << "|| " << hadoop_kerberos_kinit_command << cache_name << " -t \"" << hadoop_kerberos_keytab << "\" -k "
           << hadoop_kerberos_principal;
        auto kinit_cmd = ss.str();

        LOG_DEBUG(&Poco::Logger::get("HDFSClient"), "running kinit: {}", kinit_cmd);
        auto command = ShellCommand::execute(kinit_cmd);
        auto status = command->tryWait();
        if (status)
            throw Exception("kinit failure: " + kinit_cmd, ErrorCodes::BAD_ARGUMENTS);
    };

    if (!std::filesystem::exists(hadoop_security_kerberos_ticket_cache_path))
        run_kinit();

    if (!run_klist())
    {
        run_kinit();
        run_klist(); /// update kerberos_auth_expired_time
    }
}

static HadoopFileSystem getHadoopFS(const String & hdfs_uri, const Poco::Util::AbstractConfiguration & config)
{
    HadoopFileSystem fs;
    std::lock_guard lock(hdfs_init_mutex);
    if (auto it = hdfs_file_system_cache.find(hdfs_uri); it != hdfs_file_system_cache.end())
    {
        auto fs_with_info = it->second;
        if (fs_with_info.valid())
        {
            fs = fs_with_info.fs;
            return fs;
        }
    }

    checkKerberosAuth(config);

    auto options_result = arrow::fs::HdfsOptions::FromUri(hdfs_uri);
    if (!options_result.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Can not init hdfs client: {}", options_result.status().message());

    auto options = std::move(options_result).ValueOrDie();
    String hadoop_security_kerberos_ticket_cache_path = config.getString("hdfs.hadoop_security_kerberos_ticket_cache_path");
    options.ConfigureKerberosTicketCachePath(hadoop_security_kerberos_ticket_cache_path);

    auto hdfs_init_result = arrow::fs::HadoopFileSystem::Make(options);
    if (!hdfs_init_result.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Can not init hdfs client: {}", hdfs_init_result.status().message());

    fs = std::move(hdfs_init_result).ValueOrDie();
    hdfs_file_system_cache[hdfs_uri] = HadoopFileSystemWithInfo{.fs = fs, .expired_time = kerberos_auth_expired_time.load()};

    return fs;
}

#endif

namespace
{
    constexpr auto HDFS_URL_REGEXP = "^hdfs://[^/]*/.*";
    constexpr auto HDFS_FEDERATION_URL_REGEXP = "^viewfs://[^/]*/.*";

    enum FileSystemKind
    {
        HDFS,
        UNKNOWN
    };

    FileSystemKind getFileSystemKind(const String & uri)
    {
        if (re2::RE2::FullMatch(uri, HDFS_URL_REGEXP) || re2::RE2::FullMatch(uri, HDFS_FEDERATION_URL_REGEXP))
            return FileSystemKind::HDFS;

        return FileSystemKind::UNKNOWN;
    }

    std::shared_ptr<IReadBufferFromRemote> getReadBufferForFile(const IcebergDataFile & data_file, [[maybe_unused]] const ContextPtr & context, FileCachePtr cache, size_t size)
    {
        auto kind = getFileSystemKind(data_file.path);
        auto uri = Poco::URI(data_file.path);

        if (kind == FileSystemKind::HDFS)
        {
#if USE_LIBHDFS
            const auto & config = context->getGlobalContext()->getConfigRef();
            const String & hdfs_uri = uri.getScheme() + "://" + uri.getHost();
            const String & hdfs_file_path = uri.getPath();

            HadoopFileSystem fs = getHadoopFS(hdfs_uri, config);

            bool data_cache_enabled = context->getSettings().iceberg_data_cache_enabled;
            ReadSettings setting = context->getReadSettings();
            if (data_cache_enabled && cache != nullptr)
            {
                //one is enough(prefer not enable background download in higher version)
                auto pool = std::make_shared<BorrowedObjectPool<std::unique_ptr<ReadBufferFromHDFS>>>(1);

                auto remote_file_reader_creator = [hdfs_uri, hdfs_file_path, fs, size]()
                {
                    auto read_buf = std::make_unique<ReadBufferFromHDFS>(fs, hdfs_uri, hdfs_file_path, size, 0);
                    read_buf->initRemoteOnlyOnce();
                    return read_buf;
                };

                auto init_pool = [remote_file_reader_creator, pool]()
                {
                    pool->tryInitObjectIfLessThan(remote_file_reader_creator);
                };

                auto remote_file_reader_creator_with_pool = [pool, hdfs_uri, hdfs_file_path, fs, remote_file_reader_creator]()
                {
                    std::unique_ptr<ReadBufferFromHDFS> read_buffer_hdfs;
                    if (!pool->tryBorrowObject(read_buffer_hdfs, remote_file_reader_creator, 0)) [[unlikely]]
                    {
                        auto read_buf = remote_file_reader_creator();
                        return std::shared_ptr<ReadBufferFromHDFS>(read_buf.release());
                    }
                    return std::shared_ptr<ReadBufferFromHDFS>(read_buffer_hdfs.release(), BorrowedUniquePtrDeleteFun(pool));
                };

                return std::make_shared<CachedReadBufferFromRemoteFS>(data_file.path, cache, remote_file_reader_creator_with_pool, init_pool, setting, size, true, false, true);
            }
            else
                return std::make_shared<ReadBufferFromHDFS>(fs, hdfs_uri, hdfs_file_path, size, setting.remote_fs_buffer_size);
#else
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not read file from HDFS because libhdfs is disabled, try to rebuild ClickHouse with flag USE_LIBHDFS=1");
#endif
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "The file system of uri ({}) is not supported, only HDFS is supported now.", data_file.path);
        }
    }
}

static IcebergFileFormat stringToFormat(const String & format)
{
    auto file_format = Poco::toUpper(format);
    return magic_enum::enum_cast<IcebergFileFormat>(file_format).value_or(IcebergFileFormat::UNKNOWN);
}

IcebergFileSource::IcebergFileSource(
    ContextPtr context_,
    Block header,
    const ColumnsDescription & columns_description_,
    std::unique_ptr<std::vector<IcebergFilePtr>> iceberg_files_,
    ReadType read_type_,
    bool need_file_column_,
    IcebergExpression & expression_,
    IcebergTableMetaWithUri & table_meta_,
    std::shared_ptr<KeyCondition> key_condition_)
    : SourceWithProgress(header)
    , WithContext(context_)
    , columns_description(columns_description_)
    , iceberg_files(std::move(iceberg_files_))
    , read_type(read_type_)
    , need_file_column(need_file_column_)
    , expr(std::move(expression_))
    , table_meta(std::move(table_meta_))
    , key_condition(key_condition_)
{
    initialize();
}

std::unique_ptr<RecycleThreadPoolThread<ThreadFromGlobalPool>> DB::IcebergFileSource::static_recycle_thread;
std::mutex DB::IcebergFileSource::static_thread_mutex;

IcebergFileSource::~IcebergFileSource()
{
    if (input_format_preinit_pool && input_format_preinit_pool->active())
    {
        auto * pool = input_format_preinit_pool.release();

        //try to recycle pool async, otherwise delete in dtor
        if (!static_recycle_thread->tryPush(pool))
        {
            pool->wait();
            delete pool;
        }
    }
}

void IcebergFileSource::initialize()
{
    auto data_files = std::make_unique<IcebergDataFiles>();
    for (auto & iceberg_file : *iceberg_files)
        data_files->emplace_back(iceberg_file->scan_result.data_file);

    auto context = getContext();
    auto scan_file_metrics_index = context->newIcebergScanFileMetrics();
    auto current_scan_file_time_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    context->setScanFileStartTime(scan_file_metrics_index, current_scan_file_time_us);

    Stopwatch stop_watch;
    IcebergFileScanResults scan_results = scanIcebergFiles(
            table_meta.endpoint,
            table_meta.iceberg_database,
            table_meta.iceberg_table,
            table_meta.snapshot_id,
            *data_files,
            expr);
    context->setScanIcebergFileTimeCostMicroseconds(scan_file_metrics_index, stop_watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::IcebergScanFileTimeCostMicroseconds, stop_watch.elapsedMicroseconds());

    UInt32 total_assigned_file_count = iceberg_files->size();
    UInt64 total_assigned_file_size = 0;
    UInt32 total_assigned_split_count = 0;
    for (size_t i = 0; i < iceberg_files->size(); ++i)
    {
        auto &file = (*iceberg_files)[i];
        file->scan_result = std::move(scan_results[i]);
        if (file->inputFormatInitialized() && file->splitsInitialized())
        {
            total_assigned_split_count += file->totalSplitsSize();
            auto input_format = (*iceberg_files)[i]->input_format;
            auto *orc_input_format = dynamic_cast<NativeORCBlockInputFormat *>(input_format.get());
            if (orc_input_format)
            {
                for (auto &stripe : *(orc_input_format->getStripesToRead()))
                    total_assigned_file_size += stripe.length;
            }
        } else
            total_assigned_file_size += file->scan_result.data_file.size;
    }
    metrics_index_in_context = context->newIcebergDataStreamMetrics();

    context->setStreamAssignedFileCount(metrics_index_in_context, total_assigned_file_count);
    context->setStreamAssignedFileSize(metrics_index_in_context, total_assigned_file_size);
    context->setStreamAssignedSplitCount(metrics_index_in_context, total_assigned_split_count);

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "In current stream, totally assigned {} files with {} splits and {} bytes to read.", total_assigned_file_count, total_assigned_split_count, total_assigned_file_size);

    if (read_type == ReadType::InReverseOrder)
        std::reverse(iceberg_files->begin(), iceberg_files->end());

    const auto & settings = getContext()->getSettingsRef();
    UInt64 input_format_pre_init_pool_size = settings.iceberg_file_input_format_pre_initialization_thread_pool_size;
    if (settings.iceberg_file_input_format_pre_initialization)
    {
        input_format_preinit_pool = std::make_unique<ThreadPool>(std::min(static_cast<size_t>(input_format_pre_init_pool_size), iceberg_files->size()));
        max_pre_init_batch_size = static_cast<size_t>(input_format_preinit_pool->getMaxThreads() * settings.iceberg_file_pre_initialization_batch_size_pool_size_ratio);
        if (!static_recycle_thread)
        {
            std::lock_guard lock(static_thread_mutex);
            if (!static_recycle_thread)
                static_recycle_thread = std::make_unique<RecycleThreadPoolThread<ThreadFromGlobalPool>>(static_cast<size_t>(settings.iceberg_init_pool_max_size), [](size_t size) { CurrentMetrics::set(CurrentMetrics::IcebergFileSourceRecyclePoolSize, size); });
        }
    }
    file_iterator = iceberg_files->begin();
}

bool IcebergFileSource::prepareReader()
{
    if (file_iterator == iceberg_files->end())
    {
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "[WWG_DEBUG] it cost {} us to remove constant stripe columns, and costs {} us to add constant columns.", remove_constantness_time_cost, add_constant_columns_time_cost);
        return false;
    }

    if (current_file)
    {
        const auto * orc_input_format = dynamic_cast<NativeORCBlockInputFormat *>((*current_file_iterator)->input_format.get());
        remove_constantness_time_cost += orc_input_format->remove_constant_column_time_cost;
        add_constant_columns_time_cost += orc_input_format->add_constant_column_time_cost;

        (*current_file_iterator).reset(nullptr);
    }

    current_file_iterator = file_iterator;
    current_file = (*current_file_iterator).get();
    Stopwatch stop_watch;
    current_file->prepare(getContext(), read_type);
    stop_watch.stop();
    getContext()->increaseStreamPrepareFilesTimeCostMicroseconds(metrics_index_in_context, stop_watch.elapsedMicroseconds());

    auto input_format = current_file->input_format;
    QueryPipelineBuilder builder;
    builder.init(Pipe(input_format));

    if (columns_description.hasDefaults())
    {
        builder.addSimpleTransform(
            [&](const Block & header)
            { return std::make_shared<AddingDefaultsTransform>(header, columns_description, *input_format, getContext()); });
    }

    if (read_type == ReadType::InReverseOrder)
    {
        builder.addSimpleTransform([&](const Block & header) { return std::make_shared<ReverseTransform>(header); });
    }

    pipeline = std::make_unique<QueryPipeline>(QueryPipelineBuilder::getPipeline(std::move(builder)));
    reader = std::make_unique<PullingPipelineExecutor>(*pipeline);
    getContext()->incrementStreamReadFileCount(metrics_index_in_context);

    ++file_iterator;
    ++file_index;
    return true;
}

void IcebergFileSource::preInitFile(const ContextPtr & local_context)
{
    if (!input_format_preinit_pool->hasIdleThread() && file_index_to_open > file_index)
        return;

    auto pre_init_batch_size = std::min(max_pre_init_batch_size, iceberg_files->size() - file_index);

    //read_buffer have the same class type which means all can_nowait_init or not.
    //can_nowait_init is determined after the first file read buffer initiated
    if (can_lazy_init)
    {
        //always init at most pre_init_batch_size file
        assert(file_index + pre_init_batch_size >= file_index_to_open);
        pre_init_batch_size = file_index + pre_init_batch_size - file_index_to_open;
    }
    else
    {
        //init once per pre_init_batch_size file
        if (file_index < file_index_to_open)
            pre_init_batch_size = 0;
    }

    if (pre_init_batch_size == 0)
        return;

    auto prefetch_start_index = file_index_to_open;
    /* pre-initialize at most the input format of `pre_init_batch_size` iceberg files to read. */
    for (UInt64 i = 0; i < pre_init_batch_size; ++i)
    {
        const auto & file = (*iceberg_files)[file_index_to_open];
        if (!file->readBufferInitialized())
        {
            file->createReadBufferForFile(local_context);
        }

        if (!input_format_preinit_pool->hasIdleThread() && file_index_to_open > file_index)
            break;

        can_lazy_init &= file->read_buf->canLazyInit();
        bool async_init = true;
        if (!file->read_buf->isInitialized())
        {
            auto init_fun = [read_buf = file->read_buf, thread_group = CurrentThread::getGroup()]
            {
                if (thread_group)
                    CurrentThread::attachToIfDetached(thread_group);
                read_buf->initRemoteOnlyOnce();
            };
            async_init = input_format_preinit_pool->trySchedule(init_fun);
            //pool is full, lazy init if needed.
            if (!async_init)
            {
                //try next round unless current file not yet initialized
                if (!can_lazy_init && file_index_to_open == file_index) [[unlikely]]
                {
                    Stopwatch watch;
                    init_fun();
                    watch.stop();
                    local_context->addStreamRemoteReadInitWaitTime(metrics_index_in_context, watch.elapsedMicroseconds());
                }
            }
        }

        if (async_init || file_index_to_open == file_index)
            file_index_to_open++;

        if (!async_init)
            break;
    }

    if (!can_lazy_init)
    {
        Stopwatch watch;
        input_format_preinit_pool->wait();
        watch.stop();
        local_context->addStreamRemoteReadInitWaitTime(metrics_index_in_context, watch.elapsedMicroseconds());
    }

    for (UInt64 i = prefetch_start_index; i < file_index_to_open; ++i)
    {
        const auto & file = (*iceberg_files)[i];
        if (!file->inputFormatInitialized())
            file->createInputFormat(local_context);
    }
}

void IcebergFileSource::onCancel()
{
    std::lock_guard lock(reader_mutex);
    if (reader)
        reader->cancel();
}

Chunk IcebergFileSource::generate()
{
    const ContextPtr & local_context = getContext();
    if(!data_generate_started)
    {
        auto current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        local_context->setIcebergFileSourceReadStartTime(metrics_index_in_context, current_time_us);
        data_generate_started = true;
    }

    if (input_format_preinit_pool && file_index_to_open < iceberg_files->size())
    {
        preInitFile(local_context);
    }

    while (true)
    {
        if (!reader)
        {
            std::lock_guard lock(reader_mutex);
            prepareReader();
        }

        if (!reader || isCancelled())
            break;

        Chunk chunk;
        Stopwatch stop_watch;
        bool chunk_generated = reader->pull(chunk);
        stop_watch.stop();
        file_iceberg_file_source_read_time_cost_us += stop_watch.elapsedMicroseconds();
        auto input_format = current_file->input_format;
        UInt64 orc_to_ch_columns_time_cost_us = 0;
        if (auto * orc_input_format = dynamic_cast<NativeORCBlockInputFormat *>(input_format.get()))
            orc_to_ch_columns_time_cost_us = orc_input_format->getReadersOrcToCHColumnsTimeCost();
        if (chunk_generated)
        {
            Columns columns = chunk.getColumns();
            UInt64 num_rows = chunk.getNumRows();

            if (need_file_column)
            {
                auto column = DataTypeLowCardinality{std::make_shared<DataTypeString>()}.createColumnConst(
                    num_rows, current_file->scan_result.data_file.path);
                columns.push_back(column->convertToFullColumnIfConst());
            }

            auto * read_buffer = current_file->read_buf.get();
            if (read_buffer)
            {
                getContext()->updateIcebergFileMetrics(
                    metrics_index_in_context,
                    file_iceberg_file_source_read_time_cost_us,
                    read_buffer->getRemoteReadBytes(),
                    read_buffer->getRemoteReadTimeCostMicrosecond(),
                    read_buffer->getRemoteSeekCount(),
                    read_buffer->getRemoteReadCount(),
                    read_buffer->getRemoteSeekTimeCostMicrosecond(),
                    read_buffer->getRemoteReadInitWaitCostMicrosecond(),
                    read_buffer->getLocalReadBytes(),
                    read_buffer->getLocalReadCount(),
                    read_buffer->getLocalReadTimeCostMicrosecond(),
                    read_buffer->getLocalWriteBytes(),
                    read_buffer->getLocalWriteCount(),
                    read_buffer->getLocalWriteTimeCostMicrosecond(),
                    read_buffer->getTotalReadTimeCostMicrosecond(),
                    orc_to_ch_columns_time_cost_us
                );
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "read buffer shall never be null");
            return Chunk(std::move(columns), num_rows);
        }

        {
            std::lock_guard lock(reader_mutex);
            reader.reset(nullptr);
            pipeline.reset(nullptr);
            auto * read_buffer = current_file->read_buf.get();
            if (read_buffer)
            {
                getContext()->addIcebergFileMetricsToStreamMetrics(
                    metrics_index_in_context,
                    file_iceberg_file_source_read_time_cost_us,
                    read_buffer->getRemoteReadBytes(),
                    read_buffer->getRemoteReadTimeCostMicrosecond(),
                    read_buffer->getRemoteSeekCount(),
                    read_buffer->getRemoteReadCount(),
                    read_buffer->getRemoteSeekTimeCostMicrosecond(),
                    read_buffer->getRemoteReadInitWaitCostMicrosecond(),
                    read_buffer->getLocalReadBytes(),
                    read_buffer->getLocalReadCount(),
                    read_buffer->getLocalReadTimeCostMicrosecond(),
                    read_buffer->getLocalWriteBytes(),
                    read_buffer->getLocalWriteCount(),
                    read_buffer->getLocalWriteTimeCostMicrosecond(),
                    read_buffer->getTotalReadTimeCostMicrosecond(),
                    orc_to_ch_columns_time_cost_us
                );
                file_iceberg_file_source_read_time_cost_us = 0;
            }
            else
                throw Exception(ErrorCodes::LOGICAL_ERROR, "read buffer shall never be null");
            if (!prepareReader())
                break;
        }
    }

    return {};
}

IcebergFilePtr IcebergFileSource::createIcebergFile(
    IcebergFileScanResult scan_result,
    const ContextPtr & local_context,
    SelectQueryInfo & query_info,
    const StorageSnapshotPtr & storage_snapshot,
    ColumnsDescription columns_description_,
    size_t max_block_size,
    std::shared_ptr<KeyCondition> key_condition_,
    UInt64 & apply_filters_time_cost_us,
    bool spread_splits,
    FileCachePtr cache)
{
    IcebergFilePtr iceberg_file = IcebergFile::newFile(scan_result.data_file.format);
    auto sample_block = storage_snapshot->getSampleBlockForColumns(columns_description_.getNamesOfPhysical());

    /// prewhere
    if (query_info.prewhere_info)
    {
        auto prewhere_info = query_info.prewhere_info;
        Names pre_column_names;

        if (prewhere_info->alias_actions)
        {
            pre_column_names = prewhere_info->alias_actions->getRequiredColumnsNames();
            sample_block = prewhere_info->alias_actions->updateHeader(std::move(sample_block));
        }
        else
            pre_column_names = prewhere_info->prewhere_actions->getRequiredColumnsNames();

        sample_block = prewhere_info->prewhere_actions->updateHeader(std::move(sample_block));

        if (prewhere_info->remove_prewhere_column)
            sample_block.erase(prewhere_info->prewhere_column_name);

        if (!pre_column_names.empty())
        {
            GetColumnsOptions options(GetColumnsOptions::Kind::Ordinary);
            auto prewhere_columns = storage_snapshot->getColumnsByNames(options, pre_column_names);
            iceberg_file->prewhere_columns = prewhere_columns;

            auto actions_settings = ExpressionActionsSettings::fromContext(local_context);
            iceberg_file->prewhere_info = std::make_shared<PrewhereExprInfo>();

            if (prewhere_info->alias_actions)
                iceberg_file->prewhere_info->alias_actions
                    = std::make_shared<ExpressionActions>(prewhere_info->alias_actions, actions_settings);

            iceberg_file->prewhere_info->prewhere_actions
                = std::make_shared<ExpressionActions>(prewhere_info->prewhere_actions, actions_settings);

            iceberg_file->prewhere_info->prewhere_column_name = prewhere_info->prewhere_column_name;
            iceberg_file->prewhere_info->remove_prewhere_column = prewhere_info->remove_prewhere_column;
            iceberg_file->prewhere_info->need_filter = prewhere_info->need_filter;
        }
    }

    auto format_settings = getFormatSettings(local_context);
    /// important
    format_settings.orc.allow_missing_columns = true;
    format_settings.parquet.allow_missing_columns = true;
    format_settings.null_as_default = false;
    format_settings.defaults_for_omitted_fields = false;

    iceberg_file->scan_result = std::move(scan_result);
    iceberg_file->format_settings = format_settings;
    iceberg_file->sample_block = sample_block;
    iceberg_file->columns_description = columns_description_;
    iceberg_file->max_block_size = max_block_size;
    iceberg_file->key_condition = key_condition_;
    iceberg_file->cache = cache;

    if (spread_splits)
    {
        iceberg_file->initializeInputFormat(local_context);
        iceberg_file->initializeSplits();
        iceberg_file->applyFilters(FilterStage::FILTER_WITH_STRIPE_STATISTICS, apply_filters_time_cost_us);
        if (iceberg_file->totalSplitsSize() == 0)
            return nullptr;
    }

    return iceberg_file;
}

std::vector<IcebergFilePtr>
IcebergFileSource::splitIcebergFile(IcebergFilePtr iceberg_file, size_t request_splits, const ContextPtr & local_context)
{
    IcebergFilePtr new_file = IcebergFile::newFile(iceberg_file->scan_result.data_file.format);

    new_file->cache = iceberg_file->cache;
    new_file->scan_result = iceberg_file->scan_result;
    new_file->format_settings = iceberg_file->format_settings;
    new_file->sample_block = iceberg_file->sample_block;
    new_file->columns_description = iceberg_file->columns_description;
    new_file->max_block_size = iceberg_file->max_block_size;
    new_file->initializeInputFormat(local_context);
    new_file->prewhere_info = iceberg_file->prewhere_info;
    new_file->prewhere_columns = iceberg_file->prewhere_columns;
    new_file->key_condition = iceberg_file->key_condition;

#if USE_ORC
    if (auto * old_orc_file = dynamic_cast<IcebergORCFile *>(iceberg_file.get()))
    {
        auto * new_orc_file = dynamic_cast<IcebergORCFile *>(new_file.get());
        auto & old_stripes = old_orc_file->stripes_to_read;

        if (request_splits > old_stripes->size())
            throw Exception(
                "Can not split Iceberg file because request splits size is greater than ORC stripes size", ErrorCodes::LOGICAL_ERROR);

        new_orc_file->stripes_to_read = std::make_shared<OrcStripesInformation>();
        std::move(old_stripes->begin(), old_stripes->begin() + request_splits, std::back_inserter(*(new_orc_file->stripes_to_read)));
        old_stripes->erase(old_stripes->begin(), old_stripes->begin() + request_splits);

        new_orc_file->stripes_statistics = std::make_shared<StripesStatistics>();
        std::move(old_orc_file->stripes_statistics->begin(), old_orc_file->stripes_statistics->begin() + request_splits, std::back_inserter(*(new_orc_file->stripes_statistics)));
        old_orc_file->stripes_statistics->erase(old_orc_file->stripes_statistics->begin(), old_orc_file->stripes_statistics->begin() + request_splits);
    }
#endif

#if USE_PARQUET
    if (auto * old_parquet_file = dynamic_cast<IcebergParquetFile *>(iceberg_file.get()))
    {
        auto * new_parquet_file = dynamic_cast<IcebergParquetFile *>(new_file.get());
        auto & old_row_groups = old_parquet_file->row_groups_to_read;

        if (request_splits > old_row_groups.size())
            throw Exception(
                "Can not split Iceberg file because request splits size is greater than Parquet row groups size",
                ErrorCodes::LOGICAL_ERROR);

        auto new_row_groups = ParquetRowGroupsInformation();
        std::move(old_row_groups.end() - request_splits, old_row_groups.end(), std::back_inserter(new_row_groups));

        while (request_splits)
        {
            old_row_groups.pop_back();
            request_splits--;
        }

        new_parquet_file->row_groups_to_read = new_row_groups;
    }
#endif

    std::vector<IcebergFilePtr> result;
    result.push_back(std::move(iceberg_file));
    result.push_back(std::move(new_file));

    return result;
}

Pipe IcebergFileSource::spreadFilesAmongStreams(
    std::unique_ptr<std::vector<IcebergFilePtr>> files,
    const ContextPtr & local_context,
    Block source_block,
    ColumnsDescription columns_description,
    bool need_file_column,
    unsigned num_streams,
    IcebergExpression & filter_expression,
    IcebergTableMetaWithUri & table_meta,
    std::shared_ptr<KeyCondition> key_condition)
{
    size_t total_file_cnt = files->size();
    UInt64 total_file_size = 0;
    for (auto & file : *files)
    {
        if (file->getFormatName() == "ORC")
            total_file_size += file->getFileLength();
        else
            throw Exception("Currently, only ORC format is supported, format " + file->getFormatName() + " is not supported yet.", ErrorCodes::NOT_IMPLEMENTED);
    }

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Start to spread {} files with size {} among expected {} streams.", total_file_cnt, total_file_size, num_streams);
    auto pipes = std::make_unique<Pipes>();
    if (num_streams == 1)
    {
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading {} files to one single streams", total_file_cnt);
        return Pipe(std::make_shared<IcebergFileSource>(
            local_context, source_block, columns_description, std::move(files), ReadType::Default, need_file_column, filter_expression, table_meta, key_condition));
    }

    std::sort(files->begin(), files->end(), [&](IcebergFilePtr & lhs, IcebergFilePtr & rhs)
    {
        return lhs->getFileLength() < rhs->getFileLength();
    });

    std::vector<StreamFilesPtr> streams_files(num_streams);
    for (auto &stream_files : streams_files)
        stream_files.reset();
    std::vector<UInt64> streams_size(num_streams, 0);
    UInt32 actual_num_streams = 0;
    int begin = -1;
    int end = streams_files.size();
    int stream_index = 0;

    int step = 1;
    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Before the execution of spreading iceberg files, there are totally {} files to spread, total file size is {}.", total_file_cnt, total_file_size);
    while (!files->empty())
    {
        auto &stream_files = streams_files[stream_index];
        if (!stream_files)
        {
            stream_files = std::make_unique<std::vector<IcebergFilePtr>>();
            ++actual_num_streams;
        }
        auto file = std::move(files->back());
        files->pop_back();
        streams_size[stream_index] += file->getFileLength();
        stream_files->push_back(std::move(file));
        if (step == 1 && stream_index == end - 1) // need to change direction.
        {
            step *= -1;
            stream_index = end;
        }   
        else if (step == -1 && stream_index == 0)
        {
            step *= -1;
            stream_index = begin;
        }
        stream_index += step;
    }

    for (stream_index = 0; static_cast<UInt32>(stream_index) < actual_num_streams; ++stream_index)
    {
        auto &stream_files = streams_files[stream_index];
        UInt64 stream_total_size = streams_size[stream_index];
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Assigned {} files with size {} to {}th stream in spreadFilesAmongStreams.", stream_files->size(), stream_total_size, stream_index);
        pipes->emplace_back(std::make_shared<IcebergFileSource>(
            local_context, source_block, columns_description, std::move(stream_files), ReadType::Default, need_file_column, filter_expression, table_meta, key_condition));
    }

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading {} files among actual {} streams.", total_file_cnt, pipes->size());
    return Pipe::unitePipes(std::move(*pipes));
}

Pipe IcebergFileSource::spreadSplitsAmongStreams(
    std::unique_ptr<std::vector<IcebergFilePtr>> files,
    const ContextPtr & local_context,
    Block source_block,
    ColumnsDescription columns_description,
    bool need_file_column,
    unsigned num_streams,
    IcebergExpression & filter_expression,
    IcebergTableMetaWithUri & table_meta,
    std::shared_ptr<KeyCondition> & key_condition)
{
    size_t total_file_cnt = files->size();
    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Start to spread splits of {} files among expected {} streams.", total_file_cnt, num_streams);
    auto pipes = std::make_unique<Pipes>();

    if (num_streams == 1)
    {
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading splits of {} files to one single streams", total_file_cnt);
        return Pipe(std::make_shared<IcebergFileSource>(
            local_context, source_block, columns_description, std::move(files), ReadType::Default, need_file_column, filter_expression, table_meta, key_condition));
    }

    size_t total_split_cnt = 0;
    for (const auto & file : *files)
        total_split_cnt += file->totalSplitsSize();
    size_t avg_splits = static_cast<size_t>(std::ceilf(total_split_cnt * 1.0f / num_streams));

    std::vector<size_t> streams_splits(num_streams, 0);
    std::vector<std::vector<IcebergFilePtr>> streams;
    streams.resize(num_streams);

    auto sort = [&]()
    {
        std::sort(
            files->begin(),
            files->end(),
            [](const IcebergFilePtr & left, const IcebergFilePtr & right) { return left->totalSplitsSize() > right->totalSplitsSize(); });
    };
    sort();

    int begin = -1;
    int end = streams.size();
    int step = 1;
    int stream_id = 0;
    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Before the execution of spreading iceberg splits, total number of files is {}, total_split_cnt is {}, avg_splits is {}.", total_file_cnt, total_split_cnt, avg_splits);
    
    while (!files->empty())
    {
        IcebergFilePtr & file = files->back();
        if (streams[stream_id].empty() || streams_splits[stream_id] + file->totalSplitsSize() <= avg_splits)
        {
            streams_splits[stream_id] += file->totalSplitsSize();
            streams[stream_id].emplace_back(std::move(file));
            files->pop_back();
            if (step == 1 && stream_id == end - 1)
            {
                step *= -1;
                stream_id = end;
            }
            else if (step == -1 && stream_id == 0)
            {
                step *= -1;
                stream_id = begin;
            }
            stream_id += step;
        }
        else
        {
            if (streams_splits[stream_id] >= avg_splits)
            {
                if (step == 1 && stream_id == end - 1)
                {
                    step *= -1;
                    stream_id = end;
                }
                else if (step == -1 && stream_id == 0)
                {
                    step *= -1;
                    stream_id = begin;
                }
                stream_id += step;
                continue;
            }
            auto split_files = splitIcebergFile(std::move(file), avg_splits - streams_splits[stream_id], local_context);
            files->pop_back();
            std::move(split_files.begin(), split_files.end(), std::back_inserter(*files));
            sort();
        }
    }

    for (size_t i = 0; i < streams.size(); ++i)
    {
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Assigned {} files, {} splits to {}th stream in spreadSplitsAmongStreams.", streams[i].size(), streams_splits[i], i);
        pipes->emplace_back(std::make_shared<IcebergFileSource>(
            local_context, source_block, columns_description, std::make_unique<std::vector<IcebergFilePtr>>(std::move(streams[i])), ReadType::Default, need_file_column, filter_expression, table_meta, key_condition));
    }

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading {} splits of {} files among actual {} streams.", total_split_cnt, total_file_cnt, pipes->size());
    return Pipe::unitePipes(std::move(*pipes));
}

Pipe IcebergFileSource::spreadFilesOrSplitsAmongStreamsWithOrder(
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
    bool spread_splits)
{
    /// order key
    std::vector<SortColumnDescription> order_key_description = input_order_info->order_key_prefix_descr;
    
    if (order_key_description.size() != 1)
        throw Exception("Currently only one sorting key is supported.", ErrorCodes::NOT_IMPLEMENTED);

    FileComparator comparator = [&](const IcebergFilePtr & lhs, const IcebergFilePtr & rhs)
    {
        const IcebergORCFile * orc_file_lhs = dynamic_cast<const IcebergORCFile *>(lhs.get());
        if (!orc_file_lhs) 
            throw Exception("The file format cannot be other format except orc.", ErrorCodes::LOGICAL_ERROR);

        const IcebergORCFile * orc_file_rhs = dynamic_cast<const IcebergORCFile *>(rhs.get());
        if (!orc_file_rhs) 
            throw Exception("The file format cannot be other format except orc.", ErrorCodes::LOGICAL_ERROR);

        auto min_values_lhs = orc_file_lhs->scan_result.data_file.statistics.min.values;
        auto min_values_rhs= orc_file_rhs->scan_result.data_file.statistics.min.values;

        auto max_values_lhs = orc_file_lhs->scan_result.data_file.statistics.max.values;
        auto max_values_rhs= orc_file_rhs->scan_result.data_file.statistics.max.values;
        bool direction_asc = false;

        for (size_t i = 0; i < min_values_lhs.size(); ++i)
        {
            /// 原本是升序的小的放后面，原本是降序的大的放后面
            direction_asc = direction == 1;
            if (direction_asc)
            {
                if (i == min_values_lhs.size() - 1 && min_values_lhs[i] > min_values_rhs[i])
                    return direction_asc;
                if (min_values_lhs[i] < min_values_rhs[i])
                    return !direction_asc;
                else if (min_values_lhs[i] > min_values_rhs[i])
                    return direction_asc;
            }
            else
            {
                if (i == min_values_lhs.size() - 1 && max_values_lhs[i] >= max_values_rhs[i])
                    return direction_asc;
                if (min_values_lhs[i] < min_values_rhs[i])
                    return !direction_asc;
                else if (min_values_lhs[i] > min_values_rhs[i])
                    return direction_asc;
            }
        }
        return !direction_asc;
    };

    /// 升序的话是左边的最大值和右边的最小值
    /// 降序的话是左边的最小值和右边的最大值
    FileOverlapChecker overlap_checker = [&](const IcebergFilePtr & file_lhs, const IcebergFilePtr & file_rhs)
    {
        const IcebergORCFile * orc_file_lhs = dynamic_cast<const IcebergORCFile *>(file_lhs.get());
        if (!orc_file_lhs) 
            throw Exception("The file format cannot be other format except orc.", ErrorCodes::LOGICAL_ERROR);

        const IcebergORCFile * orc_file_rhs = dynamic_cast<const IcebergORCFile *>(file_rhs.get());
        if (!orc_file_rhs) 
            throw Exception("The file format cannot be other format except orc.", ErrorCodes::LOGICAL_ERROR);

        auto max_values_lhs = orc_file_lhs->scan_result.data_file.statistics.max.values;
        auto min_values_lhs = orc_file_lhs->scan_result.data_file.statistics.min.values;

        auto min_values_rhs = orc_file_rhs->scan_result.data_file.statistics.min.values;
        auto max_values_rhs = orc_file_rhs->scan_result.data_file.statistics.max.values;

        bool direction_asc = false;
        for (size_t i = 0; i < max_values_lhs.size(); ++i)
        {
            direction_asc = direction == 1;

            if (direction_asc)
            {
                if (i == max_values_lhs.size() - 1 && max_values_lhs[i] <= min_values_rhs[i])
                    return false;
                if (max_values_lhs[i] < min_values_rhs[i])
                    return false;
                else if (max_values_lhs[i] > min_values_rhs[i])
                    return true;
            }
            else
            {
                if (i == max_values_lhs.size() - 1 && min_values_lhs[i] >= max_values_rhs[i])
                    return false;
                if (min_values_lhs[i] > max_values_rhs[i])
                    return false;
                else if (min_values_lhs[i] < max_values_rhs[i])
                    return true;
            }
        }
        return true;
    };

    if (spread_splits)
        return spreadSplitsAmongStreamsWithOrder(
                files, order_key_description, comparator, overlap_checker, read_type, local_context,columns_description,
                table_meta, source_block, key_condition, need_file_column, num_streams, filter_expression, direction);
    else
        return spreadFilesAmongStreamsWithOrder(
                files, comparator, overlap_checker, read_type, local_context, columns_description,
                table_meta, source_block, key_condition, need_file_column, num_streams, filter_expression);
}

Pipe IcebergFileSource::spreadFilesAmongStreamsWithOrder(
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
        IcebergExpression & filter_expression)
{
    size_t total_file_cnt = files->size();
    Pipes pipes;
    UInt64 total_file_size = 0;
    for (auto & file : *files)
    {
        if (file->getFormatName() == "ORC")
            total_file_size += file->getFileLength();
        else
            throw Exception("Currently, only ORC format is supported, format " + file->getFormatName() + " is not supported yet.", ErrorCodes::NOT_IMPLEMENTED);
    }
    
    std::sort(files->begin(), files->end(), comparator);

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Start to spread {} files with size {} among expected {} streams with order.", total_file_cnt, total_file_size, num_streams);

    std::vector<std::vector<IcebergFilePtr>> streams;
    streams.resize(num_streams);
    int stream_id = 0;
    int begin = -1;
    int end = streams.size();
    int step = 1;

    std::vector<UInt64> streams_size(num_streams, 0);

    while (!files->empty())
    {
        IcebergFilePtr & file = files->back();
        
        if (streams[stream_id].empty() || !overlap_checker(streams[stream_id].back(), file))
        {
            streams_size[stream_id] += file->getFileLength();
            streams[stream_id].emplace_back(std::move(file));
            files->pop_back();

            if (step == 1 && stream_id == end - 1)
            {
                step *= -1;
                stream_id = end;
            }
            else if (step == -1 && stream_id == 0)
            {
                step *= -1;
                stream_id = begin;
            }
            stream_id += step;
        }
        else
        {    
            int original_step = step;
            int original_stream_id = stream_id;         
            step = -1; /// prevent missing any suitable stream
            do
            {
                if (step == -1 && stream_id == 0)
                    step *= -1;

                stream_id += step;
                if (stream_id >= end)
                    break;
            } while (overlap_checker(streams[stream_id].back(), file));

            if (stream_id >= end)
            {
                streams_size.emplace_back(file->getFileLength());
                std::vector<IcebergFilePtr> files_to_expand;
                files_to_expand.emplace_back(std::move(file));
                streams.emplace_back(std::move(files_to_expand));
                end++;
                files->pop_back();
            }
            else
            {
                streams_size[stream_id] += file->getFileLength();
                streams[stream_id].emplace_back(std::move(file));
                files->pop_back();
            }
            step = original_step;
            stream_id = original_stream_id;
        }
    }

    for (size_t i = 0; i < streams.size(); ++i)
    {
        if (!streams[i].empty())
        {
            LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Assigned {} files with size {} to {}th stream in spreadFilesAmongStreamsWithOrder.", streams[i].size(), streams_size[i], i);
            pipes.emplace_back(std::make_shared<IcebergFileSource>(
                local_context,
                source_block,
                columns_description,
                std::make_unique<std::vector<IcebergFilePtr>>(std::move(streams[i])),
                read_type,
                need_file_column,
                filter_expression,
                table_meta,
                key_condition));
        }
    }

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading {} files with size {} among actual {} streams with order.", total_file_cnt, total_file_size, pipes.size());    
    return Pipe::unitePipes(std::move(pipes));
}

Pipe IcebergFileSource::spreadSplitsAmongStreamsWithOrder(
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
        int & direction)
{
    size_t total_file_cnt = files->size();
    Pipes pipes;
    size_t total_split_cnt = 0;
    for (auto & file : *files)
    {
        if (file->getFormatName() == "ORC")
            total_split_cnt += file->totalSplitsSize();
        else
            throw Exception("Currently, only ORC format is supported, format " + file->getFormatName() + " is not supported yet.", ErrorCodes::NOT_IMPLEMENTED);
    }
    std::sort(files->begin(), files->end(), comparator);

    std::vector<size_t> streams_splits(num_streams, 0);
    std::vector<std::vector<IcebergFilePtr>> streams;
    streams.resize(num_streams);
    
    size_t avg_split_count = static_cast<size_t>(std::ceilf(total_split_cnt * 1.0f / num_streams));
    int begin = -1;
    int end = streams.size();
    int step = 1;
    int stream_id = 0;

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Before the execution of spreading iceberg splits, total number of files is {}, total_split_cnt is {}, avg_splits is {}.", total_file_cnt, total_split_cnt, avg_split_count);

    while (!files->empty())
    {
        IcebergFilePtr & file = files->back();
        auto file_split_count = file->totalSplitsSize();
        if (streams[stream_id].empty() || !overlap_checker(streams[stream_id].back(), file))
        {
            if (streams_splits[stream_id] + file_split_count <= avg_split_count)
            {
                streams_splits[stream_id] += file_split_count;
                streams[stream_id].emplace_back(std::move(file));
                files->pop_back();
                if (step == 1 && stream_id == end - 1)
                {
                    step *= -1;
                    stream_id = end;
                }
                else if (step == -1 && stream_id == 0)
                {
                    step *= -1;
                    stream_id = begin;
                }
                stream_id += step;
            }
            else
            {
                if (streams_splits[stream_id] >= avg_split_count)
                {
                    if ((step == 1 && stream_id == end - 1) || (step == -1 && stream_id == 0))
                        step *= -1;
                    
                    stream_id += step;
                    continue;
                }
                auto new_files = splitIcebergFile(std::move(file), avg_split_count - streams_splits[stream_id], local_context);
                new_files[1]->getFileSortingKeyRanges(order_key_description, direction);
                new_files[0]->getFileSortingKeyRanges(order_key_description, direction);
                files->pop_back();
                std::move(new_files.begin(), new_files.end(), std::back_inserter(*files));
                std::sort(files->begin(), files->end(), comparator);
            }
        }
        else
        {
            int original_step = step;
            int original_stream_id = stream_id;
            step = -1;
            do
            {
                if (step == -1 && stream_id == 0)
                    step *= -1;
                    
                stream_id += step;
                if (stream_id >= end)
                    break;
            } while (overlap_checker(streams[stream_id].back(), file) || streams_splits[stream_id] + file_split_count >= avg_split_count);

            if (stream_id >= end)
            {
                streams_splits.emplace_back(file->totalSplitsSize());
                std::vector<IcebergFilePtr> files_to_expand;
                files_to_expand.emplace_back(std::move(file));
                streams.emplace_back(std::move(files_to_expand));
                end++;
                files->pop_back();
            }
            else
            {
                streams_splits[stream_id] += file_split_count;
                streams[stream_id].emplace_back(std::move(file));
                files->pop_back();
            }
            step = original_step;
            stream_id = original_stream_id;
        }
    }

    for (size_t i = 0; i < streams.size(); ++i)
    {
        LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Assigned {} files, {} splits to {}th stream in spreadSplitsAmongStreamsWithOrder.", streams[i].size(), streams_splits[i], i);
        pipes.emplace_back(std::make_shared<IcebergFileSource>(
                local_context,
                source_block,
                columns_description,
                std::make_unique<std::vector<IcebergFilePtr>>(std::move(streams[i])),
                read_type,
                need_file_column,
                filter_expression,
                table_meta,
                key_condition));
    }

    LOG_DEBUG(&Poco::Logger::get("IcebergFileSource"), "Finished spreading {} splits of {} files among actual {} streams with order.", total_split_cnt, total_file_cnt, pipes.size());
    return Pipe::unitePipes(std::move(pipes));
}

IcebergFilePtr IcebergFile::newFile(const String & format)
{
    auto file_format = stringToFormat(format);
    switch (file_format)
    {
#if USE_ORC
        case IcebergFileFormat::ORC:
            return std::make_unique<IcebergORCFile>();
#endif
#if USE_PARQUET
        case IcebergFileFormat::PARQUET:
            return std::make_unique<IcebergParquetFile>();
#endif
        default:
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not supported file format: {}", file_format);
    }
}

void IcebergFile::initializeInputFormat(const ContextPtr & context_)
{
    if (!readBufferInitialized())
    {
        createReadBufferForFile(context_);
        if (!read_buf->canLazyInit())
        {
            Stopwatch watch;
            read_buf->initRemoteOnlyOnce();
            watch.stop();
            ProfileEvents::increment(ProfileEvents::IcebergReadBufferInitCostMicroseconds, watch.elapsedMicroseconds());
        }
    }
    createInputFormat(context_);
}


bool IcebergFile::inputFormatInitialized() const
{
    return input_format != nullptr;
}

bool IcebergFile::readBufferInitialized() const
{
    return read_buf != nullptr;
}

void IcebergFile::createReadBufferForFile(const ContextPtr & context_)
{
    read_buf = getReadBufferForFile(scan_result.data_file, context_, cache, scan_result.data_file.size);
}

void IcebergFile::createInputFormat(const ContextPtr & context_)
{
    input_format = context_->getInputFormat(getFormatName(), *read_buf, sample_block, max_block_size, format_settings);
    auto * orc_input_format = dynamic_cast<NativeORCBlockInputFormat *>(input_format.get());
    if (orc_input_format)
        orc_input_format->setRequiredColumns(columns_description);
}

}
