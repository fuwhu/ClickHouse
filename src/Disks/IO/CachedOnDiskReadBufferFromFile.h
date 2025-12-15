#pragma once

#include <Interpreters/Cache/FileCacheKey.h>
#include <Interpreters/Cache/FileCache_fwd.h>
#include <Interpreters/Cache/QueryLimit.h>
#include <IO/SeekableReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <Interpreters/FilesystemCacheLog.h>
#include <Interpreters/Cache/FileSegment.h>
#include <Interpreters/Cache/UserInfo.h>
#include "Disks/IO/IReadBufferFromRemote.h"


namespace CurrentMetrics
{
extern const Metric FilesystemCacheReadBuffers;
}

namespace DB
{

class CachedOnDiskReadBufferFromFile : public ReadBufferFromFileBase
// class CachedOnDiskReadBufferFromFile : public IReadBufferFromRemote
{
public:
    using ImplementationBufferCreator = std::function<std::unique_ptr<ReadBufferFromFileBase>()>;

    CachedOnDiskReadBufferFromFile(
        const String & source_file_path_,
        const FileCacheKey & cache_key_,
        FileCachePtr cache_,
        const FileCacheUserInfo & user_,
        ImplementationBufferCreator implementation_buffer_creator_,
        const ReadSettings & settings_,
        const String & query_id_,
        size_t file_size_,
        bool allow_seeks_after_first_read_,
        bool use_external_buffer_,
        std::optional<size_t> read_until_position_,
        std::shared_ptr<FilesystemCacheLog> cache_log_);

    ~CachedOnDiskReadBufferFromFile() override;

    bool isCached() const override { return true; }

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    String getFileName() const override { return source_file_path; }

    enum class ReadType : uint8_t
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
    };

    bool isSeekCheap() override;

    bool isContentCached(size_t offset, size_t size) override;

    ImplementationBufferCreator getCreator() const { return implementation_buffer_creator ? implementation_buffer_creator : nullptr; }

    size_t readDirect(char * to, size_t offset, size_t n) override;

    off_t seek(off_t offset_) override;

    UInt32 getRemoteSeekCount() const { return remote_seek_count; }
    UInt64 getRemoteSeekTimeCostMicrosecond() const { return remote_seek_time_cost_us; }
    UInt64 getRemoteReadBytes() const { return remote_read_bytes; }
    UInt64 getRemoteReadCount() const { return remote_read_count; }
    UInt64 getRemoteReadTimeCostMicrosecond() const { return remote_read_time_cost_us; }
    UInt64 getRemoteReadInitWaitCostMicrosecond() const { return remote_read_init_wait_cost_us; }

    UInt64 getLocalReadBytes() const { return local_read_bytes; }
    UInt64 getLocalReadCount() const { return local_read_count; }
    UInt64 getLocalReadTimeCostMicrosecond() const { return local_read_time_cost_us; }
    UInt64 getLocalWriteBytes() const { return local_write_bytes; }
    UInt64 getLocalWriteCount() const { return local_write_count; }
    UInt64 getLocalWriteTimeCostMicrosecond() const { return local_write_time_cost_us; }
    UInt64 getTotalReadTimeCostMicrosecond() const { return total_read_time_cost_us; }

private:
    using ImplementationBufferPtr = std::shared_ptr<ReadBufferFromFileBase>;

    void initialize();

    /**
     * Return a list of file segments ordered in ascending order. This list represents
     * a full contiguous interval (without holes).
     */
    FileSegmentsHolderPtr getFileSegments(size_t offset, size_t size) const;

    ImplementationBufferPtr getImplementationBuffer(FileSegment & file_segment);

    ImplementationBufferPtr getReadBufferForFileSegment(FileSegment & file_segment);

    ImplementationBufferPtr getCacheReadBuffer(const FileSegment & file_segment);

    ImplementationBufferPtr getRemoteReadBuffer(FileSegment & file_segment, ReadType read_type_);

    bool updateImplementationBufferIfNeeded();

    bool predownload(FileSegment & file_segment);

    bool nextImplStep();

    size_t getRemainingSizeToRead();

    bool completeFileSegmentAndGetNext();

    void appendFilesystemCacheLog(const FileSegment & file_segment, ReadType read_type);

    bool writeCache(char * data, size_t size, size_t offset, FileSegment & file_segment);

    static bool canStartFromCache(size_t current_offset, const FileSegment & file_segment);

    bool nextFileSegmentsBatch();

    LoggerPtr log;
    FileCacheKey cache_key;
    String source_file_path;

    FileCachePtr cache;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    ImplementationBufferCreator implementation_buffer_creator;

    /// Remote read buffer, which can only be owned by current buffer.
    ImplementationBufferPtr remote_file_reader;
    ImplementationBufferPtr cache_file_reader;

    FileSegmentsHolderPtr file_segments;

    ImplementationBufferPtr implementation_buffer;
    bool initialized = false;

    ReadType read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

    static String toString(ReadType type);

    size_t first_offset = 0;
    String nextimpl_step_log_info;
    String last_caller_id;

    String query_id;
    String current_buffer_id;
    FileCacheUserInfo user;

    bool allow_seeks_after_first_read;
    [[maybe_unused]]bool use_external_buffer;
    CurrentMetrics::Increment metric_increment{CurrentMetrics::FilesystemCacheReadBuffers};
    ProfileEvents::Counters current_file_segment_counters;

    FileCacheQueryLimit::QueryContextHolderPtr query_context_holder;

    std::shared_ptr<FilesystemCacheLog> cache_log;

    ReadBuffer swap_internal_buffer = ReadBuffer(nullptr, 0, 0);

    UInt64 remote_read_bytes = 0;
    UInt32 remote_read_count = 0;
    UInt32 remote_seek_count = 0;
    UInt64 remote_read_time_cost_us = 0;
    UInt64 remote_seek_time_cost_us = 0;
    UInt64 remote_read_init_wait_cost_us = 0;
    UInt64 local_read_bytes = 0;
    UInt32 local_read_count = 0;
    UInt64 local_read_time_cost_us = 0;
    UInt64 local_write_bytes = 0;
    UInt32 local_write_count = 0;
    UInt64 local_write_time_cost_us = 0;
    UInt64 total_read_time_cost_us = 0;
};

}
