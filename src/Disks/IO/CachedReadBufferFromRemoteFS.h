#pragma once

#include <Common/FileCache.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadSettings.h>
#include <base/logger_useful.h>
#include <Disks/IO/IReadBufferFromRemote.h>

namespace DB
{

class CachedReadBufferFromRemoteFS : public IReadBufferFromRemote
{
public:
    using RemoteFSFileReaderCreator = std::function<FileSegment::RemoteFileReaderPtr()>;
    CachedReadBufferFromRemoteFS(
        const String & remote_fs_object_path_,
        FileCachePtr cache_,
        RemoteFSFileReaderCreator remote_file_reader_creator_,
        std::function<void()> remote_file_reader_init_,
        const ReadSettings & settings_,
        size_t read_until_position_,
        bool allow_seeks_after_first_read_,
        bool use_external_buffer_,
        bool use_object_pool_);

    void initRemote() override;

    bool canLazyInit() const override { return true; }

    bool nextImpl() override;

    off_t seek(off_t off, int whence) override;

    off_t seek(off_t offset_) override;

    off_t getPosition() override;

    size_t getFileOffsetOfBufferEnd() const override { return file_offset_of_buffer_end; }

    String getInfoForLog() override;

    void setReadUntilPosition(size_t position) override;
    std::optional<size_t> getTotalSize() override { return read_until_position; }

    size_t readDirect(char * to, size_t offset, size_t n) override;

    UInt32 getRemoteSeekCount() const override { return remote_seek_count; }
    UInt64 getRemoteSeekTimeCostMicrosecond() const override { return remote_seek_time_cost_us; }
    UInt64 getRemoteReadBytes() const override { return remote_read_bytes; }
    UInt64 getRemoteReadCount() const override { return remote_read_count; }
    UInt64 getRemoteReadTimeCostMicrosecond() const override { return remote_read_time_cost_us; }
    UInt64 getRemoteReadInitWaitCostMicrosecond() const override { return remote_read_init_wait_cost_us; }

    UInt64 getLocalReadBytes() const override { return local_read_bytes; }
    UInt64 getLocalReadCount() const override { return local_read_count; }
    UInt64 getLocalReadTimeCostMicrosecond() const override { return local_read_time_cost_us; }
    UInt64 getLocalWriteBytes() const override { return local_write_bytes; }
    UInt64 getLocalWriteCount() const override { return local_write_count; }
    UInt64 getLocalWriteTimeCostMicrosecond() const override { return local_write_time_cost_us; }
    UInt64 getTotalReadTimeCostMicrosecond() const override { return total_read_time_cost_us; }

private:
    void initialize();

    SeekableReadBufferPtr getImplementationBuffer(FileSegmentPtr & file_segment);

    SeekableReadBufferPtr getReadBufferForFileSegment(FileSegmentPtr & file_segment);

    SeekableReadBufferPtr getCacheReadBuffer(size_t offset) const;

    std::optional<size_t> getLastNonDownloadedOffset() const;

    bool updateImplementationBufferIfNeeded();

    void predownload(FileSegmentPtr & file_segment);

    bool nextImplStep();

    enum class ReadType
    {
        CACHED,
        REMOTE_FS_READ_BYPASS_CACHE,
        REMOTE_FS_READ_AND_PUT_IN_CACHE,
    };

    SeekableReadBufferPtr getRemoteFSReadBuffer(FileSegmentPtr & file_segment, ReadType read_type_);

    size_t getTotalSizeToRead();
    bool completeFileSegmentAndGetNext();

    bool nextFileSegmentsBatch();

    Poco::Logger * log;
    IFileCache::Key cache_key;
    String remote_fs_object_path;
    FileCachePtr cache;
    ReadSettings settings;

    size_t read_until_position;
    size_t file_offset_of_buffer_end = 0;
    size_t bytes_to_predownload = 0;

    RemoteFSFileReaderCreator remote_file_reader_creator;
    std::optional<std::function<void()>> remote_file_reader_init = std::nullopt;

    //Remote read buffer, which can only be owned by current buffer.
    //important!! never store read buffer if read buffer is reused by object pool
    //rename variable to prevent auto merge to higher version
    FileSegment::RemoteFileReaderPtr remote_file_reader_only;

    std::optional<FileSegmentsHolder> file_segments_holder;
    FileSegments::iterator current_file_segment_it;

    //important!! always clean(reset) before =
    //rename variable to prevent auto merge to higher version
    SeekableReadBufferPtr implementation_buffer_reusable;
    bool initialized = false;

    ReadType read_type = ReadType::REMOTE_FS_READ_BYPASS_CACHE;

    static String toString(ReadType type)
    {
        switch (type)
        {
            case ReadType::CACHED:
                return "CACHED";
            case ReadType::REMOTE_FS_READ_BYPASS_CACHE:
                return "REMOTE_FS_READ_BYPASS_CACHE";
            case ReadType::REMOTE_FS_READ_AND_PUT_IN_CACHE:
                return "REMOTE_FS_READ_AND_PUT_IN_CACHE";
        }
    }
    size_t first_offset = 0;
    bool allow_seeks_after_first_read;
    [[maybe_unused]]bool use_external_buffer;
    bool use_object_pool;

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
