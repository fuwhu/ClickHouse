#pragma once

#include <Common/config.h>
#include <Disks/IO/IReadBufferFromRemote.h>

#if USE_LIBHDFS
#    include <IO/BufferWithOwnMemory.h>
#    include <IO/SeekableReadBuffer.h>
#    include <arrow/filesystem/hdfs.h>
#    include <Poco/Util/AbstractConfiguration.h>
#    include <base/Decimal.h>

namespace DB
{
class ReadBufferFromHDFS final : public IReadBufferFromRemote
{
public:
    ReadBufferFromHDFS(
        std::shared_ptr<arrow::fs::HadoopFileSystem> fs,
        const String & hdfs_uri_,
        const String & hdfs_file_path_,
        size_t file_size_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        size_t read_until_position_ = 0);

    ~ReadBufferFromHDFS() override;

    void initRemote() override;

    bool nextImpl() override;

    off_t seek(off_t offset_, int whence) override;

    off_t seek(off_t offset_) override;

    off_t getPosition() override;

    Range getRemainingReadRange() const override { return Range{ .left = static_cast<size_t>(file_offset), .right = read_until_position }; }

    std::optional<size_t> getTotalSize() override;

    size_t getFileOffsetOfBufferEnd() const override;

    void setReadUntilPosition(size_t position) override;

    void setReadUntilEnd() override;

    size_t readBig(char * to, size_t n) override;

    size_t readDirect(char * to, size_t offset, size_t n) override;

    UInt32 getRemoteSeekCount() const override;
    UInt64 getRemoteSeekTimeCostMicrosecond() const override;
    UInt64 getRemoteReadBytes() const override;
    UInt64 getRemoteReadCount() const override;
    UInt64 getRemoteReadTimeCostMicrosecond() const override;

private:
    std::shared_ptr<arrow::fs::HadoopFileSystem> fs;
    String hdfs_uri;
    String hdfs_file_path;
    off_t read_until_position = 0;

    std::shared_ptr<arrow::io::RandomAccessFile> file;
    off_t file_offset = 0;

    UInt64 hdfs_read_bytes = 0;
    UInt32 hdfs_read_count = 0;
    UInt32 hdfs_seek_count = 0;
    UInt64 hdfs_read_time_cost_us = 0;
    UInt64 hdfs_seek_time_cost_us = 0;
};
}

#endif
