#include "ReadBufferFromHDFS.h"

#if USE_LIBHDFS

#    include <IO/BufferWithOwnMemory.h>
#    include <arrow/result.h>
#    include <base/logger_useful.h>
#    include <Common/ProfileEvents.h>
#    include <Common/Stopwatch.h>

namespace ProfileEvents
{
extern const Event HDFSReadElapsedMicroseconds;
extern const Event HDFSReadBytes;
extern const Event HDFSReadSeeks;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int NETWORK_ERROR;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int SEEK_POSITION_OUT_OF_BOUND;
    extern const int UNKNOWN_EXCEPTION;
}

ReadBufferFromHDFS::ReadBufferFromHDFS(
    std::shared_ptr<arrow::fs::HadoopFileSystem> fs_,
    const String & hdfs_uri_,
    const String & hdfs_file_path_,
    size_t file_size_,
    size_t buf_size_,
    size_t read_until_position_)
    : IReadBufferFromRemote(buf_size_)
    , fs(fs_)
    , hdfs_uri(hdfs_uri_)
    , hdfs_file_path(hdfs_file_path_)
    , read_until_position(read_until_position_)
{
    file_size = file_size_;
    if (read_until_position == 0)
    {
        read_until_position = *file_size;
    }
}

ReadBufferFromHDFS::~ReadBufferFromHDFS() = default;

void ReadBufferFromHDFS::initRemote()
{
    auto open_file_result = fs->OpenInputFile(hdfs_file_path);
    if (!open_file_result.ok())
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Can not open hdfs file: {}", open_file_result.status().message());

    file = std::move(open_file_result).ValueOrDie();
}

bool ReadBufferFromHDFS::nextImpl()
{
    size_t num_bytes_to_read;
    if (read_until_position)
    {
        if (read_until_position == file_offset)
            return false;

        if (read_until_position < file_offset)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Attempt to read beyond right offset ({} > {})", file_offset, read_until_position - 1);

        num_bytes_to_read = std::min(internal_buffer.size(), static_cast<size_t>(read_until_position - file_offset));
    }
    else
    {
        num_bytes_to_read = internal_buffer.size();
    }

    Stopwatch watch;

    if (!file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "can not use file before call initRemote!!");
    auto result = file->ReadAt(file_offset, num_bytes_to_read, internal_buffer.begin());
    ++hdfs_read_count;
    if (!result.ok()) [[unlikely]]
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Fail to read from HDFS file: {}/{}.", hdfs_uri, hdfs_file_path);

    auto bytes_read = std::move(result).ValueOrDie();
    if (bytes_read < 0) [[unlikely]]
        throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to read from HDFS file: {}/{}.", hdfs_uri, hdfs_file_path);

    watch.stop();
    ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, watch.elapsedMicroseconds());
    hdfs_read_time_cost_us += watch.elapsedMicroseconds();

    if (bytes_read) [[likely]]
    {
        working_buffer = internal_buffer;
        working_buffer.resize(bytes_read);
        file_offset += bytes_read;

        ProfileEvents::increment(ProfileEvents::HDFSReadBytes, bytes_read);
        hdfs_read_bytes += bytes_read;
        return true;
    }

    return false;
}

off_t ReadBufferFromHDFS::seek(off_t offset_, int whence)
{
    if (whence != SEEK_SET) [[unlikely]]
        throw Exception("Only SEEK_SET mode is allowed.", ErrorCodes::CANNOT_SEEK_THROUGH_FILE);

    if (offset_ < 0 || static_cast<size_t>(offset_) > file_size.value()) [[unlikely]]
        throw Exception("Seek position is out of bounds. Offset: " + std::to_string(offset_), ErrorCodes::SEEK_POSITION_OUT_OF_BOUND);

    if (!working_buffer.empty() && offset_ >= getPosition() && offset_ < file_offset)
    {
        pos = working_buffer.end() - (file_offset - offset_);
        assert(pos >= working_buffer.begin());
        assert(pos <= working_buffer.end());

        return getPosition();
    }

    file_offset = offset_;

    Stopwatch watch;

    if (!file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "can not use file before call initRemote!!");
    auto seek_status = file->Seek(file_offset);
    if (!seek_status.ok()) [[unlikely]]
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}/{}", hdfs_uri, hdfs_file_path);

    watch.stop();
    ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::HDFSReadSeeks, 1);
    ++hdfs_seek_count;
    hdfs_seek_time_cost_us += watch.elapsedMicroseconds();

    resetWorkingBuffer();

    return file_offset;
}

off_t ReadBufferFromHDFS::seek(off_t offset_)
{
    if (file_offset == offset_)
        return file_offset;

    file_offset = offset_;
    Stopwatch watch;
    if (!file)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "can not use file before call initRemote!!");
    auto seek_status = file->Seek(file_offset);
    if (!seek_status.ok()) [[unlikely]]
        throw Exception(ErrorCodes::CANNOT_SEEK_THROUGH_FILE, "Fail to seek HDFS file: {}/{}", hdfs_uri, hdfs_file_path);
    watch.stop();
    ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, watch.elapsedMicroseconds());
    ProfileEvents::increment(ProfileEvents::HDFSReadSeeks, 1);
    ++hdfs_seek_count;
    hdfs_seek_time_cost_us += watch.elapsedMicroseconds();
    resetWorkingBuffer();
    return file_offset;
}

off_t ReadBufferFromHDFS::getPosition()
{
    return file_offset - available();
}

std::optional<size_t> ReadBufferFromHDFS::getTotalSize()
{
    return file_size;
}

size_t ReadBufferFromHDFS::getFileOffsetOfBufferEnd() const
{
    return file_offset;
}

void ReadBufferFromHDFS::setReadUntilPosition(size_t position)
{
    read_until_position = position;
}

void ReadBufferFromHDFS::setReadUntilEnd()
{
    read_until_position = *file_size;
}

size_t ReadBufferFromHDFS::readDirect(char * to, size_t offset, size_t n)
{
    if (offset + n > *file_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "param error in ReadBufferFromHDFS::readDirect, offset {} + n {} > file_size {}", offset, n, *file_size);
    seek(offset);
    size_t bytes_read = 0;
    Stopwatch watch;
    while (bytes_read < n)
    {
        watch.restart();
        if (!file)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "can not use file before call initRemote!!");
        auto result = file->ReadAt(file_offset, n - bytes_read, to + bytes_read);
        ++hdfs_read_count;
        if (!result.ok()) [[unlikely]]
            throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Fail to read from HDFS file: {}/{}.", hdfs_uri, hdfs_file_path);

        watch.stop();
        ProfileEvents::increment(ProfileEvents::HDFSReadElapsedMicroseconds, watch.elapsedMicroseconds());
        hdfs_read_time_cost_us += watch.elapsedMicroseconds();

        auto n_bytes_read = std::move(result).ValueOrDie();
        if (n_bytes_read <= 0) [[unlikely]]
            throw Exception(ErrorCodes::NETWORK_ERROR, "Fail to read enough data from HDFS file: {}/{}. value is {}", hdfs_uri, hdfs_file_path, n_bytes_read);

        ProfileEvents::increment(ProfileEvents::HDFSReadBytes, n_bytes_read);
        hdfs_read_bytes += n_bytes_read;
        file_offset += n_bytes_read;
        bytes_read += n_bytes_read;
    }

    return bytes_read;
}

size_t ReadBufferFromHDFS::readBig(char * to, size_t n)
{
    return readDirect(to, file_offset, n);
}

UInt32 ReadBufferFromHDFS::getRemoteSeekCount() const { return hdfs_seek_count; }
UInt64 ReadBufferFromHDFS::getRemoteSeekTimeCostMicrosecond() const { return hdfs_seek_time_cost_us; }
UInt64 ReadBufferFromHDFS::getRemoteReadBytes() const { return hdfs_read_bytes; }
UInt64 ReadBufferFromHDFS::getRemoteReadCount() const { return hdfs_read_count; }
UInt64 ReadBufferFromHDFS::getRemoteReadTimeCostMicrosecond() const { return hdfs_read_time_cost_us; }
}

#endif
