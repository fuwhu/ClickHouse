#ifdef HAS_RESERVED_IDENTIFIER
#pragma clang diagnostic ignored "-Wreserved-identifier"
#endif

#include "ArrowBufferedStreams.h"

#if USE_ARROW || USE_ORC || USE_PARQUET
#include <Common/assert_cast.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/util/future.h>
#include <arrow/result.h>
#include <arrow/memory_pool_internal.h>
#include <base/logger_useful.h>
#include <sys/stat.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FILE_SIZE;
    extern const int INCORRECT_DATA;
}

ArrowBufferedOutputStream::ArrowBufferedOutputStream(WriteBuffer & out_) : out{out_}, is_open{true}
{
}

arrow::Status ArrowBufferedOutputStream::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> ArrowBufferedOutputStream::Tell() const
{
    return arrow::Result<int64_t>(total_length);
}

arrow::Status ArrowBufferedOutputStream::Write(const void * data, int64_t length)
{
    out.write(reinterpret_cast<const char *>(data), length);
    total_length += length;
    return arrow::Status::OK();
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBuffer & in_, off_t file_size_, bool avoid_buffering_)
    : in{in_}, file_size{file_size_}, is_open{true}, avoid_buffering(avoid_buffering_)
{
}

RandomAccessFileFromSeekableReadBuffer::RandomAccessFileFromSeekableReadBuffer(SeekableReadBufferWithSize & in_, bool avoid_buffering_)
    : in{in_}, is_open{true}, avoid_buffering(avoid_buffering_)
{
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::GetSize()
{
    if (!file_size)
    {
        auto * buf_with_size = dynamic_cast<SeekableReadBufferWithSize *>(&in);
        if (buf_with_size)
            file_size = buf_with_size->getTotalSize();
        if (!file_size)
            throw Exception(ErrorCodes::UNKNOWN_FILE_SIZE, "Cannot find out size of file");
    }
    return arrow::Result<int64_t>(*file_size);
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Close()
{
    is_open = false;
    return arrow::Status::OK();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Tell() const
{
    return in.getPosition();
}

arrow::Result<int64_t> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes, void * out)
{
    try
    {
        if (avoid_buffering)
            in.setReadUntilPosition(in.getPosition() + nbytes);
        return in.readBig(reinterpret_cast<char *>(out), nbytes);
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(&Poco::Logger::get("ArrowBufferedOutputStream"), "Error while reading from arrow stream: {}", message);
        return arrow::Status::IOError(message);
    }
}

arrow::Result<std::shared_ptr<arrow::Buffer>> RandomAccessFileFromSeekableReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, ArrowMemoryPool::instance()))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Future<std::shared_ptr<arrow::Buffer>>
RandomAccessFileFromSeekableReadBuffer::ReadAsync(const arrow::io::IOContext &, int64_t position, int64_t nbytes)
{
    /// Just a stub to to avoid using internal arrow thread pool
    return arrow::Future<std::shared_ptr<arrow::Buffer>>::MakeFinished(ReadAt(position, nbytes));
}

arrow::Status RandomAccessFileFromSeekableReadBuffer::Seek(int64_t position)
{
    try
    {
        if (avoid_buffering)
        {
            // Seeking to a position above a previous setReadUntilPosition() confuses some of the
            // ReadBuffer implementations.
            in.setReadUntilEnd();
        }
        in.seek(position, SEEK_SET);
        return arrow::Status::OK();
    }
    catch (...)
    {
        auto message = getCurrentExceptionMessage(false);
        LOG_ERROR(&Poco::Logger::get("ArrowBufferedOutputStream"), "Error while seeking arrow file: {}", message);
        return arrow::Status::IOError(message);
    }
}


ArrowInputStreamFromReadBuffer::ArrowInputStreamFromReadBuffer(ReadBuffer & in_) : in(in_), is_open{true}
{
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes, void * out)
{
    return in.readBig(reinterpret_cast<char *>(out), nbytes);
}

arrow::Result<std::shared_ptr<arrow::Buffer>> ArrowInputStreamFromReadBuffer::Read(int64_t nbytes)
{
    ARROW_ASSIGN_OR_RAISE(auto buffer, arrow::AllocateResizableBuffer(nbytes, ArrowMemoryPool::instance()))
    ARROW_ASSIGN_OR_RAISE(int64_t bytes_read, Read(nbytes, buffer->mutable_data()))

    if (bytes_read < nbytes)
        RETURN_NOT_OK(buffer->Resize(bytes_read));

    return buffer;
}

arrow::Status ArrowInputStreamFromReadBuffer::Abort()
{
    return arrow::Status();
}

arrow::Result<int64_t> ArrowInputStreamFromReadBuffer::Tell() const
{
    return in.count();
}

arrow::Status ArrowInputStreamFromReadBuffer::Close()
{
    is_open = false;
    return arrow::Status();
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFile(
    ReadBuffer & in,
    const FormatSettings & settings,
    std::atomic<int> & is_cancelled,
    const std::string & format_name,
    const std::string & magic_bytes,
    bool avoid_buffering)
{
    if (auto * fd_in = dynamic_cast<ReadBufferFromFileDescriptor *>(&in))
    {
        struct stat stat;
        auto res = ::fstat(fd_in->getFD(), &stat);
        // if fd is a regular file i.e. not stdin
        if (res == 0 && S_ISREG(stat.st_mode))
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*fd_in, stat.st_size, avoid_buffering);
    }
    else if (auto * seekable_in = dynamic_cast<SeekableReadBufferWithSize *>(&in))
    {
        if (settings.seekable_read)
            return std::make_shared<RandomAccessFileFromSeekableReadBuffer>(*seekable_in, avoid_buffering);
    }

    // fallback to loading the entire file in memory
    return asArrowFileLoadIntoMemory(in, is_cancelled, format_name, magic_bytes);
}

std::shared_ptr<arrow::io::RandomAccessFile> asArrowFileLoadIntoMemory(
    ReadBuffer & in, std::atomic<int> & is_cancelled, const std::string & format_name, const std::string & magic_bytes)
{
    std::string file_data(magic_bytes.size(), '\0');

    /// Avoid loading the whole file if it doesn't seem to even be in the correct format.
    size_t bytes_read = in.read(file_data.data(), magic_bytes.size());
    if (bytes_read < magic_bytes.size() || file_data != magic_bytes)
        throw Exception(ErrorCodes::INCORRECT_DATA, "Not a {} file", format_name);

    WriteBufferFromString file_buffer(file_data, AppendModeTag{});
    copyData(in, file_buffer, is_cancelled);
    file_buffer.finalize();

    return std::make_shared<arrow::io::BufferReader>(arrow::Buffer::FromString(std::move(file_data)));
}

ArrowMemoryPool * ArrowMemoryPool::instance()
{
    static ArrowMemoryPool x;
    return &x;
}

arrow::Status ArrowMemoryPool::Allocate(int64_t size, int64_t alignment, uint8_t ** out)
{
    if (size == 0)
    {
        *out = arrow::memory_pool::internal::kZeroSizeArea;
        return arrow::Status::OK();
    }

    try // is arrow exception-safe? idk, let's avoid throwing, just in case
    {
        void * p = Allocator<false>().alloc(static_cast<size_t>(size), static_cast<size_t>(alignment));
        *out = reinterpret_cast<uint8_t *>(p);
    }
    catch (...)
    {
        return arrow::Status::OutOfMemory("allocation of size ", size, " failed");
    }

    return arrow::Status::OK();
}

arrow::Status ArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size, int64_t alignment, uint8_t ** ptr)
{
    if (old_size == 0)
    {
        assert(*ptr == arrow::memory_pool::internal::kZeroSizeArea);
        return Allocate(new_size, alignment, ptr);
    }
    if (new_size == 0)
    {
        Free(*ptr, old_size, alignment);
        *ptr = arrow::memory_pool::internal::kZeroSizeArea;
        return arrow::Status::OK();
    }

    try
    {
        void * p = Allocator<false>().realloc(*ptr, static_cast<size_t>(old_size), static_cast<size_t>(new_size), static_cast<size_t>(alignment));
        *ptr = reinterpret_cast<uint8_t *>(p);
    }
    catch (...)
    {
        return arrow::Status::OutOfMemory("reallocation of size ", new_size, " failed");
    }

    return arrow::Status::OK();
}

void ArrowMemoryPool::Free(uint8_t * buffer, int64_t size, int64_t /*alignment*/)
{
    if (size == 0)
    {
        assert(buffer == arrow::memory_pool::internal::kZeroSizeArea);
        return;
    }

    Allocator<false>().free(buffer, static_cast<size_t>(size));
}
}

#endif
