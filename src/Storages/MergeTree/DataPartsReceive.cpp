#include <IO/HashingWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/DataPartsReceive.h>
#include <Poco/Delegate.h>
#include <Poco/Logger.h>
#include <Poco/Net/HTTPResponse.h>
#include <Common/Exception.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/Throttler.h>

namespace CurrentMetrics
{
extern const Metric PartReceive;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int CORRUPTED_DATA;
    extern const int INSECURE_PATH;
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int NO_FILE_IN_DATA_PART;
    extern const int BAD_DATA_PART_NAME;
    extern const int TIMEOUT_EXCEEDED;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_MANY_SIMULTANEOUS_QUERIES;
}

/// if a receive task cost more than RECEIVE_TIMEOUT_SECONDS, we assume it had failed.
static constexpr auto RECEIVE_TIMEOUT_SECONDS = 600;

DataPartsReceive::TemporaryPartHolder::~TemporaryPartHolder()
{
    if (done)
        return;
    try
    {
        disk->removeRecursive(temp_part);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__, "DataPartsReceive: ");
    }
}

DataPartsReceive::DataPartsReceive(MergeTreeData & data_) : data(data_), log(&Poco::Logger::get(data.getLogName() + " (DataPartsReceive)"))
{
}

void DataPartsReceive::processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & /*out*/, HTTPServerResponse & /*response*/)
{
    String part_name = params.get("part");
    const auto data_settings = data.getSettings();

    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    static std::atomic_uint total_receives{0};

    if ((data_settings->max_parallel_receives && total_receives >= data_settings->max_parallel_receives)
        || (data_settings->max_parallel_receives_for_table
            && data.current_table_receives >= data_settings->max_parallel_receives_for_table)) [[unlikely]]
        throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Too many concurrent requests, try again later");

    ++total_receives;
    SCOPE_EXIT({ --total_receives; });

    ++data.current_table_receives;
    SCOPE_EXIT({ --data.current_table_receives; });

    LOG_TRACE(log, "Receiving data part {}", part_name);

    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr part;

    auto write_part_log = [&](const ExecutionStatus & execution_status)
    { data.writePartLog(PartLogElement::RECEIVE_PART, execution_status, stopwatch.elapsed(), part_name, part, {}, nullptr); };

    try
    {
        part = receivePart(part_name, body);
    }
    catch (const Exception &)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    LOG_TRACE(log, "Received data part {} successful", part_name);
    write_part_log({});
}

MergeTreeData::MutableDataPartPtr DataPartsReceive::receivePart(const String & part_name, ReadBuffer & in)
{
    String part_name_in_stream;
    readStringBinary(part_name_in_stream, in);

    if (part_name_in_stream != part_name) [[unlikely]]
        throw Exception(
            ErrorCodes::BAD_DATA_PART_NAME,
            "Part name in http params ({}) is different from part name in data stream ({})",
            part_name,
            part_name_in_stream);

    UInt64 total_file_size;
    readBinary(total_file_size, in);

    ReservationPtr reservation = data.reserveSpace(total_file_size);

    auto disk = reservation->getDisk();

    String maybe_exists_part = data.getRelativeDataPath() + "detached/" + part_name;
    if (disk->exists(maybe_exists_part)) [[unlikely]]
    {
        LOG_WARNING(
            log,
            "Directory {} already exists, probably result of a failed receive. Will remove it before receiving part.",
            fullPath(disk, maybe_exists_part));
        disk->removeRecursive(maybe_exists_part);
    }

    static const String tmp_prefix = "tmp_receive_";
    String part_relative_path = "detached/" + tmp_prefix + part_name;
    String part_download_path = data.getRelativeDataPath() + part_relative_path + "/";

    if (disk->exists(part_download_path)) [[unlikely]]
    {
        LOG_WARNING(
            log,
            "Directory {} already exists, probably result of a failed receive. Will remove it before receiving part.",
            fullPath(disk, part_download_path));
        disk->removeRecursive(part_download_path);
    }

    CurrentMetrics::Increment metric_increment{CurrentMetrics::PartReceive};

    disk->createDirectories(part_download_path);

    TemporaryPartHolder holder(disk, part_download_path);
    SyncGuardPtr sync_guard;
    if (data.getSettings()->fsync_part_directory)
        sync_guard = disk->getDirectorySyncGuard(part_download_path);

    UInt64 projections;
    readBinary(projections, in);

    for (UInt64 i = 0; i < projections; ++i)
    {
        String projection_name;
        readStringBinary(projection_name, in);
        disk->createDirectories(part_download_path + projection_name + ".proj/");
        saveBaseOrProjectionFileToDisk(part_download_path + projection_name + ".proj/", disk, in, data.parts_receive_throttler);
    }

    saveBaseOrProjectionFileToDisk(part_download_path, disk, in, data.parts_receive_throttler);

    assertEOF(in);

    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);
    MergeTreeData::MutableDataPartPtr new_data_part = data.createPart(part_name, volume, part_relative_path);
    new_data_part->modification_time = time(nullptr);
    new_data_part->loadColumnsChecksumsIndexes(true, true);

    new_data_part->is_temp = false;
    new_data_part->renameTo(fs::path("detached") / part_name, true);

    holder.done = true;

    return new_data_part;
}

void DataPartsReceive::saveBaseOrProjectionFileToDisk(
    const String & part_download_path, DiskPtr disk, ReadBuffer & in, ThrottlerPtr throttler)
{
    UInt64 files;
    readBinary(files, in);

    for (UInt64 i = 0; i < files; ++i)
    {
        String file_name;
        readStringBinary(file_name, in);
        UInt64 file_size;
        readBinary(file_size, in);

        String absolute_file_path = fs::weakly_canonical(fs::path(part_download_path) / file_name);
        if (!startsWith(absolute_file_path, fs::weakly_canonical(part_download_path).string()))
            throw Exception(
                ErrorCodes::INSECURE_PATH,
                "File path ({}) doesn't appear to be inside part path ({})."
                " This may happen if we are trying to download part from malicious replica or logical error.",
                absolute_file_path,
                part_download_path);

        auto out = disk->writeFile(fs::path(part_download_path) / file_name);

        std::atomic<bool> cancel_flag{false};
        std::future<void> future = std::async(
            std::launch::async, [&]() { copyDataWithThrottler(in, *out, file_size, blocker.getCounter(), throttler, &cancel_flag); });

        auto status = future.wait_for(std::chrono::seconds(RECEIVE_TIMEOUT_SECONDS));

        if (status == std::future_status::timeout)
        {
            cancel_flag.store(true);
            throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Receive data part timeout");
        }

        future.get();

        if (blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Receiving of part was cancelled");

        out->sync();
    }
}
}
