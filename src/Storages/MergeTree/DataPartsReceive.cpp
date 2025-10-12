#include "DataPartsReceive.h"

#include <filesystem>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/copyData.h>
#include <Server/HTTP/HTMLForm.h>
#include <Server/HTTP/HTTPServerResponse.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ProfileEventsScope.h>
#include <Disks/SingleDiskVolume.h>

namespace fs = std::filesystem;

namespace CurrentMetrics
{
extern const Metric PartReceive;
}

namespace DB
{

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 max_parallel_receives;
    extern const MergeTreeSettingsUInt64 max_parallel_receives_for_table;
    extern const MergeTreeSettingsBool fsync_part_directory;
}

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

namespace DataPartsReceive
{

[[nodiscard]] MergeTreeData::MutableDataPartPtr
receivePart(MergeTreeData & data, LoggerPtr log, const String & part_name, ReadBuffer & in, ActionBlocker & blocker);

void saveBaseOrProjectionFileToDisk(const MutableDataPartStoragePtr & data_part_storage, ReadBuffer & in, ActionBlocker & blocker);

Service::Service(MergeTreeData & data_) : data(data_), log(getLogger(data.getStorageID().getNameForLogs() + " (DataPartsReceive)"))
{
}

std::string Service::getId(const std::string & node_id) const
{
    return "DataPartsReceive:" + node_id;
}

void Service::processQuery(const HTMLForm & params, ReadBuffer & body, WriteBuffer & /*out*/, HTTPServerResponse & /*response*/)
{
    [[maybe_unused]] int client_protocol_version = parse<int>(params.get("client_protocol_version", "0"));

    String part_name = params.get("part");

    MergeTreePartInfo::fromPartName(part_name, data.format_version);

    static std::atomic_uint total_receives{0};

    const auto data_settings = data.getSettings();
    if (((*data_settings)[MergeTreeSetting::max_parallel_receives] && total_receives >= (*data_settings)[MergeTreeSetting::max_parallel_receives])
        || ((*data_settings)[MergeTreeSetting::max_parallel_receives_for_table]
            && data.current_table_receives >= (*data_settings)[MergeTreeSetting::max_parallel_receives_for_table])) [[unlikely]]
        throw Exception(ErrorCodes::TOO_MANY_SIMULTANEOUS_QUERIES, "Too many concurrent requests, try again later");

    ++total_receives;
    SCOPE_EXIT({ --total_receives; });

    ++data.current_table_receives;
    SCOPE_EXIT({ --data.current_table_receives; });

    LOG_TRACE(log, "Receiving data part {}", part_name);

    Stopwatch stopwatch;
    MergeTreeData::MutableDataPartPtr part;
    ProfileEventsScope profile_events_scope;

    auto write_part_log = [&](const ExecutionStatus & execution_status)
    {
        data.writePartLog(
            PartLogElement::RECEIVE_PART,
            execution_status,
            stopwatch.elapsed(),
            part_name,
            part,
            {},
            nullptr,
            profile_events_scope.getSnapshot());
    };

    try
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::PartReceive};

        part = receivePart(data, log, part_name, body, blocker);
        part->is_temp = false;
        part->renameTo(fs::path(MergeTreeData::DETACHED_DIR_NAME) / part_name, true);
    }
    catch (const Exception &)
    {
        write_part_log(ExecutionStatus::fromCurrentException());
        throw;
    }

    LOG_TRACE(log, "Received data part {} successful", part_name);
    write_part_log({});
}

[[nodiscard]] MergeTreeData::MutableDataPartPtr
receivePart(MergeTreeData & data, LoggerPtr log, const String & part_name, ReadBuffer & in, ActionBlocker & blocker)
{
    if (blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Receiving of part was cancelled");

    String part_name_in_stream;
    readStringBinary(part_name_in_stream, in);

    if (part_name_in_stream != part_name) [[unlikely]]
        throw Exception(
            ErrorCodes::BAD_DATA_PART_NAME,
            "Part name in http params ({}) is different from part name in data stream ({})",
            part_name,
            part_name_in_stream);

    UInt64 sum_files_size = 0;
    readBinary(sum_files_size, in);

    ReservationPtr reservation = data.reserveSpace(sum_files_size);
    auto disk = reservation->getDisk();
    auto volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);

    String part_relative_path = fs::path(data.getRelativeDataPath()) / MergeTreeData::DETACHED_DIR_NAME;

    String maybe_exists_part = fs::path(part_relative_path) / part_name;
    if (disk->existsFileOrDirectory(maybe_exists_part)) [[unlikely]]
    {
        LOG_WARNING(
            log,
            "Directory {} already exists, probably result of a failed receive. Will remove it before receiving part.",
            fullPath(disk, maybe_exists_part));
        disk->removeRecursive(maybe_exists_part);
    }

    static const String tmp_prefix = "tmp_receive_";
    auto tmp_part_dir = tmp_prefix + part_name;

    auto part_storage_for_loading = std::make_shared<DataPartStorageOnDiskFull>(volume, part_relative_path, tmp_part_dir);
    part_storage_for_loading->beginTransaction();

    if (part_storage_for_loading->exists())
    {
        LOG_WARNING(
            log,
            "Directory {} already exists, probably result of a failed receive. Will remove it before receiving part.",
            part_storage_for_loading->getFullPath());

        part_storage_for_loading->removeRecursive();
    }

    part_storage_for_loading->createDirectories();

    SyncGuardPtr sync_guard;
    if ((*data.getSettings())[MergeTreeSetting::fsync_part_directory])
        sync_guard = part_storage_for_loading->getDirectorySyncGuard();

    try
    {
        UInt64 projections_count = 0;
        readBinary(projections_count, in);

        for (UInt64 i = 0; i < projections_count; ++i)
        {
            String projection_name;
            readStringBinary(projection_name, in);

            auto projection_part_storage = part_storage_for_loading->getProjection(projection_name + ".proj");
            projection_part_storage->createDirectories();

            saveBaseOrProjectionFileToDisk(projection_part_storage, in, blocker);
        }

        saveBaseOrProjectionFileToDisk(part_storage_for_loading, in, blocker);

        assertEOF(in);
    }
    catch (...)
    {
        part_storage_for_loading->removeRecursive();
        throw;
    }

    MergeTreeData::MutableDataPartPtr new_data_part;
    {
        part_storage_for_loading->commitTransaction();

        MergeTreeDataPartBuilder builder(data, part_name, volume, part_relative_path, tmp_part_dir, getReadSettings());

        new_data_part = builder.withPartFormatFromDisk().build();

        new_data_part->version.setCreationTID(Tx::PrehistoricTID, nullptr);
        new_data_part->is_temp = true;
        new_data_part->modification_time = time(nullptr);
        new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::PRESERVE_BLOBS;
        new_data_part->loadColumnsChecksumsIndexes(true, false);
    }

    return new_data_part;
}

void saveBaseOrProjectionFileToDisk(const MutableDataPartStoragePtr & data_part_storage, ReadBuffer & in, ActionBlocker & blocker)
{
    if (blocker.isCancelled())
        throw Exception(ErrorCodes::ABORTED, "Receiving of part was cancelled");

    UInt64 files_count = 0;
    readBinary(files_count, in);

    for (UInt64 i = 0; i < files_count; ++i)
    {
        String file_name;
        readStringBinary(file_name, in);
        UInt64 file_size = 0;
        readBinary(file_size, in);

        auto out = data_part_storage->writeFile(file_name, std::min<UInt64>(DBMS_DEFAULT_BUFFER_SIZE, file_size), {});

        copyData(in, *out, file_size, blocker.getCounter());

        if (blocker.isCancelled())
            throw Exception(ErrorCodes::ABORTED, "Receiving of part was cancelled");

        out->finalize();
        out->sync();
    }
}
}
};
