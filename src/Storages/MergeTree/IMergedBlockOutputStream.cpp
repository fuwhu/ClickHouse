#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>
#include <Storages/MergeTree/IMergeTreeDataPartWriter.h>

namespace DB
{

IMergedBlockOutputStream::IMergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list,
    bool reset_columns_)
    : storage(data_part->storage)
    , metadata_snapshot(metadata_snapshot_)
    , volume(data_part->volume)
    , part_path(data_part->isStoredOnDisk() ? data_part->getFullRelativePath() : "")
    , reset_columns(reset_columns_)
{
    if (reset_columns)
    {
        SerializationInfo::Settings info_settings =
        {
            .ratio_of_defaults_for_sparse = storage.getSettings()->ratio_of_defaults_for_sparse_serialization,
            .choose_kind = false,
        };

        new_serialization_infos = SerializationInfoByName(columns_list, info_settings);
    }
}

NameSet IMergedBlockOutputStream::removeEmptyColumnsFromPart(
    const MergeTreeDataPartPtr & data_part,
    NamesAndTypesList & columns,
    SerializationInfoByName & serialization_infos,
    MergeTreeData::DataPart::Checksums & checksums)
{
    const NameSet & empty_columns = data_part->expired_columns;

    /// For compact part we have to override whole file with data, it's not
    /// worth it
    if (empty_columns.empty() || isCompactPart(data_part))
        return {};

    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    std::map<String, size_t> stream_counts;
    for (const auto & column : columns)
    {
        data_part->getSerialization(column)->enumerateStreams(
            [&](const ISerialization::SubstreamPath & substream_path)
            {
                ++stream_counts[ISerialization::getFileNameForStream(column, substream_path)];
            });
    }

    NameSet remove_files;
    const String mrk_extension = data_part->getMarksFileExtension();
    for (const auto & column_name : empty_columns)
    {
        auto column_with_type = columns.tryGetByName(column_name);
        if (!column_with_type)
           continue;

        ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
        {
            String stream_name = ISerialization::getFileNameForStream(*column_with_type, substream_path);
            /// Delete files if they are no longer shared with another column.
            if (--stream_counts[stream_name] == 0)
            {
                remove_files.emplace(stream_name + ".bin");
                remove_files.emplace(stream_name + mrk_extension);
            }
        };

        data_part->getSerialization(*column_with_type)->enumerateStreams(callback);
        
        if (isMapV2(column_with_type->type))
        {
            auto map_with_implicit = data_part->getImplicitColumsMap().find(column_with_type->name);
            for (const auto & implicit_column : map_with_implicit->second)
            {
                ISerialization::StreamCallback implicit_callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    String stream_name = ISerialization::getFileNameForStream(implicit_column, substream_path);
                    /// Delete files if they are no longer shared with another column.
                    if (--stream_counts[stream_name] == 0)
                    {
                        remove_files.emplace(stream_name + ".bin");
                        remove_files.emplace(stream_name + mrk_extension);
                    }
                };

                auto implicit_serialization = data_part->getSerialization(implicit_column);
                implicit_serialization->enumerateStreams(implicit_callback);
            }
        }
        serialization_infos.erase(column_name);
    }

    /// Remove files on disk and checksums
    for (auto itr = remove_files.begin(); itr != remove_files.end();)
    {
        if (checksums.files.contains(*itr))
        {
            checksums.files.erase(*itr);
            ++itr;
        }
        else /// If we have no file in checksums it doesn't exist on disk
        {
            LOG_TRACE(storage.log, "Files {} doesn't exist in checksums so it doesn't exist on disk, will not try to remove it", *itr);
            itr = remove_files.erase(itr);
        }
    }

    /// Remove columns from columns array
    for (const String & empty_column_name : empty_columns)
    {
        auto find_func = [&empty_column_name](const auto & pair) -> bool
        {
            return pair.name == empty_column_name;
        };
        auto remove_it
            = std::find_if(columns.begin(), columns.end(), find_func);

        if (remove_it != columns.end())
            columns.erase(remove_it);

        std::map<String, NamesAndTypesList> & map_implict
            = const_cast<std::map<String, NamesAndTypesList> &>(data_part->getImplicitColumsMap());
        if (map_implict.contains(empty_column_name))
        {
            const auto & implicit_columns = map_implict.find(empty_column_name)->second;
            for (const auto & implicit_column : implicit_columns)
            {
                auto implicit_find_func = [&implicit_column](const auto & pair) -> bool { return pair.name == implicit_column.name; };

                auto remove_implicit_column = std::find_if(columns.begin(), columns.end(), implicit_find_func);
                if (remove_implicit_column != columns.end())
                    columns.erase(remove_implicit_column);
            }
            std::erase_if(map_implict, [&empty_column_name](const auto & item) { return item.first == empty_column_name; });
        }
    }
    return remove_files;
}

}
