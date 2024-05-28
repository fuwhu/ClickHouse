#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Common/MemoryTrackerBlockerInThread.h>

#include <cstddef>
#include <utility>
#include "Columns/ColumnString.h"
#include "Columns/IColumn.h"
#include "Core/Block.h"
#include "Core/ColumnWithTypeAndName.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "DataTypes/DataTypeString.h"
#include "DataTypes/DataTypesNumber.h"

#include "IO/WriteBufferFromFileDecorator.h"
#include "Interpreters/sortBlock.h"
#include "Storages/MergeTree/MergeTreeDataWriter.h"
#include "Storages/MergeTree/UniqueEngineDataWriter.h"
#include "Storages/MergeTree/UniqueMergeTreeIndex.h"
#include "Storages/MergeTree/UniqueMergeTreeIndexCommon.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void MergeTreeDataPartWriterOnDisk::Stream::preFinalize()
{
    compressed.next();
    /// 'compressed_buf' doesn't call next() on underlying buffer ('plain_hashing'). We should do it manually.
    plain_hashing.next();
    marks.next();

    plain_file->preFinalize();
    marks_file->preFinalize();

    is_prefinalized = true;
}

void MergeTreeDataPartWriterOnDisk::Stream::finalize()
{
    if (!is_prefinalized)
        preFinalize();

    plain_file->finalize();
    marks_file->finalize();
}

void MergeTreeDataPartWriterOnDisk::Stream::sync() const
{
    plain_file->sync();
    marks_file->sync();
}

MergeTreeDataPartWriterOnDisk::Stream::Stream(
    const String & escaped_column_name_,
    DiskPtr disk_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(disk_->writeFile(data_path_ + data_file_extension, max_compress_block_size_, WriteMode::Rewrite)),
    plain_hashing(*plain_file),
    compressed_buf(plain_hashing, compression_codec_, max_compress_block_size_),
    compressed(compressed_buf),
    marks_file(disk_->writeFile(marks_path_ + marks_file_extension, 4096, WriteMode::Rewrite)), marks(*marks_file)
{
}

void MergeTreeDataPartWriterOnDisk::Stream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
}


MergeTreeDataPartWriterOnDisk::MergeTreeDataPartWriterOnDisk(
    const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const MergeTreeIndices & indices_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    const MergeTreeIndexGranularity & index_granularity_)
    : IMergeTreeDataPartWriter(data_part_,
        columns_list_, metadata_snapshot_, settings_, index_granularity_)
    , skip_indices(indices_to_recalc_)
    , part_path(data_part_->getFullRelativePath())
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity.empty())
{
    if (settings.rewrite_unique_key && storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        const auto & unique_key_index_type = storage.getSettings()->unique_key_index_type;

        unique_key_index = data_part_->createUniqueIndex(unique_key_index_type);

        if (storage.getSettings()->enable_unique_key_bucket)
            unique_key_bucket_index = std::make_shared<UniqueKeyBucketIndex>();

        unique_delete_bitmap = data_part_->createUniqueDeleteBitmap(storage.getSettings()->unique_delete_bitmap_type);

        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
            unique_key_minmax_index = std::make_shared<UniqueKeyMinMaxIndex>();

    }

    if (settings.blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);

    auto disk = data_part->volume->getDisk();
    if (!disk->exists(part_path))
        disk->createDirectories(part_path);

    if (settings.rewrite_primary_key)
        initPrimaryIndex();

    if (settings.rewrite_unique_key && storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
        initUniqueIndex();

    initSkipIndices();
}

// Implementation is split into static functions for ability
/// of making unit tests without creation instance of IMergeTreeDataPartWriter,
/// which requires a lot of dependencies and access to filesystem.
static size_t computeIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;
    if (!can_use_adaptive_index_granularity)
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = std::max(block_size_in_memory / rows_in_block, 1UL);
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);
    return index_granularity_for_block;
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    const auto storage_settings = storage.getSettings();
    return computeIndexGranularityImpl(
            block,
            storage_settings->index_granularity_bytes,
            storage_settings->index_granularity,
            settings.blocks_are_granules_size,
            settings.can_use_adaptive_granularity);
}

void MergeTreeDataPartWriterOnDisk::initPrimaryIndex()
{
    if (metadata_snapshot->hasPrimaryKey())
    {
        index_file_stream = data_part->volume->getDisk()->writeFile(part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }
}

void MergeTreeDataPartWriterOnDisk::initUniqueIndex()
{
    if (metadata_snapshot->hasUniqueKey())
    {
        const DiskPtr disk_ptr = data_part->volume->getDisk();

        const auto & unique_key_index_type = storage.getSettings()->unique_key_index_type;
        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
        {
            unique_key_index_file_stream = disk_ptr->writeFile(part_path + UNIQUE_ENGINE_KEY_INDEX, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
            unique_key_index_stream = std::make_unique<HashingWriteBuffer>(*unique_key_index_file_stream);

            if (storage.getSettings()->enable_unique_key_bucket)
            {
                unique_key_bucket_index_file_stream = disk_ptr->writeFile(part_path + UNIQUE_ENGINE_KEY_BUCKET_INDEX, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
                unique_key_bucket_index_stream = std::make_unique<HashingWriteBuffer>(*unique_key_bucket_index_file_stream);
            }

            unique_key_minmax_index_file_stream = disk_ptr->writeFile(part_path + UNIQUE_ENGINE_KEY_MINMAX_INDEX, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
            unique_key_minmax_index_stream = std::make_unique<HashingWriteBuffer>(*unique_key_minmax_index_file_stream);
        }
        
        unique_delete_bitmap_file_stream = disk_ptr->writeFile(part_path + UNIQUE_ENGINE_DELETE_BITMAP, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite);
    }
}

void MergeTreeDataPartWriterOnDisk::initSkipIndices()
{
    for (const auto & index_helper : skip_indices)
    {
        String stream_name = index_helper->getFileName();
        skip_indices_streams.emplace_back(
                std::make_unique<MergeTreeDataPartWriterOnDisk::Stream>(
                        stream_name,
                        data_part->volume->getDisk(),
                        part_path + stream_name, index_helper->getSerializedFileExtension(),
                        part_path + stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size));
        skip_indices_aggregators.push_back(index_helper->createIndexAggregator());
        skip_index_accumulated_marks.push_back(0);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write)
{
    size_t primary_columns_num = primary_index_block.columns();
    if (index_columns.empty())
    {
        index_types = primary_index_block.getDataTypes();
        index_columns.resize(primary_columns_num);
        last_block_index_columns.resize(primary_columns_num);
        for (size_t i = 0; i < primary_columns_num; ++i)
            index_columns[i] = primary_index_block.getByPosition(i).column->cloneEmpty();
    }

    {
        /** While filling index (index_columns), disable memory tracker.
         * Because memory is allocated here (maybe in context of INSERT query),
         *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
         * And otherwise it will look like excessively growing memory consumption in context of query.
         *  (observed in long INSERT SELECTs)
         */
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (const auto & granule : granules_to_write)
        {
            if (metadata_snapshot->hasPrimaryKey() && granule.mark_on_start)
            {
                for (size_t j = 0; j < primary_columns_num; ++j)
                {
                    const auto & primary_column = primary_index_block.getByPosition(j);
                    index_columns[j]->insertFrom(*primary_column.column, granule.start_row);
                    primary_column.type->getDefaultSerialization()->serializeBinary(*primary_column.column, granule.start_row, *index_stream);
                }
            }
        }
    }

    /// store last index row to write final mark at the end of column
    for (size_t j = 0; j < primary_columns_num; ++j)
        last_block_index_columns[j] = primary_index_block.getByPosition(j).column;
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregator();

                if (stream.compressed.offset() >= settings.min_compress_block_size)
                    stream.compressed.next();

                writeIntBinary(stream.plain_hashing.count(), stream.marks);
                writeIntBinary(stream.compressed.offset(), stream.marks);
                /// Actually this numbers is redundant, but we have to store them
                /// to be compatible with normal .mrk2 file format
                if (settings.can_use_adaptive_granularity)
                    writeIntBinary(1UL, stream.marks);
            }

            size_t pos = granule.start_row;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, granule.rows_to_write);
            if (granule.is_complete)
                ++skip_index_accumulated_marks[i];
        }
    }
}

void MergeTreeDataPartWriterOnDisk::calculateUniqueData(const Block & unique_key_version_block, const Granules & granules_to_write)
{
    if (unique_key_version_block.columns() < 1)
        throw Exception("UniqueMergeTree must have uniq key.", ErrorCodes::LOGICAL_ERROR);

    size_t uk_rows = unique_key_version_block.rows();

    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};
    const auto & unique_key_names = metadata_snapshot->unique_key.column_names;
    const auto & version_name = data_part->storage.merging_params.version_column;
    const auto & version_column = unique_key_version_block.getByName(version_name).column;

    auto combine_key_column = DataTypeString{}.createColumn();

    size_t last_row_count = 0;

    const auto & unique_key_index_type = storage.getSettings()->unique_key_index_type;
    if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
    {
        ColumnRawPtrs unique_key_columns;
        for (const auto & unique_key_name : unique_key_names)
            unique_key_columns.emplace_back(unique_key_version_block.getByName(unique_key_name).column.get());

        for (size_t i = 0; i < uk_rows; ++i)
        {
            Arena pool;
            auto encoded_value = serializeKeysToPoolContiguous(i, unique_key_names.size(), unique_key_columns, pool);
            combine_key_column->insertData(encoded_value.data, encoded_value.size);
        }

        FieldRef unique_key_min_field;
        FieldRef unique_key_max_field;
        combine_key_column->getExtremes(unique_key_min_field, unique_key_max_field);
        String unique_key_min_value = unique_key_min_field.get<String>();
        String unique_key_max_value = unique_key_max_field.get<String>();


        if (!unique_key_index->empty())
        {
            last_row_count = unique_key_index->size() + unique_delete_bitmap->deleteRowsSize();

            if (unique_key_minmax_index)
            {
                String exists_min = unique_key_minmax_index->getMin();
                String exists_max = unique_key_minmax_index->getMax();

                String final_min = exists_min <= unique_key_min_value ? exists_min : unique_key_min_value;
                String final_max = exists_max >= unique_key_max_value ? exists_max : unique_key_max_value;

                unique_key_minmax_index->setMinMax(final_min, final_max);
            }
        }
        else
        {
            auto * current_data_part = const_cast<MergeTreeData::DataPart *>(data_part.get());
            current_data_part->setUniqueDeleteBitmap(unique_delete_bitmap);

            current_data_part->setUniqueKeyIndex(unique_key_index);

            if (unique_key_minmax_index)
            {
                current_data_part->setUniqueKeyMinMaxIndex(unique_key_minmax_index);
                unique_key_minmax_index->setMinMax(unique_key_min_value, unique_key_max_value);
            }

            if (storage.getSettings()->enable_unique_key_bucket)
            {
                unique_key_bucket_index->init(storage.getSettings()->unique_key_bucket_size, current_data_part->rows_count);
                current_data_part->setUniqueKeyBucketIndex(unique_key_bucket_index);
                unique_key_index->initBucket(unique_key_bucket_index->getBucketNum());
            }
        }

        for (const auto & granule : granules_to_write)
        {
            size_t pos = granule.start_row;

            size_t rows_read = std::min(granule.rows_to_write, uk_rows - pos);

            for (size_t index = 0; index < rows_read; ++index)
            {
                size_t row_number = pos + index;
                UInt64 version_field = version_column.get()->getUInt(row_number);

                StringRef key_ref = combine_key_column->getDataAt(row_number);
                const String & key = key_ref.toString();

                auto version_and_row = unique_key_index->get(key);
                if (version_and_row)
                {
                    const auto & pre_version_field = std::get<0>(version_and_row.value());
                    const auto & pre_row_number = std::get<1>(version_and_row.value());

                    if (pre_version_field >= version_field)
                        unique_delete_bitmap->deleteRow(row_number + last_row_count);
                    else
                    {
                        unique_delete_bitmap->deleteRow(pre_row_number);
                        unique_key_index->add(key, std::make_tuple(version_field, row_number + last_row_count));
                    }
                }
                else
                    unique_key_index->add(key, std::make_tuple(version_field, row_number + last_row_count));
            }
        }
    }
    else if (IUniqueKeyIndex::isLevelDBUniqueKeyIndex(unique_key_index_type))
    {
        ColumnsWithTypeAndName unique_key_columns;
        for (const auto & unique_key_name : unique_key_names)
            unique_key_columns.emplace_back(unique_key_version_block.getByName(unique_key_name));

        for (size_t i = 0; i < uk_rows; ++i)
        {
            WriteBufferFromOwnString key_buf;
            for (auto & unique_key_col : unique_key_columns)
                unique_key_col.type->getDefaultSerialization()->serializeMemComparable(*unique_key_col.column, i, key_buf);
            String combine_key = key_buf.str();
            combine_key_column->insertData(combine_key.data(), combine_key.size());
        }

        Block tmp_unique_key_version_block;
        tmp_unique_key_version_block.insert(
            ColumnWithTypeAndName(std::move(combine_key_column), std::make_shared<DataTypeString>(), UNIQUE_VIRTUAL_KEY_COLUMN_NAME));
        tmp_unique_key_version_block.insert(
            ColumnWithTypeAndName(version_column, std::make_shared<DataTypeUInt64>(), UNIQUE_VIRTUAL_VERSION_COLUMN_NAME));

        auto rowid_column = DataTypeUInt64{}.createColumn();
        for (const auto & granule : granules_to_write)
        {
            size_t pos = granule.start_row;
            size_t rows_read = std::min(granule.rows_to_write, uk_rows - pos);

            for (size_t index = 0; index < rows_read; ++index)
            {
                size_t row_number = pos + index + rows_count;
                rowid_column->insert(row_number);
            }
        }

        tmp_unique_key_version_block.insert(
            ColumnWithTypeAndName(std::move(rowid_column), std::make_shared<DataTypeUInt64>(), UNIQUE_VIRTUAL_ROWID_COLUMN_NAME));
        
        /// If the part contains only one block (normal insert case or merge case), we store unique_key_version_block directly into buffered_unique_block,
        /// then serialize to leveldb in the fillUniqueDataChecksums function.

        /// If the part contains more than one blocks (merge case) and unique key is not a prefix of sorting key, 
        /// we frist store unique_key_version_block into rocksdb, then read it from rocksdb and serialize to leveldb in fillUniqueDataChecksums function.

        /// If the part contains more than one blocks (merge case) and unique key is a prefix of sorting key, 
        /// we store unique_key_version_block directly into leveldb.

        if (!tmp_rocksdb_index_writer && !leveldb_index_writer)
        {
            if (rows_count == 0)
                buffered_unique_block = std::move(tmp_unique_key_version_block);
            else if (tmp_unique_key_version_block.rows() > 0)
            {
                assert(buffered_unique_block.rows() > 0);

                if (!metadata_snapshot->isUniqueKeyPrefixToSortKey())
                {
                    rocksdb::Options opts;
                    opts.create_if_missing = true;
                    opts.error_if_exists = true;
                    opts.write_buffer_size = 16 << 20; /// 16MB
                    tmp_rocksdb_index_dir
                        = fs::path(data_part->getFullPath(false) + UniqueEngineDataWriter::TEMP_MERGING_STAGE_DIR_SUFFIX);
                    rocksdb::DB * db;
                    auto status = rocksdb::DB::Open(opts, tmp_rocksdb_index_dir, &db);
                    tmp_rocksdb_index_writer = std::unique_ptr<rocksdb::DB>(db);
                    if (!status.ok())
                        throw Exception("Can't create temp unique key, status str " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
                }
                else
                {
                    IndexFile::Options options;
                    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
                    leveldb_index_writer = std::make_unique<IndexFile::IndexFileWriter>(options);
                    String index_path = fs::path(data_part->getFullPath() + UNIQUE_ENGINE_KEY_INDEX);
                    auto status = leveldb_index_writer->Open(index_path);
                    if (!status.ok())
                        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Error while opening file {}: {}", index_path, status.ToString());
                }

                writeToUniqueKeyIndex(buffered_unique_block);
                buffered_unique_block.clear();
            }
        }

        if (tmp_rocksdb_index_writer || leveldb_index_writer)
            writeToUniqueKeyIndex(tmp_unique_key_version_block);

    }
    else
        throw Exception(
            "Invalid type(" + std::to_string(unique_key_index_type) + ") for unique key index.", ErrorCodes::BAD_ARGUMENTS);

    rows_count += uk_rows;

    double ms = total_stopwatch.elapsedMilliseconds();
    LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "part {} calculateUniqueData cost {} ms", data_part->name, ms);
}

void MergeTreeDataPartWriterOnDisk::fillPrimaryIndexChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity.appendMark(0);

    if (index_stream)
    {
        if (write_final_mark)
        {
            for (size_t j = 0; j < index_columns.size(); ++j)
            {
                const auto & column = *last_block_index_columns[j];
                size_t last_row_number = column.size() - 1;
                index_columns[j]->insertFrom(column, last_row_number);
                index_types[j]->getDefaultSerialization()->serializeBinary(column, last_row_number, *index_stream);
            }
            last_block_index_columns.clear();
        }

        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_file_stream->preFinalize();
    }
}

void MergeTreeDataPartWriterOnDisk::finishPrimaryIndexSerialization(bool sync)
{
    if (index_stream)
    {
        index_file_stream->finalize();
        if (sync)
            index_file_stream->sync();
        index_stream = nullptr;
    }
}

void MergeTreeDataPartWriterOnDisk::fillUniqueDataChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    if (data_part->storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};
        auto * new_data_part = const_cast<IMergeTreeDataPart *>(data_part.get());

        /// the effective_rows of merge part may be equal to zero.
        /// if effective_rows = 0, set the empty unique key index, unique_delete_bitmap and unique_key_bucket_index to avoid null pointer.
        new_data_part->setUniqueDeleteBitmap(unique_delete_bitmap);

        const auto & unique_key_index_type = storage.getSettings()->unique_key_index_type;
        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
        {
            new_data_part->setUniqueKeyIndex(unique_key_index);
            new_data_part->setUniqueKeyMinMaxIndex(unique_key_minmax_index);

            if (storage.getSettings()->enable_unique_key_bucket)
                new_data_part->setUniqueKeyBucketIndex(unique_key_bucket_index);

            if (!unique_key_bucket_index_stream)
                unique_key_index->serializeBinary(*unique_key_index_stream);
            else
            {
                unique_key_index->serializeBinary(*unique_key_index_stream, unique_key_bucket_index);
                unique_key_bucket_index->serializeBinary(*unique_key_bucket_index_stream);

                unique_key_bucket_index_stream->next();
                checksums.files[UNIQUE_ENGINE_KEY_BUCKET_INDEX].file_size = unique_key_bucket_index_stream->count();
                checksums.files[UNIQUE_ENGINE_KEY_BUCKET_INDEX].file_hash = unique_key_bucket_index_stream->getHash();
                unique_key_bucket_index_file_stream->preFinalize();
            }

            unique_key_index_stream->next();
            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_size = unique_key_index_stream->count();
            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_hash = unique_key_index_stream->getHash();
            unique_key_index_file_stream->preFinalize();

            unique_key_minmax_index->serializeBinary(*unique_key_minmax_index_stream);
            unique_key_minmax_index_stream->next();
            checksums.files[UNIQUE_ENGINE_KEY_MINMAX_INDEX].file_size = unique_key_minmax_index_stream->count();
            checksums.files[UNIQUE_ENGINE_KEY_MINMAX_INDEX].file_hash = unique_key_minmax_index_stream->getHash();
            unique_key_minmax_index_file_stream->preFinalize();
        }
        else if (IUniqueKeyIndex::isLevelDBUniqueKeyIndex(unique_key_index_type))
        {
            IndexFile::IndexFileInfo file_info;

            if (!tmp_rocksdb_index_writer && !leveldb_index_writer)
            {
                bool rowid_is_uinit32 = storage.getSettings()->unique_delete_bitmap_type == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;
                unique_key_index->serializeBinary(
                    data_part->getFullPath() + UNIQUE_ENGINE_KEY_INDEX,
                    buffered_unique_block,
                    unique_delete_bitmap,
                    file_info,
                    rowid_is_uinit32,
                    metadata_snapshot->isUniqueKeyPrefixToSortKey());
            }
            else 
            {
                if (tmp_rocksdb_index_writer)
                {
                    unique_key_index->serializeBinary(
                        data_part->getFullPath() + UNIQUE_ENGINE_KEY_INDEX, file_info, tmp_rocksdb_index_dir, *tmp_rocksdb_index_writer);

                    const auto & disk = data_part->volume->getDisk();
                    if (disk->exists(tmp_rocksdb_index_dir))
                        disk->removeRecursive(tmp_rocksdb_index_dir);
                }
                else
                {
                    /// TODO move this to finish* function.
                    auto status = leveldb_index_writer->Finish(&file_info);
                    if (!status.ok())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while finishing file {}", status.ToString());
                }
            }

            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_size = file_info.file_size;
            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_hash = file_info.file_hash;
        }
        else
            throw Exception(
                "Invalid type(" + std::to_string(unique_key_index_type) + ") for unique key index.", ErrorCodes::BAD_ARGUMENTS);

        unique_delete_bitmap->serializeBinary(*unique_delete_bitmap_file_stream);
        unique_delete_bitmap_file_stream->preFinalize();

        double ms = total_stopwatch.elapsedMilliseconds();
        LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "part {} fillUniqueDataChecksums cost {} ms", data_part->name, ms);
    }
}

void MergeTreeDataPartWriterOnDisk::finishUniqueDataSerialization(bool sync)
{
    if (data_part->storage.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

        const auto & unique_key_index_type = storage.getSettings()->unique_key_index_type;
        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
        {
            if (unique_key_index_stream)
            {
                unique_key_index_file_stream->finalize();
                if (sync)
                    unique_key_index_file_stream->sync();
                unique_key_index_stream = nullptr;
            }

            if (unique_key_bucket_index_stream)
            {
                unique_key_bucket_index_file_stream->finalize();
                if (sync)
                    unique_key_bucket_index_file_stream->sync();
                unique_key_bucket_index_stream = nullptr;
            }

            if (unique_key_minmax_index_stream)
            {
                unique_key_minmax_index_file_stream->finalize();
                if (sync)
                    unique_key_minmax_index_file_stream->sync();
                unique_key_minmax_index_stream = nullptr;
            }
        }

        if (unique_delete_bitmap_file_stream)
        {
            unique_delete_bitmap_file_stream->finalize();
            if (sync)
                unique_delete_bitmap_file_stream->sync();
        }

        double ms = total_stopwatch.elapsedMilliseconds();
        LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "part {} finishUniqueDataSerialization cost {} ms", data_part->name, ms);
    }
}

void MergeTreeDataPartWriterOnDisk::fillSkipIndicesChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed);
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->preFinalize();
        stream->addToChecksums(checksums);
    }
}

void MergeTreeDataPartWriterOnDisk::finishSkipIndicesSerialization(bool sync)
{
    for (auto & stream : skip_indices_streams)
    {
        stream->finalize();
        if (sync)
            stream->sync();
    }

    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_accumulated_marks.clear();
}

Names MergeTreeDataPartWriterOnDisk::getSkipIndicesColumns() const
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : skip_indices)
        std::copy(index->index.column_names.cbegin(), index->index.column_names.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    return Names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());
}

void MergeTreeDataPartWriterOnDisk::writeToUniqueKeyIndex(Block & block)
{
    size_t rows = block.rows();
    if (rows == 0)
        return;

    bool rowid_is_uinit32 = storage.getSettings()->unique_delete_bitmap_type == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;

    const auto & unique_key_col = block.getByName(UNIQUE_VIRTUAL_KEY_COLUMN_NAME);
    const auto & unique_version_col = block.getByName(UNIQUE_VIRTUAL_VERSION_COLUMN_NAME);
    const auto & unique_rowid_col = block.getByName(UNIQUE_VIRTUAL_ROWID_COLUMN_NAME);

    rocksdb::WriteOptions opts;
    bool rocksdb_index_flag = false;
    
    if (tmp_rocksdb_index_writer)
    {
        opts.disableWAL = true;
        rocksdb_index_flag = true;
    }


    for (size_t i = 0; i < rows; ++i)
    {
        auto key_ref = unique_key_col.column->getDataAt(i);
        const String & key = key_ref.toString();
        const UInt64 & version = unique_version_col.column->getUInt(i);
        const UInt64 & rowid = unique_rowid_col.column->getUInt(i);

        String value;

        if (rowid_is_uinit32)
            PutVarint32(&value, static_cast<UInt32>(rowid));
        else
            PutVarint64(&value, rowid);

        /// Handle explicit version column
        PutFixed64(&value, version); /// must use correct index, not rid

        if (rocksdb_index_flag)
        {
            auto status = tmp_rocksdb_index_writer->Put(opts, key, value);
            if (!status.ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "RocksDB Failed to add unique key {} ", status.ToString());
        }
        else 
        {
            auto status = leveldb_index_writer->Add(key, value);
            if (!status.ok())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "LevelDB Failed to add unique key {} ", status.ToString());
        }
    }
}
}
