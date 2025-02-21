#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndexGin.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/logger_useful.h>
#include <Compression/CompressionFactory.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/UniqueEngineDataWriter.h>

namespace ProfileEvents
{
extern const Event MergeTreeDataWriterSkipIndicesCalculationMicroseconds;
extern const Event MergeTreeDataWriterStatisticsCalculationMicroseconds;
}

namespace DB
{
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 index_granularity;
    extern const MergeTreeSettingsUInt64 index_granularity_bytes;
    extern const MergeTreeSettingsUInt64 max_digestion_size_per_segment;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::preFinalize()
{
    /// Here the main goal is to do preFinalize calls for plain_file and marks_file
    /// Before that all hashing and compression buffers have to be finalized
    /// Otherwise some data might stuck in the buffers above plain_file and marks_file
    /// Also the order is important
    compressed_hashing.finalize();
    compressor.finalize();
    plain_hashing.finalize();

    if constexpr (!only_plain_file)
    {
        marks_compressed_hashing.finalize();
        marks_compressor.finalize();
        marks_hashing.finalize();
    }

    plain_file->preFinalize();
    if constexpr (!only_plain_file)
        marks_file->preFinalize();

    is_prefinalized = true;
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::finalize()
{
    if (!is_prefinalized)
        preFinalize();

    plain_file->finalize();

    if constexpr (!only_plain_file)
        marks_file->finalize();
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::cancel() noexcept
{

    compressed_hashing.cancel();
    compressor.cancel();
    plain_hashing.cancel();

    if constexpr (!only_plain_file)
    {
        marks_compressed_hashing.cancel();
        marks_compressor.cancel();
        marks_hashing.cancel();
    }

    plain_file->cancel();
    if constexpr (!only_plain_file)
        marks_file->cancel();
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::sync() const
{
    plain_file->sync();
    if constexpr (!only_plain_file)
        marks_file->sync();
}

template<>
MergeTreeDataPartWriterOnDisk::Stream<false>::Stream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    const CompressionCodecPtr & marks_compression_codec_,
    size_t marks_compress_block_size_,
    const WriteSettings & query_write_settings) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(data_part_storage->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    marks_file(data_part_storage->writeFile(marks_path_ + marks_file_extension, 4096, query_write_settings)),
    marks_hashing(*marks_file),
    marks_compressor(marks_hashing, marks_compression_codec_, marks_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    marks_compressed_hashing(marks_compressor),
    compress_marks(MarkType(marks_file_extension).compressed)
{
}

template<>
MergeTreeDataPartWriterOnDisk::Stream<true>::Stream(
    const String & escaped_column_name_,
    const MutableDataPartStoragePtr & data_part_storage,
    const String & data_path_,
    const std::string & data_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    const WriteSettings & query_write_settings) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    plain_file(data_part_storage->writeFile(data_path_ + data_file_extension, max_compress_block_size_, query_write_settings)),
    plain_hashing(*plain_file),
    compressor(plain_hashing, compression_codec_, max_compress_block_size_, query_write_settings.use_adaptive_write_buffer, query_write_settings.adaptive_write_buffer_initial_size),
    compressed_hashing(compressor),
    compress_marks(false)
{
}

template<bool only_plain_file>
void MergeTreeDataPartWriterOnDisk::Stream<only_plain_file>::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed_hashing.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed_hashing.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    if constexpr (!only_plain_file)
    {
        if (compress_marks)
        {
            checksums.files[name + marks_file_extension].is_compressed = true;
            checksums.files[name + marks_file_extension].uncompressed_size = marks_compressed_hashing.count();
            checksums.files[name + marks_file_extension].uncompressed_hash = marks_compressed_hashing.getHash();
        }

        checksums.files[name + marks_file_extension].file_size = marks_hashing.count();
        checksums.files[name + marks_file_extension].file_hash = marks_hashing.getHash();
    }
}


MergeTreeDataPartWriterOnDisk::MergeTreeDataPartWriterOnDisk(
    const String & data_part_name_,
    const String & logger_name_,
    const SerializationByName & serializations_,
    MutableDataPartStoragePtr data_part_storage_,
    const MergeTreeIndexGranularityInfo & index_granularity_info_,
    const MergeTreeSettingsPtr & storage_settings_,
    const NamesAndTypesList & columns_list_,
    const StorageMetadataPtr & metadata_snapshot_,
    const VirtualsDescriptionPtr & virtual_columns_,
    const MergeTreeIndices & indices_to_recalc_,
    const ColumnsStatistics & stats_to_recalc_,
    const String & marks_file_extension_,
    const CompressionCodecPtr & default_codec_,
    const MergeTreeWriterSettings & settings_,
    MergeTreeIndexGranularityPtr index_granularity_,
    const MergeTreeData::MergingParams & merging_params_)
    : IMergeTreeDataPartWriter(
        data_part_name_, serializations_, data_part_storage_, index_granularity_info_,
        storage_settings_, columns_list_, metadata_snapshot_, virtual_columns_, settings_, std::move(index_granularity_))
    , skip_indices(indices_to_recalc_)
    , stats(stats_to_recalc_)
    , marks_file_extension(marks_file_extension_)
    , default_codec(default_codec_)
    , compute_granularity(index_granularity->empty())
    , compress_primary_key(settings.compress_primary_key)
    , merging_params(merging_params_)
    , execution_stats(skip_indices.size(), stats.size())
    , log(getLogger(logger_name_ + " (DataPartWriter)"))
{
    if (settings.blocks_are_granules_size && !index_granularity->empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Can't take information about index granularity from blocks, when non empty index_granularity array specified");

    /// We don't need to check if it exists or not, createDirectories doesn't throw
    getDataPartStorage().createDirectories();

    if (settings.rewrite_primary_key)
        initPrimaryIndex();

    if (settings.rewrite_unique_key && merging_params.mode == MergeTreeData::MergingParams::Unique){
        initUniqueIndex();
}
    initSkipIndices();
    initStatistics();
}

void MergeTreeDataPartWriterOnDisk::cancel() noexcept
{
    if (index_file_stream)
        index_file_stream->cancel();
    if (index_file_hashing_stream)
        index_file_hashing_stream->cancel();
    if (index_compressor_stream)
        index_compressor_stream->cancel();
    if (index_source_hashing_stream)
        index_source_hashing_stream->cancel();

    for (auto & stream : stats_streams)
        stream->cancel();

    for (auto & stream : skip_indices_streams)
        stream->cancel();

    for (auto & store: gin_index_stores)
        store.second->cancel();
}

size_t MergeTreeDataPartWriterOnDisk::computeIndexGranularity(const Block & block) const
{
    return DB::computeIndexGranularity(
        block.rows(),
        block.bytes(),
        (*storage_settings)[MergeTreeSetting::index_granularity_bytes],
        (*storage_settings)[MergeTreeSetting::index_granularity],
        settings.blocks_are_granules_size,
        settings.can_use_adaptive_granularity);
}

void MergeTreeDataPartWriterOnDisk::initPrimaryIndex()
{
    if (metadata_snapshot->hasPrimaryKey())
    {
        String index_name = "primary" + getIndexExtension(compress_primary_key);
        index_file_stream = getDataPartStorage().writeFile(index_name, DBMS_DEFAULT_BUFFER_SIZE, settings.query_write_settings);
        index_file_hashing_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);

        if (compress_primary_key)
        {
            CompressionCodecPtr primary_key_compression_codec = CompressionCodecFactory::instance().get(settings.primary_key_compression_codec);
            index_compressor_stream = std::make_unique<CompressedWriteBuffer>(*index_file_hashing_stream, primary_key_compression_codec, settings.primary_key_compress_block_size);
            index_source_hashing_stream = std::make_unique<HashingWriteBuffer>(*index_compressor_stream);
        }

        const auto & primary_key_types = metadata_snapshot->getPrimaryKey().data_types;
        index_serializations.reserve(primary_key_types.size());

        for (const auto & type : primary_key_types)
            index_serializations.push_back(type->getDefaultSerialization());
    }
}

void MergeTreeDataPartWriterOnDisk::initStatistics()
{
    for (const auto & stat_ptr : stats)
    {
        String stats_name = stat_ptr->getFileName();
        stats_streams.emplace_back(std::make_unique<MergeTreeDataPartWriterOnDisk::Stream<true>>(
                                       stats_name,
                                       data_part_storage,
                                       stats_name, STATS_FILE_SUFFIX,
                                       default_codec, settings.max_compress_block_size,
                                       settings.query_write_settings));
    }
}

void MergeTreeDataPartWriterOnDisk::initSkipIndices()
{
    if (skip_indices.empty())
        return;

    ParserCodec codec_parser;
    auto ast = parseQuery(codec_parser, "(" + Poco::toUpper(settings.marks_compression_codec) + ")", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    CompressionCodecPtr marks_compression_codec = CompressionCodecFactory::instance().get(ast, nullptr);

    for (const auto & skip_index : skip_indices)
    {
        String stream_name = skip_index->getFileName();

        skip_indices_streams.emplace_back(
                std::make_unique<MergeTreeDataPartWriterOnDisk::Stream<false>>(
                        stream_name,
                        data_part_storage,
                        stream_name, skip_index->getSerializedFileExtension(),
                        stream_name, marks_file_extension,
                        default_codec, settings.max_compress_block_size,
                        marks_compression_codec, settings.marks_compress_block_size,
                        settings.query_write_settings));

        GinIndexStorePtr store = nullptr;
        if (typeid_cast<const MergeTreeIndexGin *>(&*skip_index) != nullptr)
        {
            store = std::make_shared<GinIndexStore>(stream_name, data_part_storage, data_part_storage, (*storage_settings)[MergeTreeSetting::max_digestion_size_per_segment]);
            gin_index_stores[stream_name] = store;
        }

        skip_indices_aggregators.push_back(skip_index->createIndexAggregatorForPart(store, settings));
        skip_index_accumulated_marks.push_back(0);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndexRow(const Block & index_block, size_t row)
{
    chassert(index_block.columns() == index_serializations.size());
    auto & index_stream = compress_primary_key ? *index_source_hashing_stream : *index_file_hashing_stream;

    for (size_t i = 0; i < index_block.columns(); ++i)
    {
        const auto & column = index_block.getByPosition(i).column;
        index_serializations[i]->serializeBinary(*column, row, index_stream, {});

        if (settings.save_primary_index_in_memory)
            index_columns[i]->insertFrom(*column, row);
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializePrimaryIndex(const Block & primary_index_block, const Granules & granules_to_write)
{
    if (!metadata_snapshot->hasPrimaryKey())
        return;

    {
        /** While filling index (index_columns), disable memory tracker.
         * Because memory is allocated here (maybe in context of INSERT query),
         *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
         * And otherwise it will look like excessively growing memory consumption in context of query.
         *  (observed in long INSERT SELECTs)
         */
        MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;

        if (settings.save_primary_index_in_memory && index_columns.empty())
        {
            index_columns = primary_index_block.cloneEmptyColumns();
        }

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (const auto & granule : granules_to_write)
        {
            if (granule.mark_on_start)
                calculateAndSerializePrimaryIndexRow(primary_index_block, granule.start_row);
        }
    }

    /// Store block with last index row to write final mark at the end of column
    if (with_final_mark)
        last_index_block = primary_index_block;
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeStatistics(const Block & block)
{
    for (size_t i = 0; i < stats.size(); ++i)
    {
        const auto & stat_ptr = stats[i];
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterStatisticsCalculationMicroseconds);
        stat_ptr->build(block.getByName(stat_ptr->columnName()).column);
        execution_stats.statistics_build_us[i] += watch.elapsed();
    }
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeSkipIndices(const Block & skip_indexes_block, const Granules & granules_to_write)
{
    /// Filling and writing skip indices like in MergeTreeDataPartWriterWide::writeColumn
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        const auto index_helper = skip_indices[i];
        auto & stream = *skip_indices_streams[i];
        WriteBuffer & marks_out = stream.compress_marks ? stream.marks_compressed_hashing : stream.marks_hashing;

        GinIndexStorePtr store;
        if (typeid_cast<const MergeTreeIndexGin *>(&*index_helper) != nullptr)
        {
            String stream_name = index_helper->getFileName();
            auto it = gin_index_stores.find(stream_name);
            if (it == gin_index_stores.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Index '{}' does not exist", stream_name);
            store = it->second;
        }

        for (const auto & granule : granules_to_write)
        {
            if (skip_index_accumulated_marks[i] == index_helper->index.granularity)
            {
                skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed_hashing);
                skip_index_accumulated_marks[i] = 0;
            }

            if (skip_indices_aggregators[i]->empty() && granule.mark_on_start)
            {
                skip_indices_aggregators[i] = index_helper->createIndexAggregatorForPart(store, settings);

                if (stream.compressed_hashing.offset() >= settings.min_compress_block_size)
                    stream.compressed_hashing.next();

                writeBinaryLittleEndian(stream.plain_hashing.count(), marks_out);
                writeBinaryLittleEndian(stream.compressed_hashing.offset(), marks_out);

                /// Actually this numbers is redundant, but we have to store them
                /// to be compatible with the normal .mrk2 file format
                if (settings.can_use_adaptive_granularity)
                    writeBinaryLittleEndian(1UL, marks_out);
            }

            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::MergeTreeDataWriterSkipIndicesCalculationMicroseconds);

            size_t pos = granule.start_row;
            skip_indices_aggregators[i]->update(skip_indexes_block, &pos, granule.rows_to_write);
            if (granule.is_complete)
                ++skip_index_accumulated_marks[i];

            execution_stats.skip_indices_build_us[i] += watch.elapsed();
        }
    }
}

void MergeTreeDataPartWriterOnDisk::fillPrimaryIndexChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    bool write_final_mark = (with_final_mark && data_written);
    if (write_final_mark && compute_granularity)
        index_granularity->appendMark(0);

    if (index_file_hashing_stream)
    {
        if (write_final_mark && last_index_block)
        {
            MemoryTrackerBlockerInThread temporarily_disable_memory_tracker;
            calculateAndSerializePrimaryIndexRow(last_index_block, last_index_block.rows() - 1);
        }

        last_index_block.clear();

        if (compress_primary_key)
        {
            index_source_hashing_stream->finalize();
            index_compressor_stream->finalize();
        }

        index_file_hashing_stream->finalize();

        String index_name = "primary" + getIndexExtension(compress_primary_key);
        if (compress_primary_key)
        {
            checksums.files[index_name].is_compressed = true;
            checksums.files[index_name].uncompressed_size = index_source_hashing_stream->count();
            checksums.files[index_name].uncompressed_hash = index_source_hashing_stream->getHash();
        }

        checksums.files[index_name].file_size = index_file_hashing_stream->count();
        checksums.files[index_name].file_hash = index_file_hashing_stream->getHash();

        index_file_stream->preFinalize();
    }
}

void MergeTreeDataPartWriterOnDisk::finishPrimaryIndexSerialization(bool sync)
{
    if (index_file_hashing_stream)
    {
        index_file_stream->finalize();
        if (sync)
            index_file_stream->sync();

        if (compress_primary_key)
        {
            index_source_hashing_stream = nullptr;
            index_compressor_stream = nullptr;
        }

        index_file_hashing_stream = nullptr;
    }
}

void MergeTreeDataPartWriterOnDisk::fillSkipIndicesChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < skip_indices.size(); ++i)
    {
        auto & stream = *skip_indices_streams[i];
        if (!skip_indices_aggregators[i]->empty())
            skip_indices_aggregators[i]->getGranuleAndReset()->serializeBinary(stream.compressed_hashing);

        /// Register additional files written only by the full-text index. Required because otherwise DROP TABLE complains about unknown
        /// files. Note that the provided actual checksums are bogus. The problem is that at this point the file writes happened already and
        /// we'd need to re-open + hash the files (fixing this is TODO). For now, CHECK TABLE skips these four files.
        if (typeid_cast<const MergeTreeIndexGin *>(&*skip_indices[i]) != nullptr)
        {
            String filename_without_extension = skip_indices[i]->getFileName();
            checksums.files[filename_without_extension + ".gin_dict"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_post"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_seg"] = MergeTreeDataPartChecksums::Checksum();
            checksums.files[filename_without_extension + ".gin_sid"] = MergeTreeDataPartChecksums::Checksum();
        }
    }

    for (auto & stream : skip_indices_streams)
    {
        stream->preFinalize();
        stream->addToChecksums(checksums);
    }
}

void MergeTreeDataPartWriterOnDisk::finishStatisticsSerialization(bool sync)
{
    for (auto & stream : stats_streams)
    {
        stream->finalize();
        if (sync)
            stream->sync();
    }

    for (size_t i = 0; i < stats.size(); ++i)
        LOG_DEBUG(log, "Spent {} ms calculating statistics {} for the part {}", execution_stats.statistics_build_us[i] / 1000, stats[i]->columnName(), data_part_name);
}

void MergeTreeDataPartWriterOnDisk::fillStatisticsChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    for (size_t i = 0; i < stats.size(); i++)
    {
        auto & stream = *stats_streams[i];
        stats[i]->serialize(stream.compressed_hashing);
        stream.preFinalize();
        stream.addToChecksums(checksums);
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

    for (auto & store: gin_index_stores)
        store.second->finalize();

    for (size_t i = 0; i < skip_indices.size(); ++i)
        LOG_DEBUG(log, "Spent {} ms calculating index {} for the part {}", execution_stats.skip_indices_build_us[i] / 1000, skip_indices[i]->index.name, data_part_name);

    gin_index_stores.clear();
    skip_indices_streams.clear();
    skip_indices_aggregators.clear();
    skip_index_accumulated_marks.clear();
}

void MergeTreeDataPartWriterOnDisk::calculateAndSerializeUniqueData(const Block & unique_key_version_block, const Granules & granules_to_write)
{
    if (unique_key_version_block.columns() < 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "UniqueMergeTree must have uniq key.");

    size_t uk_rows = unique_key_version_block.rows();

    Stopwatch stopwatch;

    const auto & unique_key_names = metadata_snapshot->unique_key.column_names;
    const auto & version_name = merging_params.version_column;
    const auto & version_column = unique_key_version_block.getByName(version_name).column;

    auto combine_key_column = DataTypeString{}.createColumn();

    size_t last_row_count = 0;

    auto unique_key_index_type = storage_settings->unique_key_index_type;
    if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
    {
        ColumnRawPtrs unique_key_columns;
        for (const auto & unique_key_name : unique_key_names)
            unique_key_columns.emplace_back(unique_key_version_block.getByName(unique_key_name).column.get());

        Arena pool;
        for (size_t i = 0; i < uk_rows; ++i)
        {
            auto encoded_value = serializeKeysToPoolContiguous(i, unique_key_names.size(), unique_key_columns, pool);
            combine_key_column->insertData(encoded_value.data, encoded_value.size);
        }

        FieldRef unique_key_min_field;
        FieldRef unique_key_max_field;
        combine_key_column->getExtremes(unique_key_min_field, unique_key_max_field);
        String unique_key_min_value = unique_key_min_field.safeGet<String>();
        String unique_key_max_value = unique_key_max_field.safeGet<String>();

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
            if (unique_key_minmax_index)
                unique_key_minmax_index->setMinMax(unique_key_min_value, unique_key_max_value);

            if (storage_settings->enable_unique_key_bucket)
            {
                unique_key_bucket_index->init(storage_settings->unique_key_bucket_size, index_granularity.getTotalRows());
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
                    tmp_rocksdb_index_dir = fs::path(data_part_storage->getFullPath() + UniqueEngineDataWriter::TEMP_MERGING_STAGE_DIR_SUFFIX);
                    rocksdb::DB * db;
                    auto status = rocksdb::DB::Open(opts, tmp_rocksdb_index_dir, &db);
                    tmp_rocksdb_index_writer = std::unique_ptr<rocksdb::DB>(db);
                    if (!status.ok())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't create temp unique key: {}", status.ToString());
                }
                else
                {
                    IndexFile::Options options;
                    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
                    leveldb_index_writer = std::make_unique<IndexFile::IndexFileWriter>(options);
                    String index_path = fs::path(data_part_storage->getFullPath()) / UNIQUE_ENGINE_KEY_INDEX;
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique key index.", unique_key_index_type);

    rows_count += uk_rows;

    LOG_DEBUG(getLogger("UniqueMergeTreeIndex"), "part {} calculateUniqueData cost {} ms", data_part_name, stopwatch.elapsedMilliseconds());
}

void MergeTreeDataPartWriterOnDisk::fillUniqueDataChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    if (merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        Stopwatch stopwatch;

        auto unique_key_index_type = storage_settings->unique_key_index_type;
        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
        {
            if (!unique_key_bucket_index_hashing_stream)
                unique_key_index->serializeBinary(*unique_key_index_hashing_stream, nullptr);
            else
            {
                unique_key_index->serializeBinary(*unique_key_index_hashing_stream, unique_key_bucket_index);
                unique_key_bucket_index->serializeBinary(*unique_key_bucket_index_hashing_stream);

                unique_key_bucket_index_hashing_stream->finalize();
                checksums.files[UNIQUE_ENGINE_KEY_BUCKET_INDEX].file_size = unique_key_bucket_index_hashing_stream->count();
                checksums.files[UNIQUE_ENGINE_KEY_BUCKET_INDEX].file_hash = unique_key_bucket_index_hashing_stream->getHash();
                unique_key_bucket_index_file_stream->preFinalize();
            }

            unique_key_index_hashing_stream->finalize();
            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_size = unique_key_index_hashing_stream->count();
            checksums.files[UNIQUE_ENGINE_KEY_INDEX].file_hash = unique_key_index_hashing_stream->getHash();
            unique_key_index_file_stream->preFinalize();

            unique_key_minmax_index->serializeBinary(*unique_key_minmax_index_hashing_stream);
            unique_key_minmax_index_hashing_stream->finalize();
            checksums.files[UNIQUE_ENGINE_KEY_MINMAX_INDEX].file_size = unique_key_minmax_index_hashing_stream->count();
            checksums.files[UNIQUE_ENGINE_KEY_MINMAX_INDEX].file_hash = unique_key_minmax_index_hashing_stream->getHash();
            unique_key_minmax_index_file_stream->preFinalize();
        }
        else if (IUniqueKeyIndex::isLevelDBUniqueKeyIndex(unique_key_index_type))
        {
            IndexFile::IndexFileInfo file_info;

            if (!tmp_rocksdb_index_writer && !leveldb_index_writer)
            {
                bool rowid_is_uinit32 = storage_settings->unique_delete_bitmap_type == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;
                unique_key_index->serializeBinary(
                    fs::path(data_part_storage->getFullPath()) / UNIQUE_ENGINE_KEY_INDEX,
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
                        fs::path(data_part_storage->getFullPath()) / UNIQUE_ENGINE_KEY_INDEX,
                        file_info,
                        tmp_rocksdb_index_dir,
                        tmp_rocksdb_index_writer);

                    const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(getDataPartStorage());
                    auto disk = data_part_storage.volume->getDisk();
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
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique key index.", unique_key_index_type);

        unique_delete_bitmap->serializeBinary(*unique_delete_bitmap_file_stream);
        unique_delete_bitmap_file_stream->preFinalize();

        LOG_DEBUG(
            getLogger("UniqueMergeTreeIndex"),
            "part {} fillUniqueDataChecksums cost {} ms",
            data_part_name,
            stopwatch.elapsedMilliseconds());
    }
}

void MergeTreeDataPartWriterOnDisk::finishUniqueDataSerialization(bool sync)
{
    if (merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        Stopwatch stopwatch;
        if (IUniqueKeyIndex::isMapUniqueKeyIndex(storage_settings->unique_key_index_type))
        {
            if (unique_key_index_hashing_stream)
            {
                unique_key_index_file_stream->finalize();
                if (sync)
                    unique_key_index_file_stream->sync();
                unique_key_index_hashing_stream = nullptr;
            }

            if (unique_key_bucket_index_hashing_stream)
            {
                unique_key_bucket_index_file_stream->finalize();
                if (sync)
                    unique_key_bucket_index_file_stream->sync();
                unique_key_bucket_index_hashing_stream = nullptr;
            }

            if (unique_key_minmax_index_hashing_stream)
            {
                unique_key_minmax_index_file_stream->finalize();
                if (sync)
                    unique_key_minmax_index_file_stream->sync();
                unique_key_minmax_index_hashing_stream = nullptr;
            }
        }

        if (unique_delete_bitmap_file_stream)
        {
            unique_delete_bitmap_file_stream->finalize();
            if (sync)
                unique_delete_bitmap_file_stream->sync();
        }

        LOG_DEBUG(
            getLogger("UniqueMergeTreeIndex"),
            "part {} finishUniqueDataSerialization cost {} ms",
            data_part_name,
            stopwatch.elapsedMilliseconds());
    }
}

void MergeTreeDataPartWriterOnDisk::writeToUniqueKeyIndex(Block & block)
{
    size_t rows = block.rows();
    if (rows == 0)
        return;

    bool rowid_is_uinit32 = storage_settings->unique_delete_bitmap_type == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;

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

std::optional<UniqueEngineData> MergeTreeDataPartWriterOnDisk::getUniqueEngineData() const
{
    UniqueEngineData unique_data;

    /// the effective_rows of merge part may be equal to zero.
    /// if effective_rows = 0, set the empty unique key index, unique_delete_bitmap and unique_key_bucket_index to avoid null pointer.
    unique_data.unique_delete_bitmap = unique_delete_bitmap;
    auto unique_key_index_type = storage_settings->unique_key_index_type;
    if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
    {
        unique_data.unique_key_index = unique_key_index;
        unique_data.unique_key_minmax_index = unique_key_minmax_index;

        if (storage_settings->enable_unique_key_bucket)
            unique_data.unique_key_bucket_index = unique_key_bucket_index;
    }

    return unique_data;
}

void MergeTreeDataPartWriterOnDisk::initUniqueIndex()
{
    if (metadata_snapshot->hasUniqueKey())
    {
        auto unique_key_index_type = storage_settings->unique_key_index_type;

        unique_key_index = IMergeTreeDataPart::createUniqueIndex(unique_key_index_type);
        unique_delete_bitmap = IMergeTreeDataPart::createUniqueDeleteBitmap(storage_settings->unique_delete_bitmap_type);

        if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
        {
            unique_key_minmax_index = std::make_shared<UniqueKeyMinMaxIndex>();
            unique_key_index_file_stream = getDataPartStorage().writeFile(UNIQUE_ENGINE_KEY_INDEX, DBMS_DEFAULT_BUFFER_SIZE, {});
            unique_key_index_hashing_stream = std::make_unique<HashingWriteBuffer>(*unique_key_index_file_stream);

            if (storage_settings->enable_unique_key_bucket)
            {
                unique_key_bucket_index = std::make_shared<UniqueKeyBucketIndex>();
                unique_key_bucket_index_file_stream
                    = getDataPartStorage().writeFile(UNIQUE_ENGINE_KEY_BUCKET_INDEX, DBMS_DEFAULT_BUFFER_SIZE, {});
                unique_key_bucket_index_hashing_stream = std::make_unique<HashingWriteBuffer>(*unique_key_bucket_index_file_stream);
            }

            unique_key_minmax_index_file_stream
                = getDataPartStorage().writeFile(UNIQUE_ENGINE_KEY_MINMAX_INDEX, DBMS_DEFAULT_BUFFER_SIZE, {});
            unique_key_minmax_index_hashing_stream = std::make_unique<HashingWriteBuffer>(*unique_key_minmax_index_file_stream);
        }

        unique_delete_bitmap_file_stream = getDataPartStorage().writeFile(UNIQUE_ENGINE_DELETE_BITMAP, DBMS_DEFAULT_BUFFER_SIZE, {});
    }
}

Names MergeTreeDataPartWriterOnDisk::getSkipIndicesColumns() const
{
    std::unordered_set<String> skip_indexes_column_names_set;
    for (const auto & index : skip_indices)
        std::copy(index->index.column_names.cbegin(), index->index.column_names.cend(),
                  std::inserter(skip_indexes_column_names_set, skip_indexes_column_names_set.end()));
    return Names(skip_indexes_column_names_set.begin(), skip_indexes_column_names_set.end());
}

void MergeTreeDataPartWriterOnDisk::initOrAdjustDynamicStructureIfNeeded(Block & block)
{
    if (!is_dynamic_streams_initialized)
    {
        for (const auto & column : columns_list)
        {
            if (column.type->hasDynamicSubcolumns())
            {
                /// Create all streams for dynamic subcolumns using dynamic structure from block.
                auto compression = getCodecDescOrDefault(column.name, default_codec);
                addStreams(column, block.getByName(column.name).column, compression);
            }
        }
        is_dynamic_streams_initialized = true;
        block_sample = block.cloneEmpty();
    }
    else
    {
        size_t size = block.columns();
        for (size_t i = 0; i != size; ++i)
        {
            auto & column = block.getByPosition(i);
            const auto & sample_column = block_sample.getByPosition(i);
            /// Check if the dynamic structure of this column is different from the sample column.
            if (column.type->hasDynamicSubcolumns() && !column.column->dynamicStructureEquals(*sample_column.column))
            {
                /// We need to change the dynamic structure of the column so it matches the sample column.
                /// To do it, we create empty column of this type, take dynamic structure from sample column
                /// and insert data into it. Resulting column will have required dynamic structure and the content
                /// of the column in current block.
                auto new_column = sample_column.type->createColumn();
                new_column->takeDynamicStructureFromSourceColumns({sample_column.column});
                new_column->insertRangeFrom(*column.column, 0, column.column->size());
                column.column = std::move(new_column);
            }
        }
    }
}

template struct MergeTreeDataPartWriterOnDisk::Stream<false>;
template struct MergeTreeDataPartWriterOnDisk::Stream<true>;

}
