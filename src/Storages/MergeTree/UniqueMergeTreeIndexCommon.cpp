#include <Storages/MergeTree/UniqueMergeTreeIndexCommon.h>


namespace DB
{
void DeletedKeys::serializeBinary(WriteBuffer & ostr, const bool & is_write_binary) const
{
    size_t map_size = size();
    if (!map_size)
        return;

    if (!is_write_binary)
    {
        DB::writeBinary(map_size, ostr);

        for (const auto & it : *this)
        {
            writeStringBinary(it.first, ostr);
            writeVarUInt(it.second, ostr);
        }
    }
    else
    {
        writeBinary(map_size, ostr);

        for (const auto & it : *this)
        {
            writeBinary(it.first, ostr);
            writeBinary(it.second, ostr);
        }
    }
}

void DeletedKeys::deserializeBinary(ReadBuffer & istr, const bool & is_read_binary)
{
    size_t size;

    if (!is_read_binary)
    {
        DB::readBinary(size, istr);

        for (size_t index = 0; index < size; ++index)
        {
            String key;
            readStringBinary(key, istr);
            UInt64 version;
            readVarUInt(version, istr);
            insert(std::make_pair(key, version));
        }
    }
    else
    {
        readBinary(size, istr);

        for (size_t index = 0; index < size; ++index)
        {
            String key;
            readBinary(key, istr);
            UInt64 version;
            readBinary(version, istr);
            insert(std::make_pair(key, version));
        }
    }
}

Roaring64UniqueDeleteBitmap::Roaring64UniqueDeleteBitmap(Roaring64UniqueDeleteBitmap & other)
{
    rb = other.rb;
}

void Roaring64UniqueDeleteBitmap::deleteRow(size_t pos)
{
    rb.add(static_cast<uint64_t>(pos));
}

bool Roaring64UniqueDeleteBitmap::isDeleted(size_t pos) const
{
    return rb.contains(static_cast<uint64_t>(pos));
}

size_t Roaring64UniqueDeleteBitmap::deleteRowsSize()
{
    return rb.cardinality();
}

void Roaring64UniqueDeleteBitmap::serializeBinary(WriteBuffer & ostr) const
{
    auto size = rb.getSizeInBytes();
    writeVarUInt(size, ostr);
    std::unique_ptr<char[]> buf(new char[size]);
    rb.write(buf.get());
    ostr.write(buf.get(), size);
}

void Roaring64UniqueDeleteBitmap::deserializeBinary(ReadBuffer & istr)
{
    size_t size;
    readVarUInt(size, istr);
    std::unique_ptr<char[]> buf(new char[size]);
    istr.readStrict(buf.get(), size);
    rb = roaring::Roaring64Map::read(buf.get());
}

Roaring32UniqueDeleteBitmap::Roaring32UniqueDeleteBitmap(Roaring32UniqueDeleteBitmap & other)
{
    rb = other.rb;
}

void Roaring32UniqueDeleteBitmap::deleteRow(size_t pos)
{
    rb.add(static_cast<uint32_t>(pos));
}

bool Roaring32UniqueDeleteBitmap::isDeleted(size_t pos) const
{
    return rb.contains(static_cast<uint32_t>(pos));
}

size_t Roaring32UniqueDeleteBitmap::deleteRowsSize()
{
    return rb.cardinality();
}

void Roaring32UniqueDeleteBitmap::serializeBinary(WriteBuffer & ostr) const
{
    auto size = rb.getSizeInBytes();
    writeVarUInt(size, ostr);
    std::unique_ptr<char[]> buf(new char[size]);
    rb.write(buf.get());
    ostr.write(buf.get(), size);
}

void Roaring32UniqueDeleteBitmap::deserializeBinary(ReadBuffer & istr)
{
    size_t size;
    readVarUInt(size, istr);
    std::unique_ptr<char[]> buf(new char[size]);
    istr.readStrict(buf.get(), size);
    rb = roaring::Roaring::read(buf.get());
}

void UniqueKeyMinMaxIndex::setMinMax(const String & min_, const String & max_)
{
    min = min_;
    max = max_;
}

String UniqueKeyMinMaxIndex::getMin() const
{
    return min;
}

String UniqueKeyMinMaxIndex::getMax() const
{
    return max;
}

void UniqueKeyMinMaxIndex::serializeBinary(WriteBuffer & ostr) const
{
    writeStringBinary(min, ostr);
    writeStringBinary(max, ostr);
}

void UniqueKeyMinMaxIndex::deserializeBinary(ReadBuffer & istr)
{
    readStringBinary(min, istr);
    readStringBinary(max, istr);
}

void UniqueKeyBucketIndex::add(const UniqueKeyBucketInfo & bucket_info)
{
    bucket_infos.emplace_back(bucket_info);
}

void UniqueKeyBucketIndex::init(const size_t & bucket_size_, const size_t & rows_count_)
{
    bucket_size = bucket_size_;
    bucket_num = std::ceil(rows_count_ * 1.0 / bucket_size);
    LOG_DEBUG(
        &Poco::Logger::get("UniqueMergeTreeIndex"),
        "bucket bucket_size {} rows_count {} bucket_num {}",
        bucket_size,
        rows_count_,
        bucket_num);
}

size_t UniqueKeyBucketIndex::getBucketSize() const
{
    return bucket_size;
}

size_t UniqueKeyBucketIndex::getBucketNum() const
{
    return bucket_num;
}

UniqueKeyBucketInfos UniqueKeyBucketIndex::getBucketInfos() const
{
    return bucket_infos;
}

void UniqueKeyBucketIndex::serializeBinary(WriteBuffer & ostr) const
{
    writeVarUInt(bucket_size, ostr);
    writeVarUInt(bucket_num, ostr);

    for (const auto & item : bucket_infos)
    {
        const size_t & offset_in_file = item.getOffsetInFile();
        writeVarUInt(offset_in_file, ostr);
        const size_t & rows = item.getRows();
        writeVarUInt(rows, ostr);
    }
}

void UniqueKeyBucketIndex::deserializeBinary(ReadBuffer & istr)
{
    readVarUInt(bucket_size, istr);
    readVarUInt(bucket_num, istr);

    for (size_t i = 0; i < bucket_num; i++)
    {
        size_t offset_in_file;
        readVarUInt(offset_in_file, istr);
        size_t rows;
        readVarUInt(rows, istr);

        bucket_infos.emplace_back(UniqueKeyBucketInfo(offset_in_file, rows));
    }
}

void StringHashMapUniqueKeyIndex::initBucket(const size_t & bucket_num_)
{
    bucketing_enabled = true;
    bucket_num = bucket_num_;
    unique_key_index_bucket.resize(bucket_num_);
}

void StringHashMapUniqueKeyIndex::add(const String & key, const VersionAndRow & value)
{
    if (!bucketing_enabled)
        unique_key_index[key] = value;
    else
    {
        size_t bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;

        if (!unique_key_index_bucket[bucket_idx])
            unique_key_index_bucket[bucket_idx] = std::make_shared<UniqueKeyIndexMapType>();

        (*unique_key_index_bucket[bucket_idx])[key] = value;
    }
}

bool StringHashMapUniqueKeyIndex::empty() const
{
    if (!bucketing_enabled)
        return unique_key_index.empty();
    else
        return unique_key_index_bucket.empty();
}

size_t StringHashMapUniqueKeyIndex::size() const
{
    if (!bucketing_enabled)
        return unique_key_index.size();
    else
    {
        size_t total_size = 0;
        for (const auto & unique_key_index_bucket_item : unique_key_index_bucket)
            total_size += unique_key_index_bucket_item->size();

        return total_size;
    }
}

void StringHashMapUniqueKeyIndex::forEach(std::function<void(const StringRef &, const VersionAndRow &)> func)
{
    if (!bucketing_enabled)
    {
        unique_key_index.forEachValue(func);
    }
    else
    {
        for (auto & unique_key_index_bucket_item : unique_key_index_bucket)
            unique_key_index_bucket_item->forEachValue(func);
    }
}

std::optional<VersionAndRow> StringHashMapUniqueKeyIndex::get(const String & key) const
{
    if (!bucketing_enabled)
    {
        auto lookup_result = unique_key_index.find(key);
        if (lookup_result)
            return lookup_result->getMapped();
    }
    else
    {
        size_t bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;

        const auto & unique_key_index_bucket_item = unique_key_index_bucket[bucket_idx];
        auto lookup_result = unique_key_index_bucket_item->find(key);
        if (lookup_result)
            return lookup_result->getMapped();
    }

    return {};
}

std::vector<size_t> StringHashMapUniqueKeyIndex::calculateTargetBuckets(const size_t & mod_bucket_num) const
{
    std::vector<size_t> bucket_index_range;

    if (!bucketing_enabled)
    {
        unique_key_index.forEachValue(
            [&](const StringRef & key, const VersionAndRow & /*mapped*/)
            {
                size_t index_mod = CityHash_v1_0_2::CityHash64(key.data, key.size) % mod_bucket_num;
                if (bucket_index_range.empty()
                    || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                    bucket_index_range.emplace_back(index_mod);
            });
    }
    else
    {
        for (size_t index = 0; index < bucket_num; index++)
        {
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];
            unique_key_index_bucket_item->forEachValue(
                [&](const StringRef & key, const VersionAndRow & /*mapped*/)
                {
                    size_t index_mod = CityHash_v1_0_2::CityHash64(key.data, key.size) % mod_bucket_num;
                    if (bucket_index_range.empty()
                        || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                        bucket_index_range.emplace_back(index_mod);
                });
        }
    }

    return bucket_index_range;
}

void StringHashMapUniqueKeyIndex::serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const
{
    if (!bucketing_enabled)
    {
        size_t size = unique_key_index.size();
        DB::writeBinary(size, ostr);

        unique_key_index.forEachValue(
            [&](const StringRef & key, const VersionAndRow & mapped)
            {
                writeStringBinary(key, ostr);
                writeVarUInt(std::get<0>(mapped), ostr);
                writeVarUInt(std::get<1>(mapped), ostr);
            });
    }
    else
    {
        /// index serialize
        for (size_t index = 0; index < bucket_num; index++)
        {
            const size_t & offset_in_file = ostr.count();
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];

            unique_key_index_bucket_item->forEachValue(
                [&](const StringRef & key, const VersionAndRow & mapped)
                {
                    writeStringBinary(key, ostr);
                    writeVarUInt(std::get<0>(mapped), ostr);
                    writeVarUInt(std::get<1>(mapped), ostr);
                });
            const size_t & rows = unique_key_index_bucket_item->size();
            bucket_index->add(UniqueKeyBucketInfo(offset_in_file, rows));
        }
    }
}

void StringHashMapUniqueKeyIndex::deserializeBinary(
    const DiskPtr & disk,
    const String & index_path,
    UniqueKeyBucketIndexPtr bucket_index,
    LoadingBucketPoolPtr loading_bucket_pool,
    BucketIndexRangePtr bucket_range,
    size_t max_running_loading_task,
    size_t timeout_in_sec)
{
    if (!bucketing_enabled)
    {
        auto istr = disk->readFile(index_path);
        size_t size;
        DB::readBinary(size, *istr);

        for (size_t index = 0; index < size; ++index)
        {
            String key;
            readStringBinary(key, *istr);
            UInt64 version;
            readVarUInt(version, *istr);
            size_t row_num;
            readVarUInt(row_num, *istr);
            unique_key_index[key] = std::make_tuple(version, row_num);
        }
    }
    else
    {
        if (!bucket_num)
            return;

        if (bucket_range)
            LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeBucketIndex"), "will load {}/{} bucket index", bucket_range->size(), bucket_num);

        const auto & bucket_infos = bucket_index->getBucketInfos();
        CountDownLatch count_down_latch(bucket_range ? bucket_range->size() : bucket_num);
        for (size_t i = 0; i < bucket_num; i++)
        {
            if (bucket_range)
            {
                if (std::find(bucket_range->begin(), bucket_range->end(), i) == bucket_range->end())
                    continue;
            }

            const auto & bucket_info = bucket_infos[i];
            const size_t & offset_in_file = bucket_info.getOffsetInFile();
            const size_t & rows = bucket_info.getRows();

            size_t waited_secs = 0;
            bool schedule_flag = false;
            size_t busy_threads_in_pool = 0;
            while (waited_secs <= timeout_in_sec)
            {
                busy_threads_in_pool
                    = CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask].load(std::memory_order_relaxed);
                if (busy_threads_in_pool >= max_running_loading_task)
                {
                    sleepForSeconds(1);
                    ++waited_secs;
                }
                else
                {
                    loading_bucket_pool->scheduleOrThrowOnError(
                        [&, this, i, offset_in_file, rows, thread_group = CurrentThread::getGroup()]
                        {
                            setThreadName("readUniqueKeyFromBucket");
                            CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]++;

                            SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                            if (thread_group)
                                CurrentThread::attachTo(thread_group);

                            try
                            {
                                auto index_istr = disk->readFile(index_path);
                                index_istr->seek(offset_in_file, SEEK_SET);
                                UniqueKeyIndexMapType unique_key_version_tmp;
                                for (size_t rows_idx = 0; rows_idx < rows; rows_idx++)
                                {
                                    String key;
                                    readStringBinary(key, *index_istr);
                                    UInt64 version;
                                    readVarUInt(version, *index_istr);
                                    size_t row_num;
                                    readVarUInt(row_num, *index_istr);
                                    unique_key_version_tmp[key] = VersionAndRow(version, row_num);
                                }

                                {
                                    std::lock_guard<std::mutex> lck(bucket_load_mutex);
                                    UniqueKeyIndexMapTypePtr unique_key_version_tmp_ptr
                                        = std::make_shared<UniqueKeyIndexMapType>(std::move(unique_key_version_tmp));
                                    unique_key_index_bucket[i] = unique_key_version_tmp_ptr;
                                }

                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                            }
                            catch (std::exception const & ex)
                            {
                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                                throw ex;
                            }
                        });
                    schedule_flag = true;
                    break;
                }
            }

            if (!schedule_flag)
                throw Exception(
                    "Timeout while scheduling loading buckets of unique key index, timeout is " + std::to_string(timeout_in_sec)
                        + ", and BackgroundUniqueEngineLoadTask is " + std::to_string(busy_threads_in_pool),
                    ErrorCodes::TIMEOUT_EXCEEDED);
        }

        count_down_latch.await();
    }
}

void StandardMapUniqueKeyIndex::initBucket(const size_t & bucket_num_)
{
    bucketing_enabled = true;
    bucket_num = bucket_num_;
    unique_key_index_bucket.resize(bucket_num_);
}

void StandardMapUniqueKeyIndex::add(const String & key, const VersionAndRow & value)
{
    if (!bucketing_enabled)
        unique_key_index[key] = value;
    else
    {
        int bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;
        unique_key_index_bucket[bucket_idx][key] = value;
    }
}

bool StandardMapUniqueKeyIndex::empty() const
{
    if (!bucketing_enabled)
        return unique_key_index.empty();
    else
        return unique_key_index_bucket.empty();
}

size_t StandardMapUniqueKeyIndex::size() const
{
    if (!bucketing_enabled)
        return unique_key_index.size();
    else
    {
        size_t total_size = 0;
        for (const auto & unique_key_index_bucket_item : unique_key_index_bucket)
            total_size += unique_key_index_bucket_item.size();

        return total_size;
    }
}

void StandardMapUniqueKeyIndex::forEach(std::function<void(const StringRef &, const VersionAndRow &)> func)
{
    if (!bucketing_enabled)
    {
        for (const auto & pair : unique_key_index)
            func(pair.first, pair.second);
    }
    else
    {
        for (const auto & unique_key_index_bucket_item : unique_key_index_bucket)
            for (const auto & pair : unique_key_index_bucket_item)
                func(pair.first, pair.second);
    }
}

std::optional<VersionAndRow> StandardMapUniqueKeyIndex::get(const String & key) const
{
    if (!bucketing_enabled)
    {
        const auto lookup_result = unique_key_index.find(key);
        if (lookup_result != unique_key_index.end())
            return lookup_result->second;
    }
    else
    {
        int bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;

        const auto & unique_key_index_bucket_item = unique_key_index_bucket[bucket_idx];
        const auto lookup_result = unique_key_index_bucket_item.find(key);
        if (lookup_result != unique_key_index_bucket_item.end())
            return lookup_result->second;
    }

    return {};
}

std::vector<size_t> StandardMapUniqueKeyIndex::calculateTargetBuckets(const size_t & mod_bucket_num) const
{
    std::vector<size_t> bucket_index_range;

    if (!bucketing_enabled)
    {
        for (const auto & pair : unique_key_index)
        {
            const String & key = pair.first;
            size_t index_mod = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % mod_bucket_num;
            if (bucket_index_range.empty()
                || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                bucket_index_range.emplace_back(index_mod);
        }
    }
    else
    {
        for (size_t index = 0; index < bucket_num; index++)
        {
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];
            for (const auto & unique_key_index_item : unique_key_index_bucket_item)
            {
                const String & key = unique_key_index_item.first;
                size_t index_mod = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % mod_bucket_num;
                if (bucket_index_range.empty()
                    || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                    bucket_index_range.emplace_back(index_mod);
            }
        }
    }

    return bucket_index_range;
}

void StandardMapUniqueKeyIndex::serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const
{
    if (!bucketing_enabled)
    {
        size_t size = unique_key_index.size();

        DB::writeBinary(size, ostr);

        for (const auto & unique_key_index_item : unique_key_index)
        {
            writeStringBinary(unique_key_index_item.first, ostr);
            writeVarUInt(std::get<0>(unique_key_index_item.second), ostr);
            writeVarUInt(std::get<1>(unique_key_index_item.second), ostr);
        }
    }
    else
    {
        /// index serialize
        for (size_t index = 0; index < bucket_num; index++)
        {
            const size_t & offset_in_file = ostr.count();
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];

            for (const auto & unique_key_index_item : unique_key_index_bucket_item)
            {
                writeStringBinary(unique_key_index_item.first, ostr);
                writeVarUInt(std::get<0>(unique_key_index_item.second), ostr);
                writeVarUInt(std::get<1>(unique_key_index_item.second), ostr);
            }

            const size_t & rows = unique_key_index_bucket_item.size();
            bucket_index->add(UniqueKeyBucketInfo(offset_in_file, rows));
        }
    }
}

void StandardMapUniqueKeyIndex::deserializeBinary(
    const DiskPtr & disk,
    const String & index_path,
    UniqueKeyBucketIndexPtr bucket_index,
    LoadingBucketPoolPtr loading_bucket_pool,
    BucketIndexRangePtr bucket_range,
    size_t max_running_loading_task,
    size_t timeout_in_sec)
{
    if (!bucketing_enabled)
    {
        auto istr = disk->readFile(index_path);
        size_t size;
        DB::readBinary(size, *istr);

        for (size_t index = 0; index < size; ++index)
        {
            String key;
            readStringBinary(key, *istr);
            UInt64 version;
            readVarUInt(version, *istr);
            size_t row_num;
            readVarUInt(row_num, *istr);
            unique_key_index[key] = std::make_tuple(version, row_num);
        }
    }
    else
    {
        if (!bucket_num)
            return;

        if (bucket_range)
            LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeBucketIndex"), "will load {}/{} bucket index", bucket_range->size(), bucket_num);

        const auto & bucket_infos = bucket_index->getBucketInfos();
        CountDownLatch count_down_latch(bucket_range ? bucket_range->size() : bucket_num);
        for (size_t i = 0; i < bucket_num; i++)
        {
            if (bucket_range)
            {
                if (std::find(bucket_range->begin(), bucket_range->end(), i) == bucket_range->end())
                    continue;
            }

            const auto & bucket_info = bucket_infos[i];
            const size_t & offset_in_file = bucket_info.getOffsetInFile();
            const size_t & rows = bucket_info.getRows();

            size_t waited_secs = 0;
            bool schedule_flag = false;
            size_t busy_threads_in_pool = 0;
            while (waited_secs <= timeout_in_sec)
            {
                busy_threads_in_pool
                    = CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask].load(std::memory_order_relaxed);
                if (busy_threads_in_pool >= max_running_loading_task)
                {
                    sleepForSeconds(1);
                    ++waited_secs;
                }
                else
                {
                    loading_bucket_pool->scheduleOrThrowOnError(
                        [&, this, i, offset_in_file, rows, thread_group = CurrentThread::getGroup()]
                        {
                            setThreadName("readUniqueKeyFromBucket");
                            CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]++;

                            SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                            if (thread_group)
                                CurrentThread::attachTo(thread_group);

                            try
                            {
                                auto index_istr = disk->readFile(index_path);
                                index_istr->seek(offset_in_file, SEEK_SET);
                                UniqueKeyIndexMapType unique_key_version_tmp;
                                for (size_t rows_idx = 0; rows_idx < rows; rows_idx++)
                                {
                                    String key;
                                    readStringBinary(key, *index_istr);
                                    UInt64 version;
                                    readVarUInt(version, *index_istr);
                                    size_t row_num;
                                    readVarUInt(row_num, *index_istr);
                                    unique_key_version_tmp.emplace(key, VersionAndRow(version, row_num));
                                }

                                {
                                    std::lock_guard<std::mutex> lck(bucket_load_mutex);
                                    unique_key_index_bucket[i] = std::move(unique_key_version_tmp);
                                }

                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                            }
                            catch (std::exception const & ex)
                            {
                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                                throw ex;
                            }
                        });
                    schedule_flag = true;
                    break;
                }
            }

            if (!schedule_flag)
                throw Exception(
                    "Timeout while scheduling loading buckets of unique key index, timeout is " + std::to_string(timeout_in_sec)
                        + ", and BackgroundUniqueEngineLoadTask is " + std::to_string(busy_threads_in_pool),
                    ErrorCodes::TIMEOUT_EXCEEDED);
        }

        count_down_latch.await();
    }
}

void StandardUnOrderedMapUniqueKeyIndex::initBucket(const size_t & bucket_num_)
{
    bucketing_enabled = true;
    bucket_num = bucket_num_;
    unique_key_index_bucket.resize(bucket_num_);
}

void StandardUnOrderedMapUniqueKeyIndex::add(const String & key, const VersionAndRow & value)
{
    if (!bucketing_enabled)
        unique_key_index[key] = value;
    else
    {
        size_t bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;
        unique_key_index_bucket[bucket_idx][key] = value;
    }
}

bool StandardUnOrderedMapUniqueKeyIndex::empty() const
{
    if (!bucketing_enabled)
        return unique_key_index.empty();
    else
        return unique_key_index_bucket.empty();
}

size_t StandardUnOrderedMapUniqueKeyIndex::size() const
{
    if (!bucketing_enabled)
        return unique_key_index.size();
    else
    {
        size_t total_size = 0;
        for (const auto & unique_key_index_bucket_item : unique_key_index_bucket)
            total_size += unique_key_index_bucket_item.size();

        return total_size;
    }
}

void StandardUnOrderedMapUniqueKeyIndex::forEach(std::function<void(const StringRef &, const VersionAndRow &)> func)
{
    if (!bucketing_enabled)
    {
        for (const auto & pair : unique_key_index)
            func(pair.first, pair.second);
    }
    else
    {
        for (const auto & unique_key_index_bucket_item : unique_key_index_bucket)
            for (const auto & pair : unique_key_index_bucket_item)
                func(pair.first, pair.second);
    }
}

std::optional<VersionAndRow> StandardUnOrderedMapUniqueKeyIndex::get(const String & key) const
{
    if (!bucketing_enabled)
    {
        const auto lookup_result = unique_key_index.find(key);
        if (lookup_result != unique_key_index.end())
            return lookup_result->second;
    }
    else
    {
        int bucket_idx = 0;
        if (bucket_num > 1)
            bucket_idx = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % bucket_num;

        const auto & unique_key_index_bucket_item = unique_key_index_bucket[bucket_idx];
        const auto lookup_result = unique_key_index_bucket_item.find(key);
        if (lookup_result != unique_key_index_bucket_item.end())
            return lookup_result->second;
    }

    return {};
}

std::vector<size_t> StandardUnOrderedMapUniqueKeyIndex::calculateTargetBuckets(const size_t & mod_bucket_num) const
{
    std::vector<size_t> bucket_index_range;

    if (!bucketing_enabled)
    {
        for (const auto & pair : unique_key_index)
        {
            const String & key = pair.first;
            size_t index_mod = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % mod_bucket_num;
            if (bucket_index_range.empty()
                || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                bucket_index_range.emplace_back(index_mod);
        }
    }
    else
    {
        for (size_t index = 0; index < bucket_num; index++)
        {
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];
            for (const auto & unique_key_index_item : unique_key_index_bucket_item)
            {
                const String & key = unique_key_index_item.first;
                size_t index_mod = CityHash_v1_0_2::CityHash64(key.data(), key.size()) % mod_bucket_num;
                if (bucket_index_range.empty()
                    || std::find(bucket_index_range.begin(), bucket_index_range.end(), index_mod) == bucket_index_range.end())
                    bucket_index_range.emplace_back(index_mod);
            }
        }
    }

    return bucket_index_range;
}

void StandardUnOrderedMapUniqueKeyIndex::serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const
{
    if (!bucketing_enabled)
    {
        size_t size = unique_key_index.size();
        DB::writeBinary(size, ostr);

        for (const auto & unique_key_index_item : unique_key_index)
        {
            writeStringBinary(unique_key_index_item.first, ostr);
            writeVarUInt(std::get<0>(unique_key_index_item.second), ostr);
            writeVarUInt(std::get<1>(unique_key_index_item.second), ostr);
        }
    }
    else
    {
        /// index serialize
        for (size_t index = 0; index < bucket_num; index++)
        {
            const size_t & offset_in_file = ostr.count();
            const auto & unique_key_index_bucket_item = unique_key_index_bucket[index];

            for (const auto & unique_key_index_item : unique_key_index_bucket_item)
            {
                writeStringBinary(unique_key_index_item.first, ostr);
                writeVarUInt(std::get<0>(unique_key_index_item.second), ostr);
                writeVarUInt(std::get<1>(unique_key_index_item.second), ostr);
            }

            const size_t & rows = unique_key_index_bucket_item.size();
            bucket_index->add(UniqueKeyBucketInfo(offset_in_file, rows));
        }
    }
}

void StandardUnOrderedMapUniqueKeyIndex::deserializeBinary(
    const DiskPtr & disk,
    const String & index_path,
    UniqueKeyBucketIndexPtr bucket_index,
    LoadingBucketPoolPtr loading_bucket_pool,
    BucketIndexRangePtr bucket_range,
    size_t max_running_loading_task,
    size_t timeout_in_sec)
{
    if (!bucketing_enabled)
    {
        auto istr = disk->readFile(index_path);
        size_t size;
        DB::readBinary(size, *istr);

        for (size_t index = 0; index < size; ++index)
        {
            String key;
            readStringBinary(key, *istr);
            UInt64 version;
            readVarUInt(version, *istr);
            size_t row_num;
            readVarUInt(row_num, *istr);
            unique_key_index[key] = std::make_tuple(version, row_num);
        }
    }
    else
    {
        if (!bucket_num)
            return;

        if (bucket_range)
            LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeBucketIndex"), "will load {}/{} bucket index", bucket_range->size(), bucket_num);

        const auto & bucket_infos = bucket_index->getBucketInfos();
        CountDownLatch count_down_latch(bucket_range ? bucket_range->size() : bucket_num);
        for (size_t i = 0; i < bucket_num; i++)
        {
            if (bucket_range)
            {
                if (std::find(bucket_range->begin(), bucket_range->end(), i) == bucket_range->end())
                    continue;
            }

            const auto & bucket_info = bucket_infos[i];
            const size_t & offset_in_file = bucket_info.getOffsetInFile();
            const size_t & rows = bucket_info.getRows();

            size_t waited_secs = 0;
            bool schedule_flag = false;
            size_t busy_threads_in_pool = 0;
            while (waited_secs <= timeout_in_sec)
            {
                busy_threads_in_pool
                    = CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask].load(std::memory_order_relaxed);
                if (busy_threads_in_pool >= max_running_loading_task)
                {
                    sleepForSeconds(1);
                    ++waited_secs;
                }
                else
                {
                    loading_bucket_pool->scheduleOrThrowOnError(
                        [&, this, i, offset_in_file, rows, thread_group = CurrentThread::getGroup()]
                        {
                            setThreadName("readUniqueKeyFromBucket");
                            CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]++;

                            SCOPE_EXIT_SAFE(if (thread_group) CurrentThread::detachQueryIfNotDetached(););
                            if (thread_group)
                                CurrentThread::attachTo(thread_group);

                            try
                            {
                                auto index_istr = disk->readFile(index_path);
                                index_istr->seek(offset_in_file, SEEK_SET);
                                UniqueKeyIndexMapType unique_key_version_tmp;
                                for (size_t rows_idx = 0; rows_idx < rows; rows_idx++)
                                {
                                    String key;
                                    readStringBinary(key, *index_istr);
                                    UInt64 version;
                                    readVarUInt(version, *index_istr);
                                    size_t row_num;
                                    readVarUInt(row_num, *index_istr);
                                    unique_key_version_tmp.emplace(key, VersionAndRow(version, row_num));
                                }

                                {
                                    std::lock_guard<std::mutex> lck(bucket_load_mutex);
                                    unique_key_index_bucket[i] = std::move(unique_key_version_tmp);
                                }

                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                            }
                            catch (std::exception const & ex)
                            {
                                CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineLoadTask]--;
                                count_down_latch.countDown();
                                throw ex;
                            }
                        });
                    schedule_flag = true;
                    break;
                }
            }

            if (!schedule_flag)
                throw Exception(
                    "Timeout while scheduling loading buckets of unique key index, timeout is " + std::to_string(timeout_in_sec)
                        + ", and BackgroundUniqueEngineLoadTask is " + std::to_string(busy_threads_in_pool),
                    ErrorCodes::TIMEOUT_EXCEEDED);
        }

        count_down_latch.await();
    }
}

std::optional<VersionAndRow> LevelDBUniqueKeyIndex::get(const String & key, const bool & rowid_is_uinit32) const
{
    String value;
    auto status = index_reader->Get(IndexFile::ReadOptions(), key, &value);
    if (status.ok())
    {
        Slice input(value);
        UInt64 rowid;

        if (rowid_is_uinit32)
        {
            UInt32 rowid_u32;
            decodeUInt32Rowid(input, rowid_u32);
            rowid = rowid_u32;
        }
        else
            decodeUInt64Rowid(input, rowid);

        UInt64 version;
        decodeVersion(input, version);
        return VersionAndRow(version, rowid);
    }
    else if (status.IsNotFound())
        return {};
    else
        throw Exception("Failed to lookup key: " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
}

/// TODO refine, the serializeBinary function should be used to serialize this object.
void LevelDBUniqueKeyIndex::serializeBinary(
    const String & index_path,
    Block & block,
    const UniqueDeleteBitmapPtr & delete_bitmap,
    IndexFile::IndexFileInfo & file_info,
    const bool & rowid_is_uinit32,
    const bool & is_same_key) const
{
    IndexFile::Options options;
    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
    IndexFile::IndexFileWriterPtr index_writer = std::make_unique<IndexFile::IndexFileWriter>(options);
    auto status = index_writer->Open(index_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Error while opening file {}: {}", index_path, status.ToString());

    size_t rows = block.rows();
    if (rows == 0)
    {
        LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "index type LevelDBUniqueKeyIndex block rows is zero.");
        return;
    }

    if (!is_same_key)
    {
        /// sort
        SortDescription unique_sort_description;
        unique_sort_description.emplace_back(block.getPositionByName(UNIQUE_VIRTUAL_KEY_COLUMN_NAME), 1, 1);
        // unique_sort_description.emplace_back(block.getPositionByName(UNIQUE_VIRTUAL_VERSION_COLUMN_NAME), -1, 1);

        IColumn::Permutation * perm_ptr = nullptr;
        IColumn::Permutation perm;
        if (!isAlreadySorted(block, unique_sort_description))
        {
            stableGetPermutation(block, unique_sort_description, perm);
            perm_ptr = &perm;

            for (size_t i = 0; i < block.columns(); ++i)
            {
                auto & column = block.getByPosition(i);
                column.column = column.column->permute(*perm_ptr, 0);
            }
        }
        else
            LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "unique key is already sorted.");
    }
    else
        LOG_DEBUG(&Poco::Logger::get("UniqueMergeTreeIndex"), "unique key is the same as order key and doesn't need to be sorted.");

    /// dedup
    const auto & unique_key_col = block.getByName(UNIQUE_VIRTUAL_KEY_COLUMN_NAME);
    const auto & unique_rowid_col = block.getByName(UNIQUE_VIRTUAL_ROWID_COLUMN_NAME);
    const auto & unique_version_col = block.getByName(UNIQUE_VIRTUAL_VERSION_COLUMN_NAME);

    StringRef last_key;
    UInt64 last_rowid = 0;
    UInt64 last_version = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        const auto & key = unique_key_col.column->getDataAt(i);
        const UInt64 & rowid = unique_rowid_col.column->getUInt(i);
        const UInt64 & version = unique_version_col.column->getUInt(i);
        if (i == 0 || last_key != key)
        {
            last_key = key;
            last_rowid = rowid;
            last_version = version;
        }
        else
        {
            if (version > last_version)
            {
                delete_bitmap->deleteRow(last_rowid);
                last_rowid = rowid;
                last_version = version;
            }
            else
                delete_bitmap->deleteRow(rowid);
        }
    }

    for (size_t i = 0; i < rows; ++i)
    {
        const UInt64 & rowid = unique_rowid_col.column->getUInt(i);
        if (delete_bitmap->isDeleted(rowid))
            continue;

        auto key_ref = unique_key_col.column->getDataAt(i);
        const String & key = key_ref.toString();
        const UInt64 & version = unique_version_col.column->getUInt(i);

        String value;

        if (rowid_is_uinit32)
            PutVarint32(&value, static_cast<UInt32>(rowid));
        else
            PutVarint64(&value, rowid);

        /// Handle explicit version column
        PutFixed64(&value, version); /// must use correct index, not rid

        status = index_writer->Add(key, value);
        if (!status.ok())
            throw Exception("Error while adding key to " + index_path + ": " + status.ToString(), ErrorCodes::LOGICAL_ERROR);
    }

    /// TODO move this to finish* function.
    status = index_writer->Finish(&file_info);
    if (!status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while finishing file {}: {}", index_path, status.ToString());
}

void LevelDBUniqueKeyIndex::serializeBinary(
    const String & index_path, 
    IndexFile::IndexFileInfo & file_info, 
    const String & tmp_rocksdb_index_dir,
    std::unique_ptr<rocksdb::DB> & tmp_rocksdb_index_writer) const
{
    IndexFile::Options options;
    options.filter_policy.reset(IndexFile::NewBloomFilterPolicy(10));
    IndexFile::IndexFileWriterPtr index_writer = std::make_unique<IndexFile::IndexFileWriter>(options);
    auto status = index_writer->Open(index_path);
    if (!status.ok())
        throw Exception(ErrorCodes::CANNOT_OPEN_FILE, "Error while opening file {}: {}", index_path, status.ToString());

    /// merge case : create index file from temp index
    std::unique_ptr<rocksdb::Iterator> iter(tmp_rocksdb_index_writer->NewIterator(rocksdb::ReadOptions()));
    for (iter->SeekToFirst(); iter->Valid(); iter->Next())
    {
        auto key = iter->key();
        auto val = iter->value();
        status = index_writer->Add(Slice(key.data(), key.size()), Slice(val.data(), val.size()));
        if (!status.ok())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while adding key to {}: {}", index_path, status.ToString());
    }

    if (!iter->status().ok())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Error while scanning temp key index file {}: {}",
            tmp_rocksdb_index_dir,
            iter->status().ToString());
    iter.reset();

    /// TODO move this to finish* function.
    status = index_writer->Finish(&file_info);
    if (!status.ok())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Error while finishing file {}: {}", index_path, status.ToString());
}

void LevelDBUniqueKeyIndex::deserializeBinary(const String & file_path, UniqueKeyIndexBlockCachePtr block_cache)
{
    IndexFile::Options options;
    options.block_cache = std::move(block_cache);
    auto local_reader = std::make_unique<IndexFile::IndexFileReader>(options);
    auto status = local_reader->Open(file_path);
    if (!status.ok())
        throw Exception("Failed to open index file " + file_path + ": " + status.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    index_reader = std::move(local_reader);
}

UniqueKeyIterator LevelDBUniqueKeyIndex::newIterator(const IndexFile::ReadOptions & options) const
{
    if (!index_reader)
        return std::unique_ptr<IndexFile::Iterator>(IndexFile::NewEmptyIterator());
    std::unique_ptr<IndexFile::Iterator> res;
    auto st = index_reader->NewIterator(options, &res);
    if (!st.ok())
        throw Exception("Failed to get iterator: " + st.ToString(), ErrorCodes::UNKNOWN_EXCEPTION);
    return res;
}

size_t LevelDBUniqueKeyIndex::residentMemoryUsage() const
{
    return index_reader ? index_reader->ResidentMemoryUsage() : sizeof(LevelDBUniqueKeyIndex);
}

bool LevelDBUniqueKeyIndex::decodeUInt32Rowid(Slice & input, UInt32 & rowid)
{
    return GetVarint32(&input, &rowid);
}

bool LevelDBUniqueKeyIndex::decodeUInt64Rowid(Slice & input, UInt64 & rowid)
{
    return GetVarint64(&input, &rowid);
}

bool LevelDBUniqueKeyIndex::decodeVersion(Slice & input, UInt64 & version)
{
    if (input.size() >= sizeof(UInt64))
    {
        version = DecodeFixed64(input.data());
        input.remove_prefix(sizeof(UInt64));
        return true;
    }

    return false;
}

}
