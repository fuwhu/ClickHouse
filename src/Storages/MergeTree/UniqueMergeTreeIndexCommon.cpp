#include <Storages/MergeTree/UniqueMergeTreeIndexCommon.h>

namespace DB
{
void DeletedKeys::serializeBinary(WriteBuffer & ostr) const
{
    size_t map_size = size();
    if (!map_size)
        return;
    DB::writeBinary(map_size, ostr);

    for (const auto & it : *this)
    {
        writeStringBinary(it.first, ostr);
        writeVarUInt(it.second, ostr);
    }
}

void DeletedKeys::deserializeBinary(ReadBuffer & istr)
{
    size_t size;
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

std::optional<size_t> StringHashMapUniqueKeyIndex::getRowNumber(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<1>(version_row.value());
    else
        return {};
}

std::optional<UInt64> StringHashMapUniqueKeyIndex::getRowVersion(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<0>(version_row.value());
    else
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

std::optional<size_t> StandardMapUniqueKeyIndex::getRowNumber(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<1>(version_row.value());
    else
        return {};
}

std::optional<UInt64> StandardMapUniqueKeyIndex::getRowVersion(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<0>(version_row.value());
    else
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

std::optional<size_t> StandardUnOrderedMapUniqueKeyIndex::getRowNumber(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<1>(version_row.value());
    else
        return {};
}

std::optional<UInt64> StandardUnOrderedMapUniqueKeyIndex::getRowVersion(const String & key) const
{
    const auto & version_row = get(key);
    if (version_row)
        return std::get<0>(version_row.value());
    else
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
}
