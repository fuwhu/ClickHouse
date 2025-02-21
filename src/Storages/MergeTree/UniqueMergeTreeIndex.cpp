#include "UniqueMergeTreeIndex.h"

namespace DB
{
UniqueKeyBucketInfo::UniqueKeyBucketInfo(size_t offset_in_file_, size_t rows_)
{
    offset_in_file = offset_in_file_;
    rows = rows_;
}

size_t UniqueKeyBucketInfo::getOffsetInFile() const
{
    return offset_in_file;
}

size_t UniqueKeyBucketInfo::getRows() const
{
    return rows;
}

bool IUniqueKeyIndex::isMapUniqueKeyIndex(size_t unique_key_index_type)
{
    return unique_key_index_type == Type::STANDARD_MAP || unique_key_index_type == Type::STANDARD_UNORDERED_MAP
        || unique_key_index_type == Type::STRING_HASH_MAP;
}

bool IUniqueKeyIndex::isLevelDBUniqueKeyIndex(size_t unique_key_index_type)
{
    return unique_key_index_type == Type::LEVEL_DB;
}
}
