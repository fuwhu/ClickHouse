#include <Storages/MergeTree/UniqueMergeTreeIndex.h>

namespace DB
{
UniqueKeyBucketInfo::UniqueKeyBucketInfo(const size_t & offset_in_file_, const size_t & rows_)
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

}
