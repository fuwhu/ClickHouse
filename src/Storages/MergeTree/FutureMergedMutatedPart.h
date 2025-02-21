#pragma once

#include <base/types.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartType.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeType.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

class MergeTreeData;

/// Auxiliary struct holding metainformation for the future merged or mutated part.
struct FutureMergedMutatedPart
{
    using UniqueDeleteBitmapVector = std::vector<UniqueDeleteBitmapPtr>;

    String name;
    UUID uuid = UUIDHelpers::Nil;
    String path;
    MergeTreeDataPartFormat part_format;
    MergeTreePartInfo part_info;
    MergeTreeData::DataPartsVector parts;
    std::vector<MergeTreePartInfo> blocking_parts_to_remove;
    UniqueDeleteBitmapVector unique_delete_bitmaps;
    MergeType merge_type = MergeType::Regular;
    bool final = false;

    const MergeTreePartition & getPartition() const { return parts.front()->partition; }

    FutureMergedMutatedPart() = default;

    explicit FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_)
    {
        assign(std::move(parts_));
    }

    FutureMergedMutatedPart(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartFormat future_part_format)
    {
        assign(std::move(parts_), future_part_format);
    }

    void assign(MergeTreeData::DataPartsVector parts_);
    void assign(MergeTreeData::DataPartsVector parts_, MergeTreeDataPartFormat future_part_format);

    void updatePath(const MergeTreeData & storage, const IReservation * reservation);

    /// only be used for unique engine
    void setUniqueDeleteBitmaps();
};

using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

}
