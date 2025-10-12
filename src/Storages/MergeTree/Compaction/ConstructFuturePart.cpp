#include <Storages/MergeTree/Compaction/ConstructFuturePart.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>

namespace DB
{

static std::optional<MergeTreeDataPartsVector> findPartsInMemory(
    const MergeTreeData & data,
    const PartsRange & range,
    MergeTreeData::DataPartStates lookup_statuses,
    const MergeType & merge_type)
{
    LoggerPtr log = getLogger("ConstructFuturePart");
    MergeTreeDataPartsVector data_parts;
    data_parts.reserve(range.size());

    /// add restrict range, cas change part merge update status only for unique engine, avoid affecting other engines.
    if (data.merging_params.mode == MergeTreeData::MergingParams::Unique)
    {
        for (const auto & properties : range)
        {
            auto part = data.getPartIfExists(properties.info, lookup_statuses);
            if (!part)
                return std::nullopt;

            if (IMergeTreeDataPart::MergeUpdateStatus::MERGING == part->merge_update_status.load())
            {
                LOG_WARNING(
                    log,
                    "the merge_update_status of part {} is already MERGING, this may be caused by the failure of last merge.",
                    part->name);
                data_parts.push_back(std::move(part));
            }
            else
            {
                if (data.changePartMergeUpdateStatus(part, IMergeTreeDataPart::MergeUpdateStatus::NORMAL, IMergeTreeDataPart::MergeUpdateStatus::MERGING))
                    data_parts.push_back(std::move(part));
                else
                {
                    LOG_DEBUG(
                        log,
                        "Since the part {} is being updated, the parts to merge is reduced from {} to {}",
                        part->name,
                        range.size(),
                        data_parts.size());
                    break;
                }
            }
        }

        if (data_parts.empty())
        {
            LOG_DEBUG(log, "no part to merge");
            return std::nullopt;
        }

        if (data_parts.size() == 1 && merge_type == MergeType::Regular)
        {
            /// rollback merge_update_status of part
            data.changePartMergeUpdateStatus(data_parts[0], IMergeTreeDataPart::MergeUpdateStatus::MERGING, IMergeTreeDataPart::MergeUpdateStatus::NORMAL);
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Logical error: regular merge selector returned only one part that can be merged.");
        }
    }
    else
    {
        for (const auto & properties : range)
        {
            if (auto part = data.getPartIfExists(properties.info, lookup_statuses))
                data_parts.push_back(std::move(part));
            else
                return std::nullopt;
        }
    }

    return data_parts;
}

FutureMergedMutatedPartPtr constructFuturePart(const MergeTreeData & data, const MergeSelectorChoice & choice, MergeTreeData::DataPartStates lookup_statuses)
{
    auto data_parts = findPartsInMemory(data, choice.range, std::move(lookup_statuses), choice.merge_type);
    if (!data_parts.has_value())
        return nullptr;

    auto future_part = std::make_shared<FutureMergedMutatedPart>();
    future_part->merge_type = choice.merge_type;
    future_part->assign(std::move(data_parts.value()));
    future_part->final = choice.final;

    return future_part;
}

}
