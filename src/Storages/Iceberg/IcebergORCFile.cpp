#include <cassert>
#include <functional>
#include <Poco/Logger.h>
#include "IcebergFileSource.h"
#include "Processors/Formats/Impl/NativeORCBlockInputFormat.h"
#include "base/logger_useful.h"

#if USE_ORC

namespace DB
{
namespace
{
    enum FilterReason
    {
        SELECTED,
        SKIPED_BY_ENFORCED_BITMAP,
        SKIPED_BY_STATISTICS
    };

    struct RowGroupFilterStat
    {
        size_t row_group_id;
        FilterReason reason;
    };

    struct StripeFilterStat
    {
        size_t stripe_id;
        FilterReason reason;
        size_t row_group_num;
        std::vector<RowGroupFilterStat> row_group_filter_stats;
    };

    bool filterOrcStripesWithEnforedBitmap(const roaring::Roaring * mask, const OrcStripeInformation & stripe_info)
    {
        if (!mask) [[unlikely]]
            return false;

        roaring::Roaring stripe_bitmap;
        stripe_bitmap.addRange(stripe_info.first_row_of_stripe, stripe_info.first_row_of_stripe + stripe_info.num_rows);

        return !mask->intersect(stripe_bitmap);
    }

    bool filterOrcRowGroupsWithEnforedBitmap(const roaring::Roaring * mask, const OrcStripeInformation & stripe_info, int row_group_num)
    {
        if (!mask)
            return false;

        roaring::Roaring row_group_bitmap;

        auto first_row_of_row_group = stripe_info.first_row_of_stripe + row_group_num * stripe_info.row_group_size;
        auto last_row_of_row_group = std::min(
            stripe_info.first_row_of_stripe + stripe_info.num_rows,
            stripe_info.first_row_of_stripe + (row_group_num + 1) * stripe_info.row_group_size);

        row_group_bitmap.addRange(first_row_of_row_group, last_row_of_row_group);

        return !mask->intersect(row_group_bitmap);
    }

    bool filterOrcStripesWithStatistics(
        const orc::StripeStatistics & stripe_stats,
        const ColumnsDescription & columns_description,
        const Names & columns,
        const std::unordered_map<String, int> & column_name_to_index,
        std::shared_ptr<KeyCondition> key_condition)
    {
        DataTypes data_types;
        std::vector<Range> stripe_hyperrectangle;

        for (const auto & col_name : columns)
        {
            GetColumnsOptions options(GetColumnsOptions::Kind::Ordinary);
            auto col_type = columns_description.getColumn(options, col_name).type;
            data_types.emplace_back(col_type);

            const auto it = column_name_to_index.find(col_name);
            /// No this column in file
            if (it == column_name_to_index.end())
            {
                stripe_hyperrectangle.emplace_back(buildRange(nullptr));
                continue;
            }

            auto col_index = it->second;
            const auto * column_statistics = stripe_stats.getColumnStatistics(col_index);
            stripe_hyperrectangle.emplace_back(buildRange(column_statistics));
        }

        auto res = key_condition->checkInHyperrectangle(stripe_hyperrectangle, data_types);
        return !res.can_be_true;
    }

    bool filterOrcRowGroupsWithStatistics(
        const orc::StripeStatistics & stripe_stats,
        int row_group_id,
        const ColumnsDescription & columns_description,
        const Names & columns,
        const std::unordered_map<String, int> & column_name_to_index,
        std::shared_ptr<KeyCondition> key_condition)
    {
        if (!stripe_stats.hasRowIndexStatistics())
            return false;

        DataTypes data_types;
        std::vector<Range> row_group_hyperrectangle;

        for (const auto & col_name : columns)
        {
            GetColumnsOptions options(GetColumnsOptions::Kind::Ordinary);
            auto col_type = columns_description.getColumn(options, col_name).type;
            data_types.emplace_back(col_type);

            const auto it = column_name_to_index.find(col_name);
            /// No this column in file
            if (it == column_name_to_index.end())
            {
                row_group_hyperrectangle.emplace_back(buildRange(nullptr));
                continue;
            }

            auto col_index = it->second;
            const auto * row_group_stats = stripe_stats.getRowIndexStatistics(col_index, row_group_id);
            row_group_hyperrectangle.emplace_back(buildRange(row_group_stats));
        }

        auto res = key_condition->checkInHyperrectangle(row_group_hyperrectangle, data_types);
        return !res.can_be_true;
    }

    void filterOrcRowGroups(
        OrcStripeInformation & stripe,
        const std::unordered_map<String, int> & column_name_to_index,
        std::shared_ptr<KeyCondition> key_condition,
        ColumnsDescription columns_description,
        std::shared_ptr<roaring::Roaring> mask,
        orc::StripeStatistics * stripe_statistics)
    {
        Names columns = columns_description.getNamesOfPhysical();

        size_t row_group_num = stripe.row_groups.size();

        if (!key_condition && !mask)
        {
            return;
        }

        std::vector<size_t> row_groups_to_remove;
        for (size_t i = 0; i < row_group_num; ++i)
        {
            if (filterOrcRowGroupsWithEnforedBitmap(mask.get(), stripe, i))
            {
                row_groups_to_remove.emplace_back(i);
                continue;
            }

            if (key_condition
                && filterOrcRowGroupsWithStatistics(
                    *(stripe_statistics), i, columns_description, columns, column_name_to_index, key_condition))
            {
                row_groups_to_remove.emplace_back(i);
                continue;
            }
        }

        if (!row_groups_to_remove.empty())
        {
            for (int index = row_groups_to_remove.size() - 1; index >= 0; index--)
            {
                stripe.row_groups.erase(stripe.row_groups.begin() + row_groups_to_remove[index]);
            }
        }

        if (row_group_num != stripe.row_groups.size())
            LOG_DEBUG(&Poco::Logger::get("filterOrcRowGroup"), "Before filter has {} row groups, after has {} row groups", row_group_num, stripe.row_groups.size());
    }
}

void IcebergORCFile::filterOrcStripes(
    const std::unordered_map<String, int> & column_name_to_index,
    FilterStage stage)
{
    std::vector<StripeFilterStat> filter_stats;

    Names columns = columns_description.getNamesOfPhysical();
    std::vector<size_t> stripes_to_remove;
    size_t num_stripes = stripes_to_read->size();

    for (size_t i = 0; i < num_stripes; ++i)
    {
        auto stripe_info = (*stripes_to_read)[i];
        size_t row_group_num = stripe_info.row_groups.size();

        if (!key_condition && !scan_result.mask)
        {
            filter_stats.push_back(StripeFilterStat{.stripe_id = i, .reason = FilterReason::SELECTED, .row_group_num = row_group_num});
            continue;
        }

        if (scan_result.mask && stage == FilterStage::FILTER_WITH_STRIPE_ENFORCED_BITMAP
            && filterOrcStripesWithEnforedBitmap(scan_result.mask.get(), stripe_info))
        {
            stripes_to_remove.emplace_back(i);
            filter_stats.push_back(
                StripeFilterStat{.stripe_id = i, .reason = FilterReason::SKIPED_BY_ENFORCED_BITMAP, .row_group_num = row_group_num});
            continue;
        }

        if (key_condition
            && stage == FilterStage::FILTER_WITH_STRIPE_STATISTICS
            && filterOrcStripesWithStatistics(*((*stripes_statistics)[i]), columns_description, columns, column_name_to_index, key_condition))
        {
            stripes_to_remove.emplace_back(i);
            filter_stats.push_back(
                StripeFilterStat{.stripe_id = i, .reason = FilterReason::SKIPED_BY_STATISTICS, .row_group_num = row_group_num});
            continue;
        }

        StripeFilterStat filter_stat{.stripe_id = i, .reason = FilterReason::SELECTED, .row_group_num = row_group_num};

        filter_stats.push_back(std::move(filter_stat));
    }
    for (int index = stripes_to_remove.size() - 1; index >= 0; index--)
    {
        stripes_to_read->erase(stripes_to_read->begin() + stripes_to_remove[index]);
        stripes_statistics->erase(stripes_statistics->begin() + stripes_to_remove[index]);
    }

    if (num_stripes != stripes_to_read->size())
        LOG_DEBUG(&Poco::Logger::get("filterOrcStripes"), 
        "Stage: {}, Before has {} stripe after has {} stripe", 
        stage, num_stripes, stripes_to_read->size());
}

void IcebergORCFile::applyFilters(FilterStage stage, UInt64 & apply_filters_time_cost_us)
{
    Stopwatch stop_watch;
    SCOPE_EXIT({apply_filters_time_cost_us = stop_watch.elapsedMicroseconds();});
    auto * orc_input_format = getORCInputFormat();

    if (!key_condition || key_condition->alwaysUnknownOrTrue())
        return;
    
    if (stage == FilterStage::FILTER_WITH_ROW_INDEX)
    {
        auto call_back = [this](OrcStripeInformation & stripe, orc::StripeStatistics * stripe_statistics) {
            auto * input_format = this->getORCInputFormat();
            std::unordered_map<String, int> column_name_to_index = input_format->getColumnNameToIndexMapping();
            filterOrcRowGroups(stripe, column_name_to_index, this->key_condition, columns_description, scan_result.mask, stripe_statistics);
        };
        orc_input_format->registerFilterRowGroupCallback(call_back); 
    }
    else
    {
        if (stage == FilterStage::FILTER_WITH_STRIPE_STATISTICS && !splitsStatisticsLoaded())
            loadSplitsStatistics();

        if (format_settings.orc.filter_push_down)
        {
            std::unordered_map<String, int> column_name_to_index = orc_input_format->getColumnNameToIndexMapping();
            filterOrcStripes(column_name_to_index, stage);
            if (stage == FilterStage::FILTER_WITH_STRIPE_STATISTICS)
                filtered_with_stripe_statistics = true;
        }
        else if (!format_settings.orc.filter_push_down && stage == FilterStage::FILTER_WITH_STRIPE_ENFORCED_BITMAP)
            filterOrcStripes({}, stage);
    }

    orc_input_format->setStripesToRead(stripes_to_read);
    orc_input_format->setStripesStatistics(stripes_statistics);
}

void IcebergORCFile::reverseSplits()
{
    auto * orc_input_format = getORCInputFormat();

    for (auto & stripe : *stripes_to_read)
    {
        auto & row_groups = stripe.row_groups;
        std::reverse(row_groups.begin(), row_groups.end());
    }

    std::reverse(stripes_to_read->begin(), stripes_to_read->end());

    orc_input_format->ignoreBatchSizeLimit();
}

void IcebergORCFile::prepare(const ContextPtr & context, const ReadType & read_type)
{
    if (!inputFormatInitialized())
        initializeInputFormat(context);

    auto * orc_input_format = getORCInputFormat();
    orc_input_format->prepareFileReaderAndMetadata();
    orc_input_format->addPrewhere(prewhere_columns, prewhere_info);

    if (!splitsInitialized())
        initializeSplits();

    if (read_type == ReadType::InReverseOrder)
        reverseSplits();

    if (!filteredWithSplitsStatistics())
    {
        UInt64 stripe_stats_filter_time_cost_us;
        applyFilters(FilterStage::FILTER_WITH_STRIPE_STATISTICS, stripe_stats_filter_time_cost_us);
    }

    if (scan_result.mask)
    {
        UInt64 enforced_bitmap_filter_time_cost_us;
        applyFilters(FilterStage::FILTER_WITH_STRIPE_ENFORCED_BITMAP, enforced_bitmap_filter_time_cost_us);
    }

    UInt64 row_index_filter_time_cost_us;
    applyFilters(FilterStage::FILTER_WITH_ROW_INDEX, row_index_filter_time_cost_us);
}


void IcebergORCFile::getFileSortingKeyRanges(std::vector<SortColumnDescription> & order_key_description, int direction)
{
    auto * orc_input_format = getORCInputFormat();
    auto column_name_to_index = orc_input_format->getColumnNameToIndexMapping();
    auto order_key = order_key_description[0];

    int col_number = column_name_to_index[order_key.column_name];

    const auto * strip_column_statistics_begin = (*stripes_statistics)[0]->getColumnStatistics(col_number);
    const auto * strip_column_statistics_end = (*stripes_statistics)[stripes_statistics->size() - 1]->getColumnStatistics(col_number);

    auto r_begin = buildRange(strip_column_statistics_begin);
    auto r_end = buildRange(strip_column_statistics_end);

    if (scan_result.data_file.statistics.min.keys.empty())
        scan_result.data_file.statistics.min.keys.push_back(col_number);

    if (scan_result.data_file.statistics.min.values.empty())
        scan_result.data_file.statistics.min.values.push_back(direction == 1 ? r_begin.left : r_end.left);
    else
        scan_result.data_file.statistics.min.values[0] = direction == 1 ? r_begin.left : r_end.left;

    if (scan_result.data_file.statistics.max.keys.empty())
        scan_result.data_file.statistics.max.keys.push_back(col_number);

    if (scan_result.data_file.statistics.max.values.empty())
        scan_result.data_file.statistics.max.values.push_back(direction == 1 ? r_end.right : r_begin.right);
    else
        scan_result.data_file.statistics.max.values[0] = direction == 1 ? r_end.right : r_begin.right;
}

NativeORCBlockInputFormat * IcebergORCFile::getORCInputFormat()
{
    if (!input_format)
        throw Exception("orc input format is null while getting orc input format.", ErrorCodes::LOGICAL_ERROR);
    auto * orc_input_format = dynamic_cast<NativeORCBlockInputFormat *>(input_format.get());
    if (!orc_input_format)
        throw Exception("orc input format is null after dynamic casting while orc_input_format is ORC.", ErrorCodes::LOGICAL_ERROR);
    return orc_input_format;
}

void IcebergORCFile::initializeSplits()
{
    auto * orc_input_format = getORCInputFormat();
    stripes_to_read = orc_input_format->getStripesToRead();
}

void IcebergORCFile::loadSplitsStatistics()
{
    auto * orc_input_format = getORCInputFormat();
    stripes_statistics = orc_input_format->getStripesStatistics();
}

}

#endif
