#include "IcebergFileSource.h"

#if USE_PARQUET
#    include <parquet/statistics.h>

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

    std::optional<Field>
    decodePlainParquetValue(const std::string & data, parquet::Type::type physical_type, const parquet::ColumnDescriptor & descr)
    {
        auto decode_integer = [&](bool signed_) -> UInt64
        {
            size_t size;
            switch (physical_type)
            {
                case parquet::Type::type::BOOLEAN:
                    size = 1;
                    break;
                case parquet::Type::type::INT32:
                    size = 4;
                    break;
                case parquet::Type::type::INT64:
                    size = 8;
                    break;
                default:
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected physical type for number");
            }
            if (data.size() != size)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected size: {}", data.size());

            UInt64 val = 0;
            memcpy(&val, data.data(), size);

            /// Sign-extend.
            if (signed_ && size < 8 && (val >> (size * 8 - 1)) != 0)
                val |= 0 - (1ul << (size * 8));

            return val;
        };

        /// Decimal.
        do // while (false)
        {
            Int32 scale;
            if (descr.logical_type() && descr.logical_type()->is_decimal())
                scale = assert_cast<const parquet::DecimalLogicalType &>(*descr.logical_type()).scale();
            else if (descr.converted_type() == parquet::ConvertedType::type::DECIMAL)
                scale = descr.type_scale();
            else
                break;

            size_t size;
            bool big_endian = false;
            switch (physical_type)
            {
                case parquet::Type::type::BOOLEAN:
                    size = 1;
                    break;
                case parquet::Type::type::INT32:
                    size = 4;
                    break;
                case parquet::Type::type::INT64:
                    size = 8;
                    break;

                case parquet::Type::type::FIXED_LEN_BYTE_ARRAY:
                    big_endian = true;
                    size = data.size();
                    break;
                default:
                    throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal physical type");
            }
            /// Note that size is not necessarily a power of two.
            /// E.g. spark turns 8-byte unsigned integers into 9-byte signed decimals.
            if (data.size() != size || size < 1 || size > 32)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected decimal size: {} (actual {})", size, data.size());

            /// For simplicity, widen all decimals to 256-bit. It should compare correctly with values
            /// of different bitness.
            Int256 val = 0;
            memcpy(&val, data.data(), size);
            if (big_endian)
                std::reverse(reinterpret_cast<char *>(&val), reinterpret_cast<char *>(&val) + size);
            /// Sign-extend.
            if (size < 32 && (val >> (size * 8 - 1)) != 0)
                val |= ~((Int256(1) << (size * 8)) - 1);

            return Field(DecimalField<Decimal256>(Decimal256(val), static_cast<UInt32>(scale)));
        } while (false);

        /// Timestamp (decimal).
        {
            Int32 scale = -1;
            bool is_timestamp = true;
            if (descr.logical_type() && (descr.logical_type()->is_time() || descr.logical_type()->is_timestamp()))
            {
                parquet::LogicalType::TimeUnit::unit unit = descr.logical_type()->is_time()
                    ? assert_cast<const parquet::TimeLogicalType &>(*descr.logical_type()).time_unit()
                    : assert_cast<const parquet::TimestampLogicalType &>(*descr.logical_type()).time_unit();
                switch (unit)
                {
                    case parquet::LogicalType::TimeUnit::unit::MILLIS:
                        scale = 3;
                        break;
                    case parquet::LogicalType::TimeUnit::unit::MICROS:
                        scale = 6;
                        break;
                    case parquet::LogicalType::TimeUnit::unit::NANOS:
                        scale = 9;
                        break;
                    default:
                        throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unknown time unit");
                }
            }
            else
                switch (descr.converted_type())
                {
                    case parquet::ConvertedType::type::TIME_MILLIS:
                        scale = 3;
                        break;
                    case parquet::ConvertedType::type::TIME_MICROS:
                        scale = 6;
                        break;
                    case parquet::ConvertedType::type::TIMESTAMP_MILLIS:
                        scale = 3;
                        break;
                    case parquet::ConvertedType::type::TIMESTAMP_MICROS:
                        scale = 6;
                        break;
                    default:
                        is_timestamp = false;
                }

            if (is_timestamp)
            {
                Int64 val = static_cast<Int64>(decode_integer(/* signed */ true));
                return Field(DecimalField<Decimal64>(Decimal64(val), scale));
            }
        }

        /// Floats.
        if (physical_type == parquet::Type::type::FLOAT)
        {
            if (data.size() != 4)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
            Float32 val;
            memcpy(&val, data.data(), data.size());
            return Field(val);
        }

        if (physical_type == parquet::Type::type::DOUBLE)
        {
            if (data.size() != 8)
                throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Unexpected float size");
            Float64 val;
            memcpy(&val, data.data(), data.size());
            return Field(val);
        }

        /// Strings.
        if (physical_type == parquet::Type::type::BYTE_ARRAY || physical_type == parquet::Type::type::FIXED_LEN_BYTE_ARRAY)
        {
            /// Arrow's parquet decoder handles missing min/max values slightly incorrectly.
            /// In a parquet file, min and max have separate is_set flags, i.e. one may be missing even
            /// if the other is set. Arrow decoder ORs (!) these two flags together into one: HasMinMax().
            /// So, if exactly one of {min, max} is missing, Arrow reports it as empty string, with no
            /// indication that it's actually missing.
            ///
            /// How can exactly one of {min, max} be missing? This happens if one of the two strings
            /// exceeds the length limit for stats. Repro:
            ///
            ///   insert into function file('t.parquet') select arrayStringConcat(range(number*1000000)) from numbers(2) settings output_format_parquet_use_custom_encoder=0
            ///   select tupleElement(tupleElement(row_groups[1], 'columns')[1], 'statistics') from file('t.parquet', ParquetMetadata)
            ///
            /// Here the row group contains two strings: one empty, one very long. But the statistics
            /// reported by arrow are indistinguishable from statistics if all strings were empty.
            /// (Min and max are the last two tuple elements in the output of the second query. Notice
            /// how they're empty strings instead of NULLs.)
            ///
            /// So we have to be conservative and treat empty string as unknown.
            /// This is unfortunate because it's probably common for string columns to have lots of empty
            /// values, and filter pushdown would probably often be useful in that case.
            ///
            /// TODO: Remove this workaround either when we implement our own Parquet decoder that
            ///       doesn't have this bug, or if it's fixed in Arrow.
            if (data.empty())
                return std::nullopt;

            return Field(data);
        }

        /// This one's deprecated in Parquet.
        if (physical_type == parquet::Type::type::INT96)
            throw Exception(ErrorCodes::CANNOT_PARSE_NUMBER, "Parquet INT96 type is deprecated and not supported");

        /// Integers.
        bool is_signed = true;
        if (descr.logical_type() && descr.logical_type()->is_int())
            is_signed = assert_cast<const parquet::IntLogicalType &>(*descr.logical_type()).is_signed();
        else
            is_signed = descr.converted_type() != parquet::ConvertedType::type::UINT_8
                && descr.converted_type() != parquet::ConvertedType::type::UINT_16
                && descr.converted_type() != parquet::ConvertedType::type::UINT_32
                && descr.converted_type() != parquet::ConvertedType::type::UINT_64;

        UInt64 val = decode_integer(is_signed);
        Field field = is_signed ? Field(static_cast<Int64>(val)) : Field(val);

        return field;
    }

    Range buildRange(const parquet::Statistics * col_stats)
    {
        Range r;

        if (!col_stats) [[unlikely]]
            return r;

        if (col_stats->HasNullCount() && !col_stats->num_values())
        {
            r = Range(POSITIVE_INFINITY, true, POSITIVE_INFINITY, true);
            r.has_null = true;
            r.only_null = true;
            return r;
        }

        std::optional<Field> min;
        std::optional<Field> max;
        if (col_stats->HasMinMax())
        {
            min = decodePlainParquetValue(col_stats->EncodeMin(), col_stats->physical_type(), *col_stats->descr());
            max = decodePlainParquetValue(col_stats->EncodeMax(), col_stats->physical_type(), *col_stats->descr());
        }

        if (min.has_value() && max.has_value())
        {
            r = Range(*min, true, *max, true);
        }
        else if (min.has_value())
        {
            r = Range::createLeftBounded(*min, true);
        }
        else if (max.has_value())
        {
            r = Range::createRightBounded(*max, true);
        }

        if (col_stats->HasNullCount() && col_stats->null_count())
        {
            r.has_null = true;
            r.only_null = false;
        }

        return r;
    }

    bool filterParquetRowGroupsWithEnforedBitmap(const roaring::Roaring * mask, const ParquetRowGroupInformation & row_group_info)
    {
        if (!mask) [[unlikely]]
            return false;

        roaring::Roaring stripe_bitmap;
        stripe_bitmap.addRange(row_group_info.first_row_of_row_group, row_group_info.first_row_of_row_group + row_group_info.num_rows);

        return !mask->intersect(stripe_bitmap);
    }

    bool filterParquetRowGroupsWithStatistics(
        const parquet::RowGroupMetaData & row_group_metadata,
        const ColumnsDescription & columns_description,
        const Names & columns,
        const std::unordered_map<String, int> & column_name_to_index,
        std::shared_ptr<KeyCondition> key_condition)
    {
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
            auto col_chunk = row_group_metadata.ColumnChunk(col_index);
            if (!col_chunk
                /// compound types not supported
                || col_chunk->path_in_schema()->ToDotVector().size() != 1)
            {
                row_group_hyperrectangle.emplace_back(buildRange(nullptr));
                continue;
            }

            auto row_group_stats = col_chunk->statistics();
            row_group_hyperrectangle.emplace_back(buildRange(row_group_stats.get()));
        }

        auto res = key_condition->checkInHyperrectangle(row_group_hyperrectangle, data_types);
        return !res.can_be_true;
    }
}

void IcebergParquetFile::filterParquetRowGroups(
    const ParquetRowGroupsInformation & row_groups,
    const ParquetRowGroupsMetadata & metadata,
    const std::unordered_map<String, int> & column_name_to_index)
{
    std::vector<RowGroupFilterStat> filter_stats;

    Names columns = columns_description.getNamesOfPhysical();
    for (size_t i = 0, num_row_groups = row_groups.size(); i < num_row_groups; ++i)
    {
        auto row_group_info = row_groups[i];
        if (!key_condition && !scan_result.mask)
        {
            row_groups_to_read.emplace_back(row_group_info);
            filter_stats.push_back(RowGroupFilterStat{.row_group_id = i, .reason = FilterReason::SELECTED});
            continue;
        }

        if (scan_result.mask && filterParquetRowGroupsWithEnforedBitmap(scan_result.mask.get(), row_group_info))
        {
            filter_stats.push_back(RowGroupFilterStat{.row_group_id = i, .reason = FilterReason::SKIPED_BY_ENFORCED_BITMAP});
            continue;
        }

        if (key_condition
            && filterParquetRowGroupsWithStatistics(*metadata[i], columns_description, columns, column_name_to_index, key_condition))
        {
            filter_stats.push_back(RowGroupFilterStat{.row_group_id = i, .reason = FilterReason::SKIPED_BY_STATISTICS});
            continue;
        }

        filter_stats.push_back(RowGroupFilterStat{.row_group_id = i, .reason = FilterReason::SELECTED});
        row_groups_to_read.emplace_back(std::move(row_group_info));
    }

    /// Print filter stats
    int number_of_row_groups_skiped_by_enforced_bitmap = 0;
    int number_of_row_groups_skiped_by_statistics = 0;
    int total_number_of_row_groups = row_groups.size();
    for (const auto & filter_stat : filter_stats)
    {
        if (filter_stat.reason == FilterReason::SKIPED_BY_STATISTICS)
        {
            ++number_of_row_groups_skiped_by_statistics;
        }
        else if (filter_stat.reason == FilterReason::SKIPED_BY_ENFORCED_BITMAP)
        {
            ++number_of_row_groups_skiped_by_enforced_bitmap;
        }
    }

    LOG_DEBUG(
        &Poco::Logger::get("IcebergFileSource"),
        "File: {}, RowGroups filter stats: {}/{}(enforced_bitmap), {}/{}(statistics)",
        scan_result.data_file.path,
        number_of_row_groups_skiped_by_enforced_bitmap,
        total_number_of_row_groups,
        number_of_row_groups_skiped_by_statistics,
        total_number_of_row_groups - number_of_row_groups_skiped_by_enforced_bitmap);
}

void IcebergParquetFile::applyFilters(FilterStage /*stage*/, UInt64 & /*apply_filters_time_cost_us*/)
{
    auto * parquet_input_format = dynamic_cast<ParquetBlockInputFormat *>(input_format.get());
    if (!parquet_input_format)
        throw Exception("Failed to cast InputFormat to ParquetBlockInputFormat while file format is Parquet", ErrorCodes::LOGICAL_ERROR);

    auto row_groups = parquet_input_format->getRowGroups();
    if (!key_condition || key_condition->alwaysUnknownOrTrue())
    {
        row_groups_to_read = row_groups;
        return;
    }

    ParquetRowGroupsMetadata metadata;
    std::unordered_map<String, int> column_name_to_index;
    if (format_settings.parquet.filter_push_down)
    {
        metadata = parquet_input_format->getRowGroupsMetadata();
        column_name_to_index = parquet_input_format->getColumnNameToIndexMapping();
        filterParquetRowGroups(row_groups, metadata, column_name_to_index);
    }
    else
    {
        filterParquetRowGroups(row_groups, {}, {});
    }
}

void IcebergParquetFile::reverseSplits()
{
    auto * parquet_input_format = dynamic_cast<ParquetBlockInputFormat *>(input_format.get());
    if (!parquet_input_format)
        throw Exception("Failed to cast InputFormat to ParquetBlockInputFormat while file format is Parquet", ErrorCodes::LOGICAL_ERROR);

    std::reverse(row_groups_to_read.begin(), row_groups_to_read.end());

    parquet_input_format->ignoreBatchSizeLimit();
}

void IcebergParquetFile::prepare()
{
    auto * parquet_input_format = dynamic_cast<ParquetBlockInputFormat *>(input_format.get());
    if (!parquet_input_format)
        throw Exception("Failed to cast InputFormat to ParquetBlockInputFormat while file format is Parquet", ErrorCodes::LOGICAL_ERROR);

    parquet_input_format->setRowGroupsToRead(row_groups_to_read);
}
}

#endif
