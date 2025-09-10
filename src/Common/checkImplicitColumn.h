#pragma once

#include <optional>
#include <Core/Defines.h>
#include <base/types.h>

namespace DB
{
inline std::optional<std::pair<String, String>> extractImplicitColumn(const String & column_name)
{
    size_t pos = column_name.find(IMPLICIT_DELIMITER);

    if (pos == std::string::npos)
        return std::nullopt;

    auto column_map_v2_name = column_name.substr(0, pos);
    auto key_name = column_name.substr(pos);

    return std::make_pair(std::move(column_map_v2_name), std::move(key_name));
}
}
