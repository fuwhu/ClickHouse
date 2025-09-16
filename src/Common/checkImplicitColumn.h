#pragma once

#include <optional>
#include <Core/Defines.h>
#include <base/types.h>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int BAD_ARGUMENTS;
    }

inline std::optional<std::pair<String, String>> extractImplicitColumn(const String & column_name)
{
    size_t pos = column_name.find(IMPLICIT_DELIMITER);

    if (pos == std::string::npos)
        return std::nullopt;

    auto column_map_v2_name = column_name.substr(0, pos);
    auto key_name = column_name.substr(pos);

    return std::make_pair(std::move(column_map_v2_name), std::move(key_name));
}

inline bool isImplicitSubColumn(const String& column_name, bool check_implicit = false)
{
    /// TODO : include other sub-column suffixes.
    if (check_implicit)
        return column_name.contains(IMPLICIT_DELIMITER) 
            && (column_name.ends_with(".null") || column_name.ends_with(".size0"));
    else
        return column_name.ends_with(".null") || column_name.ends_with(".size0");
}

inline bool isImplicitNullMapSubColumn(const String& column_name, bool check_implicit = false)
{
    if (check_implicit)
        return column_name.contains(IMPLICIT_DELIMITER) && column_name.ends_with(".null");
    else
        return column_name.ends_with(".null");
}

inline bool isImplicitSizeSubColumn(const String& column_name, bool check_implicit = false)
{
    if (check_implicit)
        return column_name.contains(IMPLICIT_DELIMITER) && column_name.ends_with("size0");
    else
        return column_name.ends_with("size0");
}

}
