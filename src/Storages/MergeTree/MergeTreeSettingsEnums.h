#pragma once

#include <Core/SettingsFields.h>

namespace DB
{
enum class SortingMode
{
    NORMAL,
    Z_CURVE
};

DECLARE_SETTING_ENUM(SortingMode)
}
