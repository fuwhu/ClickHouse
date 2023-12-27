#include "MergeTreeSettingsEnums.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}


IMPLEMENT_SETTING_ENUM(
    SortingMode,
    ErrorCodes::BAD_ARGUMENTS,
    {
        {"normal", SortingMode::NORMAL},
        {"zcurve", SortingMode::Z_CURVE},
    })
}
