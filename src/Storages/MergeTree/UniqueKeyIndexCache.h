/*
 * Copyright (2022) Bytedance Ltd. and/or its affiliates
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <Storages/MergeTree/UUIDAndPartName.h>
#include <Storages/MergeTree/UniqueMergeTreeIndex.h>
#include <Common/CacheBase.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
extern const Event UniqueKeyIndexMetaCacheHit;
extern const Event UniqueKeyIndexMetaCacheMiss;
}

namespace DB
{

struct UniqueKeyIndexWeightFunction
{
    size_t operator()(const IUniqueKeyIndex & index) const { return index.residentMemoryUsage(); }
};

class UniqueKeyIndexCache : public CacheBase<UUIDAndPartName, IUniqueKeyIndex, UUIDAndPartNameHash, UniqueKeyIndexWeightFunction>
{
    using Base = CacheBase<UUIDAndPartName, IUniqueKeyIndex, UUIDAndPartNameHash, UniqueKeyIndexWeightFunction>;

public:
    using Base::Base;

    template <typename LoadFunc>
    std::pair<MappedPtr, bool> getOrSet(const Key & key, LoadFunc && load_func)
    {
        auto result = Base::getOrSet(key, load_func);
        if (!result.second)
            ProfileEvents::increment(ProfileEvents::UniqueKeyIndexMetaCacheHit);
        else
            ProfileEvents::increment(ProfileEvents::UniqueKeyIndexMetaCacheMiss);

        return result;
    }
};

}
