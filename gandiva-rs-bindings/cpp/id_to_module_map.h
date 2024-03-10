// Copyright 2024 JasonLi-cn
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "base.h"

namespace gandiva {

template<typename HOLDER>
class IdToModuleMap {
public:
    IdToModuleMap() : module_id_(kInitModuleId) {}

    ModuleId Insert(HOLDER holder) {
        mtx_.lock();
        ModuleId result = module_id_++;
        map_.insert(std::pair<ModuleId, HOLDER>(result, holder));
        mtx_.unlock();
        return result;
    }

    void Erase(ModuleId module_id) {
        mtx_.lock();
        map_.erase(module_id);
        mtx_.unlock();
    }

    HOLDER Lookup(ModuleId module_id) {
        HOLDER result = nullptr;
        mtx_.lock();
        try {
            result = map_.at(module_id);
        } catch (const std::out_of_range &) {
        }
        mtx_.unlock();
        return result;
    }

private:
    static const int kInitModuleId = 4;

    int64_t module_id_;
    std::mutex mtx_;
    std::unordered_map<ModuleId, HOLDER> map_;
};

}  // namespace gandiva
