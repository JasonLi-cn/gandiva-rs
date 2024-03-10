
#include <cstdint>

#include "config_holder.h"

namespace gandiva {
    ConfigId ConfigHolder::config_id_ = 1;

    // map of configuration objects created so far
    std::unordered_map<ConfigId, std::shared_ptr<Configuration>> ConfigHolder::configuration_map_;

    std::mutex ConfigHolder::g_mtx_;
}  // namespace gandiva
