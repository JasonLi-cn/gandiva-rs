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

#include <string>

#include <gandiva/configuration.h>

#include "config_builder.h"
#include "config_holder.h"

using gandiva::ConfigHolder;
using gandiva::Configuration;
using gandiva::ConfigurationBuilder;

namespace gandiva {

ConfigId BuildConfigInstance(bool optimize, bool target_host_cpu) {
  ConfigurationBuilder configuration_builder;
  std::shared_ptr<Configuration> config = configuration_builder.build();
  config->set_optimize(optimize);
  config->target_host_cpu(target_host_cpu);
  return ConfigHolder::MapInsert(config);
}

void ReleaseConfigInstance(ConfigId config_id) { ConfigHolder::MapErase(config_id); }

}  // namespace gandiva
