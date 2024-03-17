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

#include "base.h"

namespace gandiva {

struct RustMutableBuffers {
  void* addr;
  int32_t len;
};

struct RustBuffer {
  uintptr_t addr;
  int64_t size;
};

struct RustMutableBuffer {
  uintptr_t addr;
  int64_t size;
  int64_t capacity;
};

struct ProtoMessage {
  uint8_t* addr;
  int32_t len;
};

struct SelectionVectorInfo {
  SelectionVectorType type;
  int32_t rows;
  uintptr_t addr;
  int64_t size;
};

using rust_mutable_buffer_reserve_fn = void (*)(RustMutableBuffers*, uintptr_t,
                                                RustMutableBuffer*, int64_t);

ModuleId BuildProjector(ProtoMessage pb_schema, ProtoMessage pb_exprs,
                        SelectionVectorType selection_vector_type, ConfigId config_id);

void EvaluateProjector(ModuleId module_id, int64_t num_rows, RustBuffer* in_bufs,
                       int32_t in_bufs_len, SelectionVectorInfo sel_vec_info,
                       RustMutableBuffer* out_bufs, int32_t out_bufs_len,
                       RustMutableBuffers* mutable_buffers,
                       rust_mutable_buffer_reserve_fn reserve_fn);

void CloseProjector(ModuleId module_id);

ModuleId BuildFilter(ProtoMessage pb_schema, ProtoMessage pb_condition,
                     ConfigId config_id);

int32_t EvaluateFilter(ModuleId module_id, int64_t num_rows, RustBuffer* in_bufs,
                       int32_t in_bufs_len, SelectionVectorType selection_vector_type,
                       RustBuffer out_buf);

void CloseFilter(ModuleId module_id);

}  // namespace gandiva
