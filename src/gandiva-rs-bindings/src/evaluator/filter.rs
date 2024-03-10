/*
 * Copyright 2024 JasonLi-cn
 *
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

use std::sync::Arc;

use arrow::array::ArrayRef;
use arrow::datatypes::Schema;

use crate::bindings::{gandiva_ProtoMessage, gandiva_RustBuffer};
use crate::evaluator::buffer::to_g_buffers;
use crate::evaluator::selection_vector::SelectionVector;
use crate::evaluator::{ConfigId, ModuleId, DEFAULT_CONFIG_ID};
use crate::exception::gandiva_exception::GandivaResult;
use crate::expression::arrow_type_helper::ArrowTypeHelper;
use crate::expression::condition::Condition;
use crate::proto::utils::to_bytes;

pub type FilterRef = Arc<Filter>;

pub struct Filter {
    module_id: ModuleId,
}

impl Drop for Filter {
    fn drop(&mut self) {
        unsafe {
            crate::bindings::gandiva_CloseFilter(self.module_id);
        }
    }
}

impl Filter {
    pub fn make1(schema: &Schema, condition: Condition) -> GandivaResult<FilterRef> {
        Self::make0(schema, condition, DEFAULT_CONFIG_ID)
    }

    pub fn make0(
        schema: &Schema,
        condition: Condition,
        config_id: ConfigId,
    ) -> GandivaResult<FilterRef> {
        // Schema
        let mut schema_bytes = to_bytes(ArrowTypeHelper::arrow_schema_to_protobuf(schema)?);
        let pb_schema = gandiva_ProtoMessage {
            addr: schema_bytes.as_mut_ptr(),
            len: schema_bytes.len() as _,
        };

        // Condition
        let mut condition_bytes = to_bytes(condition.to_protobuf()?);
        let pb_condition = gandiva_ProtoMessage {
            addr: condition_bytes.as_mut_ptr(),
            len: condition_bytes.len() as _,
        };

        let module_id =
            unsafe { crate::bindings::gandiva_BuildFilter(pb_schema, pb_condition, config_id) };

        Ok(Arc::new(Self { module_id }))
    }

    pub fn evaluate0(&self, columns: &[ArrayRef]) -> GandivaResult<SelectionVector> {
        let num_rows = columns[0].len();

        // Input Data
        let iter = columns.iter().map(|c| c.to_data());
        let mut in_bufs = to_g_buffers(iter);

        // Output Data
        let mut selection_vector = SelectionVector::create(num_rows);
        let selection_vector_type = selection_vector.ty();
        let out_buf = gandiva_RustBuffer {
            addr: selection_vector.addr(),
            size: selection_vector.size(),
        };

        // Evaluate
        let record_count = unsafe {
            crate::bindings::gandiva_EvaluateFilter(
                self.module_id,
                num_rows as _,
                in_bufs.as_mut_ptr(),
                in_bufs.len() as _,
                selection_vector_type,
                out_buf,
            )
        };

        if record_count == -1 {
            dbg!("Evaluate Filter Error!");
        }

        selection_vector.set_record_count(record_count);

        Ok(selection_vector)
    }
}
