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

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{DataType, Schema, SchemaRef};

use crate::bindings::{gandiva_ProtoMessage, gandiva_RustMutableBuffers};
use crate::error::gandiva_error::{GandivaError, GandivaResult};
use crate::evaluator::buffer::{
    rust_reserve, to_g_buffers, to_g_mutable_buffer, to_g_mutable_buffers,
};
use crate::evaluator::selection_vector::{SelectionVector, SelectionVectorType};
use crate::evaluator::utils::{new_buffers, to_arrays};
use crate::evaluator::{ConfigId, ModuleId, DEFAULT_CONFIG_ID};
use crate::expression::arrow_type_helper::ArrowTypeHelper;
use crate::expression::expression_tree::ExpressionTree;
use crate::proto::utils::to_bytes;

pub type ProjectorRef = Arc<Projector>;

#[derive(Debug)]
pub struct Projector {
    module_id: ModuleId,
    result_schema: SchemaRef,
    result_data_types: Vec<DataType>,
}

impl Drop for Projector {
    fn drop(&mut self) {
        unsafe {
            crate::bindings::gandiva_CloseProjector(self.module_id);
        }
    }
}

impl Projector {
    pub fn make1(schema: &Schema, exprs: Vec<ExpressionTree>) -> GandivaResult<ProjectorRef> {
        Self::make0(
            schema,
            exprs,
            SelectionVectorType::SvNone,
            DEFAULT_CONFIG_ID,
        )
    }

    pub fn make0(
        schema: &Schema,
        exprs: Vec<ExpressionTree>,
        selection_vector_type: SelectionVectorType,
        config_id: ConfigId,
    ) -> GandivaResult<ProjectorRef> {
        if schema.fields().is_empty() {
            return Err(GandivaError::ProjectionMake(
                "Projector input fields can't be empty".to_string(),
            ));
        }

        // Schema
        let mut schema_bytes = to_bytes(ArrowTypeHelper::arrow_schema_to_protobuf(schema)?);
        let pb_schema = gandiva_ProtoMessage {
            addr: schema_bytes.as_mut_ptr(),
            len: schema_bytes.len() as _,
        };

        // Result Schema
        let result_fields = exprs
            .iter()
            .map(|et| et.result_field().clone())
            .collect::<Vec<_>>();
        let result_schema = SchemaRef::new(Schema::new(result_fields));

        // Exprs
        let exprs = exprs
            .into_iter()
            .map(|expr| expr.to_protobuf())
            .collect::<GandivaResult<Vec<_>>>()?;
        let mut exprs_bytes = to_bytes(crate::proto::ExpressionList { exprs });
        let pb_exprs = gandiva_ProtoMessage {
            addr: exprs_bytes.as_mut_ptr(),
            len: exprs_bytes.len() as _,
        };

        // Make
        let module_id = unsafe {
            crate::bindings::gandiva_BuildProjector(
                pb_schema,
                pb_exprs,
                selection_vector_type as _,
                config_id,
            )
        };

        let result_data_types = result_schema
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect::<Vec<_>>();

        Ok(Arc::new(Self {
            module_id,
            result_schema,
            result_data_types,
        }))
    }

    pub fn evaluate1(&self, columns: &[ArrayRef]) -> GandivaResult<RecordBatch> {
        self.evaluate0(columns, SelectionVector::default())
    }

    pub fn evaluate0(
        &self,
        columns: &[ArrayRef],
        selection_vector: SelectionVector,
    ) -> GandivaResult<RecordBatch> {
        let num_rows = columns[0].len();

        // Input Data
        let iter = columns.iter().map(|c| c.to_data());
        let mut in_bufs = to_g_buffers(iter);

        // Output Data
        let buffers = new_buffers(&self.result_data_types, num_rows);
        let g_mutable_buffers = to_g_mutable_buffers(&buffers);
        let mutable_buffers = &g_mutable_buffers as *const _ as *mut gandiva_RustMutableBuffers;
        let mut out_bufs = to_g_mutable_buffer(&buffers);

        // Selection Vector
        let sel_vec_info = selection_vector.into();

        // Evaluate
        unsafe {
            crate::bindings::gandiva_EvaluateProjector(
                self.module_id,
                num_rows as _,
                in_bufs.as_mut_ptr(),
                in_bufs.len() as _,
                sel_vec_info,
                out_bufs.as_mut_ptr(),
                out_bufs.len() as _,
                mutable_buffers,
                Some(rust_reserve),
            );
        }

        let out_columns = to_arrays(&self.result_data_types, buffers, num_rows);
        let record_batch = RecordBatch::try_new(self.result_schema.clone(), out_columns)
            .map_err(GandivaError::CreateRecordBatch)?;

        Ok(record_batch)
    }
}
