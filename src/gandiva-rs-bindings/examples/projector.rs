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

use arrow::array::{Array, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use gandiva_rs_bindings::bindings::{gandiva_ProtoMessage, gandiva_RustMutableBuffers};
use gandiva_rs_bindings::evaluator::buffer::{
    rust_reserve, to_g_buffers, to_g_mutable_buffer, to_g_mutable_buffers,
};
use gandiva_rs_bindings::evaluator::selection_vector::SelectionVector;
use gandiva_rs_bindings::evaluator::utils::{new_buffers, to_arrays};
use gandiva_rs_bindings::expression::arrow_type_helper::ArrowTypeHelper;
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::field_node::FieldNode;
use gandiva_rs_bindings::expression::function_node::FunctionNode;
use gandiva_rs_bindings::expression::int_node::IntNode;
use prost::Message;

fn main() {
    let mut schema_bytes = build_schema();
    let mut exprs_bytes = build_exprs();
    let selection_vector_type = 0;
    let config_id = 1;

    let pb_schema = gandiva_ProtoMessage {
        addr: schema_bytes.as_mut_ptr(),
        len: schema_bytes.len() as _,
    };

    let pb_exprs = gandiva_ProtoMessage {
        addr: exprs_bytes.as_mut_ptr(),
        len: exprs_bytes.len() as _,
    };

    let module_id = unsafe {
        gandiva_rs_bindings::bindings::gandiva_BuildProjector(
            pb_schema,
            pb_exprs,
            selection_vector_type,
            config_id,
        )
    };

    // evaluate

    let num_rows = 4;
    let mut builder = Int32Array::builder(num_rows as usize);
    for i in 0..num_rows {
        if i % 2 == 0 {
            builder.append_value(i as i32);
        } else {
            builder.append_null();
        }
    }
    let array = builder.finish();
    let mut in_bufs = to_g_buffers(vec![array.to_data()]);
    let sel_vec_info = SelectionVector::default().into();

    let buffers = new_buffers(&[DataType::Int32], num_rows);
    let g_mutable_buffers = to_g_mutable_buffers(&buffers);
    let mutable_buffers = &g_mutable_buffers as *const _ as *mut gandiva_RustMutableBuffers;
    let mut out_bufs = to_g_mutable_buffer(&buffers);

    unsafe {
        gandiva_rs_bindings::bindings::gandiva_EvaluateProjector(
            module_id,
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

    let out_columns = to_arrays(&[DataType::Int32], buffers, num_rows);
    let record_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "result",
            DataType::Int32,
            true,
        )])),
        out_columns,
    )
    .unwrap();
    arrow::util::pretty::print_batches(&[record_batch]).unwrap();
}

fn build_schema() -> Vec<u8> {
    let field = Field::new("num", DataType::Int32, true);
    let schema = Schema::new(vec![field]);
    let pb = ArrowTypeHelper::arrow_schema_to_protobuf(&schema).unwrap();
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}

fn build_exprs() -> Vec<u8> {
    let function = "add".to_string();
    let field_node = FieldNode::create(Field::new("num", DataType::Int32, true));
    let int_node = IntNode::create(100);
    let children = vec![Box::new(field_node) as _, Box::new(int_node) as _];
    let ret_type = DataType::Int32;
    let func_node = FunctionNode::create(function, children, ret_type);

    let result_field = Field::new("result", DataType::Int32, true);
    let expression = ExpressionTree::create(Box::new(func_node), result_field);

    let expressions = gandiva_rs_bindings::proto::ExpressionList {
        exprs: vec![expression.to_protobuf().unwrap()],
    };

    let pb = expressions;
    let mut buf = Vec::new();
    pb.encode(&mut buf).unwrap();
    buf
}
