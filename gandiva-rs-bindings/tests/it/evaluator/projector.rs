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

use arrow::array::{Int32Array, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use gandiva_rs_bindings::evaluator::projector::Projector;
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;

pub fn build_schema() -> Schema {
    let int32 = Field::new("int32", DataType::Int32, true);
    Schema::new(vec![int32])
}

// 4 * (9 + int32)
fn func_expr() -> ExpressionTree {
    let literal_node = TreeBuilder::make_int32(9);
    let column_node = TreeBuilder::make_field(Field::new("int32", DataType::Int32, true));
    let add_func_node = TreeBuilder::make_function(
        "add".to_string(),
        vec![literal_node, column_node],
        DataType::Int32,
    );

    let literal_node = TreeBuilder::make_int32(4);
    let multiply_func_node = TreeBuilder::make_function(
        "multiply".to_string(),
        vec![literal_node, add_func_node],
        DataType::Int32,
    );

    ExpressionTree::create(
        multiply_func_node,
        Field::new("result", DataType::Int32, true),
    )
}

#[test]
fn test_make() {
    let schema = build_schema();
    let expr = func_expr();
    let projector = Projector::make1(&schema, vec![expr]).unwrap();

    let capacity = 32;
    let mut builder = Int32Array::builder(capacity);
    for i in 0..(capacity as i32) {
        if i % 4 == 0 {
            builder.append_null();
        } else {
            builder.append_value(i);
        }
    }
    let column = builder.finish();
    let columns = vec![Arc::new(column) as _];

    let result = projector.evaluate1(&columns).unwrap();
    arrow::util::pretty::print_batches(&[result]).unwrap();

    // Vector
    let schema = Arc::new(arrow::datatypes::Schema::new(vec![Field::new(
        "result",
        DataType::Int32,
        true,
    )]));

    let column = &columns[0];
    let literal = Int32Array::new_scalar(9);
    let add_result = arrow::compute::kernels::numeric::add(&literal, column).unwrap();
    let literal = Int32Array::new_scalar(4);
    let mul_result = arrow::compute::kernels::numeric::mul(&literal, &add_result).unwrap();
    let result = arrow::array::RecordBatch::try_new(schema, vec![mul_result]).unwrap();
    arrow::util::pretty::print_batches(&[result]).unwrap();
}

#[test]
fn test_string_function() {
    // schema
    let name = Field::new("name", DataType::Utf8, true);
    let schema = Schema::new(vec![name.clone()]);

    // expr
    let column_node = TreeBuilder::make_field(name.clone());
    let offset_node = TreeBuilder::make_int64(1);
    let length_node = TreeBuilder::make_int64(3);
    let substr_func_node = TreeBuilder::make_function(
        "substr".to_string(),
        vec![column_node, offset_node, length_node],
        DataType::Utf8,
    );
    let expr = ExpressionTree::create(substr_func_node, Field::new("result", DataType::Utf8, true));

    // make projector
    let projector = Projector::make1(&schema, vec![expr]).unwrap();

    // data
    let capacity = 32;
    let mut builder = StringBuilder::with_capacity(capacity, capacity);
    for i in 0..capacity {
        if i % 4 == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("AB{}CDE", i));
        }
    }
    let column = builder.finish();
    let columns = vec![Arc::new(column) as _];
    arrow::util::pretty::print_columns("input", &columns).unwrap();

    let result = projector.evaluate1(&columns).unwrap();
    arrow::util::pretty::print_batches(&[result]).unwrap();
}
