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

#[macro_use]
extern crate criterion;

use std::iter::repeat;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch, StringArray, StringBuilder};
use arrow::compute::kernels::concat::concat;
use arrow::compute::kernels::numeric::{add, mul};
use arrow::compute::kernels::substring::substring;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use gandiva_rs_bindings::evaluator::projector::{Projector, ProjectorRef};
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;

use crate::criterion::Criterion;

// 4 * (9 + int32)
fn build_schema() -> Schema {
    let int32 = Field::new("int32", DataType::Int32, true);
    Schema::new(vec![int32])
}

// 4 * (9 + int32)
fn func_expr() -> ExpressionTree {
    let literal_node = TreeBuilder::make_int(9);
    let column_node = TreeBuilder::make_field(Field::new("int32", DataType::Int32, true));
    let add_func_node = TreeBuilder::make_function(
        "add".to_string(),
        vec![literal_node, column_node],
        DataType::Int32,
    );

    let literal_node = TreeBuilder::make_int(4);
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

fn build_columns(capacity: usize) -> Vec<ArrayRef> {
    let mut builder = Int32Array::builder(capacity);
    for i in 0..(capacity as i32) {
        if i % 4 == 0 {
            builder.append_null();
        } else {
            builder.append_value(i);
        }
    }
    let column = builder.finish();
    vec![Arc::new(column) as _]
}

fn make_projector(schema: &Schema, expr: ExpressionTree) -> ProjectorRef {
    Projector::make1(&schema, vec![expr]).unwrap()
}

fn evaluate_projector(projector: &ProjectorRef, columns: &[ArrayRef]) -> RecordBatch {
    projector.evaluate1(columns).unwrap()
}

fn evaluate_projector_by_vector(schema: SchemaRef, columns: &[ArrayRef]) -> RecordBatch {
    let column = &columns[0];

    let literal = Int32Array::new_scalar(9);
    let add_result = add(&literal, column).unwrap();

    let literal = Int32Array::new_scalar(4);
    let mul_result = mul(&literal, &add_result).unwrap();

    RecordBatch::try_new(schema, vec![mul_result]).unwrap()
}

// substr(col, 1, 3)
fn make_projector2() -> ProjectorRef {
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
    Projector::make1(&schema, vec![expr]).unwrap()
}

fn build_columns2(capacity: usize) -> Vec<ArrayRef> {
    let mut builder = StringBuilder::with_capacity(capacity, capacity);
    for i in 0..capacity {
        if i % 4 == 0 {
            builder.append_null();
        } else {
            builder.append_value(format!("AB{}CDE", i));
        }
    }
    let column = builder.finish();
    vec![Arc::new(column) as _]
}

fn evaluate_projector_by_vector2(schema: SchemaRef, columns: &[ArrayRef]) -> RecordBatch {
    let column = &columns[0];
    let result = substring(column, 1, Some(3)).unwrap();
    RecordBatch::try_new(schema, vec![result]).unwrap()
}

// concat(substr(col, 1, 3), 'OKAY')
fn make_projector3() -> ProjectorRef {
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
    let str_node = TreeBuilder::make_string("OKAY".to_string());
    let concat_func_node = TreeBuilder::make_function(
        "concat".to_string(),
        vec![substr_func_node, str_node],
        DataType::Utf8,
    );

    let expr = ExpressionTree::create(concat_func_node, Field::new("result", DataType::Utf8, true));

    // make projector
    Projector::make1(&schema, vec![expr]).unwrap()
}

fn evaluate_projector_by_vector3(
    schema: SchemaRef,
    columns: &[ArrayRef],
    literal: ArrayRef,
) -> RecordBatch {
    let column = &columns[0];
    let result = substring(column, 1, Some(3)).unwrap();
    let result = concat(&[&result, &literal]).unwrap();
    RecordBatch::try_new(schema, vec![result]).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let schema = build_schema();
    let expr = func_expr();
    c.bench_function("make projector", |b| {
        b.iter(|| criterion::black_box(make_projector(&schema, expr.clone())))
    });

    let projector = make_projector(&schema, expr);
    let columns = build_columns(1024 * 8);
    c.bench_function("4 * (9 + int32): gandiva", |b| {
        b.iter(|| criterion::black_box(evaluate_projector(&projector, &columns)))
    });

    let result_schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Int32,
        true,
    )]));
    c.bench_function("4 * (9 + int32): vector", |b| {
        b.iter(|| {
            criterion::black_box(evaluate_projector_by_vector(
                result_schema.clone(),
                &columns,
            ))
        })
    });

    let projector = make_projector2();
    let columns = build_columns2(1024 * 8);
    c.bench_function("substr(col, 1, 3): gandiva", |b| {
        b.iter(|| criterion::black_box(evaluate_projector(&projector, &columns)))
    });

    let result_schema = Arc::new(Schema::new(vec![Field::new(
        "result",
        DataType::Utf8,
        true,
    )]));
    c.bench_function("substr(col, 1, 3): vector", |b| {
        b.iter(|| {
            criterion::black_box(evaluate_projector_by_vector2(
                result_schema.clone(),
                &columns,
            ))
        })
    });

    let projector = make_projector3();
    c.bench_function("concat(substr(col, 1, 3), 'OKAY'): gandiva", |b| {
        b.iter(|| criterion::black_box(evaluate_projector(&projector, &columns)))
    });

    let literal = Arc::new(StringArray::from_iter_values(
        repeat("OKAY").take(columns[0].len()),
    )) as ArrayRef;
    c.bench_function("concat(substr(col, 1, 3), 'OKAY'): vector", |b| {
        b.iter(|| {
            criterion::black_box(evaluate_projector_by_vector3(
                result_schema.clone(),
                &columns,
                literal.clone(),
            ))
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
