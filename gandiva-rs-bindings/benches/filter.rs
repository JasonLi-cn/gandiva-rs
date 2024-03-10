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

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use gandiva_rs_bindings::evaluator::filter::{Filter, FilterRef};
use gandiva_rs_bindings::evaluator::selection_vector::SelectionVector;
use gandiva_rs_bindings::expression::condition::Condition;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;

use crate::criterion::Criterion;

fn build_schema() -> Schema {
    let int32 = Field::new("int32", DataType::Int32, false);
    Schema::new(vec![int32])
}

fn build_condition() -> Condition {
    let field = TreeBuilder::make_field(Field::new("int32", DataType::Int32, false));
    let literal = TreeBuilder::make_int(5846);
    let root = TreeBuilder::make_function(
        "less_than".to_string(),
        vec![field, literal],
        DataType::Boolean,
    );
    Condition::create(root)
}

fn build_condition2() -> Condition {
    let field = TreeBuilder::make_field(Field::new("int32", DataType::Int32, false));
    let literal = TreeBuilder::make_int(5846);
    let less_than = TreeBuilder::make_function(
        "less_than".to_string(),
        vec![field.clone(), literal],
        DataType::Boolean,
    );

    let literal = TreeBuilder::make_int(891);
    let greater_than = TreeBuilder::make_function(
        "greater_than".to_string(),
        vec![field, literal],
        DataType::Boolean,
    );

    let and = TreeBuilder::make_and(vec![less_than, greater_than]);
    Condition::create(and)
}

fn build_columns(capacity: usize) -> Vec<ArrayRef> {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let mut builder = Int32Array::builder(capacity);
    for _ in 0..(capacity as i32) {
        let i = rng.gen_range(0..65536);
        builder.append_value(i);
    }
    let column = builder.finish();
    vec![Arc::new(column) as _]
}

fn make_filter(schema: &Schema, condition: Condition) -> FilterRef {
    Filter::make1(&schema, condition).unwrap()
}

fn evaluate_filter(filter: &FilterRef, columns: &[ArrayRef]) -> SelectionVector {
    filter.evaluate0(columns).unwrap()
}

fn evaluate_filter_by_vector(columns: &[ArrayRef]) -> BooleanArray {
    let column = &columns[0];
    let literal = Int32Array::new_scalar(5846);
    arrow::compute::kernels::cmp::lt(column, &literal).unwrap()
}

fn evaluate_filter_by_vector2(columns: &[ArrayRef]) -> BooleanArray {
    let column = &columns[0];
    let literal = Int32Array::new_scalar(5846);
    let less_than = arrow::compute::kernels::cmp::lt(column, &literal).unwrap();

    let literal = Int32Array::new_scalar(891);
    let greater_than = arrow::compute::kernels::cmp::gt(column, &literal).unwrap();

    arrow::compute::kernels::boolean::and(&less_than, &greater_than).unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    let schema = build_schema();
    let condition = build_condition();
    c.bench_function("make filter", |b| {
        b.iter(|| criterion::black_box(make_filter(&schema, condition.clone())))
    });

    let filter = make_filter(&schema, condition);
    let columns = build_columns(8192);
    c.bench_function("int32 < 5846: gandiva", |b| {
        b.iter(|| criterion::black_box(evaluate_filter(&filter, &columns)))
    });

    c.bench_function("int32 < 5846: vector", |b| {
        b.iter(|| criterion::black_box(evaluate_filter_by_vector(&columns)))
    });

    let filter = make_filter(&schema, build_condition2());
    c.bench_function("int32 < 5846 and int32 > 891: gandiva", |b| {
        b.iter(|| criterion::black_box(evaluate_filter(&filter, &columns)))
    });

    c.bench_function("int32 < 5846 and int32 > 891: vector", |b| {
        b.iter(|| criterion::black_box(evaluate_filter_by_vector2(&columns)))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
