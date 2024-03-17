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

use arrow::array::StringBuilder;
use arrow::datatypes::{DataType, Field, Schema};
use gandiva_rs_bindings::evaluator::projector::Projector;
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;

fn main() {
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
