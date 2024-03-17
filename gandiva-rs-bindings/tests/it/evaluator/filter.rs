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

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use gandiva_rs_bindings::evaluator::filter::Filter;
use gandiva_rs_bindings::expression::condition::Condition;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;

#[test]
fn test_filter() {
    let schema = Schema::new(vec![Field::new("int32", DataType::Int32, false)]);

    let field = TreeBuilder::make_field(Field::new("int32", DataType::Int32, false));
    let literal = TreeBuilder::make_int32(4);
    let root = TreeBuilder::make_function(
        "less_than".to_string(),
        vec![field, literal],
        DataType::Boolean,
    );
    let condition = Condition::create(root);

    let filter = Filter::make1(&schema, condition).unwrap();

    let column = Int32Array::from(vec![10, 11, 2, 3, 4, 5, 6, 7]);
    let columns = vec![Arc::new(column) as _];
    let result = filter.evaluate0(&columns).unwrap();

    let array = result.clone().to_array();
    arrow::util::pretty::print_columns("result", &[array]).unwrap();

    let array = Arc::new(result.to_boolean_array());
    arrow::util::pretty::print_columns("result", &[array]).unwrap();
}
