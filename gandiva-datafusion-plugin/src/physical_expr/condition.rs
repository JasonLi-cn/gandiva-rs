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

use crate::physical_expr::expr::to_gandiva_tree_node;
use arrow::datatypes::Schema;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_expr::PhysicalExpr;
use gandiva_rs_bindings::evaluator::filter::{Filter, FilterRef};
use gandiva_rs_bindings::expression::condition::Condition;
use std::sync::Arc;

pub fn to_gandiva_condition(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Condition> {
    let root = to_gandiva_tree_node(expr, input_schema)?;
    Ok(Condition::create(root))
}

pub fn to_gandiva_filter(expr: &Arc<dyn PhysicalExpr>, input_schema: &Schema) -> Result<FilterRef> {
    let condition = to_gandiva_condition(expr, input_schema)?;
    Filter::make1(input_schema, condition).map_err(|e| DataFusionError::Plan(format!("{:?}", e)))
}
