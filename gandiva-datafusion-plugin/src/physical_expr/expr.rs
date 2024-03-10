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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use gandiva_rs_bindings::evaluator::projector::{Projector, ProjectorRef};
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::tree_builder::TreeBuilder;
use gandiva_rs_bindings::expression::tree_node::TreeNode;
use std::sync::Arc;

pub fn to_gandiva_projector(
    exprs: &[(Arc<dyn PhysicalExpr>, String)],
    input_schema: &Schema,
    out_schema: &Schema,
) -> Result<ProjectorRef> {
    let exprs = to_gandiva_exprs(exprs, input_schema, out_schema)?;
    Projector::make1(input_schema, exprs).map_err(|e| DataFusionError::Plan(format!("{:?}", e)))
}

pub fn to_gandiva_exprs(
    exprs: &[(Arc<dyn PhysicalExpr>, String)],
    input_schema: &Schema,
    out_schema: &Schema,
) -> Result<Vec<ExpressionTree>> {
    exprs
        .iter()
        .enumerate()
        .map(|(i, (expr, _))| to_gandiva_expr(expr, i, input_schema, out_schema))
        .collect::<Result<Vec<_>>>()
}

pub fn to_gandiva_expr(
    expr: &Arc<dyn PhysicalExpr>,
    expr_index: usize,
    input_schema: &Schema,
    out_schema: &Schema,
) -> Result<ExpressionTree> {
    let root = to_gandiva_tree_node(expr, input_schema)?;
    let result_field = out_schema.field(expr_index).clone();
    Ok(ExpressionTree::create(root, result_field))
}

pub fn to_gandiva_tree_node(
    expr: &Arc<dyn PhysicalExpr>,
    input_schema: &Schema,
) -> Result<Box<dyn TreeNode>> {
    let any = expr.as_any();
    if let Some(column) = any.downcast_ref::<Column>() {
        return transform_column(column, input_schema);
    }

    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return transform_binary_expr(binary, input_schema);
    }

    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return transform_cast_expr(cast, input_schema);
    }

    if let Some(function) = any.downcast_ref::<ScalarFunctionExpr>() {
        return transform_scalar_function(function, input_schema);
    }

    if let Some(literal) = any.downcast_ref::<Literal>() {
        return transform_literal(literal);
    }

    // TODO other expr

    Err(DataFusionError::NotImplemented(format!(
        "Gandiva unsupported expr: {:?}",
        expr
    )))
}

fn transform_column(column: &Column, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let data_type = column.data_type(input_schema)?;
    let nullable = column.nullable(input_schema)?;
    Ok(TreeBuilder::make_field(Field::new(
        column.name(),
        data_type,
        nullable,
    )))
}

fn transform_binary_expr(binary: &BinaryExpr, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let op = binary.op();
    let left = to_gandiva_tree_node(binary.left(), input_schema)?;
    let right = to_gandiva_tree_node(binary.right(), input_schema)?;
    let children = vec![left, right];
    let ret_type = binary.data_type(input_schema)?;

    macro_rules! make_tree_node {
        ($([$OP:ident, $FUNC_NAME:expr],)*) => {{
            match op {
                Operator::And => {
                    TreeBuilder::make_and(children)
                }
                Operator::Or => {
                    TreeBuilder::make_or(children)
                }
                $(Operator::$OP => {
                    let function = $FUNC_NAME.to_string();
                    TreeBuilder::make_function(function, children, ret_type)
                })*
                other => return Err(DataFusionError::NotImplemented(format!(
                    "Gandiva unsupported BinaryExpr Operator: {:?}",
                    other
                ))),
            }
        }};
    }

    let node = make_tree_node!(
        [Eq, "equal"],
        [NotEq, "not_equal"],
        [Lt, "less_than"],
        [LtEq, "less_than_or_equal_to"],
        [Gt, "greater_than"],
        [GtEq, "greater_than_or_equal_to"],
        [Plus, "add"],
        [Minus, "subtract"],
        [Multiply, "multiply"],
        [Divide, "div"],
        [Modulo, "mod"],
        [LikeMatch, "like"],
        [ILikeMatch, "ilike"],
    );
    Ok(node)
}

fn transform_cast_expr(cast: &CastExpr, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let children = vec![to_gandiva_tree_node(cast.expr(), input_schema)?];
    let ret_type = cast.cast_type().clone();
    let function = match cast.cast_type() {
        DataType::Int32 => "castINT",
        DataType::Int64 => "castBIGINT",
        DataType::Float32 => "castFLOAT4",
        DataType::Float64 => "castFLOAT8",
        other => unreachable!("Gandiva unsupported CastExpr {}", other), // TODO
    };
    Ok(TreeBuilder::make_function(
        function.to_string(),
        children,
        ret_type,
    ))
}

fn transform_scalar_function(
    function: &ScalarFunctionExpr,
    input_schema: &Schema,
) -> Result<Box<dyn TreeNode>> {
    let children = function
        .args()
        .iter()
        .map(|arg| to_gandiva_tree_node(arg, input_schema))
        .collect::<Result<Vec<_>>>()?;
    let ret_type = function.return_type().clone();
    let name = function.name().to_string();
    Ok(TreeBuilder::make_function(name, children, ret_type))
}

fn transform_literal(literal: &Literal) -> Result<Box<dyn TreeNode>> {
    let value = literal.value();
    let data_type = value.data_type();

    if value.is_null() {
        return Ok(TreeBuilder::make_null(data_type));
    }

    Ok(match value {
        ScalarValue::Boolean(Some(val)) => TreeBuilder::make_boolean(*val),
        ScalarValue::Float32(Some(val)) => TreeBuilder::make_float(*val),
        ScalarValue::Float64(Some(val)) => TreeBuilder::make_double(*val),
        ScalarValue::Decimal128(Some(val), precision, scale) => {
            TreeBuilder::make_decimal(val.to_string(), *precision as _, *scale as _)
        }
        ScalarValue::Decimal256(Some(val), precision, scale) => {
            TreeBuilder::make_decimal(val.to_string(), *precision as _, *scale as _)
        }
        ScalarValue::Int32(Some(val)) => TreeBuilder::make_int(*val),
        ScalarValue::Int64(Some(val)) => TreeBuilder::make_int64(*val),
        ScalarValue::Utf8(Some(val)) => TreeBuilder::make_string(val.clone()),
        _ => unreachable!(),
    })
}
