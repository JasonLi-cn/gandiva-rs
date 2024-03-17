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

use arrow::datatypes::{DataType, Field, IntervalUnit, Schema};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::*;
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use gandiva_rs_bindings::evaluator::projector::{Projector, ProjectorRef};
use gandiva_rs_bindings::expression::expression_tree::ExpressionTree;
use gandiva_rs_bindings::expression::in_node::InValues;
use gandiva_rs_bindings::expression::primitive_nodes::DecimalNode;
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

    if let Some(binary) = any.downcast_ref::<BinaryExpr>() {
        return transform_binary_expr(binary, input_schema);
    }

    if let Some(cast) = any.downcast_ref::<CastExpr>() {
        return transform_cast_expr(cast, input_schema);
    }

    if let Some(column) = any.downcast_ref::<Column>() {
        return transform_column(column, input_schema);
    }

    if let Some(in_list) = any.downcast_ref::<InListExpr>() {
        return transform_in_list(in_list, input_schema);
    }

    if let Some(isnotnull) = any.downcast_ref::<IsNotNullExpr>() {
        return transform_as_function(
            "isnotnull",
            &[isnotnull.arg().clone()],
            DataType::Boolean,
            input_schema,
        );
    }

    if let Some(isnull) = any.downcast_ref::<IsNullExpr>() {
        return transform_as_function(
            "isnull",
            &[isnull.arg().clone()],
            DataType::Boolean,
            input_schema,
        );
    }

    if let Some(like) = any.downcast_ref::<LikeExpr>() {
        return transform_like(like, input_schema);
    }

    if let Some(literal) = any.downcast_ref::<Literal>() {
        return transform_literal(literal);
    }

    if let Some(negative) = any.downcast_ref::<NegativeExpr>() {
        return transform_as_function(
            "negative",
            &[negative.arg().clone()],
            negative.data_type(input_schema)?,
            input_schema,
        );
    }

    if let Some(not) = any.downcast_ref::<NotExpr>() {
        let not_func = transform_as_function(
            "isfalse",
            &[not.arg().clone()],
            DataType::Boolean,
            input_schema,
        )?;

        if !expr.nullable(input_schema)? {
            return Ok(not_func);
        }

        // TODO support this case
        return Err(DataFusionError::NotImplemented(
            "Unsupported not expr if nullable is true".to_string(),
        ));
    }

    if let Some(function) = any.downcast_ref::<ScalarFunctionExpr>() {
        return transform_scalar_function(function, input_schema);
    }

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

    // arrow/cpp/src/gandiva/function_registry_arithmetic.cc
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
        [BitwiseAnd, "bitwise_and"],
        [BitwiseOr, "bitwise_or"],
        [BitwiseXor, "bitwise_xor"],
    );
    Ok(node)
}

fn transform_cast_expr(cast: &CastExpr, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let children = vec![to_gandiva_tree_node(cast.expr(), input_schema)?];
    let ret_type = cast.cast_type().clone();
    let input_type = cast.expr().data_type(input_schema)?;
    let function = match (&ret_type, &input_type) {
        // Perhaps we don't need to judge here, because calling the cpp interface to create a
        // Projector or Filter will error if gandiva doesn't support it.
        // cpp code:
        // - arrow/cpp/src/gandiva/function_registry_arithmetic.cc
        // - arrow/cpp/src/gandiva/function_registry_datetime.cc
        // - cpp/src/gandiva/function_registry_string.cc
        (
            DataType::Int32,
            DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Interval(IntervalUnit::MonthDayNano)
            | DataType::Utf8
            | DataType::Binary,
        ) => "castINT",
        (
            DataType::Int64,
            DataType::Int32
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Interval(..)
            | DataType::Utf8
            | DataType::Binary,
        ) => "castBIGINT",
        (
            DataType::Float32,
            DataType::Int32
            | DataType::Int64
            | DataType::Float64
            | DataType::Utf8
            | DataType::Binary,
        ) => "castFLOAT4",
        (
            DataType::Float64,
            DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Decimal128(_, _)
            | DataType::Utf8
            | DataType::Binary,
        ) => "castFLOAT8",
        (
            DataType::Decimal128(_, _),
            DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(_, _)
            | DataType::Utf8,
        ) => "castDECIMAL",
        (DataType::Date64, DataType::Utf8 | DataType::Timestamp(..)) => "castDATE",
        (DataType::Timestamp(..), DataType::Utf8 | DataType::Date64 | DataType::Int64) => {
            "castTIMESTAMP"
        }
        (DataType::Utf8, _) => "castVARCHAR",
        (DataType::Time32(..), DataType::Utf8 | DataType::Timestamp(..) | DataType::Int32) => {
            "castTIME"
        }
        (to, from) => {
            return Err(DataFusionError::NotImplemented(format!(
                "Gandiva unsupported CastExpr from {} to {}",
                from, to
            )))
        }
    };
    Ok(TreeBuilder::make_function(
        function.to_string(),
        children,
        ret_type,
    ))
}

fn transform_in_list(in_list: &InListExpr, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let data_type = in_list.data_type(input_schema)?;
    let list = in_list.list();

    macro_rules! parse_in_values {
        ($SCALAR_VALUE_ITEM:ident, $IN_VALUE_ITEM:ident) => {{
            let mut values = Vec::with_capacity(list.len());
            for val in list {
                let literal = val.as_any().downcast_ref::<Literal>().ok_or(
                    DataFusionError::NotImplemented(format!(
                        "Gandiva unsupported InList with Non-Literal values",
                    )),
                )?;
                match literal.value() {
                    ScalarValue::$SCALAR_VALUE_ITEM(Some(n)) => {
                        values.push(n.clone());
                    }
                    other => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Gandiva InList values expected {}, but found {}",
                            data_type, other
                        )))
                    }
                }
            }
            InValues::$IN_VALUE_ITEM(values)
        }};
    }

    macro_rules! parse_in_values_decimal {
        ($SCALAR_VALUE_ITEM:ident) => {{
            let mut values = Vec::with_capacity(list.len());
            for val in list {
                let literal = val.as_any().downcast_ref::<Literal>().ok_or(
                    DataFusionError::NotImplemented(format!(
                        "Gandiva unsupported InList with Non-Literal values",
                    )),
                )?;
                match literal.value() {
                    ScalarValue::$SCALAR_VALUE_ITEM(Some(n), precision, scale) => {
                        values.push(DecimalNode::create(
                            n.to_string(),
                            *precision as _,
                            *scale as _,
                        ));
                    }
                    other => {
                        return Err(DataFusionError::NotImplemented(format!(
                            "Gandiva InList values expected {}, but found {}",
                            data_type, other
                        )))
                    }
                }
            }
            InValues::Decimal(values)
        }};
    }

    let values = match data_type {
        DataType::Int32 => parse_in_values!(Int32, Int32),
        DataType::Int64 => parse_in_values!(Int64, Int64),
        DataType::Utf8 => parse_in_values!(Utf8, String),
        DataType::Binary => parse_in_values!(Binary, Binary),
        DataType::Float32 => parse_in_values!(Float32, Float32),
        DataType::Float64 => parse_in_values!(Float64, Float64),
        DataType::Decimal128(_, _) => parse_in_values_decimal!(Decimal128),
        DataType::Decimal256(_, _) => parse_in_values_decimal!(Decimal256),
        _ => {
            return Err(DataFusionError::NotImplemented(format!(
                "Gandiva InList unsupported type {}",
                data_type
            )))
        }
    };

    let input = to_gandiva_tree_node(in_list.expr(), input_schema)?;

    Ok(TreeBuilder::make_in(input, values))
}

fn transform_scalar_function(
    function: &ScalarFunctionExpr,
    input_schema: &Schema,
) -> Result<Box<dyn TreeNode>> {
    transform_as_function(
        function.name(),
        function.args(),
        function.return_type().clone(),
        input_schema,
    )
}

fn transform_like(like: &LikeExpr, input_schema: &Schema) -> Result<Box<dyn TreeNode>> {
    let name = if like.case_insensitive() {
        // TODO "ilike", Gandiva panic: (signal: 11, SIGSEGV: invalid memory reference)
        // Associated code: gandiva-rs-bindings/arrow/cpp/src/gandiva/regex_functions_holder.h
        return Err(DataFusionError::NotImplemented(
            "Unsupported 'ilike'".to_string(),
        ));
    } else {
        "like"
    };

    let like_func = transform_as_function(
        name,
        &[like.expr().clone(), like.pattern().clone()],
        DataType::Boolean,
        input_schema,
    )?;

    if !like.negated() {
        return Ok(like_func);
    }

    let not_like_func =
        TreeBuilder::make_function("isfalse".to_string(), vec![like_func], DataType::Boolean);

    Ok(not_like_func)
}

fn transform_literal(literal: &Literal) -> Result<Box<dyn TreeNode>> {
    let value = literal.value();
    let data_type = value.data_type();

    if value.is_null() {
        return Ok(TreeBuilder::make_null(data_type));
    }

    Ok(match value {
        ScalarValue::Boolean(Some(val)) => TreeBuilder::make_boolean(*val),
        ScalarValue::UInt8(Some(val)) => TreeBuilder::make_uint8(*val),
        ScalarValue::UInt16(Some(val)) => TreeBuilder::make_uint16(*val),
        ScalarValue::UInt32(Some(val)) => TreeBuilder::make_uint32(*val),
        ScalarValue::UInt64(Some(val)) => TreeBuilder::make_uint64(*val),
        ScalarValue::Int8(Some(val)) => TreeBuilder::make_int8(*val),
        ScalarValue::Int16(Some(val)) => TreeBuilder::make_int16(*val),
        ScalarValue::Int32(Some(val)) => TreeBuilder::make_int32(*val),
        ScalarValue::Int64(Some(val)) => TreeBuilder::make_int64(*val),
        ScalarValue::Float32(Some(val)) => TreeBuilder::make_float32(*val),
        ScalarValue::Float64(Some(val)) => TreeBuilder::make_float64(*val),
        ScalarValue::Utf8(Some(val)) => TreeBuilder::make_string(val.clone()),
        ScalarValue::Binary(Some(val)) => TreeBuilder::make_binary(val.clone()),
        ScalarValue::Decimal128(Some(val), precision, scale) => {
            TreeBuilder::make_decimal(val.to_string(), *precision as _, *scale as _)
        }
        ScalarValue::Decimal256(Some(val), precision, scale) => {
            TreeBuilder::make_decimal(val.to_string(), *precision as _, *scale as _)
        }
        other => {
            return Err(DataFusionError::NotImplemented(format!(
                "Gandiva unsupported Literal with type: {}",
                other.data_type()
            )))
        }
    })
}

fn transform_as_function(
    name: &str,
    args: &[Arc<dyn PhysicalExpr>],
    ret_type: DataType,
    input_schema: &Schema,
) -> Result<Box<dyn TreeNode>> {
    let children = args
        .iter()
        .map(|arg| to_gandiva_tree_node(arg, input_schema))
        .collect::<Result<Vec<_>>>()?;
    Ok(TreeBuilder::make_function(
        name.to_string(),
        children,
        ret_type,
    ))
}
