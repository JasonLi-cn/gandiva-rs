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

use arrow::datatypes::{DataType, Field};

use crate::expression::and_node::AndNode;
use crate::expression::binary_node::BinaryNode;
use crate::expression::boolean_node::BooleanNode;
use crate::expression::decimal_node::DecimalNode;
use crate::expression::double_node::DoubleNode;
use crate::expression::field_node::FieldNode;
use crate::expression::float_node::FloatNode;
use crate::expression::function_node::FunctionNode;
use crate::expression::if_node::IfNode;
use crate::expression::int64_node::Int64Node;
use crate::expression::int_node::IntNode;
use crate::expression::null_node::NullNode;
use crate::expression::or_node::OrNode;
use crate::expression::string_node::StringNode;
use crate::expression::tree_node::TreeNode;

pub struct TreeBuilder;

impl TreeBuilder {
    pub fn make_and(children: Vec<Box<dyn TreeNode>>) -> Box<dyn TreeNode> {
        Box::new(AndNode::create(children))
    }

    pub fn make_binary(value: Vec<u8>) -> Box<dyn TreeNode> {
        Box::new(BinaryNode::create(value))
    }

    pub fn make_boolean(value: bool) -> Box<dyn TreeNode> {
        Box::new(BooleanNode::create(value))
    }

    pub fn make_decimal(value: String, precision: i32, scale: i32) -> Box<dyn TreeNode> {
        Box::new(DecimalNode::create(value, precision, scale))
    }

    pub fn make_double(value: f64) -> Box<dyn TreeNode> {
        Box::new(DoubleNode::create(value))
    }

    pub fn make_field(field: Field) -> Box<dyn TreeNode> {
        Box::new(FieldNode::create(field))
    }

    pub fn make_float(value: f32) -> Box<dyn TreeNode> {
        Box::new(FloatNode::create(value))
    }

    pub fn make_function(
        function: String,
        children: Vec<Box<dyn TreeNode>>,
        ret_type: DataType,
    ) -> Box<dyn TreeNode> {
        Box::new(FunctionNode::create(function, children, ret_type))
    }

    pub fn make_if(
        condition: Box<dyn TreeNode>,
        then_node: Box<dyn TreeNode>,
        else_node: Box<dyn TreeNode>,
        ret_type: DataType,
    ) -> Box<dyn TreeNode> {
        Box::new(IfNode::create(condition, then_node, else_node, ret_type))
    }

    pub fn make_in() -> Box<dyn TreeNode> {
        todo!()
    }

    pub fn make_int(value: i32) -> Box<dyn TreeNode> {
        Box::new(IntNode::create(value))
    }

    pub fn make_int64(value: i64) -> Box<dyn TreeNode> {
        Box::new(Int64Node::create(value))
    }

    pub fn make_null(r#type: DataType) -> Box<dyn TreeNode> {
        Box::new(NullNode::create(r#type))
    }

    pub fn make_or(children: Vec<Box<dyn TreeNode>>) -> Box<dyn TreeNode> {
        Box::new(OrNode::create(children))
    }

    pub fn make_string(value: String) -> Box<dyn TreeNode> {
        Box::new(StringNode::create(value))
    }
}
