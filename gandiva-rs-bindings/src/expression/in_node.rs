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

use crate::error::gandiva_error::GandivaResult;
use crate::expression::primitive_nodes::DecimalNode;
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub enum InValues {
    Int32(Vec<i32>),
    Int64(Vec<i64>),
    Decimal(Vec<DecimalNode>),
    String(Vec<String>),
    Binary(Vec<Vec<u8>>),
    Float32(Vec<f32>),
    Float64(Vec<f64>),
}

#[derive(Clone)]
pub struct InNode {
    input: Box<dyn TreeNode>,
    values: InValues,
}

impl InNode {
    pub fn create(input: Box<dyn TreeNode>, values: InValues) -> Self {
        Self { input, values }
    }
}

impl TreeNode for InNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let input = self.input.to_protobuf()?;

        macro_rules! build_in_node {
            ($([$TYPE:ident, $NODE:ident, $CONST:ident, $CONST_FIELD:tt]),* ) => {
                match self.values {
                    $(InValues::$TYPE(values) => {
                        let $CONST_FIELD = values
                            .into_iter()
                            .map(|v| crate::proto::$NODE { value: Some(v.into()) })
                            .collect::<Vec<_>>();
                        Some(Box::new(crate::proto::InNode {
                            node: Some(Box::new(input)),
                            $CONST_FIELD: Some(crate::proto::$CONST { $CONST_FIELD }),
                            ..Default::default()
                        }))
                    },)*
                    InValues::Decimal(values) => {
                        let decimal_values = values
                            .into_iter()
                            .map(|v| v.to_pb())
                            .collect::<Vec<_>>();
                        Some(Box::new(crate::proto::InNode {
                            node: Some(Box::new(input)),
                            decimal_values: Some(crate::proto::DecimalConstants { decimal_values }),
                            ..Default::default()
                        }))
                    }
                }
            };
        }

        let in_node = build_in_node!(
            [Int32, Int32Node, Int32Constants, int32_values],
            [Int64, Int64Node, Int64Constants, int64_values],
            [String, StringNode, StringConstants, string_values],
            [Binary, BinaryNode, BinaryConstants, binary_values],
            [Float32, Float32Node, Float32Constants, float32_values],
            [Float64, Float64Node, Float64Constants, float64_values]
        );

        Ok(crate::proto::TreeNode {
            in_node,
            ..Default::default()
        })
    }
}
