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

use arrow::datatypes::DataType;

use crate::exception::gandiva_exception::GandivaResult;
use crate::expression::arrow_type_helper::ArrowTypeHelper;
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub struct IfNode {
    condition: Box<dyn TreeNode>,
    then_node: Box<dyn TreeNode>,
    else_node: Box<dyn TreeNode>,
    ret_type: DataType,
}

impl IfNode {
    pub fn create(
        condition: Box<dyn TreeNode>,
        then_node: Box<dyn TreeNode>,
        else_node: Box<dyn TreeNode>,
        ret_type: DataType,
    ) -> Self {
        Self {
            condition,
            then_node,
            else_node,
            ret_type,
        }
    }
}

impl TreeNode for IfNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let if_node = crate::proto::IfNode {
            cond: Some(Box::new(self.condition.to_protobuf()?)),
            then_node: Some(Box::new(self.then_node.to_protobuf()?)),
            else_node: Some(Box::new(self.else_node.to_protobuf()?)),
            return_type: Some(ArrowTypeHelper::arrow_type_to_protobuf(self.ret_type)?),
        };

        Ok(crate::proto::TreeNode {
            if_node: Some(Box::new(if_node)),
            ..Default::default()
        })
    }
}
