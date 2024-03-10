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

use crate::error::gandiva_error::GandivaResult;
use crate::expression::arrow_type_helper::ArrowTypeHelper;
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub struct FunctionNode {
    function: String,
    children: Vec<Box<dyn TreeNode>>,
    ret_type: DataType,
}

impl FunctionNode {
    pub fn create(function: String, children: Vec<Box<dyn TreeNode>>, ret_type: DataType) -> Self {
        Self {
            function,
            children,
            ret_type,
        }
    }
}

impl TreeNode for FunctionNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let in_args = self
            .children
            .into_iter()
            .map(|c| c.to_protobuf())
            .collect::<GandivaResult<Vec<_>>>()?;

        let fn_node = crate::proto::FunctionNode {
            function_name: Some(self.function),
            in_args,
            return_type: Some(ArrowTypeHelper::arrow_type_to_protobuf(self.ret_type)?),
        };

        Ok(crate::proto::TreeNode {
            fn_node: Some(fn_node),
            ..Default::default()
        })
    }
}
