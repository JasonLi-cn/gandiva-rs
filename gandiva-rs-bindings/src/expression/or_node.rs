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
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub struct OrNode {
    children: Vec<Box<dyn TreeNode>>,
}

impl OrNode {
    pub fn create(children: Vec<Box<dyn TreeNode>>) -> Self {
        Self { children }
    }
}

impl TreeNode for OrNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let children = self
            .children
            .into_iter()
            .map(|c| c.to_protobuf())
            .collect::<GandivaResult<Vec<_>>>()?;

        let or_node = crate::proto::OrNode { args: children };

        Ok(crate::proto::TreeNode {
            or_node: Some(or_node),
            ..Default::default()
        })
    }
}
