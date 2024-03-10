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
pub struct FloatNode {
    value: f32,
}

impl FloatNode {
    pub fn create(value: f32) -> Self {
        Self { value }
    }
}

impl TreeNode for FloatNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let float_node = crate::proto::FloatNode {
            value: Some(self.value),
        };

        Ok(crate::proto::TreeNode {
            float_node: Some(float_node),
            ..Default::default()
        })
    }
}
