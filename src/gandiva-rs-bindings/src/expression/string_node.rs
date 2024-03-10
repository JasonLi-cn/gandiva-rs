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

use crate::exception::gandiva_exception::GandivaResult;
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub struct StringNode {
    value: String,
}

impl StringNode {
    pub fn create(value: String) -> Self {
        Self { value }
    }
}

impl TreeNode for StringNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let string_node = crate::proto::StringNode {
            value: Some(self.value.into_bytes()),
        };

        Ok(crate::proto::TreeNode {
            string_node: Some(string_node),
            ..Default::default()
        })
    }
}
