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
pub struct Int64Node {
    value: i64,
}

impl Int64Node {
    pub fn create(value: i64) -> Self {
        Self { value }
    }
}

impl TreeNode for Int64Node {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let int64_node = crate::proto::Int64Node {
            value: Some(self.value),
        };

        Ok(crate::proto::TreeNode {
            int64_node: Some(int64_node),
            ..Default::default()
        })
    }
}
