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

#[derive(Clone, Eq, PartialEq, Hash)]
pub struct DecimalNode {
    value: String,
    precision: i32,
    scale: i32,
}

impl DecimalNode {
    pub fn create(value: String, precision: i32, scale: i32) -> Self {
        Self {
            value,
            precision,
            scale,
        }
    }

    pub fn to_pb(self) -> crate::proto::DecimalNode {
        crate::proto::DecimalNode {
            value: Some(self.value),
            precision: Some(self.precision),
            scale: Some(self.scale),
        }
    }
}

impl TreeNode for DecimalNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let decimal_node = crate::proto::DecimalNode {
            value: Some(self.value),
            precision: Some(self.precision),
            scale: Some(self.scale),
        };

        Ok(crate::proto::TreeNode {
            decimal_node: Some(decimal_node),
            ..Default::default()
        })
    }
}
