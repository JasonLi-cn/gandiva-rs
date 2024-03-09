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
pub struct NullNode {
    r#type: DataType,
}

impl NullNode {
    pub fn create(r#type: DataType) -> Self {
        Self { r#type }
    }
}

impl TreeNode for NullNode {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        let null_node = crate::proto::NullNode {
            r#type: Some(ArrowTypeHelper::arrow_type_to_protobuf(self.r#type)?),
        };

        Ok(crate::proto::TreeNode {
            null_node: Some(null_node),
            ..Default::default()
        })
    }
}
