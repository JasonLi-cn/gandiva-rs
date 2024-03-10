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

use arrow::datatypes::Field;

use crate::error::gandiva_error::GandivaResult;
use crate::expression::arrow_type_helper::ArrowTypeHelper;
use crate::expression::tree_node::TreeNode;

#[derive(Clone)]
pub struct ExpressionTree {
    root: Box<dyn TreeNode>,
    result_field: Field,
}

impl ExpressionTree {
    pub fn create(root: Box<dyn TreeNode>, result_field: Field) -> Self {
        Self { root, result_field }
    }

    pub fn to_protobuf(self) -> GandivaResult<crate::proto::ExpressionRoot> {
        Ok(crate::proto::ExpressionRoot {
            root: Some(self.root.to_protobuf()?),
            result_type: Some(ArrowTypeHelper::arrow_field_to_protobuf(
                &self.result_field,
            )?),
        })
    }

    pub fn result_field(&self) -> &Field {
        &self.result_field
    }
}
