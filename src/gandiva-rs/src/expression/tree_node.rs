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

pub trait TreeNode: TreeNodeBoxed + TreeNodeClone {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode>;
}

/// Enable `to_protobuf` for Box<dyn TreeNode>
/// Ref: https://quinedot.github.io/rust-learning/dyn-trait-box-impl.html
pub trait TreeNodeBoxed {
    fn to_protobuf_box(self: Box<Self>) -> GandivaResult<crate::proto::TreeNode>;
}

impl<T: TreeNode> TreeNodeBoxed for T {
    fn to_protobuf_box(self: Box<Self>) -> GandivaResult<crate::proto::TreeNode> {
        <Self as TreeNode>::to_protobuf(*self)
    }
}

impl TreeNode for Box<dyn TreeNode + '_> {
    fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
        <dyn TreeNode as TreeNodeBoxed>::to_protobuf_box(self)
    }
}

/// Enable `clone` for Box<dyn TreeNode>
/// Ref: https://quinedot.github.io/rust-learning/dyn-trait-clone.html
pub trait TreeNodeClone {
    fn clone_box<'s>(&self) -> Box<dyn TreeNode + 's>
    where
        Self: 's;
}

impl<T> TreeNodeClone for T
where
    T: Clone + TreeNode,
{
    fn clone_box<'s>(&self) -> Box<dyn TreeNode + 's>
    where
        Self: 's,
    {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn TreeNode + '_> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}
