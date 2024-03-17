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

macro_rules! make_node {
    ($NAME:ident, $NATIVE_TYPE:ty, $PB_NODE:tt) => {
        #[derive(Clone)]
        pub struct $NAME {
            value: $NATIVE_TYPE,
        }

        impl $NAME {
            pub fn create(value: $NATIVE_TYPE) -> Self {
                Self { value }
            }
        }

        impl TreeNode for $NAME {
            fn to_protobuf(self) -> GandivaResult<crate::proto::TreeNode> {
                Ok(crate::proto::TreeNode {
                    $PB_NODE: Some(crate::proto::$NAME {
                        value: Some(self.value as _),
                    }),
                    ..Default::default()
                })
            }
        }
    };
}

make_node!(BooleanNode, bool, boolean_node);
make_node!(UInt8Node, u8, uint8_node);
make_node!(UInt16Node, u16, uint16_node);
make_node!(UInt32Node, u32, uint32_node);
make_node!(UInt64Node, u64, uint64_node);
make_node!(Int8Node, i8, int8_node);
make_node!(Int16Node, i16, int16_node);
make_node!(Int32Node, i32, int32_node);
make_node!(Int64Node, i64, int64_node);
make_node!(Float32Node, f32, float32_node);
make_node!(Float64Node, f64, float64_node);

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
