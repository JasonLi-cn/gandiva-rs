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

use arrow::array::{
    as_primitive_array, make_array, ArrayData, ArrayRef, BooleanArray, BooleanBufferBuilder,
};
use arrow::buffer::{Buffer, MutableBuffer};
use arrow::datatypes::DataType;

use crate::bindings::gandiva_SelectionVectorInfo;

pub type SelectionVectorType = crate::proto::SelectionVectorType;

#[derive(Clone)]
pub struct SelectionVector {
    ty: SelectionVectorType,
    data: ArrayData,
    record_count: usize,
}

impl Default for SelectionVector {
    fn default() -> Self {
        Self {
            ty: SelectionVectorType::SvNone,
            data: ArrayData::new_empty(&DataType::Null),
            record_count: 0,
        }
    }
}

impl SelectionVector {
    pub fn create(rows: usize) -> Self {
        let ty = if rows == 0 {
            SelectionVectorType::SvNone
        } else if rows <= u16::MAX as usize {
            SelectionVectorType::SvInt16
        } else if rows <= u32::MAX as usize {
            SelectionVectorType::SvInt32
        } else {
            SelectionVectorType::SvInt64
        };
        Self::create_with_type(ty, rows)
    }

    pub fn create_with_type(ty: SelectionVectorType, rows: usize) -> Self {
        if matches!(ty, SelectionVectorType::SvNone) {
            return Self::default();
        }

        let data_type = match ty {
            SelectionVectorType::SvNone => return Self::default(),
            SelectionVectorType::SvInt16 => DataType::UInt16,
            SelectionVectorType::SvInt32 => DataType::UInt32,
            SelectionVectorType::SvInt64 => DataType::UInt64,
        };

        let len = data_type.primitive_width().unwrap() * rows;
        let buffer = Buffer::from(MutableBuffer::from_len_zeroed(len));

        let data = unsafe {
            ArrayData::new_unchecked(data_type, rows, None, None, 0, vec![buffer], vec![])
        };

        Self {
            ty,
            data,
            record_count: 0,
        }
    }

    pub fn ty(&self) -> i32 {
        self.ty as i32
    }

    pub fn rows(&self) -> i32 {
        self.data.len() as i32
    }

    pub fn addr(&self) -> usize {
        if matches!(self.ty, SelectionVectorType::SvNone) {
            return 0;
        }
        self.data.buffers()[0].as_ptr() as usize
    }

    pub fn size(&self) -> i64 {
        if matches!(self.ty, SelectionVectorType::SvNone) {
            return 0;
        }
        self.data.buffers()[0].len() as i64
    }

    pub fn set_record_count(&mut self, record_count: i32) {
        self.record_count = record_count as usize;
    }

    pub fn to_array(self) -> ArrayRef {
        make_array(self.data.slice(0, self.record_count))
    }

    pub fn to_boolean_array(self) -> BooleanArray {
        use arrow::datatypes::{UInt16Type, UInt32Type, UInt64Type};

        macro_rules! build_boolean_array {
            ($ARROW_PRIMITIVE_TYPE:ty) => {{
                let len = self.data.len();
                let array = make_array(self.data);
                let idx_array = as_primitive_array::<$ARROW_PRIMITIVE_TYPE>(&array);
                let buffer = MutableBuffer::new_null(len);
                let mut builder = BooleanBufferBuilder::new_from_buffer(buffer, len);
                for i in idx_array.values().slice(0, self.record_count).iter() {
                    builder.set_bit(*i as usize, true);
                }
                let buffer = builder.finish();
                BooleanArray::from(buffer)
            }};
        }

        match self.ty {
            SelectionVectorType::SvNone => BooleanArray::new_null(0),
            SelectionVectorType::SvInt16 => build_boolean_array!(UInt16Type),
            SelectionVectorType::SvInt32 => build_boolean_array!(UInt32Type),
            SelectionVectorType::SvInt64 => build_boolean_array!(UInt64Type),
        }
    }
}

impl From<SelectionVector> for gandiva_SelectionVectorInfo {
    fn from(value: SelectionVector) -> Self {
        gandiva_SelectionVectorInfo {
            type_: value.ty(),
            rows: value.rows(),
            addr: value.addr(),
            size: value.size(),
        }
    }
}
