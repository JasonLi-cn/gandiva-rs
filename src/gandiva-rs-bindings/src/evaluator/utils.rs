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

use std::mem;

use arrow::array::{make_array, ArrayDataBuilder, ArrayRef};
use arrow::buffer::{BooleanBuffer, Buffer, MutableBuffer, NullBuffer};
use arrow::datatypes::{DataType, UnionMode};
use arrow::util::bit_util;

#[inline]
pub fn new_buffers(data_types: &[DataType], capacity: usize) -> Vec<MutableBuffer> {
    let mut mutable_buffers = vec![];

    for data_type in data_types {
        let null_buffer = MutableBuffer::new_null(capacity);
        mutable_buffers.push(null_buffer);

        match data_type {
            DataType::Null => {
                let buffer = MutableBuffer::from_len_zeroed(0);
                mutable_buffers.push(buffer);
            }
            DataType::Boolean => {
                let bytes = bit_util::ceil(capacity, 8);
                let buffer = MutableBuffer::from_len_zeroed(bytes);
                mutable_buffers.push(buffer);
            }
            DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Time32(_)
            | DataType::Date64
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Timestamp(_, _)
            | DataType::Interval(_) => {
                let buffer =
                    MutableBuffer::from_len_zeroed(capacity * data_type.primitive_width().unwrap());
                mutable_buffers.push(buffer);
            }
            DataType::Utf8 | DataType::Binary => {
                // safety: `unsafe` code assumes that this buffer is initialized with one element
                let offsets =
                    MutableBuffer::from_len_zeroed((1 + capacity) * mem::size_of::<i32>());
                let values = MutableBuffer::from_len_zeroed(capacity * mem::size_of::<u8>());
                mutable_buffers.push(offsets);
                mutable_buffers.push(values);
            }
            DataType::LargeUtf8 | DataType::LargeBinary => {
                // safety: `unsafe` code assumes that this buffer is initialized with one element
                let offsets =
                    MutableBuffer::from_len_zeroed((1 + capacity) * mem::size_of::<i64>());
                let values = MutableBuffer::from_len_zeroed(capacity * mem::size_of::<u8>());
                mutable_buffers.push(offsets);
                mutable_buffers.push(values);
            }
            DataType::List(_) | DataType::Map(_, _) => {
                // offset buffer always starts with a zero
                let buffer = MutableBuffer::from_len_zeroed((1 + capacity) * mem::size_of::<i32>());
                mutable_buffers.push(buffer);
            }
            DataType::LargeList(_) => {
                // offset buffer always starts with a zero
                let buffer = MutableBuffer::from_len_zeroed((1 + capacity) * mem::size_of::<i64>());
                mutable_buffers.push(buffer);
            }
            DataType::FixedSizeBinary(size) => {
                let buffer = MutableBuffer::from_len_zeroed(capacity * *size as usize);
                mutable_buffers.push(buffer);
            }
            DataType::Dictionary(k, _) => {
                let buffer =
                    MutableBuffer::from_len_zeroed(capacity * k.primitive_width().unwrap());
                mutable_buffers.push(buffer);
            }
            DataType::FixedSizeList(_, _) | DataType::Struct(_) | DataType::RunEndEncoded(_, _) => {
                let buffer = MutableBuffer::from_len_zeroed(0);
                mutable_buffers.push(buffer);
            }
            DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                let buffer = MutableBuffer::from_len_zeroed(capacity * mem::size_of::<u8>());
                mutable_buffers.push(buffer);
            }
            DataType::Union(_, mode) => {
                let type_ids = MutableBuffer::from_len_zeroed(capacity * mem::size_of::<i8>());
                match mode {
                    UnionMode::Sparse => {
                        mutable_buffers.push(type_ids);
                    }
                    UnionMode::Dense => {
                        let offsets =
                            MutableBuffer::from_len_zeroed(capacity * mem::size_of::<i32>());
                        mutable_buffers.push(type_ids);
                        mutable_buffers.push(offsets);
                    }
                }
            }
        }
    }

    mutable_buffers
}

pub fn to_arrays(
    data_types: &[DataType],
    mutable_buffers: Vec<MutableBuffer>,
    len: usize,
) -> Vec<ArrayRef> {
    let mut arrays = Vec::with_capacity(data_types.len());

    let mut iter = mutable_buffers.into_iter();

    for data_type in data_types {
        let buffer: Buffer = iter.next().unwrap().into();
        let null_buffer = NullBuffer::new(BooleanBuffer::new(buffer, 0, len));

        let mut buffers = vec![];
        if matches!(
            data_type,
            DataType::Utf8 | DataType::Binary | DataType::LargeUtf8 | DataType::LargeBinary
        ) {
            let offsets: Buffer = iter.next().unwrap().into();
            buffers.push(offsets);
        }

        let values: Buffer = iter.next().unwrap().into();
        buffers.push(values);

        let builder = ArrayDataBuilder::new(data_type.clone())
            .len(len)
            .buffers(buffers)
            .nulls(Some(null_buffer));
        let data = unsafe { builder.build_unchecked() };
        arrays.push(make_array(data));
    }

    arrays
}
