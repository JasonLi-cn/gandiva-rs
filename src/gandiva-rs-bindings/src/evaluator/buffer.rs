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

use std::ptr;

use arrow::array::ArrayData;
use arrow::buffer::{Buffer, MutableBuffer};

use crate::bindings::{gandiva_RustBuffer, gandiva_RustMutableBuffer, gandiva_RustMutableBuffers};

pub fn to_g_buffers<T: IntoIterator<Item = ArrayData>>(iter: T) -> Vec<gandiva_RustBuffer> {
    let mut g_buffers = vec![];
    for data in iter {
        let g_buffer = match data.nulls() {
            Some(null_buffer) => {
                let buffer = null_buffer.inner().inner();
                buffer.into()
            }
            None => gandiva_RustBuffer { addr: 0, size: 0 },
        };
        g_buffers.push(g_buffer);

        for buffer in data.buffers() {
            g_buffers.push(buffer.into());
        }
    }
    g_buffers
}

#[inline]
pub fn to_g_mutable_buffers(buffers: &[MutableBuffer]) -> gandiva_RustMutableBuffers {
    gandiva_RustMutableBuffers {
        addr: buffers.as_ptr() as *const _ as *mut ::std::os::raw::c_void,
        len: buffers.len() as _,
    }
}

pub fn to_g_mutable_buffer(buffers: &[MutableBuffer]) -> Vec<gandiva_RustMutableBuffer> {
    buffers
        .iter()
        .map(|buffer| gandiva_RustMutableBuffer {
            addr: buffer.as_ptr() as _,
            size: buffer.len() as _,
            capacity: buffer.capacity() as _,
        })
        .collect::<Vec<_>>()
}

impl From<&Buffer> for gandiva_RustBuffer {
    fn from(buffer: &Buffer) -> Self {
        gandiva_RustBuffer {
            addr: buffer.as_ptr() as _,
            size: buffer.len() as _,
        }
    }
}

/// # Safety
///
/// This function is used to reserve MutableBuffer.
pub unsafe extern "C" fn rust_reserve(
    mutable_buffers: *mut gandiva_RustMutableBuffers,
    idx: usize,
    g_buffer: *mut gandiva_RustMutableBuffer,
    new_capacity: i64,
) {
    let buffer_ptr = (*mutable_buffers).addr as *mut MutableBuffer;
    let buffers = ptr::slice_from_raw_parts_mut(buffer_ptr, (*mutable_buffers).len as usize);
    let mutable_buffer = &mut (*buffers)[idx];

    let diff = new_capacity as usize - mutable_buffer.len();
    mutable_buffer.reserve(diff);

    (*g_buffer).addr = mutable_buffer.as_ptr() as _;
    (*g_buffer).size = mutable_buffer.len() as _;
    (*g_buffer).capacity = mutable_buffer.capacity() as _;
}
