// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

mod builder;
mod iterator;

use crate::compression::{CompressionController, CompressionOptions};
pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

pub(crate) const SIZEOF_U8: usize = std::mem::size_of::<u8>();
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U64: usize = std::mem::size_of::<u64>();

///     A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
///     ----------------------------------------------------------------------------------------------------
///     |             Data Section             |              Offset Section             |      Extra      |
///     ----------------------------------------------------------------------------------------------------
///     | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
///     ----------------------------------------------------------------------------------------------------
///     
///     
///     -----------------------------------------------------------------------
///     |                           Entry #1                            | ... |
///     -----------------------------------------------------------------------
///     | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
///     -----------------------------------------------------------------------
///     
///     
///     -------------------------------
///     |offset|offset|num_of_elements|
///     -------------------------------
///     |   0  |  12  |       2       |
///     -------------------------------
///
#[warn(clippy::empty_line_after_doc_comments)]
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self, compression_options: CompressionOptions) -> anyhow::Result<Bytes> {
        let mut buf = self.data.clone();
        let offsets_len = self.offsets.len();
        for offset in &self.offsets {
            buf.put_u16(*offset);
        }
        buf.put_u16(offsets_len as u16);
        let compression_type: u8 = compression_options.into();
        let controller = CompressionController::new(compression_options);
        let mut result = controller
            .compress(buf.as_slice())
            .map_err(|e| anyhow::anyhow!("Compression failed: {}", e))?;
        result.put_u8(compression_type);
        Ok(result.into())
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8], compression_options: CompressionOptions) -> anyhow::Result<Self> {
        // 压缩方式校验
        let compression_type = (&data[data.len() - SIZEOF_U8..]).get_u8();
        assert_eq!(
            compression_type, compression_options as u8,
            "Compression type mismatch"
        );
        let data = &data[..data.len() - SIZEOF_U8];
        let controller = CompressionController::new(compression_options);
        let mut data = controller
            .de_compress(data)
            .map_err(|e| anyhow::anyhow!("Decompression failed: {}", e))?;
        // 多少个元素
        let entry_offsets_len = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        // 数据和offset的分界线位置
        let data_end = data.len() - SIZEOF_U16 - entry_offsets_len * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks_exact(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        data.truncate(data_end);
        Ok(Self { data, offsets })
    }
}
