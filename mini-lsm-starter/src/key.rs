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

use std::{cmp::Reverse, fmt::Debug};

use bytes::Bytes;

//  Key +  ts +  type + ttl
pub struct Key<T: AsRef<[u8]>>(T, u64, Type, u64);

pub type KeySlice<'a> = Key<&'a [u8]>;
pub type KeyVec = Key<Vec<u8>>;
pub type KeyBytes = Key<Bytes>;

/// For testing purpose, should not use anywhere in your implementation.
pub const TS_ENABLED: bool = true;

/// Temporary, should remove after implementing full week 3 day 1 + 2.
pub const TS_DEFAULT: u64 = 0;

pub const TS_MAX: u64 = u64::MAX;
pub const TS_MIN: u64 = u64::MIN;
pub const TS_RANGE_BEGIN: u64 = u64::MAX;
pub const TS_RANGE_END: u64 = u64::MIN;
pub const TTL_DEFAULT: u64 = 0;

/// Get current timestamp in seconds since Unix epoch
pub fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum Type {
    DELETE = 0,
    PUT = 1,
}

impl From<Type> for u8 {
    fn from(value: Type) -> Self {
        value as u8
    }
}

impl TryFrom<u8> for Type {
    type Error = &'static str; // 错误类型

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Type::DELETE),
            _ => Ok(Type::PUT),
        }
    }
}

impl<T: AsRef<[u8]>> Key<T> {
    pub fn into_inner(self) -> T {
        self.0
    }

    pub fn key_len(&self) -> usize {
        self.0.as_ref().len()
    }

    pub fn raw_len(&self) -> usize {
        self.0.as_ref().len()
            + std::mem::size_of::<u64>()
            + std::mem::size_of::<u8>()
            + std::mem::size_of::<u64>()
    }

    pub fn is_empty(&self) -> bool {
        self.0.as_ref().is_empty()
    }

    pub fn for_testing_ts(self) -> u64 {
        self.1
    }

    pub fn key_type(&self) -> Type {
        self.2
    }

    pub fn ttl(&self) -> u64 {
        self.3
    }

    pub fn is_expired(&self, current_time: u64) -> bool {
        self.3 != TTL_DEFAULT && current_time > self.3
    }
}

impl Key<Vec<u8>> {
    pub fn new() -> Self {
        Self(Vec::new(), TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }

    /// Create a `KeyVec` from a `Vec<u8>` and a ts. Will be removed in week 3.
    pub fn from_bytes_with_all(key: Vec<u8>, ts: u64, key_type: Type, ttl: u64) -> Self {
        Self(key, ts, key_type, ttl)
    }

    pub fn from_vec_with_ttl(key: Vec<u8>, ttl: u64) -> Self {
        Self(key, TS_DEFAULT, Type::PUT, ttl)
    }

    pub fn from_vec_with_ts_ttl(key: Vec<u8>, ts: u64, ttl: u64) -> Self {
        Self(key, ts, Type::PUT, ttl)
    }

    pub fn from_vec_with_delete(key: Vec<u8>, key_type: Type) -> Self {
        Self(key, TS_DEFAULT, key_type, TTL_DEFAULT)
    }

    /// Clears the key and set ts to 0.
    pub fn clear(&mut self) {
        self.0.clear()
    }

    /// Append a slice to the end of the key
    pub fn append(&mut self, data: &[u8]) {
        self.0.extend(data)
    }

    pub fn set_ts(&mut self, ts: u64) {
        self.1 = ts;
    }

    pub fn set_type(&mut self, key_type: Type) {
        self.2 = key_type;
    }

    pub fn set_ttl(&mut self, ttl: u64) {
        self.3 = ttl;
    }

    /// Set the key from a slice without re-allocating.
    pub fn set_from_slice(&mut self, key_slice: KeySlice) {
        self.0.clear();
        self.0.extend(key_slice.0);
        self.1 = key_slice.1;
        self.2 = key_slice.2;
        self.3 = key_slice.3;
    }

    pub fn as_key_slice(&self) -> KeySlice<'_> {
        Key(self.0.as_slice(), self.1, self.2, self.3)
    }

    pub fn into_key_bytes(self) -> KeyBytes {
        Key(self.0.into(), self.1, self.2, self.3)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn for_testing_from_vec_no_ts(key: Vec<u8>) -> Self {
        Self(key, TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }
}

impl Key<Bytes> {
    pub fn new() -> Self {
        Self(Bytes::new(), TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }

    pub fn as_key_slice(&self) -> KeySlice<'_> {
        Key(&self.0, self.1, self.2, self.3)
    }

    /// Create a `KeyBytes` from a `Bytes` and a ts.
    pub fn from_bytes_with_ts(bytes: Bytes, ts: u64) -> KeyBytes {
        Key(bytes, ts, Type::PUT, TTL_DEFAULT)
    }

    /// Create a KeyBytes from a Bytes + ts + type + ttl
    pub fn from_bytes_with_all(bytes: Bytes, ts: u64, key_type: Type, ttl: u64) -> KeyBytes {
        Key(bytes, ts, key_type, ttl)
    }

    pub fn key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn set_type(&mut self, key_type: Type) {
        self.2 = key_type;
    }

    pub fn set_ttl(&mut self, ttl: u64) {
        self.3 = ttl;
    }

    pub fn for_testing_from_bytes_no_ts(bytes: Bytes) -> KeyBytes {
        Key(bytes, TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }

    pub fn for_testing_key_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<'a> Key<&'a [u8]> {
    pub fn to_key_vec(self) -> KeyVec {
        Key(self.0.to_vec(), self.1, self.2, self.3)
    }

    /// Create a key slice from a slice. Will be removed in week 3.
    pub fn from_slice(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts, Type::PUT, TTL_DEFAULT)
    }

    pub fn from_slice_with_type(slice: &'a [u8], ts: u64, key_type: Type) -> Self {
        Self(slice, ts, key_type, TTL_DEFAULT)
    }

    pub fn from_slice_with_ttl(slice: &'a [u8], ts: u64, ttl: u64) -> Self {
        Self(slice, ts, Type::PUT, ttl)
    }

    pub fn from_slice_with_type_and_ttl(
        slice: &'a [u8],
        ts: u64,
        key_type: Type,
        ttl: u64,
    ) -> Self {
        Self(slice, ts, key_type, ttl)
    }

    pub fn key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn ts(&self) -> u64 {
        self.1
    }

    pub fn for_testing_key_ref(self) -> &'a [u8] {
        self.0
    }

    pub fn for_testing_from_slice_no_ts_no_ttl(slice: &'a [u8]) -> Self {
        Self(slice, TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }

    pub fn for_testing_from_slice_with_ts(slice: &'a [u8], ts: u64) -> Self {
        Self(slice, ts, Type::PUT, TTL_DEFAULT)
    }
}

impl<T: AsRef<[u8]> + Debug> Debug for Key<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl<T: AsRef<[u8]> + Default> Default for Key<T> {
    fn default() -> Self {
        Self(T::default(), TS_DEFAULT, Type::PUT, TTL_DEFAULT)
    }
}

impl<T: AsRef<[u8]> + PartialEq> PartialEq for Key<T> {
    fn eq(&self, other: &Self) -> bool {
        (self.0.as_ref(), self.1, self.2, self.3).eq(&(other.0.as_ref(), other.1, other.2, other.3))
    }
}

impl<T: AsRef<[u8]> + Eq> Eq for Key<T> {}

impl<T: AsRef<[u8]> + Clone> Clone for Key<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone(), self.1, self.2, self.3)
    }
}

impl<T: AsRef<[u8]> + Copy> Copy for Key<T> {}

impl<T: AsRef<[u8]> + PartialOrd> PartialOrd for Key<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        (self.0.as_ref(), Reverse(self.1)).partial_cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}

impl<T: AsRef<[u8]> + Ord> Ord for Key<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ref(), Reverse(self.1)).cmp(&(other.0.as_ref(), Reverse(other.1)))
    }
}
