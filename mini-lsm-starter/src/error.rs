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

//! Error types for Mini-LSM
//!
//! This module provides comprehensive error handling for the LSM storage engine,
//! replacing unsafe unwrap() calls with proper error handling.

use std::fmt;

/// Result type alias for Mini-LSM operations
pub type LsmResult<T> = Result<T, LsmError>;

/// Comprehensive error type for Mini-LSM operations
#[derive(Debug)]
pub enum LsmError {
    /// I/O related errors
    Io(std::io::Error),

    /// Serialization/Deserialization errors
    Serialization(String),

    /// WAL (Write-Ahead Log) related errors
    Wal(WalError),

    /// SSTable related errors
    SsTable(SsTableError),

    /// Memory table related errors
    MemTable(MemTableError),

    /// Compaction related errors
    Compaction(CompactionError),

    /// MVCC/Transaction related errors
    Mvcc(MvccError),

    /// Iterator related errors
    Iterator(IteratorError),

    /// Block related errors
    Block(BlockError),

    /// Configuration/Options errors
    Config(String),

    /// Internal consistency errors
    Internal(String),

    /// Network related errors
    Network(NetworkError),
}

#[derive(Debug)]
pub enum WalError {
    /// WAL file corruption detected
    Corruption(String),
    /// Incomplete WAL entry
    IncompleteEntry,
    /// Checksum mismatch
    ChecksumMismatch { expected: u32, actual: u32 },
    /// Invalid key type in WAL
    InvalidKeyType(u8),
    /// WAL file not found during recovery
    FileNotFound(String),
}

#[derive(Debug)]
pub enum SsTableError {
    /// SSTable file corruption
    Corruption(String),
    /// Block not found in SSTable
    BlockNotFound(usize),
    /// Invalid SSTable format
    InvalidFormat(String),
    /// SSTable metadata corruption
    MetadataCorruption,
    /// Bloom filter error
    BloomFilter(String),
    /// Empty SSTable when content expected
    EmptyTable,
}

#[derive(Debug)]
pub enum MemTableError {
    /// Empty memtable when content expected
    Empty,
    /// Memtable size limit exceeded
    SizeExceeded { current: usize, limit: usize },
    /// Invalid key format
    InvalidKey(String),
}

#[derive(Debug)]
pub enum CompactionError {
    /// No SSTs available for compaction
    NoSstsAvailable,
    /// Invalid compaction task configuration
    InvalidTask(String),
    /// Compaction cancelled due to shutdown
    Cancelled,
    /// Resource exhaustion during compaction
    ResourceExhausted(String),
}

#[derive(Debug)]
pub enum MvccError {
    /// Transaction not found
    TransactionNotFound(u64),
    /// Transaction already committed
    AlreadyCommitted(u64),
    /// Serialization conflict
    SerializationConflict,
    /// Transaction timeout
    Timeout(u64),
    /// Read timestamp is too old
    ReadTimestampTooOld { read_ts: u64, min_ts: u64 },
}

#[derive(Debug)]
pub enum IteratorError {
    /// Iterator is not valid/positioned
    NotValid,
    /// Iterator has reached end
    EndOfIteration,
    /// Iterator internal state corruption
    StateCorruption(String),
}

#[derive(Debug)]
pub enum BlockError {
    /// Empty block when content expected
    Empty,
    /// Block index out of bounds
    IndexOutOfBounds { index: usize, max: usize },
    /// Invalid block format
    InvalidFormat(String),
    /// Block checksum mismatch
    ChecksumMismatch,
}

#[derive(Debug)]
pub enum NetworkError {
    /// Connection related errors
    Connection(String),
    /// Protocol buffer serialization errors
    Protobuf(String),
    /// Client rate limit exceeded
    RateLimitExceeded,
    /// Request timeout
    Timeout,
    /// Invalid request format
    InvalidRequest(String),
}

impl fmt::Display for LsmError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LsmError::Io(e) => write!(f, "I/O error: {}", e),
            LsmError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            LsmError::Wal(e) => write!(f, "WAL error: {}", e),
            LsmError::SsTable(e) => write!(f, "SSTable error: {}", e),
            LsmError::MemTable(e) => write!(f, "MemTable error: {}", e),
            LsmError::Compaction(e) => write!(f, "Compaction error: {}", e),
            LsmError::Mvcc(e) => write!(f, "MVCC error: {}", e),
            LsmError::Iterator(e) => write!(f, "Iterator error: {}", e),
            LsmError::Block(e) => write!(f, "Block error: {}", e),
            LsmError::Config(msg) => write!(f, "Configuration error: {}", msg),
            LsmError::Internal(msg) => write!(f, "Internal error: {}", msg),
            LsmError::Network(e) => write!(f, "Network error: {}", e),
        }
    }
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Corruption(msg) => write!(f, "WAL corruption: {}", msg),
            WalError::IncompleteEntry => write!(f, "Incomplete WAL entry"),
            WalError::ChecksumMismatch { expected, actual } => {
                write!(
                    f,
                    "WAL checksum mismatch: expected {}, got {}",
                    expected, actual
                )
            }
            WalError::InvalidKeyType(t) => write!(f, "Invalid key type in WAL: {}", t),
            WalError::FileNotFound(path) => write!(f, "WAL file not found: {}", path),
        }
    }
}

impl fmt::Display for SsTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SsTableError::Corruption(msg) => write!(f, "SSTable corruption: {}", msg),
            SsTableError::BlockNotFound(id) => write!(f, "Block not found: {}", id),
            SsTableError::InvalidFormat(msg) => write!(f, "Invalid SSTable format: {}", msg),
            SsTableError::MetadataCorruption => write!(f, "SSTable metadata corruption"),
            SsTableError::BloomFilter(msg) => write!(f, "Bloom filter error: {}", msg),
            SsTableError::EmptyTable => write!(f, "Empty SSTable when content expected"),
        }
    }
}

impl fmt::Display for MemTableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MemTableError::Empty => write!(f, "Empty memtable when content expected"),
            MemTableError::SizeExceeded { current, limit } => {
                write!(f, "Memtable size exceeded: {} > {}", current, limit)
            }
            MemTableError::InvalidKey(msg) => write!(f, "Invalid key: {}", msg),
        }
    }
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactionError::NoSstsAvailable => write!(f, "No SSTs available for compaction"),
            CompactionError::InvalidTask(msg) => write!(f, "Invalid compaction task: {}", msg),
            CompactionError::Cancelled => write!(f, "Compaction cancelled"),
            CompactionError::ResourceExhausted(msg) => write!(f, "Resource exhausted: {}", msg),
        }
    }
}

impl fmt::Display for MvccError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MvccError::TransactionNotFound(id) => write!(f, "Transaction not found: {}", id),
            MvccError::AlreadyCommitted(id) => write!(f, "Transaction already committed: {}", id),
            MvccError::SerializationConflict => write!(f, "Serialization conflict"),
            MvccError::Timeout(id) => write!(f, "Transaction timeout: {}", id),
            MvccError::ReadTimestampTooOld { read_ts, min_ts } => {
                write!(f, "Read timestamp {} too old, minimum: {}", read_ts, min_ts)
            }
        }
    }
}

impl fmt::Display for IteratorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            IteratorError::NotValid => write!(f, "Iterator not valid"),
            IteratorError::EndOfIteration => write!(f, "End of iteration"),
            IteratorError::StateCorruption(msg) => write!(f, "Iterator state corruption: {}", msg),
        }
    }
}

impl fmt::Display for BlockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BlockError::Empty => write!(f, "Empty block when content expected"),
            BlockError::IndexOutOfBounds { index, max } => {
                write!(f, "Block index out of bounds: {} >= {}", index, max)
            }
            BlockError::InvalidFormat(msg) => write!(f, "Invalid block format: {}", msg),
            BlockError::ChecksumMismatch => write!(f, "Block checksum mismatch"),
        }
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkError::Connection(msg) => write!(f, "Connection error: {}", msg),
            NetworkError::Protobuf(msg) => write!(f, "Protobuf error: {}", msg),
            NetworkError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            NetworkError::Timeout => write!(f, "Request timeout"),
            NetworkError::InvalidRequest(msg) => write!(f, "Invalid request: {}", msg),
        }
    }
}

impl std::error::Error for LsmError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LsmError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl std::error::Error for WalError {}
impl std::error::Error for SsTableError {}
impl std::error::Error for MemTableError {}
impl std::error::Error for CompactionError {}
impl std::error::Error for MvccError {}
impl std::error::Error for IteratorError {}
impl std::error::Error for BlockError {}
impl std::error::Error for NetworkError {}

// Conversion from standard library errors
impl From<std::io::Error> for LsmError {
    fn from(err: std::io::Error) -> Self {
        LsmError::Io(err)
    }
}

impl From<anyhow::Error> for LsmError {
    fn from(err: anyhow::Error) -> Self {
        LsmError::Internal(err.to_string())
    }
}

// Convenience macros for error creation
#[macro_export]
macro_rules! lsm_error {
    ($variant:ident, $msg:expr) => {
        $crate::error::LsmError::$variant($msg.to_string())
    };
    ($variant:ident, $inner:ident, $msg:expr) => {
        $crate::error::LsmError::$variant($crate::error::$inner::$msg)
    };
}

#[macro_export]
macro_rules! ensure_not_empty {
    ($collection:expr, $error:expr) => {
        if $collection.is_empty() {
            return Err($error);
        }
    };
}

#[macro_export]
macro_rules! ensure_valid_index {
    ($index:expr, $max:expr, $error:expr) => {
        if $index >= $max {
            return Err($error);
        }
    };
}

// Helper functions for common patterns
impl LsmError {
    pub fn wal_corruption(msg: impl Into<String>) -> Self {
        LsmError::Wal(WalError::Corruption(msg.into()))
    }

    pub fn sstable_corruption(msg: impl Into<String>) -> Self {
        LsmError::SsTable(SsTableError::Corruption(msg.into()))
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        LsmError::Internal(msg.into())
    }

    pub fn config(msg: impl Into<String>) -> Self {
        LsmError::Config(msg.into())
    }
}

// Conversion functions for easier migration
impl LsmError {
    /// Create an error from Option::None with context
    pub fn from_none<T>(context: &str) -> LsmResult<T> {
        Err(LsmError::internal(format!(
            "Expected value but got None: {}",
            context
        )))
    }

    /// Safe unwrap alternative that returns a descriptive error
    pub fn require<T>(option: Option<T>, context: &str) -> LsmResult<T> {
        option.ok_or_else(|| LsmError::internal(format!("Required value missing: {}", context)))
    }
}
