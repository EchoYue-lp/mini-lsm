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

//! Hybrid Async Interface for Mini-LSM
//!
//! This module provides an async interface that wraps the synchronous core LSM engine.
//! It uses spawn_blocking to execute CPU/IO intensive operations on dedicated threads
//! while keeping the network layer fully async for high concurrency.

use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageOptions, MiniLsm, WriteBatchRecord};
use crate::mvcc::txn::Transaction;
use anyhow::{Context, Result};
use bytes::Bytes;
use futures::stream::Stream;
use std::ops::Bound;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::task;

/// Hybrid async wrapper around the synchronous MiniLsm engine
///
/// This design provides:
/// - Async network interface for high concurrency
/// - Sync core engine for simplicity and performance
/// - Automatic thread pool management via spawn_blocking
#[derive(Clone)]
pub struct HybridAsyncLsm {
    /// The core synchronous LSM engine
    core: Arc<MiniLsm>,
}

impl HybridAsyncLsm {
    /// Open an LSM storage with hybrid async interface
    pub async fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Use spawn_blocking for the potentially IO-heavy open operation
        let core = task::spawn_blocking(move || MiniLsm::open(&path, options))
            .await
            .context("Failed to spawn open task")?
            .context("Failed to open LSM storage")?;

        Ok(Self { core })
    }

    /// Get a value using async interface with sync core
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let core = self.core.clone();
        let key = key.to_vec();

        task::spawn_blocking(move || core.get(&key))
            .await
            .context("Failed to spawn get task")?
    }

    /// Put a key-value pair using async interface with sync core
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let core = self.core.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        task::spawn_blocking(move || core.put(&key, &value))
            .await
            .context("Failed to spawn put task")?
    }

    /// Put a key-value pair with TTL using async interface with sync core
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()> {
        let core = self.core.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        task::spawn_blocking(move || core.put_with_ttl(&key, &value, ttl))
            .await
            .context("Failed to spawn put_with_ttl task")?
    }

    /// Put multiple key-value pairs in a batch
    pub async fn put_batch(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let batch: Vec<WriteBatchRecord<&[u8]>> = entries
            .iter()
            .map(|(k, v)| WriteBatchRecord::Put(k.as_slice(), v.as_slice()))
            .collect();

        self.write_batch(&batch).await
    }

    /// Delete a key using async interface with sync core
    pub async fn delete(&self, key: &[u8]) -> Result<()> {
        let core = self.core.clone();
        let key = key.to_vec();

        task::spawn_blocking(move || core.delete(&key))
            .await
            .context("Failed to spawn delete task")?
    }

    /// Write a batch of operations
    pub async fn write_batch(&self, batch: &[WriteBatchRecord<&[u8]>]) -> Result<()> {
        let core = self.core.clone();
        // Convert to owned data for moving into spawn_blocking
        let owned_batch: Vec<WriteBatchRecord<Vec<u8>>> = batch
            .iter()
            .map(|record| match record {
                WriteBatchRecord::Put(k, v) => WriteBatchRecord::Put(k.to_vec(), v.to_vec()),
                WriteBatchRecord::Del(k) => WriteBatchRecord::Del(k.to_vec()),
                WriteBatchRecord::PutWithTtl(k, v, ttl) => {
                    WriteBatchRecord::PutWithTtl(k.to_vec(), v.to_vec(), *ttl)
                }
            })
            .collect();

        task::spawn_blocking(move || {
            // Convert back to slice references for the core API
            let slice_batch: Vec<WriteBatchRecord<&[u8]>> = owned_batch
                .iter()
                .map(|record| match record {
                    WriteBatchRecord::Put(k, v) => {
                        WriteBatchRecord::Put(k.as_slice(), v.as_slice())
                    }
                    WriteBatchRecord::Del(k) => WriteBatchRecord::Del(k.as_slice()),
                    WriteBatchRecord::PutWithTtl(k, v, ttl) => {
                        WriteBatchRecord::PutWithTtl(k.as_slice(), v.as_slice(), *ttl)
                    }
                })
                .collect();

            core.write_batch(&slice_batch)
        })
        .await
        .context("Failed to spawn write_batch task")?
    }

    /// Create a new transaction using sync core
    pub async fn new_txn(&self) -> Result<HybridTransaction> {
        let core = self.core.clone();

        let txn = task::spawn_blocking(move || {
            core.inner
                .mvcc()
                .new_txn(core.inner.clone(), core.inner.options.serializable)
        })
        .await
        .context("Failed to spawn new_txn task")?;

        Ok(HybridTransaction::new(txn))
    }

    /// Scan a range of keys - returns a streaming async iterator
    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<HybridIterator> {
        let core = self.core.clone();
        let lower_owned = convert_bound_to_owned(lower);
        let upper_owned = convert_bound_to_owned(upper);

        let iter = task::spawn_blocking(move || {
            let lower_ref = convert_bound_to_ref(&lower_owned);
            let upper_ref = convert_bound_to_ref(&upper_owned);
            core.scan(lower_ref, upper_ref)
        })
        .await
        .context("Failed to spawn scan task")?
        .context("Failed to create iterator")?;

        Ok(HybridIterator::new(iter))
    }

    /// Force flush memtable
    pub async fn force_flush(&self) -> Result<()> {
        let core = self.core.clone();

        task::spawn_blocking(move || core.force_flush())
            .await
            .context("Failed to spawn force_flush task")?
    }

    /// Sync to disk
    pub async fn sync(&self) -> Result<()> {
        let core = self.core.clone();

        task::spawn_blocking(move || core.sync())
            .await
            .context("Failed to spawn sync task")?
    }

    /// Close the storage
    pub async fn close(&self) -> Result<()> {
        let core = self.core.clone();

        task::spawn_blocking(move || core.close())
            .await
            .context("Failed to spawn close task")?
    }

    /// Get access to the underlying sync engine for advanced operations
    pub fn sync_engine(&self) -> &Arc<MiniLsm> {
        &self.core
    }
}

/// Hybrid transaction that wraps sync transaction with async interface
pub struct HybridTransaction {
    txn: Arc<Transaction>,
}

impl HybridTransaction {
    fn new(txn: Arc<Transaction>) -> Self {
        Self { txn }
    }

    /// Get value in transaction
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let txn = self.txn.clone();
        let key = key.to_vec();

        task::spawn_blocking(move || txn.get(&key))
            .await
            .context("Failed to spawn txn get task")?
    }

    /// Put value in transaction (non-blocking local operation)
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // This is a local operation, no need for spawn_blocking
        self.txn.put(key, value)
    }

    /// Delete in transaction (non-blocking local operation)
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        // This is a local operation, no need for spawn_blocking
        self.txn.delete(key)
    }

    /// Scan in transaction - returns a streaming async iterator
    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<HybridIterator> {
        let txn = self.txn.clone();
        let lower_owned = convert_bound_to_owned(lower);
        let upper_owned = convert_bound_to_owned(upper);

        let iter = task::spawn_blocking(move || {
            let lower_ref = convert_bound_to_ref(&lower_owned);
            let upper_ref = convert_bound_to_ref(&upper_owned);
            txn.scan(lower_ref, upper_ref)
        })
        .await
        .context("Failed to spawn txn scan task")?
        .context("Failed to create txn iterator")?;

        Ok(HybridIterator::new(iter))
    }

    /// Commit transaction
    pub async fn commit(&self) -> Result<()> {
        let txn = self.txn.clone();

        task::spawn_blocking(move || txn.commit())
            .await
            .context("Failed to spawn commit task")?
    }
}

/// Streaming async iterator that provides true async interface for sync iterator
pub struct HybridIterator {
    inner: Arc<tokio::sync::Mutex<crate::mvcc::txn::TxnIterator>>,
}

impl HybridIterator {
    fn new(iter: crate::mvcc::txn::TxnIterator) -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(iter)),
        }
    }

    /// Get next key-value pair asynchronously
    pub async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let mut iter = futures::executor::block_on(inner.lock());
            if !iter.is_valid() {
                return Ok(None);
            }

            let key = Bytes::copy_from_slice(iter.key());
            let value = Bytes::copy_from_slice(iter.value());
            let result = (key, value);

            // Move to next item for next call
            iter.next().context("Failed to advance iterator")?;
            Ok(Some(result))
        })
        .await
        .context("Failed to spawn iterator next task")?
    }

    /// Check if iterator is valid
    pub async fn is_valid(&self) -> Result<bool> {
        let inner = self.inner.clone();
        task::spawn_blocking(move || {
            let iter = futures::executor::block_on(inner.lock());
            Ok(iter.is_valid())
        })
        .await
        .context("Failed to spawn iterator is_valid task")?
    }

    /// Collect all remaining items (for backwards compatibility)
    pub async fn collect(mut self) -> Result<Vec<(Bytes, Bytes)>> {
        let mut result = Vec::new();
        while self.is_valid().await? {
            if let Some(item) = self.next().await? {
                result.push(item);
            } else {
                break;
            }
        }
        Ok(result)
    }
}

impl Stream for HybridIterator {
    type Item = Result<(Bytes, Bytes)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let inner = self.inner.clone();
        let mut future = Box::pin(async move {
            let mut iter = inner.lock().await;
            if !iter.is_valid() {
                return None;
            }

            let key = Bytes::copy_from_slice(iter.key());
            let value = Bytes::copy_from_slice(iter.value());
            let result = (key, value);

            match iter.next() {
                Ok(()) => Some(Ok(result)),
                Err(e) => Some(Err(e)),
            }
        });

        match future.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => Poll::Pending,
        }
    }
}

// Helper functions for bound conversion
fn convert_bound_to_owned(bound: Bound<&[u8]>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Included(k) => Bound::Included(k.to_vec()),
        Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

fn convert_bound_to_ref(bound: &Bound<Vec<u8>>) -> Bound<&[u8]> {
    match bound {
        Bound::Included(k) => Bound::Included(k.as_slice()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}
