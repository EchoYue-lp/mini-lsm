use anyhow::Result;

use super::StorageIterator;
use crate::key::{KeyBytes, KeySlice};

/// Limit an inner iterator to a half-open key range [start, end).
/// Works with iterators whose key type is `KeySlice`.
pub struct RangeLimiter<I: for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> {
    inner: I,
    start: Option<KeyBytes>,
    end: Option<KeyBytes>,
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> RangeLimiter<I> {
    pub fn new(inner: I, start: Option<KeyBytes>, end: Option<KeyBytes>) -> Self {
        Self { inner, start, end }
    }

    fn advance_until_valid(&mut self) -> Result<()> {
        while self.inner.is_valid() {
            let key = self.inner.key();
            let ge_start = self.start.as_ref().is_none_or(|s| key >= s.as_key_slice());
            let lt_end = self.end.as_ref().is_none_or(|e| key < e.as_key_slice());
            if ge_start && lt_end {
                break;
            }
            // If key >= end, stop early by not advancing further (becomes invalid via is_valid)
            if let Some(end) = &self.end
                && key >= end.as_key_slice()
            {
                break;
            }
            self.inner.next()?;
        }
        Ok(())
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for RangeLimiter<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.inner.key()
    }

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        if let Some(end) = &self.end
            && self.inner.key() >= end.as_key_slice()
        {
            return false;
        }
        true
    }

    fn next(&mut self) -> Result<()> {
        if self.inner.is_valid() {
            self.inner.next()?;
        }
        self.advance_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}
