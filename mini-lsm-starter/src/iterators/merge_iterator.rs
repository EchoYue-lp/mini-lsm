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

use std::cmp::{self};
use std::collections::BinaryHeap;
use std::collections::binary_heap::PeekMut;

use crate::key::KeySlice;
use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    /// 先比较两个迭代器当前指向的 key，如果 key 相等，则比较他们的原始索引；
    /// 最后调用 reverse 函数，使得堆顶元素为最小的元素。
    /// 我悟了，因为合并之后是需要有序的，所以堆顶元素为最小的元素。
    /// 同时又因为原始索引小的数据是新的数据，因此这个堆其实维护了全局最小且最新的元素。
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    // 最大堆，堆顶为最大的元素
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        if iters.is_empty() {
            return Self {
                iters: BinaryHeap::new(),
                current: None,
            };
        }
        let mut heap = BinaryHeap::new();

        if iters.iter().all(|iter| !iter.is_valid()) {
            let mut iters = iters;
            return Self {
                iters: heap,
                current: Some(HeapWrapper(0, iters.pop().unwrap())),
            };
        }

        for (idx, item) in iters.into_iter().enumerate() {
            if item.is_valid() {
                heap.push(HeapWrapper(idx, item))
            }
        }

        // 这个 current 是 heap 的堆顶元素，也就是索引最小或者最大的元素
        let current = heap.pop().unwrap();
        Self {
            iters: heap,
            current: Some(current),
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(|t| t.1.is_valid())
            .unwrap_or(false)
    }

    fn next(&mut self) -> Result<()> {
        let current = self.current.as_mut().unwrap();
        while let Some(mut inner_iter) = self.iters.peek_mut() {
            debug_assert!(
                inner_iter.1.key() >= current.1.key(),
                "heap invariant violated"
            );

            // 这一步是为了去掉重复的 key ，因为 current 中的 key value 就是满足条件的值
            // 如果发现了后续 iter 中有相同的 key ,需要去掉。
            // 同时迭代器无效则从堆中移除
            if inner_iter.1.key() == current.1.key() {
                let result = inner_iter.1.next();

                if let e @ Err(_) = result {
                    PeekMut::pop(inner_iter);
                    return e;
                }

                // Case 2: iter is no longer valid.
                if !inner_iter.1.is_valid() {
                    PeekMut::pop(inner_iter);
                }
            } else {
                break;
            }
        }

        current.1.next()?;
        // current 中没有元素了，替换 current
        if !current.1.is_valid() {
            if let Some(iter) = self.iters.pop() {
                *current = iter;
            }
        }

        // 如果 current 中的元素比 堆顶元素大，则替换堆顶元素
        // *current < *inner_iter 这里可能会有疑惑，因为我们重写了 cmp
        if let Some(mut inner_iter) = self.iters.peek_mut() {
            if *current < *inner_iter {
                std::mem::swap(&mut *inner_iter, current);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iters
            .iter()
            .map(|x| x.1.num_active_iterators())
            .sum::<usize>()
            + self
                .current
                .as_ref()
                .map(|x| x.1.num_active_iterators())
                .unwrap_or(0)
    }
}
