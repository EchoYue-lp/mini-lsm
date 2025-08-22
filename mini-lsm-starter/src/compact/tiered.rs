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

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    // 层数
    pub num_tiers: usize,
    // 空间放大的最大容忍百分比。触发最彻底的“空间放大”压缩。
    pub max_size_amplification_percent: usize,
    // 控制“大小比例触发”的敏感度。百分比值。值越小，越容易触发压缩；值越大，越不容易触发。
    pub size_ratio: usize,
    // 一次压缩至少需要合并的文件数。
    pub min_merge_width: usize,
    // 一次压缩最多能合并的文件数。
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }
        // max_size_amplification_percent
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
        }
        // 这意味着如果“较新数据”的总大小超过了“最旧数据”大小的某个比例（默认 200%），说明空间放大很严重，
        // 需要通过一次大合并来清理过期数据。这是最彻底的压缩方式。
        let space_amplification_ratio =
            (size as f64) / (snapshot.levels.last().unwrap().1.len() as f64) * 100.0;
        if space_amplification_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amplification_ratio
            );
            return Some(TieredCompactionTask {
                // 要压缩所有的
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }
        // size_ratio 定义了文件大小差异的容忍度，从最新的文件开始向后查找（File_i 到 File_{i+1}, File_{i+2}...），
        // 如果连续的文件大小与第一个文件 File_i 的比例差距在 size_ratio 以内，则继续。
        // 当找到第一个不满足该条件的文件时，停止并将之前的所有文件合并。
        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut size = 0;
        for id in 0..(snapshot.levels.len() - 1) {
            size += snapshot.levels[id].1.len();
            let next_level_size = snapshot.levels[id + 1].1.len();
            let current_size_ratio = next_level_size as f64 / size as f64;
            if current_size_ratio > size_ratio_trigger && id + 1 >= self.options.min_merge_width {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    current_size_ratio * 100.0,
                    size_ratio_trigger * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: snapshot
                        .levels
                        .iter()
                        .take(id + 1)
                        .cloned()
                        .collect::<Vec<_>>(),
                    // Size ratio trigger will never include the bottom level
                    bottom_tier_included: false,
                });
            }
        }

        let num_tiers_to_take = snapshot
            .levels
            .len()
            .min(self.options.max_merge_width.unwrap_or(usize::MAX));
        println!("compaction triggered by reducing sorted runs");
        Some(TieredCompactionTask {
            tiers: snapshot
                .levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: snapshot.levels.len() >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        assert!(
            snapshot.l0_sstables.is_empty(),
            "should not add l0 ssts in tiered compaction"
        );
        let mut snapshot = snapshot.clone();
        let mut tier_to_remove = task
            .tiers
            .iter()
            .map(|(x, y)| (*x, y))
            .collect::<HashMap<_, _>>();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut files_to_remove = Vec::new();

        for (tier_id, files) in &snapshot.levels {
            if let Some(m_files) = tier_to_remove.remove(tier_id) {
                // the tier should be removed
                assert_eq!(m_files, files, "file changed after issuing compaction task");
                files_to_remove.extend(m_files.iter().copied());
            } else {
                // retain the tier
                levels.push((*tier_id, files.clone()));
            }
            if tier_to_remove.is_empty() && !new_tier_added {
                // add the compacted tier to the LSM tree
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }
        if !tier_to_remove.is_empty() {
            unreachable!("some tiers not found??");
        }
        snapshot.levels = levels;
        (snapshot, files_to_remove)
    }
}
