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
use std::collections::HashSet;

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub enum LeveledTaskType {
    MergeCompactionTask,
    TrivialMoveTask,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
    pub leveled_task_type: LeveledTaskType,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>();
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        target_level_size[self.options.max_levels - 1] =
            real_level_size[self.options.max_levels - 1].max(base_level_size_bytes);
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;

            // 这里的两个 if 是为了保证只有一层的大小可以小于 base_level_size_bytes
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = this_level_size;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);

            let upper_level_sst_ids = snapshot.l0_sstables.clone();
            let mut all_overlap_ssts = Vec::new();
            let mut all_upper_level_ssts = Vec::new();

            for upper_level_sst_id in upper_level_sst_ids {
                let this_one_overlap_ssts =
                    self.find_overlapping_ssts(snapshot, &[upper_level_sst_id], base_level);

                if this_one_overlap_ssts.is_empty() {
                    return Some(LeveledCompactionTask {
                        upper_level: None,
                        upper_level_sst_ids: vec![upper_level_sst_id],
                        lower_level: base_level,
                        lower_level_sst_ids: this_one_overlap_ssts,
                        is_lower_level_bottom_level: base_level == self.options.max_levels,
                        leveled_task_type: LeveledTaskType::TrivialMoveTask,
                    });
                } else {
                    all_overlap_ssts.extend(this_one_overlap_ssts);
                    all_upper_level_ssts.push(upper_level_sst_id);
                }
            }
            all_overlap_ssts.sort();
            all_overlap_ssts.dedup();

            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: all_upper_level_ssts,
                lower_level: base_level,
                lower_level_sst_ids: all_overlap_ssts,
                is_lower_level_bottom_level: base_level == self.options.max_levels,
                leveled_task_type: LeveledTaskType::MergeCompactionTask,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = real_level_size[level] as f64 / target_level_size[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );
            let level = *level;
            // select the oldest sst to compact
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
            println!(
                "compaction triggered by priority: {level} out of {:?}, select {selected_sst} for compaction",
                priorities
            );

            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, &[selected_sst], level + 1);

            let leveled_task_type = if lower_level_sst_ids.is_empty() {
                LeveledTaskType::TrivialMoveTask
            } else {
                LeveledTaskType::MergeCompactionTask
            };

            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
                leveled_task_type,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut files_to_remove = Vec::new();
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        match task.leveled_task_type {
            LeveledTaskType::MergeCompactionTask => {
                files_to_remove.extend(&task.upper_level_sst_ids);
                files_to_remove.extend(&task.lower_level_sst_ids);
            }
            _ => {}
        }

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| {
                if lower_level_sst_ids_set.remove(x) {
                    return None;
                }
                Some(*x)
            })
            .collect::<Vec<_>>();
        assert!(lower_level_sst_ids_set.is_empty());
        new_lower_level_ssts.extend(output);
        if !in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }
        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, files_to_remove)
    }
}
