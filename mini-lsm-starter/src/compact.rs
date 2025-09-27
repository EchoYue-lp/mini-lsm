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

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashSet;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

pub(crate) use crate::compact::leveled::LeveledTaskType;
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::range_limiter::RangeLimiter;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeyBytes, KeySlice, TTL_DEFAULT, Type, current_timestamp};
use crate::lsm_storage::{CompactionFilter, LsmStorageInner, LsmStorageState};
use crate::manifest::ManifestRecord;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};
use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use rayon::ThreadPool;
use rayon::iter::IntoParallelRefIterator;
use rayon::iter::{IndexedParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    /**
     * output :新生成的 SST 的id
     */
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(
                "Unsupported compaction controller and task combination: controller={:?}, task={:?}",
                self, task
            ),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

struct CompactionTaskGuard {
    counter: Arc<AtomicUsize>,
}

impl CompactionTaskGuard {
    fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::Acquire);
        Self { counter }
    }
}

impl Drop for CompactionTaskGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Release);
    }
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let state = self.state.read();
            state.clone()
        };
        match task {
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_iters = Vec::with_capacity(l0_sstables.len());
                for sst_id in l0_sstables.iter() {
                    l0_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                        snapshot.sstables.get(sst_id).unwrap().clone(),
                    )?));
                }
                let mut l1_iters = Vec::with_capacity(l1_sstables.len());
                for id in l1_sstables.iter() {
                    l1_iters.push(snapshot.sstables.get(id).unwrap().clone());
                }
                let iter = TwoMergeIterator::create(
                    MergeIterator::create(l0_iters),
                    SstConcatIterator::create_and_seek_to_first(l1_iters)?,
                )?;
                self.compact_generate_sst_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            })
            | CompactionTask::Leveled(LeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level: _,
                lower_level_sst_ids,
                ..
            }) => match upper_level {
                Some(_) => {
                    let mut upper_ssts = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }

                    // 尝试Subcompaction
                    if self.options.num_subcompactions > 1 {
                        // if let Err(e) = self.try_subcompaction(&upper_ssts, &lower_ssts, task) {
                        //     eprintln!(
                        //         "Subcompaction failed, fallback to normal compaction: {}",
                        //         e
                        //     );
                        // }

                        match self.try_subcompaction(&upper_ssts, &lower_ssts, task) {
                            Ok(Some(result)) => return Ok(result),
                            Ok(None) => {
                                // Fallback到普通compaction
                            }
                            Err(e) => {
                                eprintln!(
                                    "Subcompaction failed, fallback to normal compaction: {}",
                                    e
                                );
                            }
                        }
                    }

                    // 普通compaction fallback
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
                None => {
                    let mut upper_iters = Vec::with_capacity(upper_level_sst_ids.len());
                    for id in upper_level_sst_ids.iter() {
                        upper_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(
                            snapshot.sstables.get(id).unwrap().clone(),
                        )?));
                    }
                    let mut lower_ssts = Vec::with_capacity(lower_level_sst_ids.len());
                    for id in lower_level_sst_ids.iter() {
                        lower_ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }

                    // 对于L0->L1的compaction，upper level是迭代器形式
                    // 需要将其转换为SST以支持subcompaction
                    if self.options.num_subcompactions > 1 {
                        let upper_ssts: Vec<Arc<SsTable>> = upper_level_sst_ids
                            .iter()
                            .map(|id| snapshot.sstables.get(id).unwrap().clone())
                            .collect();

                        match self.try_subcompaction(&upper_ssts, &lower_ssts, task) {
                            Ok(Some(result)) => return Ok(result),
                            Ok(None) => {
                                // Fallback到普通compaction
                            }
                            Err(e) => {
                                eprintln!(
                                    "Subcompaction failed, fallback to normal compaction: {}",
                                    e
                                );
                            }
                        }
                    }

                    // 普通compaction fallback
                    let upper_iter = MergeIterator::create(upper_iters);
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    )
                }
            },
            CompactionTask::Tiered(TieredCompactionTask { tiers, .. }) => {
                let mut iters = Vec::with_capacity(tiers.len());
                for (_, tier_sst_ids) in tiers {
                    let mut ssts = Vec::with_capacity(tier_sst_ids.len());
                    for id in tier_sst_ids.iter() {
                        ssts.push(snapshot.sstables.get(id).unwrap().clone());
                    }
                    iters.push(Box::new(SstConcatIterator::create_and_seek_to_first(ssts)?));
                }
                self.compact_generate_sst_from_iter(
                    MergeIterator::create(iters),
                    task.compact_to_bottom_level(),
                )
            }
        }
    }

    /// 基于key分布计算Subcompaction边界，参考RocksDB算法
    fn find_subcompaction_boundaries(
        &self,
        input_ssts: &[Arc<SsTable>],
    ) -> Result<Vec<(Option<KeyBytes>, Option<KeyBytes>)>> {
        if input_ssts.is_empty() || self.options.num_subcompactions <= 1 {
            return Ok(vec![(None, None)]);
        }

        // 采样每个SST文件的key点，类似RocksDB的anchor point算法
        let mut anchor_points = Vec::new();

        for sst in input_ssts {
            let mut sample_keys = Vec::new();

            // 始终包含首尾key
            sample_keys.push(sst.first_key().clone());
            sample_keys.push(sst.last_key().clone());

            // 从block metadata中采样中间key，最多128个采样点
            let block_count = sst.block_meta.len();
            if block_count > 2 {
                let sample_step = std::cmp::max(1, block_count / 64); // 最多采样64个中间点
                for (i, block_meta) in sst.block_meta.iter().enumerate() {
                    if i % sample_step == 0 && i > 0 && i < block_count - 1 {
                        sample_keys.push(block_meta.first_key.clone());
                    }
                }
            }

            anchor_points.extend(sample_keys);
        }

        // 排序并去重
        anchor_points.sort();
        anchor_points.dedup();

        if anchor_points.len() <= 1 {
            return Ok(vec![(None, None)]);
        }

        // 计算合适的子任务数量，不超过配置的最大值
        let effective_subcompactions = std::cmp::min(
            self.options.num_subcompactions,
            anchor_points.len().saturating_sub(1),
        );

        if effective_subcompactions <= 1 {
            return Ok(vec![(None, None)]);
        }

        // 按等距原则选择分割点
        let mut boundaries = Vec::new();
        let step = anchor_points.len() / effective_subcompactions;

        let mut start: Option<KeyBytes> = None;
        for i in (step..anchor_points.len()).step_by(step) {
            if boundaries.len() >= effective_subcompactions - 1 {
                break;
            }
            let end = Some(anchor_points[i].clone());
            boundaries.push((start.clone(), end.clone()));
            start = end;
        }

        // 最后一个范围到末尾
        boundaries.push((start, None));

        Ok(boundaries)
    }

    /// 检查SST是否与指定key范围重叠
    fn sst_overlaps_range(
        sst: &Arc<SsTable>,
        start: &Option<KeyBytes>,
        end: &Option<KeyBytes>,
    ) -> bool {
        let sst_first = sst.first_key();
        let sst_last = sst.last_key();

        let after_start = start.as_ref().is_none_or(|s| sst_last >= s);
        let before_end = end.as_ref().is_none_or(|e| sst_first < e);

        after_start && before_end
    }

    /// 尝试执行Subcompaction，如果可行返回结果，否则返回None进行fallback
    fn try_subcompaction(
        &self,
        upper_ssts: &[Arc<SsTable>],
        lower_ssts: &[Arc<SsTable>],
        task: &CompactionTask,
    ) -> Result<Option<Vec<Arc<SsTable>>>> {
        // 计算分割边界：同时考虑 upper 与 lower 键分布更稳妥
        let mut all_inputs = Vec::new();
        all_inputs.extend_from_slice(upper_ssts);
        all_inputs.extend_from_slice(lower_ssts);
        let boundaries = self.find_subcompaction_boundaries(&all_inputs)?;

        // 如果只有一个边界，不值得进行subcompaction
        if boundaries.len() <= 1 {
            return Ok(None);
        }

        println!("Starting subcompaction with {} sub-tasks", boundaries.len());

        // 并行执行子任务
        let work = || -> Result<Vec<Vec<Arc<SsTable>>>> {
            boundaries
                .par_iter()
                .enumerate()
                .map(|(i, (start_key, end_key))| {
                    // 过滤出与当前范围重叠的SST
                    let mut filtered_upper: Vec<_> = upper_ssts
                        .iter()
                        .filter(|sst| Self::sst_overlaps_range(sst, start_key, end_key))
                        .cloned()
                        .collect();
                    let mut filtered_lower: Vec<_> = lower_ssts
                        .iter()
                        .filter(|sst| Self::sst_overlaps_range(sst, start_key, end_key))
                        .cloned()
                        .collect();
                    // 为满足 concat 的严格要求，确保按 first_key 排序（对 L1+ 层天然成立，这里显式保证）
                    filtered_upper.sort_by(|a, b| a.first_key().cmp(b.first_key()));
                    filtered_lower.sort_by(|a, b| a.first_key().cmp(b.first_key()));

                    // 如果没有数据需要合并，跳过这个子任务
                    if filtered_upper.is_empty() && filtered_lower.is_empty() {
                        return Ok(Vec::new());
                    }

                    println!(
                        "Subcompaction {}: {} upper SSTs, {} lower SSTs",
                        i,
                        filtered_upper.len(),
                        filtered_lower.len()
                    );

                    // 创建迭代器并执行合并：每个子任务严格限制在 [start, end)
                    let result = {
                        // 判断 upper 是否为严格有序无重叠（L1+）
                        let mut upper_is_ordered = true;
                        for w in filtered_upper.windows(2) {
                            if w[0].last_key() >= w[1].first_key() {
                                upper_is_ordered = false;
                                break;
                            }
                        }

                        let lower_iter_range = if filtered_lower.is_empty() {
                            None
                        } else {
                            let base = if let Some(sk) = start_key {
                                SstConcatIterator::create_and_seek_to_key(
                                    filtered_lower.clone(),
                                    sk.as_key_slice(),
                                )?
                            } else {
                                SstConcatIterator::create_and_seek_to_first(filtered_lower.clone())?
                            };
                            Some(RangeLimiter::new(base, start_key.clone(), end_key.clone()))
                        };

                        if filtered_upper.is_empty() {
                            if let Some(lower) = lower_iter_range {
                                self.compact_generate_sst_from_iter(
                                    lower,
                                    task.compact_to_bottom_level(),
                                )?
                            } else {
                                Vec::new()
                            }
                        } else if upper_is_ordered {
                            let base = if let Some(sk) = start_key {
                                SstConcatIterator::create_and_seek_to_key(
                                    filtered_upper.clone(),
                                    sk.as_key_slice(),
                                )?
                            } else {
                                SstConcatIterator::create_and_seek_to_first(filtered_upper.clone())?
                            };
                            let upper_range =
                                RangeLimiter::new(base, start_key.clone(), end_key.clone());
                            if let Some(lower) = lower_iter_range {
                                let merged_iter = TwoMergeIterator::create(upper_range, lower)?;
                                self.compact_generate_sst_from_iter(
                                    merged_iter,
                                    task.compact_to_bottom_level(),
                                )?
                            } else {
                                self.compact_generate_sst_from_iter(
                                    upper_range,
                                    task.compact_to_bottom_level(),
                                )?
                            }
                        } else {
                            // L0：对每个 SST 单独 seek，再 merge
                            let mut iters = Vec::new();
                            for sst in &filtered_upper {
                                let it = if let Some(sk) = start_key {
                                    SsTableIterator::create_and_seek_to_key(
                                        sst.clone(),
                                        sk.as_key_slice(),
                                    )?
                                } else {
                                    SsTableIterator::create_and_seek_to_first(sst.clone())?
                                };
                                iters.push(Box::new(it));
                            }
                            let merged_u = MergeIterator::create(iters);
                            let upper_range =
                                RangeLimiter::new(merged_u, start_key.clone(), end_key.clone());
                            if let Some(lower) = lower_iter_range {
                                let merged_iter = TwoMergeIterator::create(upper_range, lower)?;
                                self.compact_generate_sst_from_iter(
                                    merged_iter,
                                    task.compact_to_bottom_level(),
                                )?
                            } else {
                                self.compact_generate_sst_from_iter(
                                    upper_range,
                                    task.compact_to_bottom_level(),
                                )?
                            }
                        }
                    };

                    Ok(result)
                })
                .collect()
        };

        // Run subcompaction on the same pool as compaction scheduler
        let subcompaction_results: Result<Vec<Vec<Arc<SsTable>>>> =
            self.compaction_pool.install(work);

        // 合并所有子任务的结果
        let all_results = subcompaction_results?;
        let mut final_result = Vec::new();

        for sub_result in all_results {
            final_result.extend(sub_result);
        }

        // 按key范围排序输出SST
        final_result.sort_by(|a: &Arc<SsTable>, b: &Arc<SsTable>| a.first_key().cmp(b.first_key()));

        println!(
            "Subcompaction completed: generated {} SSTs",
            final_result.len()
        );
        Ok(Some(final_result))
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let CompactionOptions::NoCompaction = self.options.compaction_options else {
            panic!("full compaction can only be called with compaction is not enabled")
        };

        let snapshot = {
            let state = self.state.read();
            state.clone()
        };

        let l0_sstables = snapshot.l0_sstables.clone();
        let l1_sstables = snapshot.levels[0].1.clone();

        let compaction_task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sstables.clone(),
            l1_sstables: l1_sstables.clone(),
        };
        println!("force full compaction: {:?}", compaction_task);

        let sstables = self.compact(&compaction_task)?;

        let mut ids: Vec<usize> = Vec::with_capacity(sstables.len());

        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.read().as_ref().clone();

            for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
                let result = state.sstables.remove(sst);
                assert!(result.is_some());
            }
            for new_sst in sstables {
                ids.push(new_sst.sst_id());
                let result = state.sstables.insert(new_sst.sst_id(), new_sst);
                assert!(result.is_none());
            }
            assert_eq!(l1_sstables, state.levels[0].1);
            state.levels[0].1.clone_from(&ids);
            let mut l0_sstables_map = l0_sstables.iter().copied().collect::<HashSet<_>>();
            state.l0_sstables = state
                .l0_sstables
                .iter()
                .filter(|x| !l0_sstables_map.remove(x))
                .copied()
                .collect::<Vec<_>>();
            *self.state.write() = Arc::new(state);
        }

        for sst in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        println!("force full compaction done, new SSTs: {:?}", ids);

        Ok(())
    }

    // trivial move 的主逻辑：不做文件读写，只更新元数据，并按与普通合并一致的流程持久化。
    fn trivial_move(&self, task: LeveledCompactionTask) -> Result<()> {

        // 基本校验（锁外）
        if task.upper_level_sst_ids.is_empty() {
            return Err(anyhow::anyhow!("upper_level_sst_ids cannot be empty"));
        }

        let outputs = task.upper_level_sst_ids.clone();
        let moved_sst = outputs[0];
        let target_level = task.lower_level;

        // 在与其它状态更新串行的临界区内，基于最新快照应用变更并写入 manifest。
        let state_lock = self.state_lock.lock();

        // 1) 使用最新快照再检查一次是否仍然满足“无重叠”，避免过期任务导致状态非法。
        let latest_snapshot = self.state.read().as_ref().clone();
        // lower level 必须存在
        if target_level == 0 || target_level > latest_snapshot.levels.len() {
            // 异常的 compaction 任务，直接跳过
            return Ok(());
        }
        let moved_first = latest_snapshot.sstables[&moved_sst].first_key();
        let moved_last = latest_snapshot.sstables[&moved_sst].last_key();
        let has_overlap = latest_snapshot.levels[target_level - 1].1.iter().any(|id| {
            let sst = &latest_snapshot.sstables[id];
            let first = sst.first_key();
            let last = sst.last_key();
            !(last < moved_first || first > moved_last)
        });

        if has_overlap {
            // 当前已出现重叠，说明该 trivial move 任务已过期，跳过本次移动，留待下一轮生成正确的合并任务。
            return Ok(());
        }

        // 记录日志，便于调试
        println!(
            "Trivial move: {} from {:?} to level {}",
            moved_sst, task.upper_level, target_level
        );

        // 2) 仍然满足无重叠，应用元数据变更（复用 leveled 的 apply_compaction_result 保持一致性）。
        let (new_snapshot, _files_to_remove) = match &self.compaction_controller {
            CompactionController::Leveled(ctrl) => {
                ctrl.apply_compaction_result(&latest_snapshot, &task, &outputs, false)
            }
            _ => unreachable!("trivial move only applies to leveled compaction"),
        };

        // 更新内存状态
        {
            let mut state = self.state.write();
            *state = Arc::new(new_snapshot);
        }

        // 目录刷盘，再记录 manifest（与合并流程保持一致的持久化顺序）
        self.sync_dir()?;
        self.manifest().add_record(
            &state_lock,
            ManifestRecord::Compaction(CompactionTask::Leveled(task), outputs),
        )?;

        Ok(())
    }

    // Compact the task and get the new SSTs
    // 1、生成 compact 任务；
    // 2、执行 compact
    // 3、根据 compact 生成的新的 SSTs，对原有的 ssts 进行删除
    fn trigger_merge_compaction(&self, task: CompactionTask) -> Result<()> {
        println!("running compaction task: {:?}", task);

        // Compact the task and get the new SSTs
        let sstables = self.compact(&task)?;

        let output = sstables.iter().map(|x| x.sst_id()).collect::<Vec<_>>();

        let ssts_to_remove = {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            let mut new_sst_ids = Vec::new();
            for new_file in sstables {
                new_sst_ids.push(new_file.sst_id());
                let result = snapshot.sstables.insert(new_file.sst_id(), new_file);
                assert!(result.is_none());
            }
            let (mut snapshot, files_to_remove) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            let mut ssts_to_remove = Vec::with_capacity(files_to_remove.len());
            for file_remove in &files_to_remove {
                let result = snapshot.sstables.remove(file_remove);
                assert!(result.is_some(), "cannot remove {}.sst", file_remove);
                ssts_to_remove.push(result.unwrap());
            }
            let mut state = self.state.write();
            *state = Arc::new(snapshot);
            drop(state);
            self.sync_dir()?;
            self.manifest()
                .add_record(&state_lock, ManifestRecord::Compaction(task, new_sst_ids))?;
            ssts_to_remove
        };

        println!(
            "compaction finished: {} files removed, {} files added, output={:?}",
            ssts_to_remove.len(),
            output.len(),
            output
        );

        for sst in ssts_to_remove {
            std::fs::remove_file(self.path_of_sst(sst.sst_id()))?;
        }
        Ok(())
    }

    pub(crate) fn spawn_compaction_scheduler_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
        compaction_pool: Arc<ThreadPool>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(25));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => {
                            let this_clone = this.clone();
                            let pool = compaction_pool.clone();
                            pool.spawn(move || {
                                let _guard = CompactionTaskGuard::new(this_clone.active_compactions.clone());
                                     if let Err(e) = this_clone.try_one_compaction() {
                                         eprintln!("parallel compaction worker error: {}", e);
                                     }
                            });
                        },
                        recv(rx) -> _ => return,
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    // Try to pick and run one compaction task while ensuring no conflicting levels run in parallel.
    fn try_one_compaction(&self) -> Result<bool> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };
        let Some(task) = self
            .compaction_controller
            .generate_compaction_task(&snapshot)
        else {
            return Ok(false);
        };

        // Determine involved levels for conflict detection.
        let mut involved = HashSet::new();
        match &task {
            CompactionTask::Leveled(t) => {
                involved.insert(t.lower_level);
                involved.insert(t.upper_level.unwrap_or(0));
            }
            CompactionTask::Simple(t) => {
                involved.insert(t.lower_level);
                involved.insert(t.upper_level.unwrap_or(0));
            }
            CompactionTask::Tiered(t) => {
                for (lvl, _) in &t.tiers {
                    involved.insert(*lvl);
                }
            }
            CompactionTask::ForceFullCompaction { .. } => {
                // Not used with background compaction
                return Ok(false);
            }
        }

        // Try to reserve levels; if conflicted, skip this round.
        {
            let mut running = self.running_compaction.lock();
            if involved.iter().any(|lvl| running.contains(lvl)) {
                return Ok(false);
            }
            for lvl in &involved {
                running.insert(*lvl);
            }
        }

        // Run the selected task, then release the reservation.
        let task_owned = task;
        let res = match task_owned {
            CompactionTask::Leveled(leveled_task)
                if matches!(
                    leveled_task.leveled_task_type,
                    LeveledTaskType::TrivialMoveTask
                ) =>
            {
                self.trivial_move(leveled_task)
            }
            other => self.trigger_merge_compaction(other),
        };
        // Release
        {
            let mut running = self.running_compaction.lock();
            for lvl in involved {
                running.remove(&lvl);
            }
        }
        res?;
        Ok(true)
    }

    fn trigger_flush(&self) -> Result<()> {
        let res = {
            let state = self.state.read();
            state.imm_memtables.len() >= self.options.num_memtable_limit
        };
        if res {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();
        let watermark = self.mvcc().watermark();
        let mut last_key = Vec::<u8>::new();
        let mut first_key_below_watermark = false;
        let current_time = current_timestamp();
        let compaction_filters = self.compaction_filters.lock().clone();
        'outer: while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(
                    self.options.block_size,
                    self.compression_options,
                ));
            }

            let same_as_last_key = iter.key().key_ref() == last_key;
            if !same_as_last_key {
                first_key_below_watermark = true;
            }

            // 删除标记特殊处理
            if compact_to_bottom_level
                && !same_as_last_key
                && iter.key().ts() <= watermark
                && iter.key().key_type() == Type::DELETE
            {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                iter.next()?;
                first_key_below_watermark = false;
                continue;
            }

            // 删除过期数据
            if compact_to_bottom_level
                && !same_as_last_key
                && iter.key().ts() <= watermark
                && iter.key().ttl() != TTL_DEFAULT
                && iter.key().is_expired(current_time)
            {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
                iter.next()?;
                first_key_below_watermark = false;
                continue;
            }

            // 低于或等于水位线的版本只保留每个键的最新版本
            if iter.key().ts() <= watermark {
                if !first_key_below_watermark {
                    iter.next()?;
                    continue;
                }
                first_key_below_watermark = false;

                if !compaction_filters.is_empty() {
                    for filter in &compaction_filters {
                        match filter {
                            CompactionFilter::Prefix(pre_key) => {
                                if iter.key().key_ref().starts_with(pre_key) {
                                    iter.next()?;
                                    continue 'outer;
                                }
                            }
                        }
                    }
                }
            }

            let builder_inner = builder.as_mut().unwrap();

            if builder_inner.estimated_size() >= self.options.target_sst_size && !same_as_last_key {
                let sst_id = self.next_sst_id();
                let old_builder = builder.take().unwrap();
                let sst = Arc::new(old_builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
                builder = Some(SsTableBuilder::new(
                    self.options.block_size,
                    self.compression_options,
                ));
            }
            let builder_inner = builder.as_mut().unwrap();
            builder_inner.add(iter.key(), iter.value());

            if !same_as_last_key {
                last_key.clear();
                last_key.extend(iter.key().key_ref());
            }
            iter.next()?;
        }

        if let Some(builder) = builder {
            let sst_id = self.next_sst_id();
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }
}
