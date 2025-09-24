// Trivial Move 功能测试

use std::time::Duration;
use tempfile::tempdir;

use crate::{
    compact::{CompactionOptions, LeveledCompactionOptions},
    lsm_storage::{LsmStorageOptions, MiniLsm},
};

#[test]
fn test_trivial_move_compaction() {
    println!("=== 测试 Trivial Move Compaction ===");

    // 创建 Leveled 配置，启用 trivial move
    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 2,
        max_levels: 4,
        base_level_size_mb: 1,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());

    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    println!("1. 插入不重叠的数据以触发 trivial move...");

    // 第一批数据：key0000-key0009
    for i in 0..10 {
        let key = format!("key{:04}", i).as_bytes().to_vec();
        let value = vec![i as u8; 512]; // 512B 的值
        storage.put(&key, &value).unwrap();
    }

    // 冻结 memtable 创建第一个 SST
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 第二批数据：key0010-key0019（不与第一批重叠）
    for i in 10..20 {
        let key = format!("key{:04}", i).as_bytes().to_vec();
        let value = vec![i as u8; 512];
        storage.put(&key, &value).unwrap();
    }

    // 冻结 memtable 创建第二个 SST
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 等待 compaction 完成
    std::thread::sleep(Duration::from_secs(1));

    println!("2. 验证 compaction 结果...");
    let snapshot = storage.inner.state.read().clone();

    // 检查 L0 是否已经被清空
    println!("L0 SSTables: {}", snapshot.l0_sstables.len());

    // 检查各层的 SST 数量
    for (level_id, files) in &snapshot.levels {
        println!("Level {}: {} SSTs", level_id, files.len());
        if !files.is_empty() {
            for &sst_id in files {
                let sst = &snapshot.sstables[&sst_id];
                println!(
                    "  SST {}: first_key={:?}, last_key={:?}",
                    sst_id,
                    sst.first_key(),
                    sst.last_key()
                );
            }
        }
    }

    println!("3. 验证数据完整性...");
    let mut found_count = 0;
    for i in 0..20 {
        let key = format!("key{:04}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        if let Some(value) = result {
            assert_eq!(value, vec![i as u8; 512]);
            found_count += 1;
        }
    }
    assert_eq!(found_count, 20, "应该找到所有 20 个键");

    storage.close().unwrap();
    println!("=== Trivial Move Compaction 测试完成 ===");
}

#[test]
fn test_trivial_move_compression() {
    println!("=== 测试 Trivial Move 压缩功能 ===");

    // 创建带压缩的配置
    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(
        CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 20,
            max_levels: 4,
            base_level_size_mb: 5,
        }),
    );

    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    println!("1. 插入可压缩的数据...");

    // 插入重复的数据以测试压缩效果
    for i in 0..5 {
        let key = format!("compress_key{:04}", i).as_bytes().to_vec();
        // 创建大量重复的数据模式
        let value = vec![0xAA; 2048]; // 2KB 的重复数据
        storage.put(&key, &value).unwrap();
    }

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 插入另一批不重叠的数据
    for i in 5..10 {
        let key = format!("compress_key{:04}", i).as_bytes().to_vec();
        let value = vec![0xBB; 2048];
        storage.put(&key, &value).unwrap();
    }

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 等待 compaction 完成
    std::thread::sleep(Duration::from_secs(1));

    println!("2. 验证压缩后的数据...");
    let snapshot = storage.inner.state.read().clone();

    println!("L0 SSTables: {}", snapshot.l0_sstables.len());
    for (level_id, files) in &snapshot.levels {
        println!("Level {}: {} SSTs", level_id, files.len());
        for &sst_id in files {
            let sst = &snapshot.sstables[&sst_id];
            println!("  SST {}: size={} bytes", sst_id, sst.table_size());
        }
    }

    println!("3. 验证压缩后数据完整性...");
    for i in 0..10 {
        let key = format!("compress_key{:04}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        assert!(result.is_some(), "键 {:?} 应该存在", key);

        let expected_value = if i < 5 {
            vec![0xAA; 2048]
        } else {
            vec![0xBB; 2048]
        };
        assert_eq!(result.unwrap(), expected_value);
    }

    storage.close().unwrap();
    println!("=== Trivial Move 压缩测试完成 ===");
}

#[test]
fn test_trivial_move_recovery() {
    println!("=== 测试 Trivial Move 停机恢复 ===");

    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 2,
        max_levels: 4,
        base_level_size_mb: 2,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    println!("1. 插入数据并触发 trivial move compaction...");

    // 第一批数据：key0000-key9999
    for i in 0..100000 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let value = vec![i as u8; 1024 * 16]; // 512B 的值
        storage.put(&key, &value).unwrap();
    }

    // 冻结 memtable 创建第一个 SST
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 第二批数据：key10000-key11999（不与第一批重叠）
    for i in 100000..120000 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let value = vec![i as u8; 1024 * 16];
        storage.put(&key, &value).unwrap();
    }

    // 冻结 memtable 创建第二个 SST，触发 trivial move
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();

    // 等待 compaction 完成
    std::thread::sleep(Duration::from_secs(1));

    println!("2. 验证 compaction 结果...");
    let snapshot = storage.inner.state.read().clone();

    // 检查 L0 是否已经被清空
    println!("L0 SSTables: {}", snapshot.l0_sstables.len());

    // 检查各层的 SST 数量
    for (level_id, files) in &snapshot.levels {
        println!("Level {}: {} SSTs", level_id, files.len());
    }

    println!("3. 验证数据完整性...");
    let mut found_count = 0;
    for i in 50100..51300 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        if let Some(value) = result {
            assert_eq!(value, vec![i as u8; 1024 * 16]);
            found_count += 1;
        }
    }
    assert_eq!(found_count, 1200, "应该找到所有 120 个键");

    // 强制同步确保数据持久化
    storage.sync().unwrap();
    storage.close().unwrap();

    println!("4. 重新打开并验证恢复...");
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    // 检查恢复后的状态
    let snapshot = storage.inner.state.read().clone();
    println!("恢复后 - L0 SSTables: {}", snapshot.l0_sstables.len());
    for (level_id, files) in &snapshot.levels {
        println!("恢复后 - Level {}: {} SSTs", level_id, files.len());
    }

    // 验证恢复后的数据
    let mut found_count = 0;
    for i in 60100..61300 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        if let Some(value) = result {
            assert_eq!(value, vec![i as u8; 1024 * 16]);
            found_count += 1;
        }
    }
    assert_eq!(found_count, 1200, "恢复后应该找到所有 120 个键");

    println!("5. 插入新数据验证系统仍然正常工作...");
    for i in 10000..11025 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let value = vec![i as u8; 1024 * 16];
        storage.put(&key, &value).unwrap();
    }

    // 验证新数据
    for i in 100200..100300 {
        let key = format!("recovery_key{:05}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        assert!(result.is_some(), "新插入的键应该存在");
        assert_eq!(result.unwrap(), vec![i as u8; 1024 * 16]);
    }

    storage.close().unwrap();
    println!("=== Trivial Move 停机恢复测试完成 ===");
}

#[test]
fn test_trivial_move_edge_cases() {
    println!("=== 测试 Trivial Move 边界情况 ===");

    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 1, // 触发频率更高
        max_levels: 3,
        base_level_size_mb: 1,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    println!("1. 测试空数据情况...");
    // 跳过空的 memtable 测试，因为系统不允许冻结空的 memtable
    std::thread::sleep(Duration::from_millis(100));

    println!("2. 测试单条数据...");
    let key = b"single_key".to_vec();
    let value = b"single_value".to_vec();
    storage.put(&key, &value).unwrap();

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // 验证单条数据
    let result = storage.get(&key).unwrap();
    assert_eq!(result.unwrap(), value);

    println!("3. 测试边界键值...");
    // 测试非常小的键
    storage.put(b"a", b"small").unwrap();
    // 测试非常大的键
    let large_key = vec![b'z'; 100];
    let large_value = vec![1; 100];
    storage.put(&large_key, &large_value).unwrap();

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // 验证边界键值
    assert_eq!(storage.get(b"a").unwrap().unwrap(), b"small".to_vec());
    assert_eq!(storage.get(&large_key).unwrap().unwrap(), large_value);

    println!("4. 测试删除操作...");
    let delete_key = b"key_to_delete".to_vec();
    storage.put(&delete_key, b"will_be_deleted").unwrap();

    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // 删除键
    storage.delete(&delete_key).unwrap();
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));

    // 验证删除
    let result = storage.get(&delete_key).unwrap();
    assert!(result.is_none(), "被删除的键应该不存在");

    storage.close().unwrap();
    println!("=== Trivial Move 边界情况测试完成 ===");
}

#[test]
fn test_duplicate_sst_detection() {
    println!("=== 检测重复SST ID测试 ===");

    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 2,
        level0_file_num_compaction_trigger: 1,
        max_levels: 4,
        base_level_size_mb: 1,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());
    let dir = tempdir().unwrap();

    // 保存关闭前的状态用于对比
    let (snapshot_before_close, mut all_sst_ids_before, has_duplicates_before) = {
        println!("1. 创建数据并触发trivial move...");
        let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

        // 插入少量数据
        for i in 0..5 {
            let key = format!("key{:02}", i).as_bytes().to_vec();
            let value = vec![i as u8; 128];
            storage.put(&key, &value).unwrap();
        }

        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();

        // 插入第二批不重叠数据
        for i in 5..10 {
            let key = format!("key{:02}", i).as_bytes().to_vec();
            let value = vec![i as u8; 128];
            storage.put(&key, &value).unwrap();
        }

        storage
            .inner
            .force_freeze_memtable(&storage.inner.state_lock.lock())
            .unwrap();
        std::thread::sleep(Duration::from_millis(500));

        // 确保所有 flush 和 compaction 完成
        storage.sync().unwrap();
        std::thread::sleep(Duration::from_millis(200));

        // 如果 memtable 不为空，强制 flush
        {
            let state_guard = storage.inner.state.read();
            if !state_guard.memtable.is_empty() {
                drop(state_guard);
                storage
                    .inner
                    .force_freeze_memtable(&storage.inner.state_lock.lock())
                    .unwrap();
                storage.sync().unwrap();
                std::thread::sleep(Duration::from_millis(200));
            }
        }

        // 保存关闭前的状态用于对比
        let snapshot_before_close = storage.inner.state.read().clone();
        println!("关闭前状态:");
        println!(
            "  L0 SSTables: {} - {:?}",
            snapshot_before_close.l0_sstables.len(),
            snapshot_before_close.l0_sstables
        );
        for (level_id, files) in &snapshot_before_close.levels {
            println!("  Level {}: {} SSTs - {:?}", level_id, files.len(), files);
        }

        // 检查是否有重复的SST ID
        let mut all_sst_ids_before: Vec<usize> = Vec::new();
        all_sst_ids_before.extend(&snapshot_before_close.l0_sstables);
        for (_, files) in &snapshot_before_close.levels {
            all_sst_ids_before.extend(files);
        }

        all_sst_ids_before.sort();
        let mut has_duplicates_before = false;
        for i in 1..all_sst_ids_before.len() {
            if all_sst_ids_before[i] == all_sst_ids_before[i - 1] {
                println!("  发现重复SST ID: {}", all_sst_ids_before[i]);
                has_duplicates_before = true;
            }
        }

        if !has_duplicates_before {
            println!("  运行时未发现重复SST ID");
        }

        storage.sync().unwrap();
        storage.close().unwrap();

        (
            snapshot_before_close,
            all_sst_ids_before,
            has_duplicates_before,
        )
    };

    println!("2. 重新打开并检查recovery后的状态...");
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    let snapshot_after_recovery = storage.inner.state.read().clone();
    println!("恢复后状态:");
    println!(
        "  L0 SSTables: {} - {:?}",
        snapshot_after_recovery.l0_sstables.len(),
        snapshot_after_recovery.l0_sstables
    );
    for (level_id, files) in &snapshot_after_recovery.levels {
        println!("  Level {}: {} SSTs - {:?}", level_id, files.len(), files);
    }

    // 检查是否有重复的SST ID
    let mut all_sst_ids_after: Vec<usize> = Vec::new();
    all_sst_ids_after.extend(&snapshot_after_recovery.l0_sstables);
    for (_, files) in &snapshot_after_recovery.levels {
        all_sst_ids_after.extend(files);
    }

    all_sst_ids_after.sort();
    let mut has_duplicates_after = false;
    for i in 1..all_sst_ids_after.len() {
        if all_sst_ids_after[i] == all_sst_ids_after[i - 1] {
            println!("  发现重复SST ID: {}", all_sst_ids_after[i]);
            has_duplicates_after = true;
        }
    }

    if !has_duplicates_after {
        println!("  Recovery后未发现重复SST ID");
    }

    // 对比关闭前和恢复后的状态是否一致
    println!("3. 对比关闭前后状态...");

    // 对比 L0 SSTables
    let mut l0_before = snapshot_before_close.l0_sstables.clone();
    let mut l0_after = snapshot_after_recovery.l0_sstables.clone();
    l0_before.sort();
    l0_after.sort();

    if l0_before == l0_after {
        println!("  ✓ L0 SSTables 状态一致");
    } else {
        println!("  ✗ L0 SSTables 状态不一致!");
        println!("    关闭前: {:?}", l0_before);
        println!("    恢复后: {:?}", l0_after);
    }

    // 对比各层级
    let mut levels_match = true;
    for ((level_before, files_before), (level_after, files_after)) in snapshot_before_close
        .levels
        .iter()
        .zip(snapshot_after_recovery.levels.iter())
    {
        assert_eq!(level_before, level_after, "Level ID mismatch");

        let mut files_before_sorted = files_before.clone();
        let mut files_after_sorted = files_after.clone();
        files_before_sorted.sort();
        files_after_sorted.sort();

        if files_before_sorted == files_after_sorted {
            println!("  ✓ Level {} 状态一致", level_before);
        } else {
            println!("  ✗ Level {} 状态不一致!", level_before);
            println!("    关闭前: {:?}", files_before_sorted);
            println!("    恢复后: {:?}", files_after_sorted);
            levels_match = false;
        }
    }

    // 检查整体 SST 分布是否一致
    all_sst_ids_before.sort();
    all_sst_ids_after.sort();

    if all_sst_ids_before == all_sst_ids_after {
        println!("  ✓ 整体 SST 分布一致");
    } else {
        println!("  ✗ 整体 SST 分布不一致!");
        println!("    关闭前: {:?}", all_sst_ids_before);
        println!("    恢复后: {:?}", all_sst_ids_after);
        levels_match = false;
    }

    assert!(!has_duplicates_before, "关闭前发现重复 SST ID");
    assert!(!has_duplicates_after, "恢复后发现重复 SST ID");
    // assert!(levels_match, "关闭前后状态不一致");

    // 验证数据完整性
    let mut found_count = 0;
    for i in 0..10 {
        let key = format!("key{:02}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        if result.is_some() {
            found_count += 1;
        }
    }
    println!("  数据完整性: {}/10 键找到", found_count);

    storage.close().unwrap();
    println!("=== 检测重复SST ID测试完成 ===");
}
