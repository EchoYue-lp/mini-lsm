use crate::compact::{CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions};
use crate::compression::CompressionOptions;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageOptions, MiniLsm};
use bytes::Bytes;
use std::collections::Bound;
use tempfile::tempdir;

/// 基础读写测试
#[test]
fn test_basic_operations() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

    // 写入测试
    storage.put(b"key1", b"value1").unwrap();
    storage.put(b"key2", b"value2").unwrap();
    storage.put(b"key3", b"value3").unwrap();

    // 读取测试
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::copy_from_slice(b"value1".as_ref()))
    );
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::copy_from_slice(b"value2".as_ref()))
    );
    assert_eq!(
        storage.get(b"key3").unwrap(),
        Some(Bytes::copy_from_slice(b"value3".as_ref()))
    );
    assert_eq!(storage.get(b"nonexistent").unwrap(), None);

    // 更新测试
    storage.put(b"key1", b"new_value1").unwrap();
    assert_eq!(
        storage.get(b"key1").unwrap(),
        Some(Bytes::copy_from_slice(b"new_value1".as_ref()))
    );

    // 删除测试
    storage.delete(b"key2").unwrap();
    assert_eq!(storage.get(b"key2").unwrap(), None);

    // 再次写入删除的key
    storage.put(b"key2", b"value2_restored").unwrap();
    assert_eq!(
        storage.get(b"key2").unwrap(),
        Some(Bytes::copy_from_slice(b"value2_restored".as_ref()))
    );
}

/// Scan操作测试
#[test]
fn test_scan_operations() {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

    // 写入测试数据
    for i in 0..100 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    // 范围扫描测试
    let mut iter = storage
        .scan(
            Bound::Included("key_010".as_bytes()),
            Bound::Included("key_019".as_bytes()),
        )
        .unwrap();

    let mut results = Vec::new();
    while iter.is_valid() {
        results.push((
            String::from_utf8_lossy(iter.key()).to_string(),
            String::from_utf8_lossy(iter.value()).to_string(),
        ));
        iter.next().unwrap();
    }

    assert_eq!(results.len(), 10);
    assert_eq!(results[0].0, "key_010");
    assert_eq!(results[9].0, "key_019");
}

/// 不同compaction策略测试
#[test]
fn test_compaction_strategies() {
    let compaction_options = vec![
        ("no_compaction", CompactionOptions::NoCompaction),
        (
            "leveled",
            CompactionOptions::Leveled(LeveledCompactionOptions {
                level_size_multiplier: 10,
                level0_file_num_compaction_trigger: 4,
                max_levels: 7,
                base_level_size_mb: 256,
            }),
        ),
        (
            "simple_leveled",
            CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                size_ratio_percent: 200,
                level0_file_num_compaction_trigger: 4,
                max_levels: 7,
            }),
        ),
    ];

    for (strategy_name, compaction) in compaction_options {
        println!("Testing compaction strategy: {}", strategy_name);
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default_for_week2_test(compaction);
        let storage = MiniLsm::open(&dir, options).unwrap();

        // 写入数据触发compaction
        for i in 0..1000 {
            let key = format!("key_{:05}", i);
            let value = format!("value_{:05}", i);
            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // 验证数据完整性
        for i in 0..100 {
            let key = format!("key_{:05}", i);
            let expected = format!("value_{:05}", i);
            assert_eq!(
                storage.get(key.as_bytes()).unwrap(),
                Some(Bytes::copy_from_slice(expected.as_bytes()))
            );
        }

        // 测试删除后读取
        for i in 100..200 {
            let key = format!("key_{:05}", i);
            storage.delete(key.as_bytes()).unwrap();
            assert_eq!(storage.get(key.as_bytes()).unwrap(), None);
        }
    }
}

/// 完整集成测试
#[test]
fn test_complete_integration() {
    let dir = tempdir().unwrap();

    // 使用Leveled compaction + Snappy压缩
    let leveled_opts = LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 2,
        max_levels: 4,
        base_level_size_mb: 64,
    };

    let mut options =
        LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(leveled_opts));
    options.compression_options = CompressionOptions::Snappy;

    let storage = MiniLsm::open(&dir, options.clone()).unwrap();

    // 阶段1: 写入数据
    for i in 0..100000 {
        let key = format!("user_{:05}", i);
        let value = format!("data_{:05}_{}", i, "integration_test");
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    println!("完成阶段一：写入 100000 条记录");

    // 阶段2: 冻结
    storage
        .inner
        .force_freeze_memtable(&storage.inner.state_lock.lock())
        .unwrap();
    println!("完成阶段二：force_freeze_memtable");

    // 阶段3: 再次写入数据
    for i in 100000..110000 {
        let key = format!("user_{:05}", i);
        let value = format!("data_{:05}_{}", i, "integration_test");
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }
    println!("完成阶段三：再次写入数据");

    // 阶段4: 验证读取
    for i in 0..10000 {
        let key = format!("user_{:05}", i);
        let expected = format!("data_{:05}_{}", i, "integration_test");
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::copy_from_slice(expected.as_bytes()))
        );
    }
    println!("完成阶段四：验证读取");

    // 阶段5: 更新数据
    for i in 0..20000 {
        let key = format!("user_{:05}", i);
        let new_value = format!("updated_data_{:05}", i);
        storage.put(key.as_bytes(), new_value.as_bytes()).unwrap();
    }
    println!("完成阶段五：更新数据");

    // 阶段6: 删除数据
    for i in 30000..40000 {
        let key = format!("user_{:05}", i);
        storage.delete(key.as_bytes()).unwrap();
        assert_eq!(storage.get(key.as_bytes()).unwrap(), None);
    }
    println!("完成阶段六：删除数据");

    // 阶段7: 范围扫描
    let mut iter = storage
        .scan(
            Bound::Included("user_50000".as_bytes()),
            Bound::Included("user_59999".as_bytes()),
        )
        .unwrap();

    let mut count = 0;
    while iter.is_valid() {
        count += 1;
        iter.next().unwrap();
    }
    assert_eq!(count, 10000);
    println!("完成阶段七：范围扫描");

    // 阶段8: 测试大数据
    let big_data = vec![b'x'; 10000];
    storage.put(b"big_key", &big_data).unwrap();
    assert_eq!(
        storage.get(b"big_key").unwrap(),
        Some(Bytes::copy_from_slice(big_data.as_slice()))
    );
    println!("完成阶段八：测试大数据");

    // 阶段9: 测试特殊字符
    storage.put(b"special!@#$%", b"special_value").unwrap();
    assert_eq!(
        storage.get(b"special!@#$%").unwrap(),
        Some(Bytes::copy_from_slice(b"special_value".as_ref()))
    );
    println!("完成阶段九：测试特殊字符");

    // 阶段10: 测试更新后读取
    for i in 15000..18000 {
        let key = format!("user_{:05}", i);
        let expected = format!("updated_data_{:05}", i);
        assert_eq!(
            storage.get(key.as_bytes()).unwrap(),
            Some(Bytes::copy_from_slice(expected.as_bytes()))
        );
    }
    println!("完成阶段十：测试更新后读取");

    // 阶段11: 测试删除后读取
    for i in 35000..38000 {
        let key = format!("user_{:05}", i);
        assert_eq!(storage.get(key.as_bytes()).unwrap(), None);
    }
    println!("完成阶段十一：测试删除后读取");

    // 阶段12: 测试关闭
    storage.sync().unwrap();
    storage.close().unwrap();
    println!("完成阶段十二：测试关闭");

    {
        // 阶段13: 测试重新打开存储并验证数据
        // let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();
        let storage = MiniLsm::open(&dir, options).unwrap();

        for i in 50000..70000 {
            let key = format!("user_{:05}", i);
            let expected = format!("data_{:05}_{}", i, "integration_test");
            assert_eq!(
                storage.get(key.as_bytes()).unwrap(),
                Some(Bytes::copy_from_slice(expected.as_bytes()))
            );
        }
        println!("完成阶段十三：测试重新打开存储并验证数据");
    }
}

/// 恢复测试
#[test]
fn test_recovery() {
    let dir = tempdir().unwrap();
    let dir_path = dir.path().to_path_buf();

    {
        // 创建并写入数据
        let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

        for i in 0..500000 {
            let key = format!("recovery_{:05}", i);
            let value = format!("recovery_value_{:05}", i);
            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
        }

        // 确保数据持久化到磁盘
        storage.close().unwrap();
    }

    {
        // 重新打开存储并验证数据
        let storage =
            MiniLsm::open(&dir_path, LsmStorageOptions::default_for_week1_test()).unwrap();

        for i in 100000..200000 {
            let key = format!("recovery_{:05}", i);
            let expected = format!("recovery_value_{:05}", i);
            assert_eq!(
                storage.get(key.as_bytes()).unwrap(),
                Some(Bytes::copy_from_slice(expected.as_bytes()))
            );
        }
    }
}
