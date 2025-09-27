use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use bytes::Bytes;
use crate::lsm_storage::{LsmStorageOptions, MiniLsm};
use crate::compact::{CompactionOptions, LeveledCompactionOptions};
use std::ops::Bound;
use crate::compression::CompressionOptions;
use crate::iterators::StorageIterator;

#[test]
fn test_ttl_functionality() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    let key = b"test_key";
    let value = b"test_value";
    let ttl_secs: u64 = 2; // expire in 2 seconds

    // Put with TTL
    storage.put_with_ttl(key, value, ttl_secs).unwrap();

    // Should be able to get the value immediately
    let result = storage.get(key).unwrap();
    assert_eq!(result, Some(value.as_slice().into()));

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(ttl_secs + 1));

    // Should not be able to get the value after TTL expires
    let result = storage.get(key).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_delete_functionality() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    let key = b"test_key";
    let value = b"test_value";

    // Put a value
    storage.put(key, value).unwrap();

    // Should be able to get the value
    let result = storage.get(key).unwrap();
    assert_eq!(result, Some(value.as_slice().into()));

    // Delete the key
    storage.delete(key).unwrap();

    // Should not be able to get the value after deletion
    let result = storage.get(key).unwrap();
    assert_eq!(result, None);
}

#[test]
fn test_put_after_ttl_expires() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    let key = b"test_key";
    let value1 = b"test_value1";
    let value2 = b"test_value2";
    let ttl_secs: u64 = 1; // expire in 1 second

    // Put with TTL
    storage.put_with_ttl(key, value1, ttl_secs).unwrap();

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(ttl_secs + 1));

    // Put new value after TTL expires
    storage.put(key, value2).unwrap();

    // Should get the new value
    let result = storage.get(key).unwrap();
    assert_eq!(result, Some(value2.as_slice().into()));
}

#[test]
fn test_ttl_zero_means_no_expiration() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    let key = b"test_key";
    let value = b"test_value";

    // Put with TTL = 0 (no expiration)
    storage.put_with_ttl(key, value, 0).unwrap();

    // Should be able to get the value
    let result = storage.get(key).unwrap();
    assert_eq!(result, Some(value.as_slice().into()));

    // Wait some time
    thread::sleep(Duration::from_secs(1));

    // Should still be able to get the value
    let result = storage.get(key).unwrap();
    assert_eq!(result, Some(value.as_slice().into()));
}

#[test]
fn test_empty_value_vs_delete() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    let key1 = b"empty_value_key";
    let key2 = b"delete_key";
    let empty_value = b"";
    let normal_value = b"normal_value";

    // Put normal values first
    storage.put(key1, normal_value).unwrap();
    storage.put(key2, normal_value).unwrap();

    // Verify both keys exist
    assert_eq!(
        storage.get(key1).unwrap(),
        Some(normal_value.as_slice().into())
    );
    assert_eq!(
        storage.get(key2).unwrap(),
        Some(normal_value.as_slice().into())
    );

    // Put empty string for key1 (this should store an empty value)
    storage.put(key1, empty_value).unwrap();

    // Delete key2 (this should mark it as deleted)
    storage.delete(key2).unwrap();

    // key1 should return empty bytes (not None)
    let result1 = storage.get(key1).unwrap();
    assert_eq!(result1, Some(empty_value.as_slice().into()));

    // key2 should return None (deleted)
    let result2 = storage.get(key2).unwrap();
    assert_eq!(result2, None);

    // Test with a new key - putting empty value directly
    let key3 = b"new_empty_key";
    storage.put(key3, empty_value).unwrap();

    let result3 = storage.get(key3).unwrap();
    assert_eq!(result3, Some(empty_value.as_slice().into()));
}

#[test]
fn test_comprehensive_ttl_and_delete() {
    let dir = tempfile::tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week2_test(CompactionOptions::NoCompaction)).unwrap();

    // Test 1: TTL with regular value
    let key1 = b"ttl_key";
    let value1 = b"ttl_value";
    let ttl_secs: u64 = 1; // expire in 1 second
    storage.put_with_ttl(key1, value1, ttl_secs).unwrap();

    // Test 2: Regular put (no TTL)
    let key2 = b"regular_key";
    let value2 = b"regular_value";
    storage.put(key2, value2).unwrap();

    // Test 3: Empty value (not delete)
    let key3 = b"empty_key";
    let empty_value = b"";
    storage.put(key3, empty_value).unwrap();

    // Test 4: Delete operation
    let key4 = b"delete_key";
    let value4 = b"will_be_deleted";
    storage.put(key4, value4).unwrap();
    storage.delete(key4).unwrap();

    // Verify initial states
    assert_eq!(storage.get(key1).unwrap(), Some(value1.as_slice().into()));
    assert_eq!(storage.get(key2).unwrap(), Some(value2.as_slice().into()));
    assert_eq!(storage.get(key3).unwrap(), Some(empty_value.as_slice().into()));
    assert_eq!(storage.get(key4).unwrap(), None); // deleted

    // Wait for TTL to expire
    thread::sleep(Duration::from_secs(ttl_secs + 1));

    // Verify states after TTL expiration
    assert_eq!(storage.get(key1).unwrap(), None); // TTL expired
    assert_eq!(storage.get(key2).unwrap(), Some(value2.as_slice().into())); // no TTL
    assert_eq!(storage.get(key3).unwrap(), Some(empty_value.as_slice().into())); // empty value
    assert_eq!(storage.get(key4).unwrap(), None); // still deleted
}

/// 大规模Delete功能测试 - 类似test_complete_integration的规模
#[test]
fn test_large_scale_delete_operations() {
    println!("=== 开始大规模Delete操作测试 ===");
    let dir = tempdir().unwrap();

    // 使用Leveled compaction提高测试真实性
    let leveled_opts = LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 4,
        max_levels: 6,
        base_level_size_mb: 128,
    };

    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(leveled_opts));
    options.compression_options = CompressionOptions::Snappy;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();

    // 阶段1: 写入大量数据 - 100万条记录
    println!("阶段1: 写入100万条记录...");
    for i in 0..1_000_000 {
        let key = format!("delete_test_key_{:08}", i);
        let value = format!("delete_test_value_{:08}_{}", i, "large_scale_test");
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条记录", i);
        }
    }
    println!("完成阶段1: 写入100万条记录");

    // 阶段2: 触发compaction
    storage.inner.force_freeze_memtable(&storage.inner.state_lock.lock()).unwrap();
    thread::sleep(Duration::from_millis(500));
    println!("完成阶段2: 触发compaction");

    // 阶段3: 验证数据完整性（抽样验证）
    println!("阶段3: 验证数据完整性...");
    for i in (0..1_000_000).step_by(10_000) {
        let key = format!("delete_test_key_{:08}", i);
        let expected = format!("delete_test_value_{:08}_{}", i, "large_scale_test");
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())));
    }
    println!("完成阶段3: 验证数据完整性");

    // 阶段4: 大规模删除操作 - 删除50万条记录（偶数索引）
    println!("阶段4: 删除50万条记录（偶数索引）...");
    let mut deleted_count = 0;
    for i in (0..1_000_000).step_by(2) {
        let key = format!("delete_test_key_{:08}", i);
        storage.delete(key.as_bytes()).unwrap();
        deleted_count += 1;

        if deleted_count % 50_000 == 0 {
            println!("  已删除 {} 条记录", deleted_count);
        }
    }
    println!("完成阶段4: 删除了 {} 条记录", deleted_count);

    // 阶段5: 验证删除效果
    println!("阶段5: 验证删除效果...");
    let mut verified_deleted = 0;
    let mut verified_exists = 0;

    for i in (0..1_000_000).step_by(1000) { // 抽样验证
        let key = format!("delete_test_key_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();

        if i % 2 == 0 {
            // 偶数索引应该被删除
            assert_eq!(result, None, "键 {} 应该被删除", key);
            verified_deleted += 1;
        } else {
            // 奇数索引应该仍然存在
            let expected = format!("delete_test_value_{:08}_{}", i, "large_scale_test");
            assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())),
                      "键 {} 应该仍然存在", key);
            verified_exists += 1;
        }
    }
    println!("完成阶段5: 验证了 {} 条删除记录和 {} 条存在记录", verified_deleted, verified_exists);

    // 阶段6: 范围扫描测试（包含删除的键）
    println!("阶段6: 范围扫描测试...");
    let mut iter = storage.scan(
        Bound::Included("delete_test_key_00500000".as_bytes()),
        Bound::Included("delete_test_key_00599999".as_bytes()),
    ).unwrap();

    let mut scan_count = 0;
    while iter.is_valid() {
        scan_count += 1;
        iter.next().unwrap();
    }

    // 这个范围内有100000个键，其中50000个奇数键存在，50000个偶数键被删除
    assert_eq!(scan_count, 50000, "扫描应该找到50000个未删除的键");
    println!("完成阶段6: 扫描找到 {} 个键", scan_count);

    storage.sync().unwrap();
    storage.close().unwrap();
    println!("=== 完成大规模Delete操作测试 ===");
}

/// 大规模TTL功能测试
#[test]
fn test_large_scale_ttl_operations() {
    println!("=== 开始大规模TTL操作测试 ===");
    let dir = tempdir().unwrap();

    let leveled_opts = LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 4,
        max_levels: 6,
        base_level_size_mb: 128,
    };

    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(leveled_opts));
    options.compression_options = CompressionOptions::Snappy;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();

    // 阶段1: 写入不同TTL的大量数据
    println!("阶段1: 写入不同TTL的大量数据...");

    // 写入50万条短TTL数据（30秒后过期）
    for i in 0..500_000 {
        let key = format!("ttl_short_{:08}", i);
        let value = format!("short_ttl_value_{:08}", i);
        storage.put_with_ttl(key.as_bytes(), value.as_bytes(), 30).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条短TTL记录", i);
        }
    }

    // 写入30万条长TTL数据（60秒后过期）
    for i in 0..300_000 {
        let key = format!("ttl_long_{:08}", i);
        let value = format!("long_ttl_value_{:08}", i);
        storage.put_with_ttl(key.as_bytes(), value.as_bytes(), 60).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条长TTL记录", i);
        }
    }

    // 写入20万条无TTL数据（永不过期）
    for i in 0..200_000 {
        let key = format!("no_ttl_{:08}", i);
        let value = format!("no_ttl_value_{:08}", i);
        storage.put_with_ttl(key.as_bytes(), value.as_bytes(), 0).unwrap(); // TTL=0表示永不过期

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条无TTL记录", i);
        }
    }

    println!("完成阶段1: 写入了50万短TTL + 30万长TTL + 20万无TTL记录");

    // 阶段2: 立即验证所有数据都存在
    println!("阶段2: 验证所有数据都存在...");

    // 验证短TTL数据（抽样）
    for i in (0..500_000).step_by(10_000) {
        let key = format!("ttl_short_{:08}", i);
        let expected = format!("short_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())));
    }

    // 验证长TTL数据（抽样）
    for i in (0..300_000).step_by(10_000) {
        let key = format!("ttl_long_{:08}", i);
        let expected = format!("long_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())));
    }

    // 验证无TTL数据（抽样）
    for i in (0..200_000).step_by(10_000) {
        let key = format!("no_ttl_{:08}", i);
        let expected = format!("no_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())));
    }

    println!("完成阶段2: 验证所有数据都存在");

    // 阶段3: 等待短TTL过期
    println!("阶段3: 等待短TTL数据过期（等待35秒）...");
    thread::sleep(Duration::from_secs(35));

    // 阶段4: 验证短TTL数据已过期，其他数据仍存在
    println!("阶段4: 验证TTL过期效果...");

    let mut expired_count = 0;
    let mut still_exists_count = 0;

    // 验证短TTL数据已过期（抽样）
    for i in (0..500_000).step_by(5000) {
        let key = format!("ttl_short_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, None, "短TTL键 {} 应该已过期", key);
        expired_count += 1;
    }

    // 验证长TTL数据仍存在（抽样）
    for i in (0..300_000).step_by(5000) {
        let key = format!("ttl_long_{:08}", i);
        let expected = format!("long_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())),
                  "长TTL键 {} 应该仍然存在", key);
        still_exists_count += 1;
    }

    // 验证无TTL数据仍存在（抽样）
    for i in (0..200_000).step_by(5000) {
        let key = format!("no_ttl_{:08}", i);
        let expected = format!("no_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())),
                  "无TTL键 {} 应该仍然存在", key);
        still_exists_count += 1;
    }

    println!("完成阶段4: 验证了 {} 个已过期键和 {} 个仍存在键", expired_count, still_exists_count);

    // 阶段5: 范围扫描测试（包含已过期的键）
    println!("阶段5: 范围扫描测试（应该跳过过期键）...");

    // 扫描长TTL数据范围
    let mut iter = storage.scan(
        Bound::Included("ttl_long_00100000".as_bytes()),
        Bound::Included("ttl_long_00199999".as_bytes()),
    ).unwrap();

    let mut long_ttl_scan_count = 0;
    while iter.is_valid() {
        long_ttl_scan_count += 1;
        iter.next().unwrap();
    }

    assert_eq!(long_ttl_scan_count, 100000, "应该扫描到100000个长TTL键");

    // 扫描短TTL数据范围（应该为空，因为都过期了）
    let mut iter = storage.scan(
        Bound::Included("ttl_short_00100000".as_bytes()),
        Bound::Included("ttl_short_00199999".as_bytes()),
    ).unwrap();

    let mut short_ttl_scan_count = 0;
    while iter.is_valid() {
        short_ttl_scan_count += 1;
        iter.next().unwrap();
    }

    assert_eq!(short_ttl_scan_count, 0, "短TTL键应该都已过期，扫描结果应为空");

    println!("完成阶段5: 长TTL扫描 {} 个键，短TTL扫描 {} 个键", long_ttl_scan_count, short_ttl_scan_count);

    // 阶段6: 等待长TTL也过期
    println!("阶段6: 等待长TTL数据也过期（再等待30秒）...");
    thread::sleep(Duration::from_secs(30));

    // 阶段7: 验证只有无TTL数据仍存在
    println!("阶段7: 验证只有无TTL数据仍存在...");

    // 验证长TTL数据也已过期（抽样）
    for i in (0..300_000).step_by(10_000) {
        let key = format!("ttl_long_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, None, "长TTL键 {} 应该已过期", key);
    }

    // 验证无TTL数据仍存在（抽样）
    let mut remaining_count = 0;
    for i in (0..200_000).step_by(1000) {
        let key = format!("no_ttl_{:08}", i);
        let expected = format!("no_ttl_value_{:08}", i);
        let result = storage.get(key.as_bytes()).unwrap();
        assert_eq!(result, Some(Bytes::copy_from_slice(expected.as_bytes())),
                  "无TTL键 {} 应该仍然存在", key);
        remaining_count += 1;
    }

    println!("完成阶段7: 验证了 {} 个无TTL键仍然存在", remaining_count);

    storage.sync().unwrap();
    storage.close().unwrap();
    println!("=== 完成大规模TTL操作测试 ===");
}

/// 综合性大规模测试：Delete + TTL + Scan
#[test]
fn test_comprehensive_large_scale_operations() {
    println!("=== 开始综合性大规模测试（Delete + TTL + Scan）===");
    let dir = tempdir().unwrap();

    let leveled_opts = LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 3,
        max_levels: 7,
        base_level_size_mb: 256,
    };

    let mut options = LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(leveled_opts));
    options.compression_options = CompressionOptions::Snappy;
    let storage = MiniLsm::open(&dir, options.clone()).unwrap();

    // 阶段1: 写入混合数据（普通、TTL、将被删除的）
    println!("阶段1: 写入200万条混合数据...");

    // 100万条普通数据
    for i in 0..1_000_000 {
        let key = format!("normal_{:08}", i);
        let value = format!("normal_value_{:08}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 200_000 == 0 && i > 0 {
            println!("  已写入 {} 条普通数据", i);
        }
    }

    // 50万条短TTL数据（20秒后过期）
    for i in 0..500_000 {
        let key = format!("ttl_{:08}", i);
        let value = format!("ttl_value_{:08}", i);
        storage.put_with_ttl(key.as_bytes(), value.as_bytes(), 20).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条TTL数据", i);
        }
    }

    // 50万条将被删除的数据
    for i in 0..500_000 {
        let key = format!("todelete_{:08}", i);
        let value = format!("todelete_value_{:08}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已写入 {} 条待删除数据", i);
        }
    }

    println!("完成阶段1: 写入了100万普通 + 50万TTL + 50万待删除数据");

    // 阶段2: 触发compaction
    storage.inner.force_freeze_memtable(&storage.inner.state_lock.lock()).unwrap();
    thread::sleep(Duration::from_millis(1000));
    println!("完成阶段2: 触发compaction");

    // 阶段3: 大规模扫描验证初始状态
    println!("阶段3: 大规模扫描验证初始状态...");

    // 扫描所有normal数据
    let mut iter = storage.scan(
        Bound::Included("normal_00000000".as_bytes()),
        Bound::Included("normal_00999999".as_bytes()),
    ).unwrap();

    let mut normal_count = 0;
    while iter.is_valid() {
        normal_count += 1;
        iter.next().unwrap();
    }
    assert_eq!(normal_count, 1_000_000, "应该扫描到100万条normal记录");

    // 扫描所有TTL数据
    let mut iter = storage.scan(
        Bound::Included("ttl_00000000".as_bytes()),
        Bound::Included("ttl_00499999".as_bytes()),
    ).unwrap();

    let mut ttl_count = 0;
    while iter.is_valid() {
        ttl_count += 1;
        iter.next().unwrap();
    }
    assert_eq!(ttl_count, 500_000, "应该扫描到50万条TTL记录");

    println!("完成阶段3: 扫描验证normal {} 条，TTL {} 条", normal_count, ttl_count);

    // 阶段4: 大批量删除操作
    println!("阶段4: 删除50万条todelete数据...");
    for i in 0..500_000 {
        let key = format!("todelete_{:08}", i);
        storage.delete(key.as_bytes()).unwrap();

        if i % 100_000 == 0 && i > 0 {
            println!("  已删除 {} 条记录", i);
        }
    }
    println!("完成阶段4: 删除了50万条记录");

    // 阶段5: 等待TTL过期
    println!("阶段5: 等待TTL数据过期（等待25秒）...");
    thread::sleep(Duration::from_secs(25));

    // 阶段6: 综合扫描验证
    println!("阶段6: 综合扫描验证最终状态...");

    // 验证normal数据仍存在
    let mut iter = storage.scan(
        Bound::Included("normal_00000000".as_bytes()),
        Bound::Included("normal_00999999".as_bytes()),
    ).unwrap();

    let mut final_normal_count = 0;
    while iter.is_valid() {
        final_normal_count += 1;
        iter.next().unwrap();
    }
    assert_eq!(final_normal_count, 1_000_000, "normal数据应该仍然全部存在");

    // 验证TTL数据已过期（扫描应为空）
    let mut iter = storage.scan(
        Bound::Included("ttl_00000000".as_bytes()),
        Bound::Included("ttl_00499999".as_bytes()),
    ).unwrap();

    let mut final_ttl_count = 0;
    while iter.is_valid() {
        final_ttl_count += 1;
        iter.next().unwrap();
    }
    assert_eq!(final_ttl_count, 0, "TTL数据应该都已过期");

    // 验证删除的数据不存在（扫描应为空）
    let mut iter = storage.scan(
        Bound::Included("todelete_00000000".as_bytes()),
        Bound::Included("todelete_00499999".as_bytes()),
    ).unwrap();

    let mut final_deleted_count = 0;
    while iter.is_valid() {
        final_deleted_count += 1;
        iter.next().unwrap();
    }
    assert_eq!(final_deleted_count, 0, "删除的数据应该都不存在");

    println!("完成阶段6: 最终normal {} 条，TTL {} 条，deleted {} 条",
             final_normal_count, final_ttl_count, final_deleted_count);

    // 阶段7: 混合范围扫描测试
    println!("阶段7: 混合范围扫描测试...");

    // 跨类型扫描：从normal到ttl范围
    let mut iter = storage.scan(
        Bound::Included("normal_00999990".as_bytes()),
        Bound::Included("ttl_00000010".as_bytes()),
    ).unwrap();

    let mut cross_scan_count = 0;
    while iter.is_valid() {
        cross_scan_count += 1;
        iter.next().unwrap();
    }

    // 应该只包含normal_00999990到normal_00999999的10个键
    assert_eq!(cross_scan_count, 10, "跨类型扫描应该只找到10个normal键");

    println!("完成阶段7: 跨类型扫描找到 {} 个键", cross_scan_count);

    // 阶段8: 性能测试 - 随机访问
    println!("阶段8: 随机访问性能测试...");

    use std::time::Instant;
    let start = Instant::now();

    // 随机访问10万次
    for i in 0..100_000 {
        let random_id = (i * 7) % 1_000_000; // 简单的伪随机
        let key = format!("normal_{:08}", random_id);
        let result = storage.get(key.as_bytes()).unwrap();
        assert!(result.is_some(), "随机访问的normal键应该存在");
    }

    let duration = start.elapsed();
    println!("完成阶段8: 10万次随机访问耗时 {:?}", duration);

    storage.sync().unwrap();
    storage.close().unwrap();
    println!("=== 完成综合性大规模测试 ===");
}
