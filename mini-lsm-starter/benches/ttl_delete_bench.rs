use criterion::{Criterion, black_box, criterion_group, criterion_main, BatchSize};
use mini_lsm_starter::compact::{CompactionOptions, LeveledCompactionOptions};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::collections::Bound;
use std::time::{SystemTime, UNIX_EPOCH};
use tempfile::tempdir;

/// 获取当前时间戳（秒）
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// 生成测试数据
fn generate_test_data(count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key = format!("key_{:08}", i);
            let value = format!("value_{:08}_{}", i, "x".repeat(50));
            (key.into_bytes(), value.into_bytes())
        })
        .collect()
}

/// Benchmark: TTL写入性能
fn bench_ttl_write_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_write");

    // 测试不同数据规模的TTL写入性能
    for &size in [1000, 5000, 10000].iter() {
        let test_data = generate_test_data(size);

        // 普通put vs put_with_ttl性能对比
        group.bench_with_input(
            criterion::BenchmarkId::new("normal_put", size),
            &size,
            |b, &_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::Leveled(LeveledCompactionOptions {
                                level_size_multiplier: 10,
                                level0_file_num_compaction_trigger: 4,
                                max_levels: 7,
                                base_level_size_mb: 256,
                            }),
                        );
                        let storage = MiniLsm::open(&dir, options).unwrap();
                        (storage, test_data.clone())
                    },
                    |(storage, data)| {
                        for (key, value) in data {
                            storage.put(&key, &value).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        group.bench_with_input(
            criterion::BenchmarkId::new("ttl_put", size),
            &size,
            |b, &_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::Leveled(LeveledCompactionOptions {
                                level_size_multiplier: 10,
                                level0_file_num_compaction_trigger: 4,
                                max_levels: 7,
                                base_level_size_mb: 256,
                            }),
                        );
                        let storage = MiniLsm::open(&dir, options).unwrap();
                        (storage, test_data.clone())
                    },
                    |(storage, data)| {
                        for (key, value) in data {
                            // 设置3600秒(1小时)TTL
                            storage.put_with_ttl(&key, &value, 3600).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: Delete操作性能
fn bench_delete_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("delete_operations");

    for &size in [1000, 5000, 10000].iter() {
        let test_data = generate_test_data(size);

        group.bench_with_input(
            criterion::BenchmarkId::new("delete_after_put", size),
            &size,
            |b, &_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let dir_path = dir.path().to_path_buf();
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::Leveled(LeveledCompactionOptions {
                                level_size_multiplier: 10,
                                level0_file_num_compaction_trigger: 4,
                                max_levels: 7,
                                base_level_size_mb: 256,
                            }),
                        );
                        let storage = MiniLsm::open(&dir_path, options).unwrap();

                        // 预先写入数据
                        for (key, value) in &test_data {
                            storage.put(key, value).unwrap();
                        }
                        (storage, test_data.clone(), dir)
                    },
                    |(storage, data, _dir)| {
                        // 删除所有数据
                        for (key, _) in data {
                            storage.delete(&key).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: TTL读取性能
fn bench_ttl_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_read");

    let test_data = generate_test_data(5000);

    // 创建存储并预加载数据
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(
        CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 4,
            max_levels: 7,
            base_level_size_mb: 256,
        }),
    );
    let storage = MiniLsm::open(&dir, options).unwrap();

    // 写入不同TTL的数据
    for (i, (key, value)) in test_data.iter().enumerate() {
        if i % 3 == 0 {
            // 1/3 普通数据
            storage.put(key, value).unwrap();
        } else if i % 3 == 1 {
            // 1/3 长TTL数据 (1小时)
            storage.put_with_ttl(key, value, 3600).unwrap();
        } else {
            // 1/3 短TTL数据 (10秒，测试时不会过期)
            storage.put_with_ttl(key, value, 10).unwrap();
        }
    }

    group.bench_function("normal_data_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(0)); // 普通数据
            storage.get(key.as_bytes()).unwrap();
        });
    });

    group.bench_function("long_ttl_data_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(1)); // 长TTL数据
            storage.get(key.as_bytes()).unwrap();
        });
    });

    group.bench_function("short_ttl_data_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(2)); // 短TTL数据
            storage.get(key.as_bytes()).unwrap();
        });
    });

    group.finish();
}

/// Benchmark: TTL扫描性能
fn bench_ttl_scan_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_scan");

    let test_data = generate_test_data(10000);

    // 创建存储并预加载数据
    let dir = tempdir().unwrap();
    let options = LsmStorageOptions::default_for_week2_test(
        CompactionOptions::Leveled(LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 4,
            max_levels: 7,
            base_level_size_mb: 256,
        }),
    );
    let storage = MiniLsm::open(&dir, options).unwrap();

    // 写入混合数据：普通数据 + TTL数据
    for (i, (key, value)) in test_data.iter().enumerate() {
        if i % 2 == 0 {
            storage.put(key, value).unwrap();
        } else {
            storage.put_with_ttl(key, value, 3600).unwrap(); // 1小时TTL
        }
    }

    group.bench_function("mixed_data_scan", |b| {
        b.iter(|| {
            let mut iter = storage
                .scan(
                    Bound::Included("key_00001000".as_bytes()),
                    Bound::Included("key_00001999".as_bytes()),
                )
                .unwrap();
            let mut count = 0;
            while iter.is_valid() {
                black_box(iter.key());
                black_box(iter.value());
                count += 1;
                iter.next().unwrap();
            }
            // 预期扫描1000个键
            assert_eq!(count, 1000);
        });
    });

    group.finish();
}

/// Benchmark: TTL过期处理性能
fn bench_ttl_expiration_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_expiration");

    let test_data = generate_test_data(1000);

    group.bench_function("expired_data_access", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let options = LsmStorageOptions::default_for_week2_test(
                    CompactionOptions::Leveled(LeveledCompactionOptions {
                        level_size_multiplier: 10,
                        level0_file_num_compaction_trigger: 4,
                        max_levels: 7,
                        base_level_size_mb: 256,
                    }),
                );
                let storage = MiniLsm::open(&dir, options).unwrap();

                // 写入已过期的数据（TTL = 1秒，然后等待2秒）
                for (key, value) in &test_data {
                    storage.put_with_ttl(key, value, 1).unwrap();
                }

                // 等待数据过期
                std::thread::sleep(std::time::Duration::from_secs(2));

                (storage, test_data.clone())
            },
            |(storage, data)| {
                // 尝试读取过期数据
                for (key, _) in data {
                    let result = storage.get(&key).unwrap();
                    // 过期数据应该返回None
                    assert!(result.is_none());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark: 混合操作性能 (写入、读取、删除、TTL)
fn bench_mixed_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_operations");

    let test_data = generate_test_data(2000);

    group.bench_function("mixed_workload", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let options = LsmStorageOptions::default_for_week2_test(
                    CompactionOptions::Leveled(LeveledCompactionOptions {
                        level_size_multiplier: 10,
                        level0_file_num_compaction_trigger: 4,
                        max_levels: 7,
                        base_level_size_mb: 256,
                    }),
                );
                let storage = MiniLsm::open(&dir, options).unwrap();
                (storage, test_data.clone())
            },
            |(storage, data)| {
                for (i, (key, value)) in data.iter().enumerate() {
                    match i % 4 {
                        0 => {
                            // 25% 普通写入
                            storage.put(key, value).unwrap();
                        }
                        1 => {
                            // 25% TTL写入
                            storage.put_with_ttl(key, value, 3600).unwrap();
                        }
                        2 => {
                            // 25% 读取操作
                            if i > 0 {
                                let prev_key = format!("key_{:08}", i - 1);
                                storage.get(prev_key.as_bytes()).unwrap();
                            }
                        }
                        3 => {
                            // 25% 删除操作
                            if i > 1 {
                                let prev_key = format!("key_{:08}", i - 2);
                                storage.delete(prev_key.as_bytes()).unwrap();
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_ttl_write_performance,
    bench_delete_performance,
    bench_ttl_read_performance,
    bench_ttl_scan_performance,
    bench_ttl_expiration_performance,
    bench_mixed_operations
);
criterion_main!(benches);