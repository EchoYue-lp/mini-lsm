use criterion::{BatchSize, Criterion, black_box, criterion_group, criterion_main};
use mini_lsm_starter::compact::{CompactionOptions, LeveledCompactionOptions};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::collections::Bound;
use tempfile::tempdir;

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

/// 创建标准LSM配置
fn create_lsm_options() -> LsmStorageOptions {
    LsmStorageOptions::default_for_week2_test(CompactionOptions::Leveled(
        LeveledCompactionOptions {
            level_size_multiplier: 10,
            level0_file_num_compaction_trigger: 4,
            max_levels: 7,
            base_level_size_mb: 256,
        },
    ))
}

/// Benchmark: 基础LSM写入性能
fn bench_lsm_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("lsm_write");

    // 测试不同大小的写入
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let dir_path = dir.path().to_path_buf();
                        let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();
                        (storage, dir)
                    },
                    |(storage, _dir)| {
                        for i in 0..size {
                            let key = format!("key_{:08}", i);
                            let value = format!("value_{:08}", i);
                            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

/// Benchmark: 基础LSM读取性能
fn bench_lsm_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, create_lsm_options()).unwrap();

    // 预先写入数据
    for i in 0..10000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let mut group = c.benchmark_group("lsm_read");

    // 测试随机读取
    group.bench_function("random_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(5000));
            storage.get(key.as_bytes()).unwrap();
        })
    });

    group.finish();
}

/// Benchmark: 基础LSM扫描性能
fn bench_lsm_scan(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, create_lsm_options()).unwrap();

    // 预先写入数据
    for i in 0..10000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}", i);
        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
    }

    let mut group = c.benchmark_group("lsm_scan");

    // 测试范围扫描
    group.bench_function("range_scan_100", |b| {
        b.iter(|| {
            let mut iter = storage
                .scan(
                    Bound::Included("key_00001000".as_bytes()),
                    Bound::Included("key_00001099".as_bytes()),
                )
                .unwrap();
            let mut count = 0;
            while iter.is_valid() {
                count += 1;
                iter.next().unwrap();
            }
            assert_eq!(count, 100);
        })
    });

    group.finish();
}

/// Benchmark: TTL vs 普通写入性能对比
fn bench_ttl_vs_normal_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_vs_normal_write");

    for &size in [1000, 5000].iter() {
        let test_data = generate_test_data(size);

        // 普通写入
        group.bench_with_input(
            criterion::BenchmarkId::new("normal_put", size),
            &size,
            |b, &_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let dir_path = dir.path().to_path_buf();
                        let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();
                        (storage, test_data.clone(), dir)
                    },
                    |(storage, data, _dir)| {
                        for (key, value) in data {
                            storage.put(&key, &value).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );

        // TTL写入
        group.bench_with_input(
            criterion::BenchmarkId::new("ttl_put", size),
            &size,
            |b, &_size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let dir_path = dir.path().to_path_buf();
                        let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();
                        (storage, test_data.clone(), dir)
                    },
                    |(storage, data, _dir)| {
                        for (key, value) in data {
                            storage.put_with_ttl(&key, &value, 3600).unwrap(); // 1小时TTL
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
    let mut group = c.benchmark_group("delete_performance");

    for &size in [1000, 3000].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::new("delete_after_put", size),
            &size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let dir_path = dir.path().to_path_buf();
                        let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();

                        // 预先写入数据
                        for i in 0..size {
                            let key = format!("key_{:08}", i);
                            let value = format!("value_{:08}_{}", i, "x".repeat(50));
                            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        (storage, dir)
                    },
                    |(storage, _dir)| {
                        // 删除所有数据
                        for i in 0..size {
                            let key = format!("key_{:08}", i);
                            storage.delete(key.as_bytes()).unwrap();
                        }
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// Benchmark: TTL读取性能对比
fn bench_ttl_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_read_performance");

    // 创建存储并预加载数据
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, create_lsm_options()).unwrap();

    // 写入混合数据
    for i in 0..5000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}_{}", i, "x".repeat(50));

        if i % 2 == 0 {
            // 50% 普通数据
            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else {
            // 50% TTL数据
            storage
                .put_with_ttl(key.as_bytes(), value.as_bytes(), 3600)
                .unwrap();
        }
    }

    group.bench_function("normal_data_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(0)); // 普通数据
            storage.get(key.as_bytes()).unwrap();
        });
    });

    group.bench_function("ttl_data_read", |b| {
        b.iter(|| {
            let key = format!("key_{:08}", black_box(1)); // TTL数据
            storage.get(key.as_bytes()).unwrap();
        });
    });

    group.finish();
}

/// Benchmark: TTL扫描性能
fn bench_ttl_scan_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("ttl_scan_performance");

    // 创建存储并预加载数据
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, create_lsm_options()).unwrap();

    // 写入混合数据：普通数据 + TTL数据
    for i in 0..10000 {
        let key = format!("key_{:08}", i);
        let value = format!("value_{:08}_{}", i, "x".repeat(50));

        if i % 2 == 0 {
            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
        } else {
            storage
                .put_with_ttl(key.as_bytes(), value.as_bytes(), 3600)
                .unwrap(); // 1小时TTL
        }
    }

    group.bench_function("mixed_data_scan_1000", |b| {
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

/// Benchmark: Flush性能测试
fn bench_flush_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("flush_performance");

    group.bench_function("force_flush_memtable", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let dir_path = dir.path().to_path_buf();
                let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();

                // 写入数据到内存表
                for i in 0..5000 {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}_{}", i, "x".repeat(50));
                    storage.put(key.as_bytes(), value.as_bytes()).unwrap();
                }
                // 返回存储实例和目录句柄，保持目录存活
                (storage, dir)
            },
            |(storage, _dir)| {
                // 测试强制flush操作性能，_dir确保目录在此期间不被清理
                storage.force_flush().unwrap();
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

/// Benchmark: 混合操作性能测试
fn bench_mixed_operations_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_operations");

    group.bench_function("mixed_workload", |b| {
        b.iter_batched(
            || {
                let dir = tempdir().unwrap();
                let dir_path = dir.path().to_path_buf();
                let storage = MiniLsm::open(&dir_path, create_lsm_options()).unwrap();
                (storage, dir)
            },
            |(storage, _dir)| {
                for i in 0..1000 {
                    let key = format!("key_{:08}", i);
                    let value = format!("value_{:08}_{}", i, "x".repeat(50));

                    match i % 4 {
                        0 => {
                            // 25% 普通写入
                            storage.put(key.as_bytes(), value.as_bytes()).unwrap();
                        }
                        1 => {
                            // 25% TTL写入
                            storage
                                .put_with_ttl(key.as_bytes(), value.as_bytes(), 3600)
                                .unwrap();
                        }
                        2 => {
                            // 25% 读取操作
                            if i > 0 {
                                let prev_key = format!("key_{:08}", i - 1);
                                let _ = storage.get(prev_key.as_bytes()).unwrap();
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
    bench_lsm_write,
    bench_lsm_read,
    bench_lsm_scan,
    bench_ttl_vs_normal_write,
    bench_delete_performance,
    bench_ttl_read_performance,
    bench_ttl_scan_performance,
    bench_flush_performance,
    bench_mixed_operations_performance
);
criterion_main!(benches);
