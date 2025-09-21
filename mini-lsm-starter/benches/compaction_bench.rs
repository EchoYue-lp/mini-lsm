use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mini_lsm_starter::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
};
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
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

/// 基准测试：不同compaction策略的写入性能
fn bench_compaction_write_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_write");

    // 测试数据大小
    let data_sizes = [1000, 5000];

    for size in data_sizes.iter() {
        let test_data = generate_test_data(*size);

        // No Compaction
        group.bench_with_input(
            criterion::BenchmarkId::new("no_compaction", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::NoCompaction,
                        );
                        let storage = MiniLsm::open(&dir, options).unwrap();
                        (storage, test_data.clone())
                    },
                    |(storage, data)| {
                        for (key, value) in data {
                            storage.put(&key, &value).unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Leveled Compaction
        group.bench_with_input(
            criterion::BenchmarkId::new("leveled", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let leveled_opts = LeveledCompactionOptions {
                            level_size_multiplier: 10,
                            level0_file_num_compaction_trigger: 4,
                            max_levels: 7,
                            base_level_size_mb: 256,
                        };
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::Leveled(leveled_opts),
                        );
                        let storage = MiniLsm::open(&dir, options).unwrap();
                        (storage, test_data.clone())
                    },
                    |(storage, data)| {
                        for (key, value) in data {
                            storage.put(&key, &value).unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Simple Leveled Compaction
        group.bench_with_input(
            criterion::BenchmarkId::new("simple_leveled", size),
            size,
            |b, &size| {
                b.iter_batched(
                    || {
                        let dir = tempdir().unwrap();
                        let simple_opts = SimpleLeveledCompactionOptions {
                            size_ratio_percent: 200,
                            level0_file_num_compaction_trigger: 4,
                            max_levels: 7,
                        };
                        let options = LsmStorageOptions::default_for_week2_test(
                            CompactionOptions::Simple(simple_opts),
                        );
                        let storage = MiniLsm::open(&dir, options).unwrap();
                        (storage, test_data.clone())
                    },
                    |(storage, data)| {
                        for (key, value) in data {
                            storage.put(&key, &value).unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

/// 基准测试：不同compaction策略的读取性能
fn bench_compaction_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("compaction_read");

    let test_data = generate_test_data(2000);
    let compaction_strategies = [
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

    for (name, compaction) in compaction_strategies.iter() {
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default_for_week2_test(compaction.clone());
        let storage = MiniLsm::open(&dir, options).unwrap();

        // 预加载数据
        for (key, value) in test_data.iter() {
            storage.put(key, value).unwrap();
        }

        group.bench_function(*name, |b| {
            b.iter(|| {
                let key = format!("key_{:08}", black_box(1000));
                storage.get(key.as_bytes()).unwrap();
            });
        });
    }

    group.finish();
}

/// 基准测试：不同compaction策略的扫描性能
fn bench_compaction_scan_performance(c: &mut Criterion) {
    use mini_lsm_starter::iterators::StorageIterator;
    use std::collections::Bound;

    let mut group = c.benchmark_group("compaction_scan");

    let test_data = generate_test_data(5000);
    let compaction_strategies = [
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

    for (name, compaction) in compaction_strategies.iter() {
        let dir = tempdir().unwrap();
        let options = LsmStorageOptions::default_for_week2_test(compaction.clone());
        let storage = MiniLsm::open(&dir, options).unwrap();

        // 预加载数据
        for (key, value) in test_data.iter() {
            storage.put(key, value).unwrap();
        }

        group.bench_function(*name, |b| {
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
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_compaction_write_performance,
    bench_compaction_read_performance,
    bench_compaction_scan_performance
);
criterion_main!(benches);
