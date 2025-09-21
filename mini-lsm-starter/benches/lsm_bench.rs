use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mini_lsm_starter::iterators::StorageIterator;
use mini_lsm_starter::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::collections::Bound;
use tempfile::tempdir;

fn bench_lsm_write(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

    let mut group = c.benchmark_group("lsm_write");

    // 测试不同大小的写入
    for size in [100, 1000, 10000].iter() {
        group.bench_with_input(
            criterion::BenchmarkId::from_parameter(size),
            size,
            |b, &size| {
                b.iter(|| {
                    for i in 0..size {
                        let key = format!("key_{:08}", i);
                        let value = format!("value_{:08}", i);
                        storage.put(key.as_bytes(), value.as_bytes()).unwrap();
                    }
                })
            },
        );
    }
    group.finish();
}

fn bench_lsm_read(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

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

fn bench_lsm_scan(c: &mut Criterion) {
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, LsmStorageOptions::default_for_week1_test()).unwrap();

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

criterion_group!(benches, bench_lsm_write, bench_lsm_read, bench_lsm_scan);
criterion_main!(benches);
