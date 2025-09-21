use criterion::{Criterion, black_box, criterion_group, criterion_main};
use mini_lsm_starter::compression::{CompressionController, CompressionOptions};

fn bench_compression_algorithms(c: &mut Criterion) {
    // 不同大小的测试数据
    let test_data_1kb: Vec<u8> = vec![b'1'; 1024];
    let test_data_10kb: Vec<u8> = vec![b'1'; 10240];
    let test_data_100kb: Vec<u8> = vec![b'1'; 102400];

    let mut group = c.benchmark_group("compression");

    // 测试 Snappy 压缩
    group.bench_with_input("snappy_1kb", &test_data_1kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Snappy);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("snappy_10kb", &test_data_10kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Snappy);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("snappy_100kb", &test_data_100kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Snappy);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    // 测试 LZ4 压缩
    group.bench_with_input("lz4_1kb", &test_data_1kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Lz4);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("lz4_10kb", &test_data_10kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Lz4);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("lz4_100kb", &test_data_100kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Lz4);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    // 测试 Gzip 压缩
    group.bench_with_input("gzip_1kb", &test_data_1kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Gz);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("gzip_10kb", &test_data_10kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Gz);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.bench_with_input("gzip_100kb", &test_data_100kb, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Gz);
        b.iter(|| {
            let compressed = controller.compress(black_box(data)).unwrap();
            black_box(compressed)
        })
    });

    group.finish();
}

fn bench_decompression_algorithms(c: &mut Criterion) {
    let test_data: Vec<u8> = vec![b'1'; 102400];

    let snappy_controller = CompressionController::new(CompressionOptions::Snappy);
    let lz4_controller = CompressionController::new(CompressionOptions::Lz4);
    let gzip_controller = CompressionController::new(CompressionOptions::Gz);

    let snappy_compressed = snappy_controller.compress(&test_data).unwrap();
    let lz4_compressed = lz4_controller.compress(&test_data).unwrap();
    let gzip_compressed = gzip_controller.compress(&test_data).unwrap();

    let mut group = c.benchmark_group("decompression");

    group.bench_with_input("snappy_decompress", &snappy_compressed, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Snappy);
        b.iter(|| {
            let decompressed = controller.de_compress(black_box(data)).unwrap();
            black_box(decompressed)
        })
    });

    group.bench_with_input("lz4_decompress", &lz4_compressed, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Lz4);
        b.iter(|| {
            let decompressed = controller.de_compress(black_box(data)).unwrap();
            black_box(decompressed)
        })
    });

    group.bench_with_input("gzip_decompress", &gzip_compressed, |b, data| {
        let controller = CompressionController::new(CompressionOptions::Gz);
        b.iter(|| {
            let decompressed = controller.de_compress(black_box(data)).unwrap();
            black_box(decompressed)
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_compression_algorithms,
    bench_decompression_algorithms
);
criterion_main!(benches);
