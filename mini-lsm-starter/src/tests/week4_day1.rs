use crate::compression::gz::GzCompressionController;
use crate::compression::lz4::Lz4CompressionController;
use crate::compression::snappy::SnappyCompressionController;
use crate::compression::{CompressionController, CompressionOptions};
use std::time::Instant;

#[test]
fn test_task1_snappy_should_work() {
    let uncompressed_data = b"This is some data to be compressed using Snappy.";
    println!("ori size: {}", uncompressed_data.len());

    let compressed_result = SnappyCompressionController::compress(uncompressed_data).unwrap();

    println!("Compressed size: {}", compressed_result.len());

    let decompressed_buffer =
        SnappyCompressionController::de_compress(compressed_result.as_slice()).unwrap();

    assert_eq!(uncompressed_data.to_vec(), decompressed_buffer);

    println!(
        "Decompressed data: {}",
        String::from_utf8_lossy(&decompressed_buffer)
    );
}

#[test]
fn test_task1_lz4_should_work() {
    let uncompressed_data = b"This is some data to be compressed using lz4.";
    println!("ori size: {}", uncompressed_data.len());

    let compressed_result = Lz4CompressionController::compress(uncompressed_data).unwrap();

    println!("Compressed size: {}", compressed_result.len());

    let decompressed_buffer =
        Lz4CompressionController::de_compress(compressed_result.as_slice()).unwrap();

    assert_eq!(uncompressed_data.to_vec(), decompressed_buffer);

    println!(
        "Decompressed data: {}",
        String::from_utf8_lossy(&decompressed_buffer)
    );
}

#[test]
fn test_task1_gz_should_work() {
    let uncompressed_data = b"This is some data to be compressed using gz.";
    println!("ori size: {}", uncompressed_data.len());

    let compressed_result = GzCompressionController::compress(uncompressed_data).unwrap();

    println!("Compressed size: {}", compressed_result.len());

    let decompressed_buffer =
        GzCompressionController::de_compress(compressed_result.as_slice()).unwrap();

    assert_eq!(uncompressed_data.to_vec(), decompressed_buffer);

    println!(
        "Decompressed data: {}",
        String::from_utf8_lossy(&decompressed_buffer)
    );
}

#[test]
fn test_enum_to_u8() {
    let compression = CompressionOptions::Snappy;
    let mut buffer: Vec<u8> = Vec::new();

    buffer.push(compression.into());

    println!("选择的压缩类型: {:?}", compression);
    println!("写入 Vec<u8> 后的内容: {:?}", buffer);

    if let Some(byte_value) = buffer.first() {
        match CompressionOptions::try_from(*byte_value) {
            Ok(decompressed_type) => {
                println!("从 Vec<u8> 读回的类型: {:?}", decompressed_type);
                assert_eq!(compression, decompressed_type);
            }
            Err(e) => {
                println!("解析失败: {}", e);
            }
        }
    }

    let invalid_byte: u8 = 99;
    match CompressionOptions::try_from(invalid_byte) {
        Ok(t) => println!("成功解析无效值: {:?}", t), // 这不会发生
        Err(e) => println!("尝试解析无效值 {} 失败: {}", invalid_byte, e),
    }
}

#[test]
fn test_compression_work() {
    // 创建测试数据 - 类似测试中的 1KB 值
    let test_data: Vec<u8> = vec![b'1'; 1024];

    // 测试每种压缩算法
    let algorithms = [
        (CompressionOptions::None, "None"),
        (CompressionOptions::Snappy, "Snappy"),
        (CompressionOptions::Lz4, "LZ4"),
        (CompressionOptions::Gz, "Gzip"),
    ];

    for (compression, name) in algorithms {
        let controller = CompressionController::new(compression);

        // 预热
        for _ in 0..10 {
            let _ = controller.compress(&test_data);
        }

        // 基准测试
        let start = Instant::now();
        let mut total_size = 0;
        for _ in 0..1000 {
            let compressed = controller.compress(&test_data).unwrap();
            total_size += compressed.len();
            let _decompressed = controller.de_compress(&compressed).unwrap();
        }
        let duration = start.elapsed();

        let avg_size = total_size as f64 / 1000.0;
        let compression_ratio = (test_data.len() as f64 / avg_size) as f64;

        println!("{}:", name);
        println!("  时间: {:?} (1000次操作)", duration);
        println!("  平均压缩后大小: {:.2} bytes", avg_size);
        println!("  压缩比: {:.2}x", compression_ratio);
        println!();
    }
}

// 简单的 Trivial Move 测试验证
#[test]
fn test_trivial_move_simple_verification() {
    use std::time::Duration;
    use tempfile::tempdir;
    use crate::{
        compact::{
            CompactionOptions, LeveledCompactionOptions,
        },
        lsm_storage::{LsmStorageOptions, MiniLsm},
    };

    println!("=== 简单 Trivial Move 验证 ===");

    // 创建 Leveled 配置
    let compaction_options = CompactionOptions::Leveled(LeveledCompactionOptions {
        level_size_multiplier: 10,
        level0_file_num_compaction_trigger: 2,
        max_levels: 4,
        base_level_size_mb: 1,
    });

    let lsm_storage_options = LsmStorageOptions::default_for_week2_test(compaction_options.clone());
    let dir = tempdir().unwrap();
    let storage = MiniLsm::open(&dir, lsm_storage_options.clone()).unwrap();

    // 插入少量数据
    for i in 0..4 {
        let key = format!("key{:02}", i).as_bytes().to_vec();
        let value = vec![i as u8; 128];
        storage.put(&key, &value).unwrap();
    }

    // 冻结 memtable
    storage.inner.force_freeze_memtable(&storage.inner.state_lock.lock()).unwrap();
    std::thread::sleep(Duration::from_millis(500));

    // 验证数据
    for i in 0..4 {
        let key = format!("key{:02}", i).as_bytes().to_vec();
        let result = storage.get(&key).unwrap();
        assert!(result.is_some(), "键 {:?} 应该存在", key);
        assert_eq!(result.unwrap(), vec![i as u8; 128]);
    }

    storage.close().unwrap();
    println!("=== 简单 Trivial Move 验证完成 ===");
}
