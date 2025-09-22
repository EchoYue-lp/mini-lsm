#!/bin/bash

echo "=== Mini-LSM Benchmarking Suite ==="
echo

# 创建benchmark结果目录
mkdir -p benchmark_results

echo "1. 运行压缩算法benchmark..."
cargo bench --bench compression_bench 2>&1 | tee benchmark_results/compression_results.txt

echo
echo "2. 运行LSM存储操作benchmark..."
cargo bench --bench lsm_bench 2>&1 | tee benchmark_results/lsm_results.txt

echo
echo "3. 运行compaction策略benchmark..."
cargo bench --bench compaction_bench 2>&1 | tee benchmark_results/compaction_results.txt

echo
echo "4. 运行现有的压缩性能测试..."
cargo run --example bench_compression 2>&1 | tee benchmark_results/compression_example_results.txt

echo
echo "=== Benchmark完成 ==="
echo "结果已保存在 benchmark_results/ 目录中"
echo
echo "查看HTML报告:"
echo "- 压缩算法: target/criterion/compression/report/index.html"
echo "- LSM操作: target/criterion/lsm_write/report/index.html"
echo "- Compaction策略: target/criterion/compaction_write/report/index.html"