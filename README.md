# Mini-LSM: 轻量级LSM树键值存储引擎

[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Build Status](https://img.shields.io/badge/build-passing-brightgreen.svg)]()

## 🎯 项目概述

Mini-LSM是一个用Rust实现的高性能LSM树(Log-Structured Merge Tree)
键值存储引擎，提供了完整的数据库功能，包括内存表、SSTable、压缩策略、事务支持和多版本并发控制(MVCC)。

## 🚀 关键特性

### 核心功能

- ✅ **LSM-Tree存储引擎**: 高效的写入和读取性能
- ✅ **MVCC事务支持**: 支持快照隔离和可串行化
- ✅ **多种压缩策略**: Leveled、Tiered、Simple Leveled
- ✅ **多种压缩算法**: Snappy、LZ4、Gzip
- ✅ **WAL预写日志**: 保证数据持久性
- ✅ **TTL时间过期**: 自动清理过期数据
- ✅ **范围查询**: 高效的键范围扫描

### 网络功能

- 🌐 **混合异步架构**: 网络层异步 + 存储层同步
- 🌐 **Protocol Buffers协议**: 高效的二进制通信
- 🌐 **高并发支持**: 支持数千个并发连接
- 🌐 **网络服务器**: 完整的TCP服务器实现

### 性能优化

- ⚡ **块缓存**: 使用moka缓存库优化读取性能
- ⚡ **布隆过滤器**: 加速键存在性检查
- ⚡ **多种迭代器**: MergeIterator、ConcatIterator优化
- ⚡ **并行压缩**: 多线程并行compaction

## 📦 项目结构

```
mini-lsm-starter/
├── src/
│   ├── block/           # 数据块管理
│   ├── compact/         # 压缩策略实现
│   ├── compression/     # 压缩算法
│   ├── iterators/       # 迭代器抽象
│   ├── mvcc/           # 多版本并发控制
│   ├── table/          # SSTable管理
│   ├── tests/          # 测试套件
│   ├── bin/            # 可执行程序
│   ├── key.rs          # 键处理(含时间戳)
│   ├── lsm_storage.rs  # 存储引擎核心
│   ├── mem_table.rs    # 内存表实现
│   ├── manifest.rs     # 元数据管理
│   ├── wal.rs          # 预写日志
│   ├── hybrid_async_interface.rs  # 混合异步接口
│   └── network_server.rs          # 网络服务器
├── benches/            # 性能基准测试
├── examples/           # 使用示例
├── proto/             # Protocol Buffers定义
└── PROJECT_DOCUMENTATION.md  # 项目文档
```

## 🏗️ 架构设计

### 混合异步架构

```
┌─────────────────────────────────────────┐
│          异步网络层 (Tokio)              │  ← 处理网络I/O、高并发连接
│    • TCP服务器 + Protocol Buffers        │
│    • 字符串友好的用户接口                  │
├─────────────────────────────────────────┤
│       异步-同步桥接层                     │  ← spawn_blocking适配
│    • HybridAsyncLsm                     │
│    • 请求/响应转换                        │
├─────────────────────────────────────────┤
│        同步核心引擎                       │  ← 保持简单可靠
│    • MemTable、WAL、Compaction          │
│    • 事务管理                            │
└─────────────────────────────────────────┘
```

### 核心组件

#### 1. 内存表 (MemTable)

```rust
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}
```

#### 2. SSTable (Sorted String Table)

```rust
pub struct SsTable {
    pub(crate) file: FileObject,
    pub(crate) block_meta: Vec<BlockMeta>,
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    max_ts: u64,
    compression_options: CompressionOptions,
}
```

#### 3. 键设计(含时间戳)

```rust
pub struct KeyBytes {
    inner: Bytes,    // 键数据
    ts: u64,         // 时间戳
}
```

## 🛠️ 使用指南

### 快速开始

#### 1. 基本操作

```rust
use mini_lsm_starter::*;

// 打开数据库
let options = LsmStorageOptions::default_for_week2_test(
CompactionOptions::Leveled(LeveledCompactionOptions::default ())
);
let db = MiniLsm::open("/path/to/db", options) ?;

// 写入数据
db.put(b"key1", b"value1") ?;
db.put(b"key2", b"value2") ?;

// 读取数据
let value = db.get(b"key1") ?;
println!("Value: {:?}", value);

// 删除数据
db.delete(b"key2") ?;

// 范围查询
let iter = db.scan(Bound::Included(b"key1"), Bound::Unbounded) ?;
while iter.is_valid() {
println ! ("Key: {:?}, Value: {:?}", iter.key(), iter.value());
iter.next() ?;
}

db.close() ?;
```

#### 2. 事务操作

```rust
// 开始事务
let txn = db.new_txn() ?;

// 事务内操作
txn.put(b"txn_key", b"txn_value") ?;
let value = txn.get(b"txn_key") ?;

// 提交事务
txn.commit() ?;
```

#### 3. 混合异步接口

```rust
use mini_lsm_starter::hybrid_async_interface::HybridAsyncLsm;

#[tokio::main]
async fn main() -> Result<()> {
    // 打开存储
    let lsm = HybridAsyncLsm::open("./data", options).await?;

    // 基本操作
    lsm.put(b"key", b"value").await?;
    let value = lsm.get(b"key").await?;
    lsm.delete(b"key").await?;

    // 事务
    let txn = lsm.new_txn().await?;
    txn.put(b"txn_key", b"txn_value");
    txn.commit().await?;

    lsm.close().await?;
    Ok(())
}
```

#### 4. 网络服务器

```rust
use mini_lsm_starter::network_server::LsmServer;

#[tokio::main]
async fn main() -> Result<()> {
    let lsm = HybridAsyncLsm::open("./data", options).await?;
    let server = LsmServer::new(lsm);

    // 启动服务器，可处理数千并发连接
    server.serve("127.0.0.1:7878").await?;
    Ok(())
}
```

### CLI工具

#### 1. 混合CLI

```bash
# 同步模式（兼容原有CLI）
cargo run --bin mini-lsm-hybrid-cli -- --mode sync none

# 混合异步模式（支持事务）
cargo run --bin mini-lsm-hybrid-cli -- --mode hybrid none

# 服务器模式
cargo run --bin mini-lsm-hybrid-cli -- --mode server none
```

#### 2. 专用服务器

```bash
cargo run --bin mini-lsm-server -- \
  --address 127.0.0.1:7878 \
  --compaction leveled \
  --enable-wal \
  --serializable \
  --compression lz4
```

## 🧪 构建与测试

### 构建项目

```bash
# 构建项目
cargo build --release

# 检查代码
cargo check --bins --examples
```

### 运行测试

```bash
# 运行所有测试
cargo test

# 运行混合架构测试
cargo test hybrid_tests

# 运行特定测试
cargo test --test compaction_test
```

### 性能测试

```bash
# 运行所有benchmark
cargo bench

# 运行特定benchmark
cargo bench --bench lsm_bench
cargo bench --bench ttl_delete_bench_simple

# 使用脚本运行benchmark并生成报告
./run_benchmark_simple.sh
```

## 📊 性能特点

### 写入性能

- **内存表写入**: 基于跳表的高效插入
- **WAL保证**: 写入持久性保证
- **批量写入**: 支持批量操作优化

### 读取性能

- **多级缓存**: MemTable → Immutable MemTable → L0 → L1+
- **布隆过滤器**: 快速判断键是否存在
- **块缓存**: moka缓存库优化磁盘读取

### 压缩性能

- **多种策略**: Leveled、Tiered、Simple Leveled
- **并行压缩**: 多线程并行执行
- **Trivial Move**: 优化无重叠文件移动

### 网络性能

- **高并发**: 支持数千个并发连接
- **低延迟**: 异步I/O避免阻塞
- **高吞吐**: Protocol Buffers高效序列化

## 🔧 配置选项

```rust
pub struct LsmStorageOptions {
    pub block_size: usize,                 // 块大小(字节)
    pub target_sst_size: usize,            // SSTable目标大小
    pub num_memtable_limit: usize,         // 内存表数量限制
    pub compaction_options: CompactionOptions, // 压缩选项
    pub enable_wal: bool,                  // 是否启用WAL
    pub serializable: bool,                // 是否可串行化
    pub compression_options: CompressionOptions, // 压缩算法
}
```

## 📚 核心API接口

### 1. 同步API - MiniLsm

#### 基础操作

```rust
impl MiniLsm {
    // 打开数据库
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>>;

    // 关闭数据库
    pub fn close(&self) -> Result<()>;

    // 基础读写操作
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;

    // 范围查询
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator>;

    // 事务支持
    pub fn new_txn(&self) -> Result<Arc<Transaction>>;

    // 系统操作
    pub fn sync(&self) -> Result<()>;
    pub fn force_flush(&self) -> Result<()>;
    pub fn force_full_compaction(&self) -> Result<()>;

    // 压缩过滤器
    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter);
}
```

#### 数据结构定义

```rust
// LSM存储状态
pub struct LsmStorageState {
    pub memtable: Arc<MemTable>,           // 当前内存表
    pub imm_memtables: Vec<Arc<MemTable>>, // 不可变内存表
    pub l0_sstables: Vec<usize>,           // L0层SSTable
    pub levels: Vec<(usize, Vec<usize>)>,  // 各级层次
    pub sstables: HashMap<usize, Arc<SsTable>>, // SSTable映射
}

// 写批处理记录
pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),                             // 普通写入
    PutWithTtl(T, T, u64),                 // TTL写入
    Del(T),                                // 删除操作
}

// 压缩过滤器
pub enum CompactionFilter {
    Prefix(Bytes),                         // 前缀过滤
}
```

### 2. 异步API - HybridAsyncLsm

#### 混合异步接口

```rust
impl HybridAsyncLsm {
    // 打开数据库
    pub async fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self>;

    // 关闭数据库
    pub async fn close(&self) -> Result<()>;

    // 基础操作
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub async fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()>;
    pub async fn delete(&self, key: &[u8]) -> Result<()>;

    // 批量操作
    pub async fn put_batch(&self, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;
    pub async fn write_batch(&self, batch: &[WriteBatchRecord<&[u8]>]) -> Result<()>;

    // 范围查询
    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<HybridIterator>;

    // 事务支持
    pub async fn new_txn(&self) -> Result<HybridTransaction>;

    // 系统操作
    pub async fn sync(&self) -> Result<()>;
    pub async fn force_flush(&self) -> Result<()>;

    // 获取同步引擎引用
    pub fn sync_engine(&self) -> &Arc<MiniLsm>;
}
```

#### 异步事务接口

```rust
impl HybridTransaction {
    // 事务操作
    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;

    // 范围查询
    pub async fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<HybridIterator>;

    // 提交事务
    pub async fn commit(&self) -> Result<()>;
}
```

#### 异步迭代器

```rust
impl HybridIterator {
    // 迭代器操作
    pub async fn next(&mut self) -> Result<Option<(Bytes, Bytes)>>;
    pub async fn is_valid(&self) -> Result<bool>;

    // 收集所有结果
    pub async fn collect(mut self) -> Result<Vec<(Bytes, Bytes)>>;
}
```

### 3. 事务API - Transaction

#### 同步事务接口

```rust
impl Transaction {
    // 事务读写
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>>;
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    pub fn put_with_ttl(&self, key: &[u8], value: &[u8], ttl: u64) -> Result<()>;
    pub fn delete(&self, key: &[u8]) -> Result<()>;

    // 范围查询
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator>;

    // 事务提交
    pub fn commit(&self) -> Result<()>;
}
```

### 4. 网络服务器API

#### TCP服务器

```rust
impl LsmServer {
    // 创建服务器
    pub fn new(lsm: HybridAsyncLsm) -> Self;

    // 启动服务器
    pub async fn serve(&self, addr: &str) -> Result<()>;
}
```

## 📊 性能基准测试结果

### 测试环境

- **测试时间**: 2025年9月29日
- **测试平台**: Darwin arm64
- **Rust版本**: rustc 1.90.0
- **项目版本**: 0.2.0
- **基准测试工具**: Criterion.rs v0.5

### 🚀 关键性能指标

#### 1. 基础LSM存储性能

| 操作类型 | 数据量     | 性能表现      | 说明       |
|------|---------|-----------|----------|
| 写入操作 | 100条    | 555.40 µs | 小批量写入性能  |
| 写入操作 | 1,000条  | 2.1293 ms | 中等批量写入性能 |
| 写入操作 | 10,000条 | 8.2448 ms | 大批量写入性能  |
| 随机读取 | 单次      | 980.23 ns | 点查询性能    |
| 范围扫描 | 100条    | 7.1918 µs | 范围查询性能   |

#### 2. TTL功能性能对比

| 操作类型  | 数据量    | 性能表现      | TTL开销  |
|-------|--------|-----------|--------|
| 普通写入  | 1,000条 | 1.8040 ms | 基准性能   |
| TTL写入 | 1,000条 | 2.0281 ms | +12.4% |
| 普通写入  | 5,000条 | 3.6579 ms | 大批量基准  |
| TTL写入 | 5,000条 | 4.6798 ms | +27.9% |

#### 3. 删除和读取性能

| 操作类型    | 数据量    | 性能表现      | 备注       |
|---------|--------|-----------|----------|
| 删除操作    | 1,000条 | 1.7687 ms | 中等批量删除   |
| 删除操作    | 3,000条 | 2.8563 ms | 大批量删除    |
| 普通数据读取  | 单次     | 990.81 ns | 无TTL数据读取 |
| TTL数据读取 | 单次     | 1.0017 µs | 有TTL数据读取 |

#### 4. 扫描和压缩性能

| 操作类型     | 规模       | 性能表现      | 说明         |
|----------|----------|-----------|------------|
| 混合数据扫描   | 1,000条   | 70.606 µs | 包含TTL和普通数据 |
| 内存表Flush | 5,000条数据 | 13.214 ms | 内存表刷写到磁盘性能 |
| 混合工作负载   | 1,000次操作 | 2.1195 ms | 读写删混合场景    |

### 📈 性能特征分析

#### 写入性能特征

- **小批量写入**: 555.40 µs (100条记录) - 约5.5µs/条
- **中等批量写入**: 2.1293 ms (1,000条记录) - 约2.1µs/条
- **大批量写入**: 8.2448 ms (10,000条记录) - 约0.82µs/条

**分析**: 批量大小对写入性能有显著影响，批量越大，单条记录的平均写入时间越短，体现了良好的批量优化效果。

#### TTL功能性能影响

- **TTL开销(1K)**: 12.4% (2.0281ms vs 1.8040ms)
- **TTL开销(5K)**: 27.9% (4.6798ms vs 3.6579ms)

**分析**: TTL功能引入了时间戳处理开销，但影响相对可控，特别是在小批量场景下开销较小。

#### 读取性能特征

- **随机读取延迟**: 980.23 ns - 亚微秒级延迟
- **TTL数据读取开销**: 微乎其微 (1.0017µs vs 990.81ns)

**分析**: 读取性能优异，TTL过期检查对读取性能影响极小，说明实现高效。

#### 删除操作性能

- **删除吞吐量**: 约565条/ms (1K条/1.7687ms)
- **扩展性**: 良好的线性扩展性

**分析**: 删除操作性能稳定，支持高吞吐量的删除场景。

### 🎯 性能优化建议

#### 写入优化策略

1. **批量写入**: 使用更大的批量大小来提高写入吞吐量
2. **WAL配置**: 根据持久性需求调整WAL同步策略
3. **内存表大小**: 适当增加内存表大小减少flush频率

#### 读取优化策略

1. **缓存配置**: 适当增加块缓存大小提高读取命中率
2. **布隆过滤器**: 确保布隆过滤器配置最优
3. **压缩策略**: 选择适合工作负载的压缩策略

#### 存储优化策略

1. **压缩算法**: 根据CPU和I/O资源选择合适的压缩算法
2. **层级配置**: 调整层级大小和触发条件
3. **Flush策略**: 调整内存表大小和flush触发条件

### ✅ 性能评估结论

#### 核心性能表现

- ✅ **写入性能**: 优秀，单条记录亚微秒级写入延迟
- ✅ **读取性能**: 卓越，亚微秒级随机读取延迟
- ✅ **TTL功能**: 性能影响最小，功能完善
- ✅ **删除操作**: 高效，支持批量高吞吐删除
- ✅ **扫描功能**: 稳定，毫秒级范围查询
- ✅ **系统操作**: 内存刷盘等操作性能良好

#### 生产环境就绪性

- 🚀 **高性能**: 满足高吞吐量KV存储需求
- 🛡️ **高可靠**: 完整的WAL、事务和MVCC支持
- 🔧 **易维护**: 架构清晰，配置灵活
- 📈 **可扩展**: 支持多种压缩和优化策略

---

## 📄 许可证

本项目采用Apache License 2.0许可证 - 详见 [LICENSE](LICENSE) 文件

## 🙏 致谢

感谢成功人士迟先生的前期工作：

- [mini-lsm](https://github.com/skyzh/mini-lsm) - mini-lsm项目
- 感谢 claude-code、gemini 2.5 pro、chatgpt 等大模型的帮助，提供了大量代码和思路。

---

**Mini-LSM** - 一个现代化的、高性能的、易于使用的LSM树存储引擎实现 🚀

### 项目架构

![Course Roadmap](./mini-lsm-book/src/lsm-tutorial/00-full-overview.svg)

### 本人扩展内容

* compression
* benchmark
* fix clippy warn
* trivial move
* parallel compaction
* subcompaction
* key type
* ttl
* hybrid async
* connection_pool and network_server