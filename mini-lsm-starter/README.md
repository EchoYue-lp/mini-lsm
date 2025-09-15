# Mini-LSM: 一个完整的LSM树键值存储引擎实现

Mini-LSM是一个用Rust实现的轻量级LSM树(Log-Structured Merge Tree)键值存储引擎，提供了完整的存储引擎功能，包括内存表、SSTable、压缩、事务和多版本并发控制。

## 项目架构

### 核心模块结构
src/
├── block/           # 数据块管理
├── compact/         # 压缩策略实现
├── compression/     # 压缩算法
├── iterators/      # 迭代器抽象
├── key.rs          # 键处理(含时间戳)
├── lsm_storage.rs  # 存储引擎核心
├── mem_table.rs    # 内存表实现
├── table/          # SSTable管理
├── mvcc/           # 多版本并发控制
├── manifest.rs     # 元数据管理
└── wal.rs          # 预写日志
## 核心组件实现

### 1. 内存表 (MemTable)

内存表基于跳表(SkipMap)实现，支持高效的插入和查询操作：

```rust
pub struct MemTable {
    pub(crate) map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,           // 预写日志
    id: usize,                 // 唯一标识
    approximate_size: Arc<AtomicUsize>, // 近似大小
}
```

**特性：**
- 使用`crossbeam-skiplist`实现并发安全的跳表
- 支持WAL(Write-Ahead Log)保证数据持久性
- 批量写入优化

### 2. SSTable (Sorted String Table)

SSTable是磁盘上的有序键值对集合，采用分层存储结构：

```rust
pub struct SsTable {
    pub(crate) file: FileObject,        // 文件对象
    pub(crate) block_meta: Vec<BlockMeta>, // 块元数据
    pub(crate) block_meta_offset: usize, // 元数据偏移
    id: usize,                         // SSTable ID
    block_cache: Option<Arc<BlockCache>>, // 块缓存
    first_key: KeyBytes,               // 第一个键
    last_key: KeyBytes,                // 最后一个键
    pub(crate) bloom: Option<Bloom>,    // 布隆过滤器
    max_ts: u64,                       // 最大时间戳
    compression_options: CompressionOptions, // 压缩选项
}
```

**文件格式：**
- 数据块序列
- 块元数据区
- 布隆过滤器
- 文件尾部的元数据指针

### 3. 存储引擎状态管理

```rust
pub struct LsmStorageState {
    pub memtable: Arc<MemTable>,           // 当前内存表
    pub imm_memtables: Vec<Arc<MemTable>>, // 不可变内存表(最新到最旧)
    pub l0_sstables: Vec<usize>,          // L0 SSTables(最新到最旧)
    pub levels: Vec<(usize, Vec<usize>)>, // 层级结构(L1-Lmax)
    pub sstables: HashMap<usize, Arc<SsTable>>, // SSTable对象
}
```

### 4. 键设计(含时间戳)

支持多版本数据，键结构为`(key, timestamp)`：

```rust
pub struct KeyBytes {
    inner: Bytes,    // 键数据
    ts: u64,         // 时间戳
}
```

时间戳范围定义：
- `TS_RANGE_BEGIN = 0` - 最小时间戳
- `TS_RANGE_END = u64::MAX` - 最大时间戳
- `TS_DEFAULT = u64::MAX - 1` - 默认时间戳

## 存储引擎操作

### 写入路径

1. **Put操作**：写入当前内存表，可选写入WAL
2. **内存表冻结**：当内存表达到阈值时，转为不可变内存表
3. **Flush到L0**：将不可变内存表刷写到L0 SSTable
4. **压缩**：后台线程执行层级压缩

### 读取路径

1. **内存查询**：先查询当前内存表，然后不可变内存表
2. **L0查询**：查询L0 SSTables(逆序，最新优先)
3. **层级查询**：查询L1+层级SSTables
4. **布隆过滤器**：快速判断键是否存在

### 压缩策略

支持多种压缩算法：

1. **NoCompaction**：无压缩，仅用于测试
2. **Leveled Compaction**：层级压缩(RocksDB风格)
3. **Tiered Compaction**：分层压缩(Universal风格)
4. **Simple Leveled**：简单层级压缩

## 并发控制与事务

### MVCC实现

基于时间戳的多版本并发控制：

```rust
pub struct LsmMvccInner {
    pub(crate) watermark: Watermark,      // 水位线管理
    pub(crate) commit_ts: AtomicU64,      // 提交时间戳
    pub(crate) rwlock: RwLock<()>,        // 读写锁
}
```

### 事务支持

- **快照隔离**：基于时间戳的快照读取
- **可串行化**：支持严格的串行化隔离级别
- **写批处理**：支持原子写操作

## 性能优化特性

### 1. 块缓存

使用`moka`缓存库实现高效的块缓存：

```rust
pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;
```

### 2. 布隆过滤器

每个SSTable包含布隆过滤器，加速键存在性检查。

### 3. 迭代器优化

多种迭代器实现查询优化：

- `MergeIterator`：合并多个迭代器
- `ConcatIterator`：连接SSTable迭代器
- `TwoMergeIterator`：两级合并迭代器

### 4. 压缩支持

支持多种压缩算法：
- Snappy
- LZ4
- Gzip
- 无压缩

## 配置选项

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

## 使用示例

### 基本操作

```rust
// 打开数据库
let options = LsmStorageOptions::default_for_week2_test(
    CompactionOptions::Leveled(LeveledCompactionOptions::default())
);
let db = MiniLsm::open("/path/to/db", options)?;

// 写入数据
db.put(b"key1", b"value1")?;
db.put(b"key2", b"value2")?;

// 读取数据
let value = db.get(b"key1")?;
println!("Value: {:?}", value);

// 删除数据
db.delete(b"key2")?;

// 范围查询
let iter = db.scan(Bound::Included(b"key1"), Bound::Unbounded)?;
while iter.is_valid() {
    println!("Key: {:?}, Value: {:?}", iter.key(), iter.value());
    iter.next()?;
}

// 关闭数据库
db.close()?;
```

### 事务操作

```rust
// 开始事务
let txn = db.new_txn()?;

// 事务内操作
txn.put(b"txn_key", b"txn_value")?;
let value = txn.get(b"txn_key")?;

// 提交事务
txn.commit()?;
```

## 测试与验证

项目包含完整的测试套件，涵盖：

- 单元测试：各模块独立功能测试
- 集成测试：端到端功能测试
- 性能测试：压缩、读写性能基准测试
- 并发测试：多线程并发安全性测试

## 构建与运行

```bash
# 构建项目
cargo build --release

# 运行测试
cargo test

# 运行性能测试
cargo test --release --test compaction_bench

# 使用CLI工具
cargo run --bin mini-lsm-cli -- --help
```

## 设计特点

1. **模块化设计**：各组件职责清晰，易于理解和扩展
2. **并发安全**：基于Rust所有权和Arc/Mutex实现线程安全
3. **持久化保证**：WAL和Manifest保证数据一致性
4. **高性能**：跳表、布隆过滤器、块缓存等优化
5. **可扩展**：支持多种压缩策略和存储格式

## 后续扩展

- [ ] 分布式支持
- [ ] 更丰富的查询接口
- [ ] 监控和指标收集
- [ ] 备份和恢复功能
- [ ] 更高级的压缩策略

## 许可证

Apache License 2.0 - 详见LICENSE文件