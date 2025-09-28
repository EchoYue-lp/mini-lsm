// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Mini-LSM Hybrid CLI
//!
//! This CLI supports both sync and async (hybrid) modes for interacting with Mini-LSM.
//! The hybrid mode demonstrates the new architecture while maintaining compatibility.

mod wrapper;

use rustyline::DefaultEditor;
use wrapper::mini_lsm_wrapper;

use anyhow::Result;
use bytes::Bytes;
use clap::{Parser, ValueEnum};
use mini_lsm_starter::compression::CompressionOptions;
use mini_lsm_starter::hybrid_async_interface::HybridAsyncLsm;
use mini_lsm_wrapper::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
    TieredCompactionOptions,
};
use mini_lsm_wrapper::iterators::StorageIterator;
use mini_lsm_wrapper::lsm_storage::{LsmStorageOptions, MiniLsm};
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Debug, Clone, ValueEnum)]
enum CompactionStrategy {
    Simple,
    Leveled,
    Tiered,
    None,
}

#[derive(Debug, Clone, ValueEnum)]
enum CompressionStrategy {
    Lz4,
    Snappy,
    Gz,
    None,
}

#[derive(Debug, Clone, ValueEnum)]
enum ExecutionMode {
    Sync,
    Hybrid,
    Server,
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "lsm.db")]
    path: PathBuf,
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,
    #[arg(long)]
    enable_wal: bool,
    #[arg(long)]
    serializable: bool,
    compression: CompressionStrategy,
    #[arg(long, default_value = "sync")]
    mode: ExecutionMode,
    #[arg(long, default_value = "127.0.0.1:7878")]
    server_addr: String,
}

// Unified command interface
#[derive(Debug)]
enum Command {
    Fill {
        begin: u64,
        end: u64,
    },
    Del {
        key: String,
    },
    Get {
        key: String,
    },
    Scan {
        begin: Option<String>,
        end: Option<String>,
    },
    TxnBegin,
    TxnGet {
        key: String,
    },
    TxnPut {
        key: String,
        value: String,
    },
    TxnDelete {
        key: String,
    },
    TxnCommit,
    Dump,
    Flush,
    FullCompaction,
    Quit,
    Close,
}

impl Command {
    pub fn parse(input: &str) -> Result<Self> {
        use nom::branch::*;
        use nom::bytes::complete::*;
        use nom::character::complete::*;
        use nom::combinator::*;
        use nom::sequence::*;

        let uint = |i| {
            map_res(digit1::<&str, nom::error::Error<_>>, |s: &str| {
                s.parse()
                    .map_err(|_| nom::error::Error::new(s, nom::error::ErrorKind::Digit))
            })(i)
        };

        let string = |i| {
            map(take_till1(|c: char| c.is_whitespace()), |s: &str| {
                s.to_string()
            })(i)
        };

        let fill = |i| {
            map(
                tuple((tag_no_case("fill"), space1, uint, space1, uint)),
                |(_, _, begin, _, end)| Command::Fill { begin, end },
            )(i)
        };

        let del = |i| {
            map(
                tuple((tag_no_case("del"), space1, string)),
                |(_, _, key)| Command::Del { key },
            )(i)
        };

        let get = |i| {
            map(
                tuple((tag_no_case("get"), space1, string)),
                |(_, _, key)| Command::Get { key },
            )(i)
        };

        let scan = |i| {
            map(
                tuple((
                    tag_no_case("scan"),
                    opt(tuple((space1, string, space1, string))),
                )),
                |(_, opt_args)| {
                    let (begin, end) = opt_args
                        .map_or((None, None), |(_, begin, _, end)| (Some(begin), Some(end)));
                    Command::Scan { begin, end }
                },
            )(i)
        };

        let txn_get = |i| {
            map(
                tuple((tag_no_case("txn_get"), space1, string)),
                |(_, _, key)| Command::TxnGet { key },
            )(i)
        };

        let txn_put = |i| {
            map(
                tuple((tag_no_case("txn_put"), space1, string, space1, string)),
                |(_, _, key, _, value)| Command::TxnPut { key, value },
            )(i)
        };

        let txn_delete = |i| {
            map(
                tuple((tag_no_case("txn_delete"), space1, string)),
                |(_, _, key)| Command::TxnDelete { key },
            )(i)
        };

        let command = |i| {
            alt((
                fill,
                del,
                get,
                scan,
                map(tag_no_case("txn_begin"), |_| Command::TxnBegin),
                txn_get,
                txn_put,
                txn_delete,
                map(tag_no_case("txn_commit"), |_| Command::TxnCommit),
                map(tag_no_case("dump"), |_| Command::Dump),
                map(tag_no_case("flush"), |_| Command::Flush),
                map(tag_no_case("full_compaction"), |_| Command::FullCompaction),
                map(tag_no_case("quit"), |_| Command::Quit),
                map(tag_no_case("close"), |_| Command::Close),
            ))(i)
        };

        command(input)
            .map(|(_, c)| c)
            .map_err(|e| anyhow::anyhow!("{}", e))
    }
}

// Sync handler
struct SyncReplHandler {
    epoch: u64,
    lsm: Arc<MiniLsm>,
}

impl SyncReplHandler {
    fn handle(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::Fill { begin, end } => {
                for i in *begin..=*end {
                    self.lsm.put(
                        format!("{}", i).as_bytes(),
                        format!("value{}@{}", i, self.epoch).as_bytes(),
                    )?;
                }
                println!(
                    "{} values filled with epoch {}",
                    end - begin + 1,
                    self.epoch
                );
            }
            Command::Del { key } => {
                self.lsm.delete(key.as_bytes())?;
                println!("{} deleted", key);
            }
            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes())? {
                    println!("{}={:?}", key, value);
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Scan { begin, end } => match (begin, end) {
                (None, None) => {
                    let mut iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!("{} keys scanned", cnt);
                }
                (Some(begin), Some(end)) => {
                    let mut iter = self.lsm.scan(
                        std::ops::Bound::Included(begin.as_bytes()),
                        std::ops::Bound::Included(end.as_bytes()),
                    )?;
                    let mut cnt = 0;
                    while iter.is_valid() {
                        println!(
                            "{:?}={:?}",
                            Bytes::copy_from_slice(iter.key()),
                            Bytes::copy_from_slice(iter.value()),
                        );
                        iter.next()?;
                        cnt += 1;
                    }
                    println!("{} keys scanned", cnt);
                }
                _ => println!("invalid command"),
            },
            Command::Dump => {
                self.lsm.dump_structure();
                println!("dump success");
            }
            Command::Flush => {
                self.lsm.force_flush()?;
                println!("flush success");
            }
            Command::FullCompaction => {
                self.lsm.force_full_compaction()?;
                println!("full compaction success");
            }
            Command::Quit | Command::Close => {
                self.lsm.close()?;
                std::process::exit(0);
            }
            _ => {
                println!("Transaction commands not supported in sync mode");
            }
        }

        self.epoch += 1;
        Ok(())
    }
}

// Hybrid async handler
struct HybridReplHandler {
    epoch: u64,
    lsm: Arc<HybridAsyncLsm>,
    current_txn: Option<mini_lsm_starter::hybrid_async_interface::HybridTransaction>,
}

impl HybridReplHandler {
    async fn handle(&mut self, command: &Command) -> Result<()> {
        match command {
            Command::Fill { begin, end } => {
                for i in *begin..=*end {
                    self.lsm
                        .put(
                            format!("{}", i).as_bytes(),
                            format!("value{}@{}", i, self.epoch).as_bytes(),
                        )
                        .await?;
                }
                println!(
                    "{} values filled with epoch {}",
                    end - begin + 1,
                    self.epoch
                );
            }
            Command::Del { key } => {
                self.lsm.delete(key.as_bytes()).await?;
                println!("{} deleted", key);
            }
            Command::Get { key } => {
                if let Some(value) = self.lsm.get(key.as_bytes()).await? {
                    println!("{}={}", key, String::from_utf8_lossy(&value));
                } else {
                    println!("{} not exist", key);
                }
            }
            Command::Scan { begin, end } => match (begin, end) {
                (None, None) => {
                    let iter = self
                        .lsm
                        .scan(std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)
                        .await?;
                    let results = iter.collect().await?;
                    for (key, value) in &results {
                        println!(
                            "{}={}",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        );
                    }
                    println!("{} keys scanned", results.len());
                }
                (Some(begin), Some(end)) => {
                    let iter = self
                        .lsm
                        .scan(
                            std::ops::Bound::Included(begin.as_bytes()),
                            std::ops::Bound::Included(end.as_bytes()),
                        )
                        .await?;
                    let results = iter.collect().await?;
                    for (key, value) in &results {
                        println!(
                            "{}={}",
                            String::from_utf8_lossy(key),
                            String::from_utf8_lossy(value)
                        );
                    }
                    println!("{} keys scanned", results.len());
                }
                _ => println!("invalid command"),
            },
            Command::TxnBegin => {
                if self.current_txn.is_some() {
                    println!("Transaction already active. Commit or abort first.");
                } else {
                    let txn = self.lsm.new_txn().await?;
                    self.current_txn = Some(txn);
                    println!("Transaction started");
                }
            }
            Command::TxnGet { key } => {
                if let Some(txn) = &self.current_txn {
                    if let Some(value) = txn.get(key.as_bytes()).await? {
                        println!("{}={}", key, String::from_utf8_lossy(&value));
                    } else {
                        println!("{} not exist in transaction", key);
                    }
                } else {
                    println!("No active transaction");
                }
            }
            Command::TxnPut { key, value } => {
                if let Some(txn) = &self.current_txn {
                    match txn.put(key.as_bytes(), value.as_bytes()) {
                        Ok(()) => println!("Put {} in transaction", key),
                        Err(e) => println!("Failed to put {} in transaction: {}", key, e),
                    }
                } else {
                    println!("No active transaction");
                }
            }
            Command::TxnDelete { key } => {
                if let Some(txn) = &self.current_txn {
                    match txn.delete(key.as_bytes()) {
                        Ok(()) => println!("Delete {} in transaction", key),
                        Err(e) => println!("Failed to delete {} in transaction: {}", key, e),
                    }
                } else {
                    println!("No active transaction");
                }
            }
            Command::TxnCommit => {
                if let Some(txn) = self.current_txn.take() {
                    txn.commit().await?;
                    println!("Transaction committed");
                } else {
                    println!("No active transaction");
                }
            }
            Command::Dump => {
                // Access sync engine for dump
                self.lsm.sync_engine().dump_structure();
                println!("dump success");
            }
            Command::Flush => {
                self.lsm.force_flush().await?;
                println!("flush success");
            }
            Command::FullCompaction => {
                // Access sync engine for full compaction
                self.lsm.sync_engine().force_full_compaction()?;
                println!("full compaction success");
            }
            Command::Quit | Command::Close => {
                self.lsm.close().await?;
                std::process::exit(0);
            }
        }

        self.epoch += 1;
        Ok(())
    }
}

struct Repl {
    app_name: String,
    description: String,
    prompt: String,
    editor: DefaultEditor,
}

impl Repl {
    pub fn run_sync(mut self, mut handler: SyncReplHandler) -> Result<()> {
        self.bootstrap()?;

        loop {
            let readline = self.editor.readline(&self.prompt)?;
            if readline.trim().is_empty() {
                continue;
            }
            let command = Command::parse(&readline)?;
            handler.handle(&command)?;
            self.editor.add_history_entry(readline)?;
        }
    }

    pub async fn run_hybrid(mut self, mut handler: HybridReplHandler) -> Result<()> {
        self.bootstrap()?;

        loop {
            let readline = self.editor.readline(&self.prompt)?;
            if readline.trim().is_empty() {
                continue;
            }
            let command = Command::parse(&readline)?;
            handler.handle(&command).await?;
            self.editor.add_history_entry(readline)?;
        }
    }

    fn bootstrap(&mut self) -> Result<()> {
        println!("Welcome to {}!", self.app_name);
        println!("{}", self.description);
        println!();
        Ok(())
    }
}

fn create_lsm_options(args: &Args) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: 4096,
        target_sst_size: 2 << 20, // 2MB
        num_memtable_limit: 3,
        num_subcompactions: 4,
        enable_ttl: false,
        compaction_options: match args.compaction {
            CompactionStrategy::None => CompactionOptions::NoCompaction,
            CompactionStrategy::Simple => {
                CompactionOptions::Simple(SimpleLeveledCompactionOptions {
                    size_ratio_percent: 200,
                    level0_file_num_compaction_trigger: 2,
                    max_levels: 4,
                })
            }
            CompactionStrategy::Tiered => CompactionOptions::Tiered(TieredCompactionOptions {
                num_tiers: 3,
                max_size_amplification_percent: 200,
                size_ratio: 1,
                min_merge_width: 2,
                max_merge_width: None,
            }),
            CompactionStrategy::Leveled => CompactionOptions::Leveled(LeveledCompactionOptions {
                level0_file_num_compaction_trigger: 2,
                max_levels: 4,
                base_level_size_mb: 128,
                level_size_multiplier: 2,
            }),
        },
        num_compaction_thread_limit: 2,
        enable_wal: args.enable_wal,
        serializable: args.serializable,
        compression_options: match args.compression {
            CompressionStrategy::Snappy => CompressionOptions::Snappy,
            CompressionStrategy::Lz4 => CompressionOptions::Lz4,
            CompressionStrategy::Gz => CompressionOptions::Gz,
            CompressionStrategy::None => CompressionOptions::None,
        },
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    match args.mode {
        ExecutionMode::Sync => {
            let lsm = MiniLsm::open(args.path.clone(), create_lsm_options(&args))?;
            let repl = Repl {
                app_name: "mini-lsm-hybrid-cli (Sync Mode)".to_string(),
                description: "A CLI for mini-lsm using synchronous interface".to_string(),
                prompt: "sync> ".to_string(),
                editor: DefaultEditor::new()?,
            };
            repl.run_sync(SyncReplHandler { epoch: 0, lsm })?;
        }

        ExecutionMode::Hybrid => {
            let lsm = HybridAsyncLsm::open(args.path.clone(), create_lsm_options(&args)).await?;
            let repl = Repl {
                app_name: "mini-lsm-hybrid-cli (Hybrid Mode)".to_string(),
                description:
                    "A CLI for mini-lsm using hybrid async interface with transaction support"
                        .to_string(),
                prompt: "hybrid> ".to_string(),
                editor: DefaultEditor::new()?,
            };
            repl.run_hybrid(HybridReplHandler {
                epoch: 0,
                lsm: Arc::new(lsm),
                current_txn: None,
            })
            .await?;
        }

        ExecutionMode::Server => {
            println!("🌐 Starting Mini-LSM Network Server");
            println!("==================================");

            let lsm = HybridAsyncLsm::open(args.path.clone(), create_lsm_options(&args)).await?;
            let server = mini_lsm_starter::network_server::LsmServer::new(lsm);

            println!("🚀 Server listening on {}", args.server_addr);
            println!("   Protocol: Protocol Buffers over TCP");
            println!("   Features: GET, PUT, DELETE, SCAN, Transactions");
            println!("   Press Ctrl+C to shutdown");

            server.serve(&args.server_addr).await?;
        }
    }

    Ok(())
}
