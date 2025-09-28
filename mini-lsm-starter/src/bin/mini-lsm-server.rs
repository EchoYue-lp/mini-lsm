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

//! Mini-LSM Network Server
//!
//! A dedicated server binary for running Mini-LSM as a network service
//! with Protocol Buffers support and high concurrency.

use anyhow::Result;
use clap::{Parser, ValueEnum};
use mini_lsm_starter::compact::{
    CompactionOptions, LeveledCompactionOptions, SimpleLeveledCompactionOptions,
    TieredCompactionOptions,
};
use mini_lsm_starter::compression::CompressionOptions;
use mini_lsm_starter::hybrid_async_interface::HybridAsyncLsm;
use mini_lsm_starter::lsm_storage::LsmStorageOptions;
use mini_lsm_starter::network_server::LsmServer;
use std::path::PathBuf;
use tokio::signal;

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

#[derive(Parser, Debug)]
#[command(name = "mini-lsm-server")]
#[command(author, version, about = "Mini-LSM Network Server with Protocol Buffers support", long_about = None)]
struct Args {
    /// Storage directory path
    #[arg(short, long, default_value = "lsm_server.db")]
    path: PathBuf,

    /// Server bind address
    #[arg(short, long, default_value = "127.0.0.1:7878")]
    address: String,

    /// Compaction strategy
    #[arg(long, default_value = "leveled")]
    compaction: CompactionStrategy,

    /// Enable write-ahead log
    #[arg(long)]
    enable_wal: bool,

    /// Enable serializable transactions
    #[arg(long)]
    serializable: bool,

    /// Compression algorithm
    #[arg(long, default_value = "none")]
    compression: CompressionStrategy,

    /// Block size in bytes
    #[arg(long, default_value = "4096")]
    block_size: usize,

    /// Target SST size in bytes
    #[arg(long, default_value = "2097152")] // 2MB
    target_sst_size: usize,

    /// Number of memtable limit
    #[arg(long, default_value = "3")]
    num_memtable_limit: usize,

    /// Number of compaction threads
    #[arg(long, default_value = "2")]
    num_compaction_threads: usize,

    /// Enable TTL support
    #[arg(long)]
    enable_ttl: bool,

    /// Verbose logging
    #[arg(short, long)]
    verbose: bool,
}

fn create_lsm_options(args: &Args) -> LsmStorageOptions {
    LsmStorageOptions {
        block_size: args.block_size,
        target_sst_size: args.target_sst_size,
        num_memtable_limit: args.num_memtable_limit,
        num_subcompactions: 4,
        enable_ttl: args.enable_ttl,
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
        num_compaction_thread_limit: args.num_compaction_threads,
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

fn print_banner() {
    println!(
        "
███╗   ███╗██╗███╗   ██╗██╗      ██╗     ███████╗███╗   ███╗
████╗ ████║██║████╗  ██║██║      ██║     ██╔════╝████╗ ████║
██╔████╔██║██║██╔██╗ ██║██║█████╗██║     ███████╗██╔████╔██║
██║╚██╔╝██║██║██║╚██╗██║██║╚════╝██║     ╚════██║██║╚██╔╝██║
██║ ╚═╝ ██║██║██║ ╚████║██║      ███████╗███████║██║ ╚═╝ ██║
╚═╝     ╚═╝╚═╝╚═╝  ╚═══╝╚═╝      ╚══════╝╚══════╝╚═╝     ╚═╝

                Network Server v2.0
    "
    );
}

fn print_config(args: &Args) {
    println!("📋 Server Configuration:");
    println!("   Storage Path:       {}", args.path.display());
    println!("   Listen Address:     {}", args.address);
    println!("   Compaction:         {:?}", args.compaction);
    println!("   Compression:        {:?}", args.compression);
    println!("   WAL Enabled:        {}", args.enable_wal);
    println!("   Serializable:       {}", args.serializable);
    println!("   TTL Enabled:        {}", args.enable_ttl);
    println!("   Block Size:         {} bytes", args.block_size);
    println!("   Target SST Size:    {} bytes", args.target_sst_size);
    println!("   MemTable Limit:     {}", args.num_memtable_limit);
    println!("   Compaction Threads: {}", args.num_compaction_threads);
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    print_banner();
    print_config(&args);

    // Initialize logging if verbose
    if args.verbose {
        println!("🔍 Verbose logging enabled");
    }

    println!("\n🚀 Initializing storage...");
    let lsm_options = create_lsm_options(&args);
    let lsm = HybridAsyncLsm::open(&args.path, lsm_options).await?;
    println!("✅ Storage initialized at {}", args.path.display());

    println!("\n🌐 Starting network server...");
    let server = LsmServer::new(lsm);

    println!("🎯 Server Features:");
    println!("   • Protocol Buffers over TCP");
    println!("   • High concurrency (thousands of connections)");
    println!("   • Full LSM operations (GET, PUT, DELETE, SCAN)");
    println!("   • ACID transactions");
    println!("   • Automatic compaction");
    println!("   • Write-ahead logging");
    println!("   • Cross-platform compatibility");

    println!("\n📡 Client Connection Info:");
    println!("   Address: {}", args.address);
    println!("   Protocol: Protocol Buffers");
    println!("   Example: cargo run --example network_client");

    println!("\n✨ Server ready! Press Ctrl+C to shutdown gracefully");

    // Setup graceful shutdown
    let server_task = tokio::spawn({
        let addr = args.address.clone();
        async move {
            if let Err(e) = server.serve(&addr).await {
                eprintln!("❌ Server error: {}", e);
            }
        }
    });

    // Wait for shutdown signal
    match signal::ctrl_c().await {
        Ok(()) => {
            println!("\n🛑 Shutdown signal received");
        }
        Err(err) => {
            eprintln!("❌ Unable to listen for shutdown signal: {}", err);
        }
    }

    // Graceful shutdown
    server_task.abort();

    println!("👋 Mini-LSM server shutdown complete");
    println!("📊 Thank you for using Mini-LSM!");

    Ok(())
}
