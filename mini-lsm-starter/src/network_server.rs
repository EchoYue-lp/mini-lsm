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

//! Network Server for Mini-LSM
//!
//! This module provides a high-performance async network server that can handle
//! thousands of concurrent connections while using the synchronous LSM core.
//! Uses Protocol Buffers for cross-platform compatibility and performance.

use crate::connection_pool::{ConnectionPool, ConnectionPoolConfig};
use crate::hybrid_async_interface::HybridAsyncLsm;
use anyhow::{Context, Result};
use bytes::BytesMut;
use prost::Message;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant, timeout};

// Include the generated protobuf code
pub mod lsm_proto {
    include!(concat!(env!("OUT_DIR"), "/lsm.rs"));
}

use lsm_proto::*;

/// Transaction metadata for cleanup management
struct TransactionInfo {
    transaction: crate::hybrid_async_interface::HybridTransaction,
    created_at: Instant,
    last_access: Instant,
}

impl TransactionInfo {
    fn new(transaction: crate::hybrid_async_interface::HybridTransaction) -> Self {
        let now = Instant::now();
        Self {
            transaction,
            created_at: now,
            last_access: now,
        }
    }

    fn update_access(&mut self) {
        self.last_access = Instant::now();
    }

    fn is_expired(&self, timeout: Duration) -> bool {
        self.last_access.elapsed() > timeout
    }
}

/// Security configuration for DoS protection
#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub max_request_size: usize,
    pub read_timeout: Duration,
    pub max_transactions_per_client: usize,
    pub max_scan_limit: u32,
    pub connection_pool: ConnectionPoolConfig,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_request_size: 64 * 1024,           // 64KB max request
            read_timeout: Duration::from_secs(30), // 30s read timeout
            max_transactions_per_client: 10,       // Max 10 transactions per client
            max_scan_limit: 10000,                 // Max 10K items per scan
            connection_pool: ConnectionPoolConfig::default(),
        }
    }
}

/// Client transaction tracking info
#[derive(Debug)]
struct ClientInfo {
    transaction_count: usize,
}

impl ClientInfo {
    fn new() -> Self {
        Self {
            transaction_count: 0,
        }
    }
}

/// Connection handler that manages client connections with DoS protection
pub struct ConnectionHandler {
    lsm: Arc<HybridAsyncLsm>,
    next_txn_id: std::sync::atomic::AtomicU64,
    active_txns: Mutex<std::collections::HashMap<u64, TransactionInfo>>,
    txn_timeout: Duration,
    security_config: SecurityConfig,
    client_info: Mutex<std::collections::HashMap<std::net::SocketAddr, ClientInfo>>,
    connection_pool: Arc<ConnectionPool>,
}

impl ConnectionHandler {
    pub fn new(lsm: Arc<HybridAsyncLsm>) -> Self {
        Self::new_with_config(lsm, SecurityConfig::default())
    }

    pub fn new_with_config(lsm: Arc<HybridAsyncLsm>, security_config: SecurityConfig) -> Self {
        let connection_pool =
            Arc::new(ConnectionPool::new(security_config.connection_pool.clone()));

        Self {
            lsm,
            next_txn_id: std::sync::atomic::AtomicU64::new(1),
            active_txns: Mutex::new(std::collections::HashMap::new()),
            txn_timeout: Duration::from_secs(300), // 5 minutes default timeout
            security_config,
            client_info: Mutex::new(std::collections::HashMap::new()),
            connection_pool,
        }
    }

    /// Clean up expired transactions
    async fn cleanup_expired_transactions(&self) {
        let mut active_txns = self.active_txns.lock().await;
        let mut expired_txn_ids = Vec::new();

        for (txn_id, txn_info) in active_txns.iter() {
            if txn_info.is_expired(self.txn_timeout) {
                expired_txn_ids.push(*txn_id);
            }
        }

        for txn_id in expired_txn_ids {
            if let Some(txn_info) = active_txns.remove(&txn_id) {
                println!(
                    "Cleaning up expired transaction {}, created at {:?}, last accessed {:?}",
                    txn_id, txn_info.created_at, txn_info.last_access
                );
            }
        }

        if !active_txns.is_empty() {
            println!("Active transactions: {}", active_txns.len());
        }
    }

    /// Clean up all transactions for a disconnected client
    async fn cleanup_client_transactions(&self, client_transactions: &[u64]) {
        if client_transactions.is_empty() {
            return;
        }

        let mut active_txns = self.active_txns.lock().await;
        for txn_id in client_transactions {
            if let Some(txn_info) = active_txns.remove(txn_id) {
                println!(
                    "Cleaning up transaction {} for disconnected client, was active for {:?}",
                    txn_id,
                    txn_info.created_at.elapsed()
                );
            }
        }
    }

    /// Handle a single client connection with DoS protection
    pub async fn handle_connection(&self, mut stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr().context("Failed to get peer address")?;

        // Acquire connection from pool
        let connection_info = match self.connection_pool.acquire_connection(peer_addr).await {
            Ok(info) => info,
            Err(e) => {
                println!("Connection rejected from {}: {}", peer_addr, e);
                return Err(anyhow::anyhow!("Connection pool full: {}", e));
            }
        };

        println!(
            "New connection from: {} (id: {})",
            peer_addr, connection_info.id
        );

        // Initialize client info for transaction tracking
        {
            let mut client_info = self.client_info.lock().await;
            client_info.insert(peer_addr, ClientInfo::new());
        }

        let mut buffer = BytesMut::with_capacity(8192);
        let mut client_transactions = Vec::new(); // Track transactions for this client
        let mut request_count = 0u32;

        let result = self
            .handle_connection_inner(
                &mut stream,
                peer_addr,
                &mut buffer,
                &mut client_transactions,
                &mut request_count,
                &connection_info,
            )
            .await;

        // Cleanup on disconnect
        self.connection_pool
            .release_connection(connection_info.id)
            .await;
        self.cleanup_client_transactions(&client_transactions).await;
        {
            let mut client_info = self.client_info.lock().await;
            client_info.remove(&peer_addr);
        }
        println!("Client {} disconnected", peer_addr);

        result
    }

    async fn handle_connection_inner(
        &self,
        stream: &mut TcpStream,
        peer_addr: std::net::SocketAddr,
        buffer: &mut BytesMut,
        client_transactions: &mut Vec<u64>,
        request_count: &mut u32,
        connection_info: &crate::connection_pool::ConnectionInfo,
    ) -> Result<()> {
        loop {
            // Rate limiting check using connection pool
            if !connection_info.check_rate_limit(1).await {
                return Err(anyhow::anyhow!(
                    "Rate limit exceeded for client {}",
                    peer_addr
                ));
            }

            // Update connection activity
            connection_info.update_activity().await;
            connection_info.start_request();

            // Read request length with timeout
            let mut len_buf = [0u8; 4];
            match timeout(
                self.security_config.read_timeout,
                stream.read_exact(&mut len_buf),
            )
            .await
            {
                Ok(Ok(_)) => {}
                Ok(Err(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break; // Normal disconnect
                }
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!("Read timeout for client {}", peer_addr));
                }
            }

            let request_len = u32::from_be_bytes(len_buf) as usize;

            // Enhanced request size validation
            if request_len == 0 {
                return Err(anyhow::anyhow!("Invalid request size: 0"));
            }
            if request_len > self.security_config.max_request_size {
                return Err(anyhow::anyhow!(
                    "Request too large: {} bytes (max: {})",
                    request_len,
                    self.security_config.max_request_size
                ));
            }

            // Read request data with timeout
            buffer.clear();
            buffer.resize(request_len, 0);
            match timeout(self.security_config.read_timeout, stream.read_exact(buffer)).await {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e.into()),
                Err(_) => {
                    return Err(anyhow::anyhow!(
                        "Read data timeout for client {}",
                        peer_addr
                    ));
                }
            }

            // Parse protobuf request
            let request =
                LsmRequest::decode(&buffer[..]).context("Failed to decode protobuf request")?;

            // Periodic cleanup of expired transactions
            *request_count += 1;
            if (*request_count).is_multiple_of(50) {
                self.cleanup_expired_transactions().await;
            }

            // Process request with security checks
            let response = self
                .process_request_secure(request, client_transactions, peer_addr)
                .await;

            // Serialize protobuf response
            let mut response_data = Vec::new();
            response
                .encode(&mut response_data)
                .context("Failed to encode protobuf response")?;

            // Track bytes transferred
            let bytes_transferred = response_data.len() as u64;

            // Send response with timeout
            let write_result = timeout(self.security_config.read_timeout, async {
                stream.write_u32(response_data.len() as u32).await?;
                stream.write_all(&response_data).await?;
                stream.flush().await?;
                Ok::<(), anyhow::Error>(())
            })
            .await;

            // Finish request tracking (always called regardless of write result)
            connection_info.finish_request(bytes_transferred);

            match write_result {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => return Err(e),
                Err(_) => {
                    return Err(anyhow::anyhow!("Write timeout for client {}", peer_addr));
                }
            }
        }

        Ok(())
    }

    /// Process a single request with security checks
    async fn process_request_secure(
        &self,
        request: LsmRequest,
        client_transactions: &mut Vec<u64>,
        peer_addr: std::net::SocketAddr,
    ) -> LsmResponse {
        if let Some(req) = request.request {
            match req {
                lsm_proto::lsm_request::Request::Get(get_req) => {
                    match self.lsm.get(&get_req.key).await {
                        Ok(value) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::GetResult(
                                lsm_proto::GetResponse {
                                    value: value.map(|v| v.as_ref().to_vec()),
                                },
                            )),
                        },
                        Err(e) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Get failed: {}", e),
                                },
                            )),
                        },
                    }
                }

                lsm_proto::lsm_request::Request::Put(put_req) => {
                    match self.lsm.put(&put_req.key, &put_req.value).await {
                        Ok(()) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::PutResult(
                                lsm_proto::PutResponse {},
                            )),
                        },
                        Err(e) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Put failed: {}", e),
                                },
                            )),
                        },
                    }
                }

                lsm_proto::lsm_request::Request::Delete(del_req) => {
                    match self.lsm.delete(&del_req.key).await {
                        Ok(()) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::DeleteResult(
                                lsm_proto::DeleteResponse {},
                            )),
                        },
                        Err(e) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Delete failed: {}", e),
                                },
                            )),
                        },
                    }
                }

                lsm_proto::lsm_request::Request::Scan(scan_req) => {
                    // Enforce scan limit to prevent DoS
                    let effective_limit = match scan_req.limit {
                        Some(limit) => std::cmp::min(limit, self.security_config.max_scan_limit),
                        None => self.security_config.max_scan_limit,
                    };

                    if effective_limit == 0 {
                        return LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: "Invalid scan limit: 0".to_string(),
                                },
                            )),
                        };
                    }

                    let start_bound = scan_req
                        .start
                        .as_ref()
                        .map(|s| std::ops::Bound::Included(s.as_slice()))
                        .unwrap_or(std::ops::Bound::Unbounded);
                    let end_bound = scan_req
                        .end
                        .as_ref()
                        .map(|s| std::ops::Bound::Excluded(s.as_slice()))
                        .unwrap_or(std::ops::Bound::Unbounded);

                    match self.lsm.scan(start_bound, end_bound).await {
                        Ok(iter) => {
                            let items = match iter.collect().await {
                                Ok(items) => items,
                                Err(e) => {
                                    return LsmResponse {
                                        response: Some(lsm_proto::lsm_response::Response::Error(
                                            lsm_proto::ErrorResponse {
                                                message: format!(
                                                    "Scan iterator collect failed: {}",
                                                    e
                                                ),
                                            },
                                        )),
                                    };
                                }
                            };
                            let mut items = items;
                            items.truncate(effective_limit as usize);
                            // Optimize: Use into_iter to avoid extra clones where possible
                            let result_items: Vec<lsm_proto::KeyValuePair> = items
                                .into_iter()
                                .map(|(k, v)| lsm_proto::KeyValuePair {
                                    key: k.as_ref().to_vec(),   // Convert Bytes to Vec<u8>
                                    value: v.as_ref().to_vec(), // Convert Bytes to Vec<u8>
                                })
                                .collect();
                            LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::ScanResult(
                                    lsm_proto::ScanResponse {
                                        items: result_items,
                                    },
                                )),
                            }
                        }
                        Err(e) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Scan failed: {}", e),
                                },
                            )),
                        },
                    }
                }

                lsm_proto::lsm_request::Request::BeginTxn(_) => {
                    // Check transaction limit per client
                    if client_transactions.len() >= self.security_config.max_transactions_per_client
                    {
                        return LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!(
                                        "Transaction limit exceeded for client {} (max: {})",
                                        peer_addr, self.security_config.max_transactions_per_client
                                    ),
                                },
                            )),
                        };
                    }

                    match self.lsm.new_txn().await {
                        Ok(txn) => {
                            let txn_id = self
                                .next_txn_id
                                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
                            let mut active_txns = self.active_txns.lock().await;
                            active_txns.insert(txn_id, TransactionInfo::new(txn));

                            // Track transaction for this client
                            client_transactions.push(txn_id);

                            // Update client transaction count
                            {
                                let mut client_info = self.client_info.lock().await;
                                if let Some(info) = client_info.get_mut(&peer_addr) {
                                    info.transaction_count += 1;
                                }
                            }

                            LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::TxnStarted(
                                    lsm_proto::BeginTxnResponse { txn_id },
                                )),
                            }
                        }
                        Err(e) => LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Begin transaction failed: {}", e),
                                },
                            )),
                        },
                    }
                }

                lsm_proto::lsm_request::Request::CommitTxn(commit_req) => {
                    let txn_info = {
                        let mut active_txns = self.active_txns.lock().await;
                        active_txns.remove(&commit_req.txn_id)
                    };

                    // Remove from client tracking
                    if client_transactions.contains(&commit_req.txn_id) {
                        client_transactions.retain(|&id| id != commit_req.txn_id);
                        // Update client transaction count
                        let mut client_info = self.client_info.lock().await;
                        if let Some(info) = client_info.get_mut(&peer_addr) {
                            info.transaction_count = info.transaction_count.saturating_sub(1);
                        }
                    }

                    if let Some(txn_info) = txn_info {
                        match txn_info.transaction.commit().await {
                            Ok(()) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::TxnCommitted(
                                    lsm_proto::CommitTxnResponse {},
                                )),
                            },
                            Err(e) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::Error(
                                    lsm_proto::ErrorResponse {
                                        message: format!("Commit failed: {}", e),
                                    },
                                )),
                            },
                        }
                    } else {
                        LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!("Transaction {} not found", commit_req.txn_id),
                                },
                            )),
                        }
                    }
                }

                lsm_proto::lsm_request::Request::TxnGet(txn_get_req) => {
                    let mut active_txns = self.active_txns.lock().await;
                    if let Some(txn_info) = active_txns.get_mut(&txn_get_req.txn_id) {
                        txn_info.update_access(); // Update last access time
                        let txn = &txn_info.transaction;
                        match txn.get(&txn_get_req.key).await {
                            Ok(value) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::TxnGetResult(
                                    lsm_proto::TxnGetResponse {
                                        value: value.map(|v| v.as_ref().to_vec()),
                                    },
                                )),
                            },
                            Err(e) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::Error(
                                    lsm_proto::ErrorResponse {
                                        message: format!("Transaction get failed: {}", e),
                                    },
                                )),
                            },
                        }
                    } else {
                        LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!(
                                        "Transaction {} not found",
                                        txn_get_req.txn_id
                                    ),
                                },
                            )),
                        }
                    }
                }

                lsm_proto::lsm_request::Request::TxnPut(txn_put_req) => {
                    let mut active_txns = self.active_txns.lock().await;
                    if let Some(txn_info) = active_txns.get_mut(&txn_put_req.txn_id) {
                        txn_info.update_access(); // Update last access time
                        let txn = &txn_info.transaction;
                        match txn.put(&txn_put_req.key, &txn_put_req.value) {
                            Ok(()) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::TxnPutResult(
                                    lsm_proto::TxnPutResponse {},
                                )),
                            },
                            Err(e) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::Error(
                                    lsm_proto::ErrorResponse {
                                        message: format!("Transaction put failed: {}", e),
                                    },
                                )),
                            },
                        }
                    } else {
                        LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!(
                                        "Transaction {} not found",
                                        txn_put_req.txn_id
                                    ),
                                },
                            )),
                        }
                    }
                }

                lsm_proto::lsm_request::Request::TxnDelete(txn_del_req) => {
                    let mut active_txns = self.active_txns.lock().await;
                    if let Some(txn_info) = active_txns.get_mut(&txn_del_req.txn_id) {
                        txn_info.update_access(); // Update last access time
                        let txn = &txn_info.transaction;
                        match txn.delete(&txn_del_req.key) {
                            Ok(()) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::TxnDeleteResult(
                                    lsm_proto::TxnDeleteResponse {},
                                )),
                            },
                            Err(e) => LsmResponse {
                                response: Some(lsm_proto::lsm_response::Response::Error(
                                    lsm_proto::ErrorResponse {
                                        message: format!("Transaction delete failed: {}", e),
                                    },
                                )),
                            },
                        }
                    } else {
                        LsmResponse {
                            response: Some(lsm_proto::lsm_response::Response::Error(
                                lsm_proto::ErrorResponse {
                                    message: format!(
                                        "Transaction {} not found",
                                        txn_del_req.txn_id
                                    ),
                                },
                            )),
                        }
                    }
                }
            }
        } else {
            LsmResponse {
                response: Some(lsm_proto::lsm_response::Response::Error(
                    lsm_proto::ErrorResponse {
                        message: "Invalid request: no operation specified".to_string(),
                    },
                )),
            }
        }
    }
}

/// Async LSM server with DoS protection
pub struct LsmServer {
    lsm: Arc<HybridAsyncLsm>,
    handler: Arc<ConnectionHandler>,
}

impl LsmServer {
    pub fn new(lsm: HybridAsyncLsm) -> Self {
        Self::new_with_config(lsm, SecurityConfig::default())
    }

    pub fn new_with_config(lsm: HybridAsyncLsm, security_config: SecurityConfig) -> Self {
        let lsm = Arc::new(lsm);
        let handler = Arc::new(ConnectionHandler::new_with_config(
            lsm.clone(),
            security_config,
        ));
        Self { lsm, handler }
    }

    /// Start the server on the given address
    pub async fn serve(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr)
            .await
            .context("Failed to bind to address")?;

        println!("LSM Server listening on {}", addr);

        // Start background cleanup tasks
        let handler_for_cleanup = self.handler.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30)); // Cleanup every 30 seconds
            loop {
                interval.tick().await;
                handler_for_cleanup.cleanup_expired_transactions().await;
            }
        });

        // Start connection pool cleanup task
        self.handler
            .connection_pool
            .clone()
            .start_cleanup_task()
            .await;

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let handler = self.handler.clone();

                    // Spawn a new task for each connection
                    tokio::spawn(async move {
                        println!("Accepted connection from: {}", addr);
                        if let Err(e) = handler.handle_connection(stream).await {
                            eprintln!("Connection error from {}: {}", addr, e);
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Gracefully shutdown the server
    pub async fn shutdown(&self) -> Result<()> {
        println!("Shutting down LSM server...");
        self.lsm.close().await?;
        println!("LSM server shutdown complete");
        Ok(())
    }
}
