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

//! Connection Pool and Rate Limiting for Mini-LSM Network Server
//!
//! This module provides:
//! - Connection pool management for efficient resource utilization
//! - Token bucket based rate limiting per client
//! - Connection health monitoring and cleanup
//! - Configurable limits and timeouts

use crate::error::{LsmError, LsmResult, NetworkError};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};

/// Configuration for connection pool and rate limiting
#[derive(Debug, Clone)]
pub struct ConnectionPoolConfig {
    /// Maximum number of concurrent connections
    pub max_connections: usize,
    /// Maximum connections per IP address
    pub max_connections_per_ip: usize,
    /// Connection idle timeout
    pub idle_timeout: Duration,
    /// Rate limit: requests per second per client
    pub rate_limit_rps: u32,
    /// Token bucket capacity (burst size)
    pub rate_limit_burst: u32,
    /// Rate limit window duration
    pub rate_limit_window: Duration,
    /// Connection cleanup interval
    pub cleanup_interval: Duration,
    /// Maximum request size per connection
    pub max_request_size: usize,
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            max_connections: 1000,
            max_connections_per_ip: 100,
            idle_timeout: Duration::from_secs(300), // 5 minutes
            rate_limit_rps: 100,
            rate_limit_burst: 200,
            rate_limit_window: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(60), // 1 minute
            max_request_size: 64 * 1024,               // 64KB
        }
    }
}

/// Token bucket for rate limiting
#[derive(Debug)]
pub struct TokenBucket {
    /// Current number of tokens
    tokens: AtomicU64,
    /// Maximum tokens (burst capacity)
    capacity: u64,
    /// Token refill rate (tokens per second)
    refill_rate: u64,
    /// Last refill timestamp
    last_refill: Mutex<Instant>,
}

impl TokenBucket {
    /// Create a new token bucket
    pub fn new(capacity: u64, refill_rate: u64) -> Self {
        Self {
            tokens: AtomicU64::new(capacity),
            capacity,
            refill_rate,
            last_refill: Mutex::new(Instant::now()),
        }
    }

    /// Try to consume tokens from the bucket
    pub async fn try_consume(&self, tokens: u64) -> bool {
        self.refill().await;

        let current = self.tokens.load(Ordering::Acquire);
        if current >= tokens {
            let new_value = current - tokens;
            // Use compare_exchange to handle race conditions
            match self.tokens.compare_exchange_weak(
                current,
                new_value,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => true,
                Err(_) => {
                    // Retry once more in case of race condition
                    let current = self.tokens.load(Ordering::Acquire);
                    if current >= tokens {
                        self.tokens.store(current - tokens, Ordering::Release);
                        true
                    } else {
                        false
                    }
                }
            }
        } else {
            false
        }
    }

    /// Refill tokens based on elapsed time
    async fn refill(&self) {
        let mut last_refill = self.last_refill.lock().await;
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        if elapsed >= Duration::from_millis(100) {
            // Refill every 100ms minimum
            let tokens_to_add = (elapsed.as_secs_f64() * self.refill_rate as f64) as u64;
            if tokens_to_add > 0 {
                let current = self.tokens.load(Ordering::Acquire);
                let new_tokens = (current + tokens_to_add).min(self.capacity);
                self.tokens.store(new_tokens, Ordering::Release);
                *last_refill = now;
            }
        }
    }

    /// Get current token count
    pub fn current_tokens(&self) -> u64 {
        self.tokens.load(Ordering::Acquire)
    }
}

/// Connection information for tracking and rate limiting
#[derive(Debug)]
pub struct ConnectionInfo {
    /// Connection ID
    pub id: u64,
    /// Client IP address
    pub addr: SocketAddr,
    /// Connection creation time
    pub created_at: Instant,
    /// Last activity time
    pub last_activity: Mutex<Instant>,
    /// Token bucket for rate limiting
    pub rate_limiter: TokenBucket,
    /// Number of active requests
    pub active_requests: AtomicUsize,
    /// Total requests served
    pub total_requests: AtomicU64,
    /// Total bytes transferred
    pub total_bytes: AtomicU64,
}

impl ConnectionInfo {
    pub fn new(id: u64, addr: SocketAddr, config: &ConnectionPoolConfig) -> Self {
        let now = Instant::now();
        Self {
            id,
            addr,
            created_at: now,
            last_activity: Mutex::new(now),
            rate_limiter: TokenBucket::new(
                config.rate_limit_burst as u64,
                config.rate_limit_rps as u64,
            ),
            active_requests: AtomicUsize::new(0),
            total_requests: AtomicU64::new(0),
            total_bytes: AtomicU64::new(0),
        }
    }

    /// Update last activity timestamp
    pub async fn update_activity(&self) {
        *self.last_activity.lock().await = Instant::now();
    }

    /// Check if connection is idle
    pub async fn is_idle(&self, timeout: Duration) -> bool {
        let last_activity = *self.last_activity.lock().await;
        last_activity.elapsed() > timeout
    }

    /// Increment request counter
    pub fn start_request(&self) {
        self.active_requests.fetch_add(1, Ordering::Relaxed);
        self.total_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement request counter and add bytes transferred
    pub fn finish_request(&self, bytes_transferred: u64) {
        self.active_requests.fetch_sub(1, Ordering::Relaxed);
        self.total_bytes
            .fetch_add(bytes_transferred, Ordering::Relaxed);
    }

    /// Check rate limit
    pub async fn check_rate_limit(&self, tokens: u64) -> bool {
        self.rate_limiter.try_consume(tokens).await
    }
}

/// Connection pool manager
pub struct ConnectionPool {
    /// Pool configuration
    config: ConnectionPoolConfig,
    /// Active connections
    connections: RwLock<HashMap<u64, Arc<ConnectionInfo>>>,
    /// Connections per IP address
    ip_connections: RwLock<HashMap<SocketAddr, Vec<u64>>>,
    /// Connection ID counter
    next_connection_id: AtomicU64,
    /// Global connection semaphore
    connection_semaphore: Arc<Semaphore>,
    /// Pool statistics
    stats: ConnectionPoolStats,
}

#[derive(Debug, Default)]
pub struct ConnectionPoolStats {
    /// Total connections created
    pub total_connections: AtomicU64,
    /// Total connections rejected
    pub rejected_connections: AtomicU64,
    /// Total rate limited requests
    pub rate_limited_requests: AtomicU64,
    /// Current active connections
    pub active_connections: AtomicUsize,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: ConnectionPoolConfig) -> Self {
        let connection_semaphore = Arc::new(Semaphore::new(config.max_connections));

        Self {
            config,
            connections: RwLock::new(HashMap::new()),
            ip_connections: RwLock::new(HashMap::new()),
            next_connection_id: AtomicU64::new(1),
            connection_semaphore,
            stats: ConnectionPoolStats::default(),
        }
    }

    /// Acquire a connection slot
    pub async fn acquire_connection(&self, addr: SocketAddr) -> LsmResult<Arc<ConnectionInfo>> {
        // Check global connection limit
        let _permit = self.connection_semaphore.acquire().await.map_err(|_| {
            LsmError::Network(NetworkError::Connection(
                "Connection limit reached".to_string(),
            ))
        })?;

        // Check per-IP connection limit
        {
            let ip_connections = self.ip_connections.read().await;
            if let Some(connections) = ip_connections.get(&addr)
                && connections.len() >= self.config.max_connections_per_ip
            {
                self.stats
                    .rejected_connections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(LsmError::Network(NetworkError::Connection(format!(
                    "Too many connections from IP: {}",
                    addr
                ))));
            }
        }

        // Create new connection
        let connection_id = self.next_connection_id.fetch_add(1, Ordering::Relaxed);
        let connection_info = Arc::new(ConnectionInfo::new(connection_id, addr, &self.config));

        // Add to tracking structures
        {
            let mut connections = self.connections.write().await;
            connections.insert(connection_id, connection_info.clone());
        }

        {
            let mut ip_connections = self.ip_connections.write().await;
            ip_connections
                .entry(addr)
                .or_insert_with(Vec::new)
                .push(connection_id);
        }

        // Update statistics
        self.stats.total_connections.fetch_add(1, Ordering::Relaxed);
        self.stats
            .active_connections
            .fetch_add(1, Ordering::Relaxed);

        // Forget the permit to keep the connection slot
        std::mem::forget(_permit);

        Ok(connection_info)
    }

    /// Release a connection
    pub async fn release_connection(&self, connection_id: u64) {
        let connection_info = {
            let mut connections = self.connections.write().await;
            connections.remove(&connection_id)
        };

        if let Some(info) = connection_info {
            // Remove from IP tracking
            {
                let mut ip_connections = self.ip_connections.write().await;
                if let Some(connections) = ip_connections.get_mut(&info.addr) {
                    connections.retain(|&id| id != connection_id);
                    if connections.is_empty() {
                        ip_connections.remove(&info.addr);
                    }
                }
            }

            // Update statistics
            self.stats
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);

            // Release connection slot
            self.connection_semaphore.add_permits(1);
        }
    }

    /// Check rate limit for a connection
    pub async fn check_rate_limit(&self, connection_id: u64, tokens: u64) -> bool {
        let connections = self.connections.read().await;
        if let Some(connection) = connections.get(&connection_id) {
            let allowed = connection.check_rate_limit(tokens).await;
            if !allowed {
                self.stats
                    .rate_limited_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
            allowed
        } else {
            false
        }
    }

    /// Start background cleanup task
    pub async fn start_cleanup_task(self: Arc<Self>) {
        let pool = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(pool.config.cleanup_interval);
            loop {
                interval.tick().await;
                pool.cleanup_idle_connections().await;
            }
        });
    }

    /// Clean up idle connections
    async fn cleanup_idle_connections(&self) {
        let mut to_remove = Vec::new();

        {
            let connections = self.connections.read().await;
            for (id, connection) in connections.iter() {
                if connection.is_idle(self.config.idle_timeout).await
                    && connection.active_requests.load(Ordering::Relaxed) == 0
                {
                    to_remove.push(*id);
                }
            }
        }

        for connection_id in to_remove {
            self.release_connection(connection_id).await;
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> &ConnectionPoolStats {
        &self.stats
    }

    /// Get current active connections count
    pub async fn active_connections_count(&self) -> usize {
        self.connections.read().await.len()
    }

    /// Get detailed connection information
    pub async fn get_connection_info(&self, connection_id: u64) -> Option<Arc<ConnectionInfo>> {
        let connections = self.connections.read().await;
        connections.get(&connection_id).cloned()
    }

    /// Force close connections from a specific IP
    pub async fn close_connections_from_ip(&self, addr: SocketAddr) -> usize {
        let connection_ids = {
            let ip_connections = self.ip_connections.read().await;
            ip_connections.get(&addr).cloned().unwrap_or_default()
        };

        let closed_count = connection_ids.len();
        for connection_id in connection_ids {
            self.release_connection(connection_id).await;
        }

        closed_count
    }
}

/// Rate limiter for global request rate limiting
pub struct GlobalRateLimiter {
    token_bucket: TokenBucket,
}

impl GlobalRateLimiter {
    pub fn new(rps: u64, burst: u64) -> Self {
        Self {
            token_bucket: TokenBucket::new(burst, rps),
        }
    }

    pub async fn check_rate_limit(&self, tokens: u64) -> bool {
        self.token_bucket.try_consume(tokens).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn test_token_bucket() {
        let bucket = TokenBucket::new(10, 5); // 10 capacity, 5 tokens/sec

        // Should allow initial burst
        assert!(bucket.try_consume(5).await);
        assert!(bucket.try_consume(5).await);

        // Should be empty now
        assert!(!bucket.try_consume(1).await);

        // Wait for refill
        tokio::time::sleep(Duration::from_millis(1200)).await;

        // Should have refilled some tokens
        assert!(bucket.try_consume(3).await);
    }

    #[tokio::test]
    async fn test_connection_pool() {
        let config = ConnectionPoolConfig {
            max_connections: 2,
            max_connections_per_ip: 1,
            ..Default::default()
        };

        let pool = Arc::new(ConnectionPool::new(config));
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Should allow first connection
        let conn1 = pool.acquire_connection(addr).await.unwrap();

        // Should reject second connection from same IP
        assert!(pool.acquire_connection(addr).await.is_err());

        // Release first connection
        pool.release_connection(conn1.id).await;

        // Should allow connection again
        let _conn2 = pool.acquire_connection(addr).await.unwrap();
    }
}
