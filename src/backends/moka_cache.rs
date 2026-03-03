//! Moka Cache - In-Memory Cache Backend
//!
//! High-performance in-memory cache using Moka for hot data storage.

use anyhow::Result;
use moka::future::Cache;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Cache entry with TTL information
#[derive(Debug, Clone)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Instant,
}

impl CacheEntry {
    fn new(value: Vec<u8>, ttl: Duration) -> Self {
        Self {
            value,
            expires_at: Instant::now() + ttl,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expires_at
    }
}

/// Configuration for `MokaCache`
#[derive(Debug, Clone, Copy)]
pub struct MokaCacheConfig {
    /// Max capacity of the cache
    pub max_capacity: u64,
    /// Time to live for cache entries
    pub time_to_live: Duration,
    /// Time to idle for cache entries
    pub time_to_idle: Duration,
}

impl Default for MokaCacheConfig {
    fn default() -> Self {
        Self {
            max_capacity: 2000,
            time_to_live: Duration::from_secs(3600),
            time_to_idle: Duration::from_secs(120),
        }
    }
}

/// Moka in-memory cache with per-key TTL support
///
/// This is the default L1 (hot tier) cache backend, providing:
/// - Fast in-memory access (< 1ms latency)
/// - Automatic eviction via LRU
/// - Per-key TTL support
/// - Statistics tracking
pub struct MokaCache {
    /// Moka cache instance
    cache: Cache<String, CacheEntry>,
    /// Hit counter
    hits: Arc<AtomicU64>,
    /// Miss counter
    misses: Arc<AtomicU64>,
    /// Set counter
    sets: Arc<AtomicU64>,
    /// Coalesced requests counter (requests that waited for an ongoing computation)
    #[allow(dead_code)]
    coalesced_requests: Arc<AtomicU64>,
}

impl MokaCache {
    /// Create new Moka cache
    /// # Errors
    ///
    /// Returns an error if the cache cannot be initialized.
    pub fn new(config: MokaCacheConfig) -> Result<Self> {
        info!("Initializing Moka Cache");

        let cache = Cache::builder()
            .max_capacity(config.max_capacity)
            .time_to_live(config.time_to_live)
            .time_to_idle(config.time_to_idle)
            .build();

        info!(
            capacity = config.max_capacity,
            "Moka Cache initialized with per-key TTL support"
        );

        Ok(Self {
            cache,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            sets: Arc::new(AtomicU64::new(0)),
            coalesced_requests: Arc::new(AtomicU64::new(0)),
        })
    }
}

// ===== Trait Implementations =====

use crate::traits::{CacheBackend, L2CacheBackend};
use async_trait::async_trait;

/// Implement `CacheBackend` trait for `MokaCache`
///
/// This allows `MokaCache` to be used as a pluggable backend in the multi-tier cache system.
#[async_trait]
impl CacheBackend for MokaCache {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(entry) = self.cache.get(key).await {
            if entry.is_expired() {
                // Remove expired entry
                let _ = self.cache.remove(key).await;
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            } else {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(entry.value)
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        let entry = CacheEntry::new(value.to_vec(), ttl);
        self.cache.insert(key.to_string(), entry).await;
        self.sets.fetch_add(1, Ordering::Relaxed);
        debug!(key = %key, ttl_secs = %ttl.as_secs(), "[Moka] Cached key with TTL");
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.cache.remove(key).await;
        Ok(())
    }

    async fn health_check(&self) -> bool {
        // Test basic functionality with custom TTL
        let test_key = "health_check_moka";
        let test_value = b"health_check_value".to_vec();

        match self
            .set_with_ttl(test_key, &test_value, Duration::from_secs(60))
            .await
        {
            Ok(()) => match self.get(test_key).await {
                Some(retrieved) => {
                    let _ = self.remove(test_key).await;
                    retrieved == test_value
                }
                None => false,
            },
            Err(_) => false,
        }
    }

    async fn remove_pattern(&self, pattern: &str) -> Result<()> {
        let pattern_owned = pattern.to_string();

        let mut keys_to_remove = Vec::new();

        // Use iter() to find keys matching the pattern
        for (k, _v) in self.cache.iter() {
            if pattern_owned.ends_with('*') {
                let prefix = &pattern_owned[..pattern_owned.len() - 1];
                if k.starts_with(prefix) {
                    keys_to_remove.push(k.as_ref().clone());
                }
            } else if k.as_str() == pattern_owned {
                keys_to_remove.push(k.as_ref().clone());
            }
        }

        for k in keys_to_remove {
            self.cache.remove(&k).await;
        }

        debug!("Invalidated pattern '{}' from Moka cache", pattern);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "Moka"
    }
}

/// Implement `L2CacheBackend` trait for `MokaCache`
#[async_trait]
impl L2CacheBackend for MokaCache {
    async fn get_with_ttl(&self, key: &str) -> Option<(Vec<u8>, Option<Duration>)> {
        if let Some(entry) = self.cache.get(key).await {
            if entry.is_expired() {
                let _ = self.cache.remove(key).await;
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            } else {
                self.hits.fetch_add(1, Ordering::Relaxed);
                let now = Instant::now();
                let ttl = if entry.expires_at > now {
                    Some(entry.expires_at - now)
                } else {
                    None
                };
                Some((entry.value, ttl))
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}

/// Cache statistics
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub sets: u64,
    pub coalesced_requests: u64,
    pub size: u64,
}
