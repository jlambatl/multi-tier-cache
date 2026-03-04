//! Redis Cache - Distributed Cache Backend
//!
//! Redis-based distributed cache for warm data storage with persistence.

use anyhow::{Context, Result};
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Client};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

use crate::utils::redact_url;

/// Redis distributed cache with `ConnectionManager` for automatic reconnection
///
/// This is the default L2 (warm tier) cache backend, providing:
/// - Distributed caching across multiple instances
/// - Persistence to disk
/// - Automatic reconnection via `ConnectionManager`
/// - TTL introspection for cache promotion
/// - Pattern-based key scanning
/// - Memory limit enforcement with configurable eviction policies
pub struct RedisCache {
    /// Redis connection manager - handles reconnection automatically
    conn_manager: ConnectionManager,
    /// Hit counter
    hits: Arc<AtomicU64>,
    /// Miss counter
    misses: Arc<AtomicU64>,
    /// Set counter
    sets: Arc<AtomicU64>,
    /// Optional memory limit (in bytes) - if set, configures Redis maxmemory
    #[allow(dead_code)]
    max_memory: Option<u64>,
}

/// Configuration for Redis memory management
#[derive(Debug, Clone)]
pub struct RedisMemoryConfig {
    /// Maximum memory in bytes (e.g., 1GB = `1_073_741_824`)
    pub max_memory_bytes: u64,
    /// Eviction policy when maxmemory is reached
    /// Options: "noeviction", "allkeys-lru", "volatile-lru", "allkeys-lfu", "volatile-lfu",
    /// "allkeys-random", "volatile-random", "volatile-ttl"
    /// Default: "allkeys-lru"
    pub eviction_policy: String,
}

impl Default for RedisMemoryConfig {
    fn default() -> Self {
        Self {
            max_memory_bytes: 1_073_741_824, // 1GB default
            eviction_policy: "allkeys-lru".to_string(),
        }
    }
}

impl RedisCache {
    /// Create new Redis cache with `ConnectionManager` for automatic reconnection
    /// # Errors
    ///
    /// Returns an error if the Redis client cannot be created or connection fails.
    pub async fn new() -> Result<Self> {
        let redis_url =
            std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
        Self::with_url(&redis_url).await
    }

    /// Create new Redis cache with custom URL
    ///
    /// # Arguments
    ///
    /// * `redis_url` - Redis connection string (e.g., `<redis://localhost:6379>`)
    /// # Errors
    ///
    /// Returns an error if the Redis client cannot be created or connection fails.
    pub async fn with_url(redis_url: &str) -> Result<Self> {
        Self::with_url_and_memory_config(redis_url, None).await
    }

    /// Create new Redis cache with custom URL and memory limits
    ///
    /// # Arguments
    ///
    /// * `redis_url` - Redis connection string (e.g., `<redis://localhost:6379>`)
    /// * `memory_config` - Optional memory limit and eviction policy configuration
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use multi_tier_cache::backends::{RedisCache, RedisMemoryConfig};
    /// # async fn example() -> anyhow::Result<()> {
    /// let config = RedisMemoryConfig {
    ///     max_memory_bytes: 2_147_483_648, // 2GB
    ///     eviction_policy: "allkeys-lru".to_string(),
    /// };
    /// let cache = RedisCache::with_url_and_memory_config(
    ///     "redis://127.0.0.1:6379",
    ///     Some(config)
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// # Errors
    ///
    /// Returns an error if the Redis client cannot be created or connection fails.
    pub async fn with_url_and_memory_config(
        redis_url: &str,
        memory_config: Option<RedisMemoryConfig>,
    ) -> Result<Self> {
        info!(redis_url = %redact_url(redis_url), "Initializing Redis Cache with ConnectionManager");

        let client = Client::open(redis_url).with_context(|| {
            format!(
                "Failed to create Redis client with URL: {}",
                redact_url(redis_url)
            )
        })?;

        // Create ConnectionManager - handles reconnection automatically
        let conn_manager = ConnectionManager::new(client)
            .await
            .context("Failed to establish Redis connection manager")?;

        // Test connection
        let mut conn = conn_manager.clone();
        let _: String = redis::cmd("PING")
            .query_async(&mut conn)
            .await
            .context("Redis PING health check failed")?;

        // Configure memory limits if provided
        let max_memory = if let Some(config) = memory_config {
            info!(
                max_memory_mb = config.max_memory_bytes / 1_048_576,
                policy = %config.eviction_policy,
                "Configuring Redis memory limits"
            );

            // Set maxmemory
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg("maxmemory")
                .arg(config.max_memory_bytes)
                .query_async(&mut conn)
                .await
                .context("Failed to set Redis maxmemory")?;

            // Set maxmemory-policy
            let _: () = redis::cmd("CONFIG")
                .arg("SET")
                .arg("maxmemory-policy")
                .arg(&config.eviction_policy)
                .query_async(&mut conn)
                .await
                .with_context(|| {
                    format!(
                        "Failed to set Redis maxmemory-policy to {}",
                        config.eviction_policy
                    )
                })?;

            Some(config.max_memory_bytes)
        } else {
            None
        };

        info!(redis_url = %redact_url(redis_url), "Redis Cache connected successfully (ConnectionManager enabled)");

        Ok(Self {
            conn_manager,
            hits: Arc::new(AtomicU64::new(0)),
            misses: Arc::new(AtomicU64::new(0)),
            sets: Arc::new(AtomicU64::new(0)),
            max_memory,
        })
    }

    /// Get current memory usage statistics from Redis
    ///
    /// # Returns
    ///
    /// Tuple of (`used_memory_bytes`, `max_memory_bytes`, `memory_fragmentation_ratio`)
    ///
    /// # Errors
    ///
    /// Returns an error if the Redis INFO command fails
    pub async fn get_memory_stats(&self) -> Result<(u64, u64, f64)> {
        let mut conn = self.conn_manager.clone();

        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await
            .context("Failed to get Redis INFO memory")?;

        // Parse used_memory, maxmemory, and fragmentation ratio from INFO output
        let mut used_memory: u64 = 0;
        let mut max_memory: u64 = 0;
        let mut fragmentation_ratio: f64 = 1.0;

        for line in info.lines() {
            if let Some(value) = line.strip_prefix("used_memory:") {
                used_memory = value.trim().parse().unwrap_or(0);
            } else if let Some(value) = line.strip_prefix("maxmemory:") {
                max_memory = value.trim().parse().unwrap_or(0);
            } else if let Some(value) = line.strip_prefix("mem_fragmentation_ratio:") {
                fragmentation_ratio = value.trim().parse().unwrap_or(1.0);
            }
        }

        Ok((used_memory, max_memory, fragmentation_ratio))
    }

    /// Check if memory usage is approaching the configured limit
    ///
    /// # Arguments
    ///
    /// * `threshold` - Percentage threshold (0.0-1.0). For example, 0.8 means 80% full
    ///
    /// # Returns
    ///
    /// `true` if memory usage is above the threshold, `false` otherwise
    ///
    /// # Errors
    ///
    /// Returns an error if memory stats cannot be retrieved
    pub async fn is_memory_pressure(&self, threshold: f64) -> Result<bool> {
        let (used, max, _) = self.get_memory_stats().await?;

        if max == 0 {
            // No memory limit configured
            return Ok(false);
        }

        #[allow(clippy::cast_precision_loss)]
        let usage_ratio = used as f64 / max as f64;
        Ok(usage_ratio >= threshold)
    }

    /// Scan keys matching a pattern (glob-style: *, ?, [])
    ///
    /// Uses Redis SCAN command (non-blocking, cursor-based iteration)
    /// This is safe for production use, unlike KEYS command.
    ///
    /// # Arguments
    /// * `pattern` - Glob-style pattern (e.g., "user:*", "product:123:*")
    ///
    /// # Returns
    /// Vector of matching key names
    ///
    /// # Examples
    /// ```no_run
    /// # use multi_tier_cache::backends::RedisCache;
    /// # async fn example() -> anyhow::Result<()> {
    /// # let cache = RedisCache::new().await?;
    /// // Find all user cache keys
    /// let keys = cache.scan_keys("user:*", None).await?;
    ///
    /// // Find specific user's cache keys
    /// let keys = cache.scan_keys("user:123:*", None).await?;
    /// # Ok(())
    /// # }
    /// ```
    /// # Errors
    ///
    /// Returns an error if the Redis command fails.
    pub async fn scan_keys(&self, pattern: &str, limit: Option<usize>) -> Result<Vec<String>> {
        let mut conn = self.conn_manager.clone();
        let mut keys = Vec::new();
        let mut cursor: u64 = 0;

        loop {
            if let Some(limit_val) = limit {
                if keys.len() >= limit_val {
                    break;
                }
            }

            // SCAN cursor MATCH pattern COUNT 100
            let result: (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(pattern)
                .arg("COUNT")
                .arg(100) // Fetch 100 keys per iteration
                .query_async(&mut conn)
                .await?;

            cursor = result.0;
            keys.extend(result.1);

            // Cursor 0 means iteration is complete
            if cursor == 0 {
                break;
            }
        }

        if let Some(limit_val) = limit {
            if keys.len() > limit_val {
                keys.truncate(limit_val);
            }
        }

        debug!(pattern = %pattern, count = keys.len(), "[Redis] Scanned keys matching pattern");
        Ok(keys)
    }

    /// Remove multiple keys at once (bulk delete)
    ///
    /// More efficient than calling `remove()` multiple times
    /// # Errors
    ///
    /// Returns an error if the Redis command fails.
    pub async fn remove_bulk(&self, keys: &[String]) -> Result<usize> {
        if keys.is_empty() {
            return Ok(0);
        }

        let mut conn = self.conn_manager.clone();
        let count: usize = conn.del(keys).await?;
        debug!(count = count, "[Redis] Removed keys in bulk");
        Ok(count)
    }
}

// ===== Trait Implementations =====

use crate::traits::{CacheBackend, L2CacheBackend};
use async_trait::async_trait;

/// Implement `CacheBackend` trait for `RedisCache`
///
/// This allows `RedisCache` to be used as a pluggable backend in the multi-tier cache system.
#[async_trait]
impl CacheBackend for RedisCache {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut conn = self.conn_manager.clone();

        match conn.get::<_, Vec<u8>>(key).await {
            Ok(bytes) if !bytes.is_empty() => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(bytes)
            }
            Ok(_) | Err(_) => {
                // Empty bytes or error treated as miss
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        let mut conn = self.conn_manager.clone();

        // Use pset_ex for millisecond precision
        let ttl_millis = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        let _: () = conn.pset_ex(key, value, ttl_millis).await?;

        self.sets.fetch_add(1, Ordering::Relaxed);
        debug!(key = %key, ttl_ms = %ttl_millis, "[Redis] Cached key with TTL");
        Ok(())
    }

    async fn remove(&self, key: &str) -> Result<()> {
        let mut conn = self.conn_manager.clone();
        let _: () = conn.del(key).await?;
        Ok(())
    }

    async fn health_check(&self) -> bool {
        let test_key = "health_check_redis";
        let test_value = b"health_check_value".to_vec();

        match self
            .set_with_ttl(test_key, &test_value, Duration::from_secs(10))
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

    fn name(&self) -> &'static str {
        "Redis"
    }
}

/// Implement `L2CacheBackend` trait for `RedisCache`
///
/// This extends `CacheBackend` with TTL introspection capabilities needed for L2->L1 promotion.
#[async_trait]
impl L2CacheBackend for RedisCache {
    async fn get_with_ttl(&self, key: &str) -> Option<(Vec<u8>, Option<Duration>)> {
        let mut conn = self.conn_manager.clone();

        // Use pipeline to get value and PTTL in a single round-trip
        let result: (Option<Vec<u8>>, i64) = if let Ok(r) = redis::pipe()
            .atomic()
            .cmd("GET")
            .arg(key)
            .cmd("PTTL")
            .arg(key)
            .query_async(&mut conn)
            .await
        {
            r
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };

        let (value_opt, pttl_ms) = result;

        if let Some(value) = value_opt {
            if value.is_empty() {
                self.misses.fetch_add(1, Ordering::Relaxed);
                return None;
            }

            self.hits.fetch_add(1, Ordering::Relaxed);

            // PTTL returns:
            // -2 if the key does not exist
            // -1 if the key exists but has no associated expire
            // >= 0 is the remaining time in milliseconds
            if pttl_ms > 0 {
                #[allow(clippy::cast_sign_loss)]
                let ttl_u64 = pttl_ms as u64;
                Some((value, Some(Duration::from_millis(ttl_u64))))
            } else {
                Some((value, None))
            }
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
}
