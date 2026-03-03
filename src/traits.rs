//! Cache Backend Traits
//!
//! This module defines the trait abstractions that allow users to implement
//! custom cache backends for both L1 (in-memory) and L2 (distributed) caches.
//!
//! # Architecture
//!
//! - `CacheBackend`: Core trait for all cache implementations
//! - `L2CacheBackend`: Extended trait for L2 caches with TTL introspection
//! - `StreamingBackend`: Optional trait for event streaming capabilities
//!
//! # Example: Custom L1 Backend
//!
/// ```rust,no_run
/// use multi_tier_cache::{CacheBackend, async_trait};
/// use std::time::Duration;
/// use anyhow::Result;
///
/// struct MyCustomCache;
///
/// #[async_trait]
/// impl CacheBackend for MyCustomCache {
///     async fn get(&self, key: &str) -> Option<Vec<u8>> {
///         None
///     }
///
///     async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
///         Ok(())
///     }
///
///     async fn remove(&self, key: &str) -> Result<()> {
///         Ok(())
///     }
///
///     async fn health_check(&self) -> bool {
///         true
///     }
/// }
/// ```
use anyhow::Result;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fmt::Debug;
use std::time::Duration;

/// Trait for cache serialization codecs
///
/// Codecs define how Rust types are serialized to bytes for storage and
/// deserialized back into Rust types.
pub trait CacheCodec: Send + Sync + Debug {
    /// Serialize a value to bytes
    fn serialize<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>>;

    /// Deserialize bytes to a value
    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T>;

    /// Get the name of this codec
    fn name(&self) -> &'static str;
}

/// Core cache backend trait for both L1 and L2 caches
///
/// This trait defines the essential operations that any cache backend must support.
/// Implement this trait to create custom L1 (in-memory) or L2 (distributed) cache backends.
///
/// # Required Operations
///
/// - `get`: Retrieve a value by key
/// - `set_with_ttl`: Store a value with a time-to-live
/// - `remove`: Delete a value by key
/// - `health_check`: Verify cache backend is operational
///
/// # Thread Safety
///
/// Implementations must be `Send + Sync` to support concurrent access across async tasks.
///
/// # Performance Considerations
///
/// - `get` operations should be optimized for low latency (target: <1ms for L1, <5ms for L2)
/// - `set_with_ttl` operations can be slightly slower but should still be fast
/// - Consider connection pooling for distributed backends
#[async_trait]
pub trait CacheBackend: Send + Sync {
    /// Get value from cache by key
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key to retrieve
    ///
    /// # Returns
    ///
    /// * `Some(value)` - Value found in cache (as raw bytes)
    /// * `None` - Key not found or expired
    async fn get(&self, key: &str) -> Option<Vec<u8>>;

    /// Set value in cache with time-to-live
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key
    /// * `value` - The value to store (raw bytes)
    /// * `ttl` - Time-to-live duration
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Value successfully cached
    /// * `Err(e)` - Cache operation failed
    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()>;

    /// Remove value from cache
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key to remove
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Value removed (or didn't exist)
    /// * `Err(e)` - Cache operation failed
    async fn remove(&self, key: &str) -> Result<()>;

    /// Check if cache backend is healthy
    ///
    /// This method should verify that the cache backend is operational.
    /// For distributed caches, this typically involves a ping or connectivity check.
    ///
    /// # Returns
    ///
    /// * `true` - Cache is healthy and operational
    /// * `false` - Cache is unhealthy or unreachable
    async fn health_check(&self) -> bool;

    /// Remove keys matching a pattern
    ///
    /// # Arguments
    ///
    /// * `pattern` - Glob-style pattern (e.g. "user:*")
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Pattern processed
    /// * `Err(e)` - Operation failed
    async fn remove_pattern(&self, _pattern: &str) -> Result<()> {
        // Default implementation does nothing (for backward compatibility)
        Ok(())
    }

    /// Get the name of this cache backend
    ///
    /// This is used for logging and debugging purposes.
    ///
    /// # Returns
    ///
    /// A string identifying this cache backend (e.g., "Moka", "Redis", "Memcached")
    fn name(&self) -> &'static str {
        "unknown"
    }
}

/// Extended trait for L2 cache backends with TTL introspection
///
/// This trait extends `CacheBackend` with the ability to retrieve both a value
/// and its remaining TTL. This is essential for implementing efficient L2-to-L1
/// promotion with accurate TTL propagation.
#[async_trait]
pub trait L2CacheBackend: CacheBackend {
    /// Get value with its remaining TTL from L2 cache
    ///
    /// This method retrieves both the value and its remaining time-to-live.
    /// This is used by the cache manager to promote entries from L2 to L1
    /// with the correct TTL.
    ///
    /// # Arguments
    ///
    /// * `key` - The cache key to retrieve
    ///
    /// # Returns
    ///
    /// * `Some((value, Some(ttl)))` - Value found with remaining TTL
    /// * `Some((value, None))` - Value found but no expiration set (never expires)
    /// * `None` - Key not found or expired
    async fn get_with_ttl(&self, key: &str) -> Option<(Vec<u8>, Option<Duration>)>;
}

/// Optional trait for cache backends that support event streaming
///
/// This trait defines operations for event-driven architectures using
/// streaming data structures like Redis Streams.
///
/// # Capabilities
///
/// - Publish events to streams with automatic trimming
/// - Read latest entries (newest first)
/// - Read entries with blocking support
///
/// # Backend Requirements
///
/// Not all cache backends support streaming. This trait is optional and
/// should only be implemented by backends with native streaming support
/// (e.g., Redis Streams, Kafka, Pulsar).
///
/// # Example
///
/// ```rust,no_run
/// use multi_tier_cache::{StreamingBackend, async_trait};
/// use anyhow::Result;
///
/// struct MyStreamingCache;
///
/// #[async_trait]
/// impl StreamingBackend for MyStreamingCache {
///     async fn stream_add(
///         &self,
///         stream_key: &str,
///         fields: Vec<(String, String)>,
///         maxlen: Option<usize>,
///     ) -> Result<String> {
///         Ok("entry-id".to_string())
///     }
///
///     async fn stream_read_latest(
///         &self,
///         stream_key: &str,
///         count: usize,
///     ) -> Result<Vec<(String, Vec<(String, String)>)>> {
///         Ok(vec![])
///     }
///
///     async fn stream_read(
///         &self,
///         stream_key: &str,
///         last_id: &str,
///         count: usize,
///         block_ms: Option<usize>,
///     ) -> Result<Vec<(String, Vec<(String, String)>)>> {
///         Ok(vec![])
///     }
/// }
/// ```
#[async_trait]
pub trait StreamingBackend: Send + Sync {
    /// Add an entry to a stream
    ///
    /// # Arguments
    ///
    /// * `stream_key` - Name of the stream (e.g., "`events_stream`")
    /// * `fields` - Vector of field-value pairs to add
    /// * `maxlen` - Optional maximum stream length (older entries are trimmed)
    ///
    /// # Returns
    ///
    /// * `Ok(entry_id)` - The generated entry ID (e.g., "1234567890-0")
    /// * `Err(e)` - Stream operation failed
    ///
    /// # Trimming Behavior
    ///
    /// If `maxlen` is specified, the stream is automatically trimmed to keep
    /// approximately that many entries (oldest entries are removed).
    async fn stream_add(
        &self,
        stream_key: &str,
        fields: Vec<(String, String)>,
        maxlen: Option<usize>,
    ) -> Result<String>;

    /// Read the latest N entries from a stream (newest first)
    ///
    /// # Arguments
    ///
    /// * `stream_key` - Name of the stream
    /// * `count` - Maximum number of entries to retrieve
    ///
    /// # Returns
    ///
    /// * `Ok(entries)` - Vector of (`entry_id`, fields) tuples (newest first)
    /// * `Err(e)` - Stream operation failed
    ///
    /// # Ordering
    ///
    /// Entries are returned in reverse chronological order (newest first).
    async fn stream_read_latest(
        &self,
        stream_key: &str,
        count: usize,
    ) -> Result<Vec<(String, Vec<(String, String)>)>>;

    /// Read entries from a stream with optional blocking
    ///
    /// # Arguments
    ///
    /// * `stream_key` - Name of the stream
    /// * `last_id` - Last entry ID seen ("0" for beginning, "$" for new only)
    /// * `count` - Maximum number of entries to retrieve
    /// * `block_ms` - Optional blocking timeout in milliseconds (None = non-blocking)
    ///
    /// # Returns
    ///
    /// * `Ok(entries)` - Vector of (`entry_id`, fields) tuples
    /// * `Err(e)` - Stream operation failed
    ///
    /// # Blocking Behavior
    ///
    /// - `None`: Non-blocking, returns immediately
    /// - `Some(ms)`: Blocks up to `ms` milliseconds waiting for new entries
    ///
    /// # Use Cases
    ///
    /// - Non-blocking: Poll for new events
    /// - Blocking: Long-polling for real-time event consumption
    async fn stream_read(
        &self,
        stream_key: &str,
        last_id: &str,
        count: usize,
        block_ms: Option<usize>,
    ) -> Result<Vec<(String, Vec<(String, String)>)>>;
}
