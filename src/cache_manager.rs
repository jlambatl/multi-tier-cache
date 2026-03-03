//! Cache Manager - Unified Cache Operations
//!
//! Manages operations across L1 (Moka) and L2 (Redis) caches with intelligent fallback.

use anyhow::Result;
use dashmap::DashMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use super::invalidation::{
    AtomicInvalidationStats, InvalidationConfig, InvalidationMessage, InvalidationPublisher,
    InvalidationStats, InvalidationSubscriber,
};
use crate::backends::{L1Cache, L2Cache};
use crate::codecs::JsonCodec;
use crate::traits::{CacheBackend, CacheCodec, L2CacheBackend, StreamingBackend};

/// Type alias for the in-flight requests map.
type InFlightMap = DashMap<Arc<str>, Arc<Mutex<()>>>;

/// RAII cleanup guard for in-flight request tracking
struct CleanupGuard<'a> {
    map: &'a InFlightMap,
    key: Arc<str>,
}

impl Drop for CleanupGuard<'_> {
    fn drop(&mut self) {
        self.map.remove(&self.key);
    }
}

/// Cache strategies
#[derive(Debug, Clone, Copy)]
pub enum CacheStrategy {
    RealTime,
    ShortTerm,
    MediumTerm,
    LongTerm,
    Custom(Duration),
    Default,
}

impl CacheStrategy {
    #[must_use]
    pub fn to_duration(&self) -> Duration {
        match self {
            Self::RealTime => Duration::from_secs(10),
            Self::ShortTerm | Self::Default => Duration::from_secs(300),
            Self::MediumTerm => Duration::from_secs(3600),
            Self::LongTerm => Duration::from_secs(10800),
            Self::Custom(duration) => *duration,
        }
    }
}

/// Statistics for a single cache tier
#[derive(Debug)]
pub struct TierStats {
    pub tier_level: usize,
    pub hits: AtomicU64,
    pub backend_name: String,
}

impl Clone for TierStats {
    fn clone(&self) -> Self {
        Self {
            tier_level: self.tier_level,
            hits: AtomicU64::new(self.hits.load(Ordering::Relaxed)),
            backend_name: self.backend_name.clone(),
        }
    }
}

impl TierStats {
    fn new(tier_level: usize, backend_name: String) -> Self {
        Self {
            tier_level,
            hits: AtomicU64::new(0),
            backend_name,
        }
    }

    /// Get current hit count
    pub fn hit_count(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }
}

/// A single cache tier
pub struct CacheTier {
    backend: Arc<dyn L2CacheBackend>,
    tier_level: usize,
    promotion_enabled: bool,
    ttl_scale: f64,
    stats: TierStats,
}

impl CacheTier {
    pub fn new(
        backend: Arc<dyn L2CacheBackend>,
        tier_level: usize,
        promotion_enabled: bool,
        ttl_scale: f64,
    ) -> Self {
        let backend_name = backend.name().to_string();
        Self {
            backend,
            tier_level,
            promotion_enabled,
            ttl_scale,
            stats: TierStats::new(tier_level, backend_name),
        }
    }

    async fn get_with_ttl(&self, key: &str) -> Option<(Vec<u8>, Option<Duration>)> {
        self.backend.get_with_ttl(key).await
    }

    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        let scaled_ttl = Duration::from_secs_f64(ttl.as_secs_f64() * self.ttl_scale);
        self.backend.set_with_ttl(key, value, scaled_ttl).await
    }

    fn record_hit(&self) {
        self.stats.hits.fetch_add(1, Ordering::Relaxed);
    }
}

/// Configuration for a cache tier
#[derive(Debug, Clone)]
pub struct TierConfig {
    pub tier_level: usize,
    pub promotion_enabled: bool,
    pub ttl_scale: f64,
}

impl TierConfig {
    pub fn new(tier_level: usize) -> Self {
        Self {
            tier_level,
            promotion_enabled: true,
            ttl_scale: 1.0,
        }
    }

    /// Configure as L1 (hot tier)
    pub fn as_l1() -> Self {
        Self {
            tier_level: 1,
            promotion_enabled: false,
            ttl_scale: 1.0,
        }
    }

    /// Configure as L2 (warm tier)
    pub fn as_l2() -> Self {
        Self {
            tier_level: 2,
            promotion_enabled: true,
            ttl_scale: 1.0,
        }
    }

    /// Configure as L3 (cold tier)
    pub fn as_l3() -> Self {
        Self {
            tier_level: 3,
            promotion_enabled: true,
            ttl_scale: 2.0,
        }
    }

    /// Configure as L4 (archive tier)
    pub fn as_l4() -> Self {
        Self {
            tier_level: 4,
            promotion_enabled: true,
            ttl_scale: 8.0,
        }
    }

    /// Set promotion enabled
    #[must_use]
    pub fn with_promotion(mut self, enabled: bool) -> Self {
        self.promotion_enabled = enabled;
        self
    }

    /// Set TTL scale factor
    #[must_use]
    pub fn with_ttl_scale(mut self, scale: f64) -> Self {
        self.ttl_scale = scale;
        self
    }

    /// Set tier level
    #[must_use]
    pub fn with_level(mut self, level: usize) -> Self {
        self.tier_level = level;
        self
    }
}

/// Proxy wrapper
struct ProxyCacheBackend {
    backend: Arc<dyn L2CacheBackend>,
}

#[async_trait::async_trait]
impl CacheBackend for ProxyCacheBackend {
    async fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.backend.get(key).await
    }

    async fn set_with_ttl(&self, key: &str, value: &[u8], ttl: Duration) -> Result<()> {
        self.backend.set_with_ttl(key, value, ttl).await
    }

    async fn remove(&self, key: &str) -> Result<()> {
        self.backend.remove(key).await
    }

    async fn health_check(&self) -> bool {
        self.backend.health_check().await
    }

    fn name(&self) -> &'static str {
        self.backend.name()
    }

    async fn remove_pattern(&self, pattern: &str) -> Result<()> {
        self.backend.remove_pattern(pattern).await
    }
}

/// Inner state
pub struct CacheManagerInner<C: CacheCodec = JsonCodec> {
    tiers: Option<Vec<CacheTier>>,
    l1_cache: Arc<dyn CacheBackend>,
    l2_cache: Arc<dyn L2CacheBackend>,
    l2_cache_concrete: Option<Arc<L2Cache>>,
    streaming_backend: Option<Arc<dyn StreamingBackend>>,
    codec: Arc<C>,
    total_requests: AtomicU64,
    l1_hits: AtomicU64,
    l2_hits: AtomicU64,
    misses: AtomicU64,
    promotions: AtomicU64,
    in_flight_requests: InFlightMap,
    invalidation_publisher: Option<Mutex<InvalidationPublisher>>,
    invalidation_subscriber: Option<InvalidationSubscriber>,
    invalidation_stats: Arc<AtomicInvalidationStats>,
}

/// Cache Manager
#[derive(Clone)]
pub struct CacheManager<C: CacheCodec = JsonCodec> {
    inner: Arc<CacheManagerInner<C>>,
}

impl<C: CacheCodec> CacheManager<C> {
    /// Get stats
    pub fn get_stats(&self) -> CacheManagerStats {
        let stats = CacheManagerStats {
            total_requests: self.inner.total_requests.load(Ordering::Relaxed),
            l1_hits: self.inner.l1_hits.load(Ordering::Relaxed),
            l2_hits: self.inner.l2_hits.load(Ordering::Relaxed),
            misses: self.inner.misses.load(Ordering::Relaxed),
            promotions: self.inner.promotions.load(Ordering::Relaxed),
            in_flight_requests: self.inner.in_flight_requests.len(),
            hit_rate: 0.0,
            l1_hit_rate: 0.0,
            l2_hit_rate: 0.0,
            invalidation_stats: self.inner.invalidation_stats.snapshot(),
            tiers: self
                .inner
                .tiers
                .as_ref()
                .map(|tiers| tiers.iter().map(|t| t.stats.clone()).collect()),
        };
        stats.calculate_hit_rates()
    }

    /// Create with codec
    pub fn with_codec(
        l1_cache: Arc<dyn CacheBackend>,
        l2_cache: Arc<dyn L2CacheBackend>,
        streaming_backend: Option<Arc<dyn StreamingBackend>>,
        codec: C,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(CacheManagerInner {
                tiers: None,
                l1_cache,
                l2_cache,
                l2_cache_concrete: None,
                streaming_backend,
                codec: Arc::new(codec),
                total_requests: AtomicU64::new(0),
                l1_hits: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                promotions: AtomicU64::new(0),
                in_flight_requests: DashMap::new(),
                invalidation_publisher: None,
                invalidation_subscriber: None,
                invalidation_stats: Arc::new(AtomicInvalidationStats::default()),
            }),
        })
    }

    /// Create with tiers and codec
    pub fn with_tiers_and_codec(
        tiers: Vec<CacheTier>,
        streaming_backend: Option<Arc<dyn StreamingBackend>>,
        codec: C,
    ) -> Result<Self> {
        // Validation: Tiers must be sorted by tier_level
        for i in 1..tiers.len() {
            let prev = &tiers[i - 1];
            let curr = &tiers[i];
            if curr.tier_level <= prev.tier_level {
                anyhow::bail!(
                    "Tiers must be strictly ascending by level (found L{} after L{})",
                    curr.tier_level,
                    prev.tier_level
                );
            }
        }

        let l1_cache = tiers.first().unwrap().backend.clone();
        let l2_cache = if tiers.len() >= 2 {
            tiers[1].backend.clone()
        } else {
            l1_cache.clone()
        };

        let l1_backend: Arc<dyn CacheBackend> = Arc::new(ProxyCacheBackend {
            backend: l1_cache.clone(),
        });

        Ok(Self {
            inner: Arc::new(CacheManagerInner {
                tiers: Some(tiers),
                l1_cache: l1_backend,
                l2_cache,
                l2_cache_concrete: None,
                streaming_backend,
                codec: Arc::new(codec),
                total_requests: AtomicU64::new(0),
                l1_hits: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                promotions: AtomicU64::new(0),
                in_flight_requests: DashMap::new(),
                invalidation_publisher: None,
                invalidation_subscriber: None,
                invalidation_stats: Arc::new(AtomicInvalidationStats::default()),
            }),
        })
    }

    /// Get value
    pub async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>> {
        self.inner.total_requests.fetch_add(1, Ordering::Relaxed);

        if let Some(tiers) = self.inner.tiers.as_ref() {
            // Multi-tier
            if let Some(tier1) = tiers.first() {
                if let Some((value, _)) = tier1.get_with_ttl(key).await {
                    match self.inner.codec.deserialize(&value) {
                        Ok(v) => {
                            tier1.record_hit();
                            self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(Some(v));
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Cache deserialization failed for key '{}' in L{}: {}",
                                key,
                                tier1.tier_level,
                                e
                            );
                        }
                    }
                }
            }

            // Stampede lock
            let key_arc: Arc<str> = Arc::from(key);
            let lock_guard = self
                .inner
                .in_flight_requests
                .entry(Arc::clone(&key_arc))
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone();
            let _guard = lock_guard.lock().await;
            let _cleanup = CleanupGuard {
                map: &self.inner.in_flight_requests,
                key: key_arc,
            };

            // Double check
            if let Some(tier1) = tiers.first() {
                if let Some((value, _)) = tier1.get_with_ttl(key).await {
                    match self.inner.codec.deserialize(&value) {
                        Ok(v) => {
                            tier1.record_hit();
                            self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
                            return Ok(Some(v));
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Cache deserialization failed for key '{}' in L{}: {}",
                                key,
                                tier1.tier_level,
                                e
                            );
                        }
                    }
                }
            }

            // Other tiers
            for (idx, tier) in tiers.iter().enumerate().skip(1) {
                if let Some((value, ttl)) = tier.get_with_ttl(key).await {
                    match self.inner.codec.deserialize(&value) {
                        Ok(v) => {
                            tier.record_hit();
                            if tier.promotion_enabled {
                                let promo_ttl =
                                    ttl.unwrap_or_else(|| CacheStrategy::Default.to_duration());
                                for up in tiers.iter().take(idx).rev() {
                                    let _ = up.set_with_ttl(key, &value, promo_ttl).await;
                                    tracing::debug!(
                                        "Promoted '{}' from L{} to L{}",
                                        key,
                                        tier.tier_level,
                                        up.tier_level
                                    );
                                }
                                self.inner.promotions.fetch_add(1, Ordering::Relaxed);
                            }
                            if idx >= 1 {
                                self.inner.l2_hits.fetch_add(1, Ordering::Relaxed);
                            }
                            return Ok(Some(v));
                        }
                        Err(e) => {
                            tracing::warn!(
                                "Cache deserialization failed for key '{}' in L{}: {}",
                                key,
                                tier.tier_level,
                                e
                            );
                            continue;
                        }
                    }
                }
            }
        } else {
            // Legacy L1+L2
            if let Some(value) = self.inner.l1_cache.get(key).await {
                match self.inner.codec.deserialize(&value) {
                    Ok(v) => {
                        self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(v));
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Cache deserialization failed for key '{}' in L1: {}",
                            key,
                            e
                        );
                    }
                }
            }

            // Lock
            let key_arc: Arc<str> = Arc::from(key);
            let lock_guard = self
                .inner
                .in_flight_requests
                .entry(Arc::clone(&key_arc))
                .or_insert_with(|| Arc::new(Mutex::new(())))
                .clone();
            let _guard = lock_guard.lock().await;
            let _cleanup = CleanupGuard {
                map: &self.inner.in_flight_requests,
                key: key_arc,
            };

            if let Some(value) = self.inner.l1_cache.get(key).await {
                match self.inner.codec.deserialize(&value) {
                    Ok(v) => {
                        self.inner.l1_hits.fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(v));
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Cache deserialization failed for key '{}' in L1: {}",
                            key,
                            e
                        );
                    }
                }
            }

            if let Some((value, ttl)) = self.inner.l2_cache.get_with_ttl(key).await {
                match self.inner.codec.deserialize(&value) {
                    Ok(v) => {
                        self.inner.l2_hits.fetch_add(1, Ordering::Relaxed);
                        let promo_ttl = ttl.unwrap_or_else(|| CacheStrategy::Default.to_duration());
                        let _ = self
                            .inner
                            .l1_cache
                            .set_with_ttl(key, &value, promo_ttl)
                            .await;
                        self.inner.promotions.fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(v));
                    }
                    Err(e) => {
                        tracing::warn!(
                            "Cache deserialization failed for key '{}' in L2: {}",
                            key,
                            e
                        );
                    }
                }
            }
        }

        self.inner.misses.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    /// Set value
    pub async fn set_with_strategy<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        strategy: CacheStrategy,
    ) -> Result<()> {
        let ttl = strategy.to_duration();
        let bytes = self.inner.codec.serialize(value)?;

        if let Some(tiers) = &self.inner.tiers {
            let mut ok = false;
            for tier in tiers {
                if tier.set_with_ttl(key, &bytes, ttl).await.is_ok() {
                    ok = true;
                }
            }
            if ok {
                Ok(())
            } else {
                Err(anyhow::anyhow!("All tiers failed"))
            }
        } else {
            let l1 = self.inner.l1_cache.set_with_ttl(key, &bytes, ttl).await;
            let l2 = self.inner.l2_cache.set_with_ttl(key, &bytes, ttl).await;
            if l1.is_err() && l2.is_err() {
                Err(anyhow::anyhow!("Both failed"))
            } else {
                Ok(())
            }
        }
    }

    /// Get or compute
    pub async fn get_or_compute<T, F, Fut>(
        &self,
        key: &str,
        strategy: CacheStrategy,
        compute_fn: F,
    ) -> Result<T>
    where
        T: Serialize + DeserializeOwned + Send,
        F: FnOnce() -> Fut + Send,
        Fut: Future<Output = Result<T>> + Send,
    {
        if let Some(val) = self.get(key).await? {
            return Ok(val);
        }

        // Compute lock
        let key_arc: Arc<str> = Arc::from(key);
        let lock_guard = self
            .inner
            .in_flight_requests
            .entry(Arc::clone(&key_arc))
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone();
        let _guard = lock_guard.lock().await;
        let _cleanup = CleanupGuard {
            map: &self.inner.in_flight_requests,
            key: key_arc,
        };

        // Double check
        if let Some(val) = self.get(key).await? {
            return Ok(val);
        }

        let val = compute_fn().await?;
        self.set_with_strategy(key, &val, strategy).await?;
        Ok(val)
    }

    /// Invalidate key
    pub async fn invalidate(&self, key: &str) -> Result<()> {
        if let Some(tiers) = &self.inner.tiers {
            for tier in tiers {
                let _ = tier.backend.remove(key).await;
            }
        } else {
            let _ = self.inner.l1_cache.remove(key).await;
            let _ = self.inner.l2_cache.remove(key).await;
        }

        if let Some(publ) = &self.inner.invalidation_publisher {
            let mut p = publ.lock().await;
            p.publish(&InvalidationMessage::remove(key)).await?;
            self.inner
                .invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Update cache and broadcast
    pub async fn update_cache<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        strategy: CacheStrategy,
    ) -> Result<()> {
        self.set_with_strategy(key, value, strategy).await?;

        if let Some(publ) = &self.inner.invalidation_publisher {
            let bytes = self.inner.codec.serialize(value)?;
            let msg = InvalidationMessage::update(key, bytes, Some(strategy.to_duration()));
            let mut p = publ.lock().await;
            p.publish(&msg).await?;
            self.inner
                .invalidation_stats
                .messages_sent
                .fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Set with broadcast (alias for `update_cache`)
    pub async fn set_with_broadcast<T: Serialize + ?Sized>(
        &self,
        key: &str,
        value: &T,
        strategy: CacheStrategy,
    ) -> Result<()> {
        self.update_cache(key, value, strategy).await
    }

    /// Invalidate pattern
    pub async fn invalidate_pattern(&self, pattern: &str) -> Result<()> {
        if let Some(tiers) = &self.inner.tiers {
            for tier in tiers {
                let _ = tier.backend.remove_pattern(pattern).await;
            }
        } else {
            let _ = self.inner.l1_cache.remove_pattern(pattern).await;
            // L2 pattern removal via scan_keys if concrete L2 available
            if let Some(l2) = &self.inner.l2_cache_concrete {
                let keys = l2.scan_keys(pattern, None).await?;
                l2.remove_bulk(&keys).await?;
            }
        }

        if let Some(publ) = &self.inner.invalidation_publisher {
            let mut p = publ.lock().await;
            p.publish(&InvalidationMessage::remove_pattern(pattern))
                .await?;
        }
        Ok(())
    }

    // ===== Streaming Methods =====

    /// Publish event to stream
    pub async fn publish_to_stream(
        &self,
        stream_key: &str,
        fields: Vec<(String, String)>,
        maxlen: Option<usize>,
    ) -> Result<String> {
        if let Some(backend) = &self.inner.streaming_backend {
            backend.stream_add(stream_key, fields, maxlen).await
        } else {
            Err(anyhow::anyhow!("Streaming backend not configured"))
        }
    }

    /// Read latest events from stream
    pub async fn read_stream_latest(
        &self,
        stream_key: &str,
        count: usize,
    ) -> Result<Vec<(String, Vec<(String, String)>)>> {
        if let Some(backend) = &self.inner.streaming_backend {
            backend.stream_read_latest(stream_key, count).await
        } else {
            Err(anyhow::anyhow!("Streaming backend not configured"))
        }
    }

    /// Read events from stream (blocking)
    pub async fn read_stream(
        &self,
        stream_key: &str,
        last_id: &str,
        count: usize,
        block_ms: Option<usize>,
    ) -> Result<Vec<(String, Vec<(String, String)>)>> {
        if let Some(backend) = &self.inner.streaming_backend {
            backend
                .stream_read(stream_key, last_id, count, block_ms)
                .await
        } else {
            Err(anyhow::anyhow!("Streaming backend not configured"))
        }
    }
}

#[derive(Debug, Clone)]
pub struct CacheManagerStats {
    pub total_requests: u64,
    pub l1_hits: u64,
    pub l2_hits: u64,
    pub misses: u64,
    pub promotions: u64,
    pub in_flight_requests: usize,
    pub hit_rate: f64,
    pub l1_hit_rate: f64,
    pub l2_hit_rate: f64,
    pub invalidation_stats: InvalidationStats,
    pub tiers: Option<Vec<TierStats>>,
}

impl CacheManagerStats {
    #[must_use]
    #[allow(clippy::cast_precision_loss)]
    pub fn calculate_hit_rates(mut self) -> Self {
        let total = self.total_requests as f64;
        if total > 0.0 {
            self.l1_hit_rate = self.l1_hits as f64 / total * 100.0;
            self.l2_hit_rate = self.l2_hits as f64 / total * 100.0;
            self.hit_rate = (self.l1_hits + self.l2_hits) as f64 / total * 100.0;
        }
        self
    }
}

impl CacheManager<JsonCodec> {
    pub fn new(l1: Arc<L1Cache>, l2: Arc<L2Cache>) -> Result<Self> {
        let l1_backend: Arc<dyn CacheBackend> = l1.clone();
        let l2_backend: Arc<dyn L2CacheBackend> = l2.clone();
        Self::with_codec(l1_backend, l2_backend, None, JsonCodec::new())
    }

    pub async fn new_with_invalidation(
        l1: Arc<L1Cache>,
        l2: Arc<L2Cache>,
        redis_url: &str,
        config: InvalidationConfig,
    ) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let conn_manager = redis::aio::ConnectionManager::new(client).await?;
        let publisher = InvalidationPublisher::new(conn_manager, config.clone());
        let subscriber = InvalidationSubscriber::new(redis_url, config.clone())?;

        let l1_backend: Arc<dyn CacheBackend> = l1.clone();
        let l2_backend: Arc<dyn L2CacheBackend> = l2.clone();
        let l2_concrete = Some(l2.clone());

        let manager = Self {
            inner: Arc::new(CacheManagerInner {
                tiers: None,
                l1_cache: l1_backend,
                l2_cache: l2_backend,
                l2_cache_concrete: l2_concrete,
                streaming_backend: None,
                codec: Arc::new(JsonCodec::new()),
                total_requests: AtomicU64::new(0),
                l1_hits: AtomicU64::new(0),
                l2_hits: AtomicU64::new(0),
                misses: AtomicU64::new(0),
                promotions: AtomicU64::new(0),
                in_flight_requests: DashMap::new(),
                invalidation_publisher: Some(Mutex::new(publisher)),
                invalidation_subscriber: Some(subscriber),
                invalidation_stats: Arc::new(AtomicInvalidationStats::default()),
            }),
        };

        // Start subscriber logic
        if let Some(sub) = &manager.inner.invalidation_subscriber {
            let l1_ref = manager.inner.l1_cache.clone();
            sub.start(move |msg| {
                let l1 = l1_ref.clone();
                async move {
                    match msg {
                        InvalidationMessage::Remove { key } => {
                            let _ = l1.remove(&key).await;
                        }
                        InvalidationMessage::Update {
                            key,
                            value,
                            ttl_secs,
                        } => {
                            let ttl =
                                ttl_secs.map_or(Duration::from_secs(300), Duration::from_secs);
                            let _ = l1.set_with_ttl(&key, &value, ttl).await;
                        }
                        InvalidationMessage::RemovePattern { pattern } => {
                            let _ = l1.remove_pattern(&pattern).await;
                        }
                        InvalidationMessage::RemoveBulk { keys } => {
                            for k in keys {
                                let _ = l1.remove(&k).await;
                            }
                        }
                    }
                    Ok(())
                }
            });
        }

        Ok(manager)
    }
}
