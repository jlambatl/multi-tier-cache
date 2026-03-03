//! Cache System Builder
//!
//! Provides a flexible builder pattern for constructing `CacheSystem` with custom backends.

use crate::backends::MokaCacheConfig;
use crate::codecs::JsonCodec;
use crate::traits::{CacheBackend, CacheCodec, L2CacheBackend, StreamingBackend};
use crate::{CacheManager, CacheSystem, CacheTier, L1Cache, L2Cache, TierConfig};
use anyhow::Result;
use std::sync::Arc;
use tracing::info;

/// Builder for constructing `CacheSystem` with custom backends
pub struct CacheSystemBuilder<C: CacheCodec = JsonCodec> {
    // Legacy 2-tier configuration
    l1_backend: Option<Arc<dyn CacheBackend>>,
    l2_backend: Option<Arc<dyn L2CacheBackend>>,

    streaming_backend: Option<Arc<dyn StreamingBackend>>,
    moka_config: Option<MokaCacheConfig>,

    // Multi-tier configuration
    tiers: Vec<(Arc<dyn L2CacheBackend>, TierConfig)>,
    
    // Codec
    codec: C,
}

impl CacheSystemBuilder<JsonCodec> {
    /// Create a new builder
    #[must_use]
    pub fn new() -> Self {
        Self {
            l1_backend: None,
            l2_backend: None,
            streaming_backend: None,
            moka_config: None,
            tiers: Vec::new(),
            codec: JsonCodec::new(),
        }
    }
}

impl<C: CacheCodec + Clone + 'static> CacheSystemBuilder<C> {
    /// Configure custom codec
    #[must_use]
    pub fn with_codec<NewC: CacheCodec + Clone + 'static>(self, codec: NewC) -> CacheSystemBuilder<NewC> {
        CacheSystemBuilder {
            l1_backend: self.l1_backend,
            l2_backend: self.l2_backend,
            streaming_backend: self.streaming_backend,
            moka_config: self.moka_config,
            tiers: self.tiers,
            codec,
        }
    }

    /// Configure a custom L1 (in-memory) cache backend
    #[must_use]
    pub fn with_l1(mut self, backend: Arc<dyn CacheBackend>) -> Self {
        self.l1_backend = Some(backend);
        self
    }

    /// Configure custom configuration for default L1 (Moka) backend
    #[must_use]
    pub fn with_moka_config(mut self, config: MokaCacheConfig) -> Self {
        self.moka_config = Some(config);
        self
    }

    /// Configure a custom L2 (distributed) cache backend
    #[must_use]
    pub fn with_l2(mut self, backend: Arc<dyn L2CacheBackend>) -> Self {
        self.l2_backend = Some(backend);
        self
    }

    /// Configure a custom streaming backend
    #[must_use]
    pub fn with_streams(mut self, backend: Arc<dyn StreamingBackend>) -> Self {
        self.streaming_backend = Some(backend);
        self
    }

    /// Configure a cache tier with custom settings (v0.5.0+)
    #[must_use]
    pub fn with_tier(mut self, backend: Arc<dyn L2CacheBackend>, config: TierConfig) -> Self {
        self.tiers.push((backend, config));
        self
    }

    /// Convenience method to add L3 cache tier
    #[must_use]
    pub fn with_l3(mut self, backend: Arc<dyn L2CacheBackend>) -> Self {
        self.tiers.push((backend, TierConfig::as_l3()));
        self
    }

    /// Convenience method to add L4 cache tier
    #[must_use]
    pub fn with_l4(mut self, backend: Arc<dyn L2CacheBackend>) -> Self {
        self.tiers.push((backend, TierConfig::as_l4()));
        self
    }

    /// Build the `CacheSystem` with configured or default backends
    pub async fn build(self) -> Result<CacheSystem<C>> {
        info!("Building Multi-Tier Cache System (Codec: {})", self.codec.name());

        // Multi-tier mode
        if !self.tiers.is_empty() {
            info!(tier_count = self.tiers.len(), "Initializing multi-tier architecture");

            let mut tiers = self.tiers;
            tiers.sort_by_key(|(_, config)| config.tier_level);

            let cache_tiers: Vec<CacheTier> = tiers
                .into_iter()
                .map(|(backend, config)| {
                    CacheTier::new(
                        backend,
                        config.tier_level,
                        config.promotion_enabled,
                        config.ttl_scale,
                    )
                })
                .collect();

            // Note: CacheManager constructors now take ownership of codec if not Copy, or clone. 
            // CacheManager::with_tiers_and_codec takes C.
            let cache_manager = CacheManager::with_tiers_and_codec(
                cache_tiers,
                self.streaming_backend,
                self.codec,
            )?;

            info!("Multi-Tier Cache System built successfully");

            return Ok(CacheSystem {
                cache_manager: Arc::new(cache_manager),
                l1_cache: None,
                l2_cache: None,
            });
        }

        // LEGACY: 2-tier mode
        if self.l1_backend.is_none() && self.l2_backend.is_none() {
            info!("Initializing default backends (Moka + Redis)");

            let l1_cache = Arc::new(L1Cache::new(self.moka_config.unwrap_or_default())?);
            let l2_cache = Arc::new(L2Cache::new().await?);

            // We need to use with_codec because new() assumes JsonCodec.
            // If C is JsonCodec, we could use new(), but generic C prevents that easily.
            // We'll construct backends and use with_codec.
            let l1_backend: Arc<dyn CacheBackend> = l1_cache.clone();
            let l2_backend: Arc<dyn L2CacheBackend> = l2_cache.clone();
            
            // Check if L2 implements StreamingBackend (RedisCache does)
            // But we don't have upcasting.
            // We'll use RedisStreams if we built default L2.
            let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
            let redis_streams = crate::redis_streams::RedisStreams::new(&redis_url).await?;
            let streaming_backend: Arc<dyn StreamingBackend> = Arc::new(redis_streams);

            let cache_manager = CacheManager::with_codec(
                l1_backend,
                l2_backend,
                Some(streaming_backend),
                self.codec,
            )?;

            info!("Multi-Tier Cache System built successfully");

            Ok(CacheSystem {
                cache_manager: Arc::new(cache_manager),
                l1_cache: Some(l1_cache),
                l2_cache: Some(l2_cache),
            })
        } else {
            info!("Building with custom backends");

            let l1_backend: Arc<dyn CacheBackend> = if let Some(backend) = self.l1_backend {
                info!(backend = %backend.name(), "Using custom L1 backend");
                backend
            } else {
                info!("Using default L1 backend (Moka)");
                let config = self.moka_config.unwrap_or_default();
                Arc::new(L1Cache::new(config)?)
            };

            let l2_backend: Arc<dyn L2CacheBackend> = if let Some(backend) = self.l2_backend {
                info!(backend = %backend.name(), "Using custom L2 backend");
                backend
            } else {
                info!("Using default L2 backend (Redis)");
                Arc::new(L2Cache::new().await?)
            };

            let cache_manager = CacheManager::with_codec(
                l1_backend,
                l2_backend,
                self.streaming_backend,
                self.codec,
            )?;

            info!("Multi-Tier Cache System built with custom backends");

            Ok(CacheSystem {
                cache_manager: Arc::new(cache_manager),
                l1_cache: None,
                l2_cache: None,
            })
        }
    }
}

impl Default for CacheSystemBuilder<JsonCodec> {
    fn default() -> Self {
        Self::new()
    }
}
