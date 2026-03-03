#![allow(clippy::unwrap_used)]
#![allow(clippy::expect_used)]

use anyhow::Result;
use multi_tier_cache::{CacheStrategy, CacheSystem};
use serde::{Deserialize, Serialize};

#[cfg(feature = "codec-sonic-rs")]
use multi_tier_cache::codecs::SonicRsCodec;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestData {
    id: u32,
    name: String,
    items: Vec<String>,
}

#[cfg(feature = "codec-sonic-rs")]
#[tokio::test]
async fn test_sonic_codec_operations() -> Result<()> {
    // 1. Setup cache with Sonic codec
    let cache = CacheSystem::with_codec(SonicRsCodec::new())?;
    let manager = cache.cache_manager();

    // 2. Prepare test data
    let key = "sonic:test:1";
    let data = TestData {
        id: 42,
        name: "Sonic Speed".to_string(),
        items: vec!["ring".to_string(), "emerald".to_string()],
    };

    // 3. Set data
    manager
        .set_with_strategy(key, &data, CacheStrategy::ShortTerm)
        .await?;

    // 4. Get data (L1 hit)
    let retrieved: Option<TestData> = manager.get(key).await?;
    assert_eq!(retrieved, Some(data.clone()), "L1 get failed");

    // 5. Invalidate and check miss
    manager.invalidate(key).await?;
    let miss: Option<TestData> = manager.get(key).await?;
    assert!(miss.is_none(), "Should be miss after invalidation");

    // 6. Get or compute
    let computed = manager
        .get_or_compute(key, CacheStrategy::ShortTerm, || async { Ok(data.clone()) })
        .await?;
    assert_eq!(computed, data, "Compute failed");

    Ok(())
}
