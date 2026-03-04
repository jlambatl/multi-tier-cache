#![allow(clippy::unwrap_used)]

use multi_tier_cache::{CacheStrategy, CacheSystem, CacheSystemBuilder};
use serde::{Deserialize, Serialize};
use std::env;
use tokio::runtime::Runtime;

#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestData64k {
    id: u64,
    key: String,
    payload: Vec<u8>,
}

impl TestData64k {
    fn new(id: u64, key_suffix: u64) -> Self {
        Self {
            id,
            key: format!("key_{key_suffix:032}"),
            payload: vec![0xAB; 65536 - 128],
        }
    }
}

#[cfg(feature = "codec-sonic-rs")]
fn run_sonic_profile() -> anyhow::Result<()> {
    use multi_tier_cache::codecs::SonicRsCodec;
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let cache = CacheSystemBuilder::new()
            .with_codec(SonicRsCodec::new())
            .build()
            .await?;
        run_workload(&cache).await
    })
}

#[cfg(feature = "codec-postcard")]
fn run_postcard_profile() -> anyhow::Result<()> {
    use multi_tier_cache::codecs::PostcardCodec;
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let cache = CacheSystemBuilder::new()
            .with_codec(PostcardCodec::new())
            .build()
            .await?;
        run_workload(&cache).await
    })
}

fn run_json_profile() -> anyhow::Result<()> {
    let runtime = Runtime::new()?;
    runtime.block_on(async {
        let cache = CacheSystem::new().await?;
        run_workload(&cache).await
    })
}

async fn run_workload<C>(cache: &CacheSystem<C>) -> anyhow::Result<()>
where
    C: multi_tier_cache::traits::CacheCodec + Clone + 'static,
{
    let manager = cache.cache_manager();

    let request_count: u64 = 10_000;
    let key_space: u64 = 1_000;

    for i in 0..key_space {
        let value = TestData64k::new(i, i);
        manager
            .set_with_strategy(&value.key, &value, CacheStrategy::Default)
            .await?;
    }

    for i in 0..request_count {
        let key = format!("key_{:032}", i % key_space);
        let _ = manager.get::<TestData64k>(&key).await?;
    }

    Ok(())
}

fn main() {
    let codec_name = env::var("CODEC").unwrap_or_else(|_| "json".to_string());

    // Set output file based on codec
    env::set_var(
        "DHAT_OUT_FILE",
        format!("dhat-heap-{}.json", codec_name.to_lowercase()),
    );

    let _profiler = dhat::Profiler::new_heap();

    let result = match codec_name.to_lowercase().as_str() {
        #[cfg(feature = "codec-sonic-rs")]
        "sonic" | "sonic-rs" => run_sonic_profile(),
        #[cfg(feature = "codec-postcard")]
        "postcard" => run_postcard_profile(),
        "json" => run_json_profile(),
        _ => {
            eprintln!("Unknown codec: {codec_name}. Supported: json, sonic-rs, postcard");
            std::process::exit(1);
        }
    };

    if let Err(error) = result {
        eprintln!("heap profiling workload failed: {error}");
        std::process::exit(1);
    }
}
