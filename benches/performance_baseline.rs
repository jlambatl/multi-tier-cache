use anyhow::Result;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use multi_tier_cache::{CacheStrategy, CacheSystem};
use serde::{Deserialize, Serialize};
use tokio::runtime::Runtime;

#[cfg(feature = "codec-sonic-rs")]
use multi_tier_cache::codecs::SonicRsCodec;

use multi_tier_cache::codecs::JsonCodec;

#[cfg(feature = "codec-postcard")]
use multi_tier_cache::codecs::PostcardCodec;

/// Test data structure matching 64kb values on 32-byte keys
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct TestData64k {
    id: u64,
    key: String,      // 32 bytes
    payload: Vec<u8>, // ~64kb
}

impl TestData64k {
    fn new(id: u64, key_suffix: u64) -> Self {
        Self {
            id,
            key: format!("key_{key_suffix:032}"),
            payload: vec![0xAB; 65536 - 128], // ~64kb payload (accounting for other fields)
        }
    }
}

/// Benchmark: JSON codec, get operations with L1 hits (70%+ hit rate)
fn bench_get_json_hits(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("get_json_hits");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(JsonCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        for i in 0..num_keys {
                            let key = format!("key_{i:032}");
                            let _ = manager.get::<TestData64k>(&key).await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: JSON codec, set operations
fn bench_set_json(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("set_json");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(JsonCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Sonic-RS codec, get operations with L1 hits (70%+ hit rate)
#[cfg(feature = "codec-sonic-rs")]
fn bench_get_sonic_hits(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("get_sonic_hits");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(SonicRsCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        for i in 0..num_keys {
                            let key = format!("key_{i:032}");
                            let _ = manager.get::<TestData64k>(&key).await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Sonic-RS codec, set operations
#[cfg(feature = "codec-sonic-rs")]
fn bench_set_sonic(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("set_sonic");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(SonicRsCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Postcard codec, get operations with L1 hits (70%+ hit rate)
#[cfg(feature = "codec-postcard")]
fn bench_get_postcard_hits(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("get_postcard_hits");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(PostcardCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        for i in 0..num_keys {
                            let key = format!("key_{i:032}");
                            let _ = manager.get::<TestData64k>(&key).await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: Postcard codec, set operations
#[cfg(feature = "codec-postcard")]
fn bench_set_postcard(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("set_postcard");

    group.throughput(Throughput::Elements(1));

    for num_keys in &[100_u64, 1000_u64] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_keys),
            num_keys,
            |b, &num_keys| {
                b.iter(|| {
                    let result: Result<()> = rt.block_on(async {
                        let cache = CacheSystem::with_codec(PostcardCodec::new()).await?;
                        let manager = cache.cache_manager();

                        for i in 0..num_keys {
                            let data = TestData64k::new(i, i);
                            manager
                                .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                                .await?;
                        }

                        Ok(())
                    });

                    if let Err(error) = result {
                        eprintln!("benchmark failed: {error}");
                    }
                });
            },
        );
    }

    group.finish();
}

/// Benchmark: End-to-end latency comparison (JSON vs Sonic-RS)
fn bench_end_to_end_latency(c: &mut Criterion) {
    let rt = match Runtime::new() {
        Ok(rt) => rt,
        Err(error) => {
            eprintln!("failed to create runtime: {error}");
            return;
        }
    };
    let mut group = c.benchmark_group("end_to_end_latency");

    // Single operation per benchmark to measure latency accurately
    group.bench_function("json_single_get_64kb", |b| {
        b.iter(|| {
            let result: Result<()> = rt.block_on(async {
                let cache = CacheSystem::with_codec(JsonCodec::new()).await?;
                let manager = cache.cache_manager();

                let data = TestData64k::new(1, 1);
                manager
                    .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                    .await?;

                let _ = manager.get::<TestData64k>(&data.key).await?;

                Ok(())
            });

            if let Err(error) = result {
                eprintln!("benchmark failed: {error}");
            }
        });
    });

    #[cfg(feature = "codec-sonic-rs")]
    group.bench_function("sonic_single_get_64kb", |b| {
        b.iter(|| {
            let result: Result<()> = rt.block_on(async {
                let cache = CacheSystem::with_codec(SonicRsCodec::new()).await?;
                let manager = cache.cache_manager();

                let data = TestData64k::new(1, 1);
                manager
                    .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                    .await?;

                let _ = manager.get::<TestData64k>(&data.key).await?;

                Ok(())
            });

            if let Err(error) = result {
                eprintln!("benchmark failed: {error}");
            }
        });
    });

    #[cfg(feature = "codec-postcard")]
    group.bench_function("postcard_single_get_64kb", |b| {
        b.iter(|| {
            let result: Result<()> = rt.block_on(async {
                let cache = CacheSystem::with_codec(PostcardCodec::new()).await?;
                let manager = cache.cache_manager();

                let data = TestData64k::new(1, 1);
                manager
                    .set_with_strategy(&data.key, &data, CacheStrategy::Default)
                    .await?;

                let _ = manager.get::<TestData64k>(&data.key).await?;

                Ok(())
            });

            if let Err(error) = result {
                eprintln!("benchmark failed: {error}");
            }
        });
    });

    group.finish();
}

#[cfg(all(feature = "codec-sonic-rs", feature = "codec-postcard"))]
criterion_group!(
    benches,
    bench_get_json_hits,
    bench_set_json,
    bench_get_sonic_hits,
    bench_set_sonic,
    bench_get_postcard_hits,
    bench_set_postcard,
    bench_end_to_end_latency
);

#[cfg(all(feature = "codec-sonic-rs", not(feature = "codec-postcard")))]
criterion_group!(
    benches,
    bench_get_json_hits,
    bench_set_json,
    bench_get_sonic_hits,
    bench_set_sonic,
    bench_end_to_end_latency
);

#[cfg(all(not(feature = "codec-sonic-rs"), feature = "codec-postcard"))]
criterion_group!(
    benches,
    bench_get_json_hits,
    bench_set_json,
    bench_get_postcard_hits,
    bench_set_postcard,
    bench_end_to_end_latency
);

#[cfg(all(not(feature = "codec-sonic-rs"), not(feature = "codec-postcard")))]
criterion_group!(
    benches,
    bench_get_json_hits,
    bench_set_json,
    bench_end_to_end_latency
);

criterion_main!(benches);
