//! Benchmarks for serialization and type-safe caching

use anyhow::Result;
use criterion::{
    criterion_group, criterion_main, measurement::WallTime, BenchmarkGroup, BenchmarkId, Criterion,
};
use multi_tier_cache::{CacheStrategy, CacheSystem, CacheSystemBuilder};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::hint::black_box;
use std::time::Duration;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct User {
    id: u64,
    name: String,
    email: String,
}

impl User {
    fn new(id: u64) -> Self {
        Self {
            id,
            name: format!("User {id}"),
            email: format!("user{id}@example.com"),
        }
    }
}

fn setup_cache() -> Result<(CacheSystem, Runtime)> {
    let rt = Runtime::new()?;
    let cache = rt.block_on(async {
        std::env::set_var("REDIS_URL", "redis://127.0.0.1:6379");
        CacheSystem::new().await
    })?;
    Ok((cache, rt))
}

/// Benchmark JSON vs typed caching
fn bench_json_vs_typed(c: &mut Criterion) {
    let (cache, rt) = match setup_cache() {
        Ok(cache) => cache,
        Err(error) => {
            eprintln!("failed to set up cache: {error}");
            return;
        }
    };

    let mut group = c.benchmark_group("serialization");

    bench_json_cache(&mut group, &cache, &rt);
    bench_typed_cache(&mut group, &cache, &rt);

    #[cfg(feature = "codec-sonic-rs")]
    bench_sonic_cache(&mut group, &rt);

    group.finish();
}

fn bench_json_cache(group: &mut BenchmarkGroup<'_, WallTime>, cache: &CacheSystem, rt: &Runtime) {
    group.bench_function("json_cache", |b| {
        b.iter(|| {
            rt.block_on(async {
                let key = format!("bench:json:{}", rand::random::<u32>());
                let user = json!({
                    "id": 123,
                    "name": "Test User",
                    "email": "test@example.com"
                });

                if let Err(error) = cache
                    .cache_manager()
                    .set_with_strategy(&key, &user, CacheStrategy::ShortTerm)
                    .await
                {
                    eprintln!("Failed to set cache: {error}");
                    return;
                }

                black_box(
                    cache
                        .cache_manager()
                        .get::<serde_json::Value>(&key)
                        .await
                        .unwrap_or_else(|error| {
                            eprintln!("Failed to get cache: {error}");
                            None
                        }),
                );
            });
        });
    });
}

fn bench_typed_cache(group: &mut BenchmarkGroup<'_, WallTime>, cache: &CacheSystem, rt: &Runtime) {
    group.bench_function("typed_cache", |b| {
        b.iter(|| {
            rt.block_on(async {
                let key = format!("bench:typed:{}", rand::random::<u32>());
                let user = User::new(123);

                if let Err(error) = cache
                    .cache_manager()
                    .get_or_compute(&key, CacheStrategy::ShortTerm, || {
                        let u = user.clone();
                        async move { Ok(u) }
                    })
                    .await
                {
                    eprintln!("Failed to set cache: {error}");
                    return;
                }

                black_box(
                    cache
                        .cache_manager()
                        .get_or_compute::<User, _, _>(&key, CacheStrategy::ShortTerm, || {
                            let u = user.clone();
                            async move { Ok(u) }
                        })
                        .await
                        .unwrap_or_else(|error| {
                            eprintln!("Failed to get cache: {error}");
                            user.clone()
                        }),
                );
            });
        });
    });
}

#[cfg(feature = "codec-sonic-rs")]
fn bench_sonic_cache(group: &mut BenchmarkGroup<'_, WallTime>, rt: &Runtime) {
    use multi_tier_cache::codecs::SonicRsCodec;

    group.bench_function("sonic_cache", |b| {
        let sonic_cache = match rt.block_on(async {
            CacheSystemBuilder::new()
                .with_codec(SonicRsCodec::new())
                .build()
                .await
        }) {
            Ok(cache) => cache,
            Err(error) => {
                eprintln!("failed to build sonic cache: {error}");
                return;
            }
        };

        b.iter(|| {
            rt.block_on(async {
                let key = format!("bench:sonic:{}", rand::random::<u32>());
                let user = User::new(123);

                if let Err(error) = sonic_cache
                    .cache_manager()
                    .get_or_compute(&key, CacheStrategy::ShortTerm, || {
                        let u = user.clone();
                        async move { Ok(u) }
                    })
                    .await
                {
                    eprintln!("Failed to set cache: {error}");
                    return;
                }

                black_box(
                    sonic_cache
                        .cache_manager()
                        .get_or_compute::<User, _, _>(&key, CacheStrategy::ShortTerm, || {
                            let u = user.clone();
                            async move { Ok(u) }
                        })
                        .await
                        .unwrap_or_else(|error| {
                            eprintln!("Failed to get cache: {error}");
                            user.clone()
                        }),
                );
            });
        });
    });
}

/// Benchmark different data sizes
fn bench_data_sizes(c: &mut Criterion) {
    let (cache, rt) = match setup_cache() {
        Ok(cache) => cache,
        Err(error) => {
            eprintln!("failed to set up cache: {error}");
            return;
        }
    };

    let mut group = c.benchmark_group("data_size");
    group.measurement_time(Duration::from_secs(10));

    for size in &[100, 1024, 10240] {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            b.iter(|| {
                rt.block_on(async {
                    let key = format!("bench:size:{}", rand::random::<u32>());
                    let data = json!({"data": "x".repeat(size)});

                    if let Err(error) = cache
                        .cache_manager()
                        .set_with_strategy(&key, &data, CacheStrategy::ShortTerm)
                        .await
                    {
                        eprintln!("Failed to set cache: {error}");
                        return;
                    }

                    black_box(
                        cache
                            .cache_manager()
                            .get::<serde_json::Value>(&key)
                            .await
                            .unwrap_or_else(|error| {
                                eprintln!("Failed to get cache: {error}");
                                None
                            }),
                    );
                });
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_json_vs_typed, bench_data_sizes);
criterion_main!(benches);
