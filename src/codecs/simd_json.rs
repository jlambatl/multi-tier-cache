//! SIMD-JSON Codec
//!
//! High-performance JSON serialization using `simd-json`.

use crate::traits::CacheCodec;
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

/// SIMD-JSON codec
#[derive(Debug, Clone, Default)]
pub struct SimdJsonCodec;

impl SimdJsonCodec {
    /// Create a new SIMD-JSON codec
    pub fn new() -> Self {
        Self
    }
}

impl CacheCodec for SimdJsonCodec {
    fn serialize<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>> {
        simd_json::to_vec(value).map_err(|e| anyhow::anyhow!("SimdJson serialization failed: {e}"))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        // simd_json modifies the buffer in place for performance
        let mut bytes_copy = bytes.to_vec();
        simd_json::from_slice(&mut bytes_copy).map_err(|e| anyhow::anyhow!("SimdJson deserialization failed: {e}"))
    }

    fn name(&self) -> &'static str {
        "simd-json"
    }
}
