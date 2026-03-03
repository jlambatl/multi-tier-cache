//! Sonic-RS Codec
//!
//! SIMD-accelerated JSON serialization using `sonic-rs`.

use crate::traits::CacheCodec;
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

/// Sonic-RS JSON codec
#[derive(Debug, Clone, Default)]
pub struct SonicRsCodec;

impl SonicRsCodec {
    /// Create a new Sonic-RS codec
    pub fn new() -> Self {
        Self
    }
}

impl CacheCodec for SonicRsCodec {
    fn serialize<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>> {
        sonic_rs::to_vec(value).map_err(|e| anyhow::anyhow!("SonicRs serialization failed: {e}"))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        sonic_rs::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("SonicRs deserialization failed: {e}"))
    }

    fn name(&self) -> &'static str {
        "sonic-rs"
    }
}
