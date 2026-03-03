//! JSON Codec
//!
//! The default codec using `serde_json` for serialization.

use crate::traits::CacheCodec;
use anyhow::Result;
use serde::de::DeserializeOwned;
use serde::Serialize;

/// Default JSON codec using `serde_json`
#[derive(Debug, Clone, Default)]
pub struct JsonCodec;

impl JsonCodec {
    /// Create a new JSON codec
    pub fn new() -> Self {
        Self
    }
}

impl CacheCodec for JsonCodec {
    fn serialize<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>> {
        serde_json::to_vec(value).map_err(|e| anyhow::anyhow!("JSON serialization failed: {e}"))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes)
            .map_err(|e| anyhow::anyhow!("JSON deserialization failed: {e}"))
    }

    fn name(&self) -> &'static str {
        "json"
    }
}
