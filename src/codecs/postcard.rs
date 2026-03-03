//! Postcard Codec (Binary)
//!
//! Compact binary serialization using `postcard`.

use crate::traits::CacheCodec;
use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

/// Postcard binary codec
#[derive(Debug, Clone, Default)]
pub struct PostcardCodec;

impl PostcardCodec {
    /// Create a new Postcard codec
    pub fn new() -> Self {
        Self
    }
}

impl CacheCodec for PostcardCodec {
    fn serialize<T: Serialize + ?Sized>(&self, value: &T) -> Result<Vec<u8>> {
        postcard::to_allocvec(value).map_err(|e| anyhow::anyhow!("Postcard serialization failed: {e}"))
    }

    fn deserialize<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<T> {
        postcard::from_bytes(bytes).map_err(|e| anyhow::anyhow!("Postcard deserialization failed: {e}"))
    }

    fn name(&self) -> &'static str {
        "postcard"
    }
}
