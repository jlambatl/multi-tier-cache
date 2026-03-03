//! Cache Codecs
//!
//! This module defines the serialization codecs used by the cache system.
//! Codecs determine how data is converted to bytes for storage in the backend.

pub mod json;

#[cfg(feature = "codec-postcard")]
pub mod postcard;

#[cfg(feature = "codec-simd-json")]
pub mod simd_json;

#[cfg(feature = "codec-sonic-rs")]
pub mod sonic_rs;

pub use json::JsonCodec;

#[cfg(feature = "codec-postcard")]
pub use postcard::PostcardCodec;

#[cfg(feature = "codec-simd-json")]
pub use simd_json::SimdJsonCodec;

#[cfg(feature = "codec-sonic-rs")]
pub use sonic_rs::SonicRsCodec;
