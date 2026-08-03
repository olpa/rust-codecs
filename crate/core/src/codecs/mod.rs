//! Concrete [`Codec`](crate::Codec) implementations, one per feature.

#[cfg(feature = "identity")]
pub mod identity;

#[cfg(feature = "rot13")]
pub mod rot13;

#[cfg(feature = "base64")]
pub mod base64;

#[cfg(feature = "json")]
pub mod json;
