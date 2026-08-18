//! Concrete [`Codec`](crate::Codec) implementations, one per feature.

pub mod identity;

#[cfg(any(feature = "rot13", test))]
pub mod rot13;

#[cfg(feature = "base64")]
pub mod base64;

#[cfg(feature = "json")]
pub mod json;
