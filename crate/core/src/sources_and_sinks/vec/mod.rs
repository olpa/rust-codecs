//! Adapters for `Vec<u8>` backend, and to-string helpers.
//!
//! Behind the `alloc` feature flag.

mod adapter;
mod string;

pub use adapter::{VecSink, VecSource};
pub use string::{encode_str, encode_string, EncodeError};
