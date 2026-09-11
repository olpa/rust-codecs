//! Adapters for `std::io` backend.
//!
//! Behind the `std` feature flag.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
