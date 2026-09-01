//! Adapters for `std::io` backend.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
