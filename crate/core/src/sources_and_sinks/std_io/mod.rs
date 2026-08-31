//! Adapters for `std::io` backends.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
