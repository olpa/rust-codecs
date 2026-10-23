//! Adapters for `std::io` backend.
//!
//! Behind the `std` feature flag.
//!
//! The wrappers return every failure as an `io::Error`. Its payload is a
//! [`StreamError`](crate::StreamError).

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};

pub use super::shared_io::CodecWriterFinishError;
