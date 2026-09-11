//! Adapters for `embedded_io` backend.
//!
//! Behind the `embedded-io` feature flag.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, EmbeddedSink, EmbeddedSource, WriteError};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter, EmbeddedError};
