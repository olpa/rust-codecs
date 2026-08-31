//! Adapters for `embedded_io` backends.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, EmbeddedSink, EmbeddedSource, WriteError};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter, EmbeddedError};
