mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, EmbeddedSink, EmbeddedSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter, EmbeddedError};
