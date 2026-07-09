//! Rust twin of the Python `codecs` stream-wrapping reference (`../python-ref`).
//!
//! Python's `codecs` module wraps an existing stream to get a new one that
//! encodes/decodes on the fly. The equivalents here:
//!
//! | Python                          | Rust twin                          |
//! |---------------------------------|------------------------------------|
//! | `codecs.IncrementalDecoder` / `IncrementalEncoder` | the [`Codec`] trait |
//! | the registered `"my-rot13"` codec | the explicit [`Rot13`] value      |
//! | `codecs.getreader(enc)(stream)` | [`StreamReader::new(stream, codec)`] |
//! | `codecs.getwriter(enc)(stream)` | [`StreamWriter::new(stream, codec)`] |
//!
//! There is no codec registry: instead of looking a codec up by name, the
//! caller constructs one explicitly and hands it to the wrapper.

mod codec;
mod rot13;
mod stream;

pub use codec::Codec;
pub use rot13::Rot13;
pub use stream::{StreamReader, StreamWriter};
