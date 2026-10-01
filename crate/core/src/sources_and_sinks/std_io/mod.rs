//! Adapters for `std::io` backend.
//!
//! Behind the `std` feature flag.
//!
//! The wrappers return a codec failure as an `io::Error`. Its kind is:
//!
//! - `InvalidData` for [`ErrorKind::CorruptStream`](crate::ErrorKind::CorruptStream)
//! - `UnexpectedEof` for [`ErrorKind::UnexpectedEnd`](crate::ErrorKind::UnexpectedEnd)
//! - `Other` for the other kinds. They mean a codec bug, not bad data.
//!
//! Its payload is the codec's [`Error`](crate::Error). To get it back,
//! use `io_error.get_ref()` and
//! `downcast_ref::<rust_codecs_core::Error>()`.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
