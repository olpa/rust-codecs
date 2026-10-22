//! Adapters for `std::io` backend.
//!
//! Behind the `std` feature flag.
//!
//! The wrappers return a codec failure or a stalled transfer as an
//! `io::Error`. Its kind is:
//!
//! - `InvalidData` for [`ErrorKind::CorruptStream`](crate::ErrorKind::CorruptStream)
//! - `UnexpectedEof` for [`ErrorKind::UnexpectedEnd`](crate::ErrorKind::UnexpectedEnd)
//! - `WriteZero` for [`StallError::SinkExhausted`](crate::StallError::SinkExhausted)
//! - `Other` for the other codec kinds and for
//!   [`StallError::NoProgress`](crate::StallError::NoProgress). They mean
//!   a bug in the codec or in an adapter, not bad data.
//!
//! The payload of a codec failure is a [`StreamError`](crate::StreamError).
//! Its `error` field has the original [`Error`](crate::Error). Its `moved`
//! field counts the bytes that the whole stream moved before the failure.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};

pub use super::shared_io::CodecWriterFinishError;
