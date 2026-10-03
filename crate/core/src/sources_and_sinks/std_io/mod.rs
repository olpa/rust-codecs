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
//! The payload is the original [`Error`](crate::Error) or
//! [`StallError`](crate::StallError). To get it back, use
//! `io_error.get_ref()`, then `downcast_ref::<Error>()` or
//! `downcast_ref::<StallError>()`.

mod adapter;
mod wrapper;

pub use adapter::{BufReadSource, StdSink, StdSource};
pub use wrapper::{BufReadCodecReader, CodecReader, CodecWriter};
