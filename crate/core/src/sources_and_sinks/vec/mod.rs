//! `Vec<u8>` backend: adapts an owned vector into the driver's
//! [`Source`](crate::Source)/[`Sink`](crate::Sink) traits. Fully
//! in-memory, so there's no `std::io`/`embedded_io`-style wrapper
//! to build on top — [`stream_to_stream`](crate::stream_to_stream) is
//! the entry point.

mod adapter;

pub use adapter::{VecSource, VecSink};

#[cfg(feature = "alloc")]
use crate::{stream_to_stream, Codec, DriveError, Source};

/// Error from [`to_string`]: the source failed, the codec failed, the
/// pump stalled without ending the stream, or the collected bytes
/// weren't valid UTF-8.
#[cfg(feature = "alloc")]
#[derive(Debug)]
pub enum ToStringError<EI> {
    Source(EI),
    Codec(crate::Error),
    NoProgress,
    Utf8(alloc::string::FromUtf8Error),
}

/// Run `codec` over `input`, collecting the result into a `String`.
///
/// A convenience combinator over [`VecSink`]/[`stream_to_stream`] for
/// any [`Source`] — not just [`VecSource`] — whose codec output is
/// text.
#[cfg(feature = "alloc")]
pub fn to_string<I: Source>(
    codec: impl Codec,
    mut input: I,
) -> Result<alloc::string::String, ToStringError<I::Error>> {
    let mut sink = VecSink::default();
    stream_to_stream(&mut input, codec, &mut sink).map_err(|error| match error {
        DriveError::Source(error) => ToStringError::Source(error),
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => ToStringError::Codec(error),
        DriveError::NoProgress => ToStringError::NoProgress,
        // VecSink's spare capacity always grows to fit; it can never
        // decline to offer any.
        DriveError::SinkExhausted => unreachable!("VecSink always has spare capacity"),
    })?;
    alloc::string::String::from_utf8(sink.into_inner()).map_err(ToStringError::Utf8)
}
