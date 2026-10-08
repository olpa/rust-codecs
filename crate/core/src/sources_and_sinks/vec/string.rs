//! Convenience combinators for running a codec over a borrowed
//! string, collecting the result into an in-memory `Vec<u8>`/`String`.

use core::convert::Infallible;

use super::VecSink;
use crate::{stream_to_stream, BoundaryAwareCodec, DriveError, DriveErrorKind};

/// Everything that can go wrong in [`encode_str`]/[`encode_string`].
#[derive(Debug)]
pub enum EncodeError {
    Codec(crate::Error),
    NoProgress,
    Utf8(alloc::string::FromUtf8Error),
}

fn from_drive_error(error: DriveError<Infallible, Infallible>) -> EncodeError {
    match error.kind {
        DriveErrorKind::Source(never) | DriveErrorKind::Sink(never) => match never {},
        DriveErrorKind::Codec(error) => EncodeError::Codec(error),
        DriveErrorKind::NoProgress => EncodeError::NoProgress,
        // VecSink's spare capacity always grows to fit; it can
        // never decline to offer any.
        DriveErrorKind::SinkExhausted => unreachable!("VecSink always has spare capacity"),
    }
}

impl core::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Codec(_) => f.write_str("codec error"),
            Self::NoProgress => f.write_str("no progress on input or output"),
            Self::Utf8(_) => f.write_str("output is not UTF-8"),
        }
    }
}

impl core::error::Error for EncodeError {
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Codec(error) => Some(error),
            Self::NoProgress => None,
            Self::Utf8(error) => Some(error),
        }
    }
}

impl From<alloc::string::FromUtf8Error> for EncodeError {
    fn from(error: alloc::string::FromUtf8Error) -> Self {
        Self::Utf8(error)
    }
}

/// Run `codec` over a `str`, collecting the result into a `Vec<u8>`.
///
/// A convenience combinator over
/// [`crate::sources_and_sinks::slice::SliceSource`]/[`VecSink`]/
/// [`stream_to_stream`].
///
/// Behind the `alloc` feature flag.
pub fn encode_str(
    codec: impl BoundaryAwareCodec,
    input: impl AsRef<str>,
) -> Result<alloc::vec::Vec<u8>, EncodeError> {
    let input = input.as_ref().as_bytes();
    let mut source = crate::sources_and_sinks::slice::SliceSource::new(input);
    let mut sink = VecSink::new(alloc::vec::Vec::with_capacity(input.len()));
    stream_to_stream(&mut source, codec, &mut sink).map_err(from_drive_error)?;
    Ok(sink.into_inner())
}

/// Run `codec` over a `str`, collecting the result into a `String`.
///
/// Built on [`encode_str`], for codecs whose output is text.
///
/// Behind the `alloc` feature flag.
pub fn encode_string(
    codec: impl BoundaryAwareCodec,
    input: impl AsRef<str>,
) -> Result<alloc::string::String, EncodeError> {
    Ok(alloc::string::String::from_utf8(encode_str(codec, input)?)?)
}
