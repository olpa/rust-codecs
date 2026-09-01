//! Convenience combinators for running a codec over a borrowed
//! string, collecting the result into an in-memory `Vec<u8>`/`String`.

use super::VecSink;
use crate::{stream_to_stream, DriveError, EndCapableCodec};

/// Everything that can go wrong in [`encode_str`]/[`encode_string`].
#[derive(Debug)]
pub enum EncodeError {
    Codec(crate::Error),
    NoProgress,
    Utf8(alloc::string::FromUtf8Error),
}

impl<EI, EO> From<DriveError<EI, EO>> for EncodeError {
    fn from(error: DriveError<EI, EO>) -> Self {
        match error {
            DriveError::Source(_) | DriveError::Sink(_) => {
                unreachable!("in-memory source/sink errors are Infallible")
            }
            DriveError::Codec(error) => Self::Codec(error),
            DriveError::NoProgress => Self::NoProgress,
            // VecSink's spare capacity always grows to fit; it can
            // never decline to offer any.
            DriveError::SinkExhausted => unreachable!("VecSink always has spare capacity"),
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
pub fn encode_str(
    codec: impl EndCapableCodec,
    input: impl AsRef<str>,
) -> Result<alloc::vec::Vec<u8>, EncodeError> {
    let input = input.as_ref().as_bytes();
    let mut source = crate::sources_and_sinks::slice::SliceSource::new(input);
    let mut sink = VecSink::new(alloc::vec::Vec::with_capacity(input.len()));
    stream_to_stream(&mut source, codec, &mut sink)?;
    Ok(sink.into_inner())
}

/// Run `codec` over a `str`, collecting the result into a `String`.
///
/// Built on [`encode_str`], for codecs whose output is text.
pub fn encode_string(
    codec: impl EndCapableCodec,
    input: impl AsRef<str>,
) -> Result<alloc::string::String, EncodeError> {
    Ok(alloc::string::String::from_utf8(encode_str(codec, input)?)?)
}
