//! `Vec<u8>` backend: adapts an owned vector into the driver's
//! [`Source`](crate::Source)/[`Sink`](crate::Sink) traits. Fully
//! in-memory, so there's no `std::io`/`embedded_io`-style wrapper
//! to build on top — [`stream_to_stream`] is the entry point.

mod adapter;

pub use adapter::{VecSink, VecSource};

use crate::{stream_to_stream, DriveError, EndCapableCodec};

/// Error from [`encode_str`]/[`encode_string`]: the codec failed, the
/// pump stalled without ending the stream, or (for [`encode_string`])
/// the collected bytes weren't valid UTF-8. Both run entirely over an
/// in-memory source and sink, so there's no source/sink error to
/// report.
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

/// Run `codec` over a borrowed string, collecting the result into a
/// `Vec<u8>`.
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

/// Run `codec` over a borrowed string, collecting the result into a
/// `String`.
///
/// Built on [`encode_str`], for codecs whose output is text.
pub fn encode_string(
    codec: impl EndCapableCodec,
    input: impl AsRef<str>,
) -> Result<alloc::string::String, EncodeError> {
    Ok(alloc::string::String::from_utf8(encode_str(codec, input)?)?)
}
