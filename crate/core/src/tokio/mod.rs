//! Adapters between `tokio_util::codec::Decoder` and this crate's codecs.
//!
//! - [`TokioDecoderCodec`] runs a `Decoder` as a [`BoundaryAwareCodec`](crate::BoundaryAwareCodec).
//! - [`TokioReader`] reads the objects of a `Decoder` from a [`Source`](crate::Source).

mod decoder_to_codec;
mod read_error;
mod reader;
#[cfg(test)]
mod samples;
#[cfg(test)]
mod test_support;

pub use decoder_to_codec::TokioDecoderCodec;
pub use read_error::ReadError;
pub use reader::TokioReader;
