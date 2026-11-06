//! Adapters between `tokio_util::codec::Decoder` and this crate's codecs.
//!
//! - [`TokioDecoderAsBoundaryAwareCodec`] runs a `Decoder` as a [`BoundaryAwareCodec`](crate::BoundaryAwareCodec).
//! - [`TokioReader`] reads the objects of a `Decoder` from a [`Source`](crate::Source).
//!
//! The other direction runs a codec of this crate as a tokio codec:
//! - [`CodecAsTokioDecoder`] runs a [`Codec`](crate::Codec) as a `Decoder`.
//! - [`BoundaryAwareCodecAsTokioDecoder`] runs a
//!   [`BoundaryAwareCodec`](crate::BoundaryAwareCodec) as a `Decoder`.
//! - [`CodecAsTokioEncoder`] runs a [`Codec`](crate::Codec) as an `Encoder`.
//!
//! Why two decoders: the rule for an item is different. For a plain
//! codec, an item is a chunk of output, and the client gets it as soon
//! as there is output. For a boundary-aware codec, an item is the
//! output of one stream, and the client gets it at the boundary.
//!
//! The encoder accepts only a plain codec. A boundary is a marker in
//! the input of a self-delimiting format. That fits a decoder, which
//! cuts a byte stream into items. The input of an encoder is a payload,
//! and the edges of the items already mark the streams.
//!
//! Tokio does not tell an `Encoder` that the stream is over. So the
//! client sends [`Finish`] as the last item. The encoder then calls
//! `finish` of the codec.
//!
//! All adapters use `std::io::Error` as the error type, like
//! `BytesCodec` of tokio-util. The crate [`Error`](crate::Error) is its
//! source, so a client can get it back with `downcast_ref`. The kind
//! maps as `CorruptStream` to `InvalidData`, `UnexpectedEnd` to
//! `UnexpectedEof` and the other kinds to `Other`.

mod boundary_decoder;
mod codec_decoder;
mod codec_encoder;
mod codec_support;
mod decoder_to_codec;
mod encoder_to_codec;
mod read_error;
mod reader;
#[cfg(test)]
mod samples;
#[cfg(test)]
mod test_support;

pub use boundary_decoder::BoundaryAwareCodecAsTokioDecoder;
pub use codec_decoder::CodecAsTokioDecoder;
pub use codec_encoder::{CodecAsTokioEncoder, Finish};
pub use decoder_to_codec::TokioDecoderAsBoundaryAwareCodec;
pub use encoder_to_codec::TokioEncoderAsBoundaryAwareCodec;
pub use read_error::ReadError;
pub use reader::TokioReader;
