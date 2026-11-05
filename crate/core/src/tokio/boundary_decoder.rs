//! `BoundaryAwareCodecAsTokioDecoder`: a boundary-aware codec as a tokio
//! `Decoder`.

use std::io;

use bytes::{Buf, BytesMut};
use tokio_util::codec::Decoder;

use super::codec_support::{bounded_step, finish_all, to_io_error};
use crate::{BoundaryAwareCodec, BoundaryAwareProgress};

/// Runs a [`BoundaryAwareCodec`] as a tokio `Decoder`. It works like
/// tokio-util `LinesCodec`: an item is all output of one logical
/// stream. The item comes at the boundary. A stream can span many
/// reads.
///
/// At a boundary, the decoder calls `finish` of the codec. Then it
/// uses the same codec for the next stream, with the bytes that follow
/// the boundary. So the codec must be reusable after `finish`.
///
/// At the end of input, the decoder ends the last stream and returns
/// its output. If no byte of a stream arrived, there is no item.
///
/// The error type is `std::io::Error`. The crate [`Error`](crate::Error)
/// is its source.
pub struct BoundaryAwareCodecAsTokioDecoder<C: BoundaryAwareCodec> {
    codec: C,
    /// The output of the current stream.
    out: BytesMut,
    /// The codec got input in the current stream.
    has_input: bool,
}

impl<C: BoundaryAwareCodec> BoundaryAwareCodecAsTokioDecoder<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            out: BytesMut::new(),
            has_input: false,
        }
    }

    pub fn codec(&self) -> &C {
        &self.codec
    }

    pub fn codec_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    pub fn into_inner(self) -> C {
        self.codec
    }

    /// Finish the current stream and take its output.
    fn end_stream(&mut self) -> Result<BytesMut, io::Error> {
        finish_all(&mut self.codec, &mut self.out).map_err(to_io_error)?;
        self.has_input = false;
        Ok(self.out.split())
    }
}

impl<C: BoundaryAwareCodec> Decoder for BoundaryAwareCodecAsTokioDecoder<C> {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        while !src.is_empty() {
            self.has_input = true;
            match bounded_step(&mut self.codec, &mut self.out, src).map_err(to_io_error)? {
                BoundaryAwareProgress::InputConsumed { .. } => {
                    src.clear();
                    return Ok(None);
                }
                BoundaryAwareProgress::OutputFilled { consumed } => src.advance(consumed),
                BoundaryAwareProgress::Boundary { consumed, .. } => {
                    src.advance(consumed);
                    return self.end_stream().map(Some);
                }
            }
        }
        Ok(None)
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        if let Some(item) = self.decode(src)? {
            return Ok(Some(item));
        }
        if !self.has_input {
            return Ok(None);
        }
        self.end_stream().map(Some)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    use super::BoundaryAwareCodecAsTokioDecoder;
    use crate::codecs::rot13::Rot13;
    use crate::codecs::test_support::{EndsAtBar, HoldsOutput};

    /// A stream that gives no output is still a stream.
    #[test]
    fn empty_stream_is_an_empty_item() {
        let mut decoder = BoundaryAwareCodecAsTokioDecoder::new(EndsAtBar::<Rot13>::default());
        let mut buf = BytesMut::from(&b"|"[..]);

        let item = decoder.decode(&mut buf).unwrap().unwrap();

        assert!(item.is_empty());
    }

    #[test]
    fn eof_without_input_gives_nothing() {
        let mut decoder = BoundaryAwareCodecAsTokioDecoder::new(EndsAtBar::<Rot13>::default());
        let mut buf = BytesMut::from(&b"uryyb|"[..]);

        assert!(decoder.decode(&mut buf).unwrap().is_some());
        assert_eq!(decoder.decode_eof(&mut buf).unwrap(), None);
    }

    /// `finish` at the boundary writes more than one call can take.
    #[test]
    fn finish_output_larger_than_one_call() {
        let inner = HoldsOutput {
            per_input: 5_000,
            ..Default::default()
        };
        let codec = EndsAtBar {
            inner,
            finish_calls: 0,
        };
        let mut decoder = BoundaryAwareCodecAsTokioDecoder::new(codec);
        let mut buf = BytesMut::from(&b"ab|"[..]);

        let item = decoder.decode(&mut buf).unwrap().unwrap();

        assert_eq!(item.len(), 10_000);
    }
}
