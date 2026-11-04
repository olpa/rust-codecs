//! `TokioDecoderCodec`: a tokio `Decoder` as a boundary-aware codec.

use core::mem::MaybeUninit;

use bytes::BytesMut;
use tokio_util::codec::Decoder;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind,
};

/// Runs a tokio `Decoder` as a [`BoundaryAwareCodec`]. Each logical
/// stream ends after the first decoded object. The client takes the
/// object with [`take_item`](Self::take_item).
///
/// The codec writes nothing to its output; the object travels only
/// through `take_item`. An object that the client did not take is
/// overwritten by the next one.
pub struct TokioDecoderCodec<D: Decoder> {
    decoder: D,
    buf: BytesMut,
    item: Option<D::Item>,
}

impl<D: Decoder> TokioDecoderCodec<D> {
    pub fn new(decoder: D) -> Self {
        Self {
            decoder,
            buf: BytesMut::new(),
            item: None,
        }
    }

    /// Take the object decoded by the last logical stream.
    pub fn take_item(&mut self) -> Option<D::Item> {
        self.item.take()
    }
}

impl<D: Decoder> DrainCodec for TokioDecoderCodec<D> {
    fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }

    /// Lets the decoder see the end of input. This can yield a last
    /// object or report a truncated one.
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let result = self.decoder.decode_eof(&mut self.buf);
        let leftover = !self.buf.is_empty();
        self.buf.clear();
        match result {
            Ok(item) => {
                if item.is_some() {
                    self.item = item;
                }
                Ok(DrainProgress::Done { written: 0 })
            }
            Err(_) if leftover => Err(Error::new(ErrorKind::UnexpectedEnd, 0, 0)),
            Err(_) => Err(Error::new(ErrorKind::CorruptStream, 0, 0)),
        }
    }
}

impl<D: Decoder> BoundaryAwareCodec for TokioDecoderCodec<D> {
    fn process_bounded(
        &mut self,
        input: &[u8],
        _output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        self.buf.extend_from_slice(input);
        // What the decoder leaves in `buf` is the tail of `input`.
        match self.decoder.decode(&mut self.buf) {
            Ok(None) => Ok(BoundaryAwareProgress::InputConsumed { written: 0 }),
            Ok(Some(item)) => {
                self.item = Some(item);
                let consumed = input.len().saturating_sub(self.buf.len());
                // The caller supplies the tail again for the next stream.
                self.buf.clear();
                Ok(BoundaryAwareProgress::Boundary {
                    consumed,
                    written: 0,
                })
            }
            Err(_) => {
                let consumed = input.len().saturating_sub(self.buf.len());
                Err(Error::new(ErrorKind::CorruptStream, consumed, 0))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::TokioDecoderCodec;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::sources_and_sinks::vec::VecSink;
    use crate::tokio::test_support::{three_strings_input, LengthPrefixedStringTokioDecoder};
    use crate::{stream_to_stream, BoundaryAwareCodec};

    #[test]
    fn stream_to_stream_stops_after_each_object() {
        let input = three_strings_input();
        let mut source = SliceSource::new(&input);
        let mut adapter = TokioDecoderCodec::new(LengthPrefixedStringTokioDecoder {});

        let mut items = Vec::new();
        for _ in 0..3 {
            let mut sink = VecSink::default();
            // `&mut dyn` lets the caller keep the adapter.
            let codec: &mut dyn BoundaryAwareCodec = &mut adapter;
            stream_to_stream(&mut source, codec, &mut sink).unwrap();
            items.push(adapter.take_item().unwrap());
        }

        assert_eq!(items, ["hello", "tokio", "world"]);
    }
}
