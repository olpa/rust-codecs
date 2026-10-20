//! Adapter from `tokio_util::codec::Decoder` to this crate's codecs.
//!
//! Work in progress.

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
pub struct TokioDecoder<D: Decoder> {
    decoder: D,
    buf: BytesMut,
    item: Option<D::Item>,
}

impl<D: Decoder> TokioDecoder<D> {
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

impl<D: Decoder> DrainCodec for TokioDecoder<D> {
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

impl<D: Decoder> BoundaryAwareCodec for TokioDecoder<D> {
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
    use bytes::{Buf, BytesMut};
    use tokio_util::codec::Decoder;

    use super::TokioDecoder;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::sources_and_sinks::vec::VecSink;
    use crate::{stream_to_stream, BoundaryAwareCodec};

    // Copied from the `tokio_util::codec` module documentation:
    // a four-byte little-endian length followed by that many bytes of
    // UTF-8 string data.
    struct MyStringDecoder {}

    const MAX: usize = 8 * 1024 * 1024;

    impl Decoder for MyStringDecoder {
        type Item = String;
        type Error = std::io::Error;

        fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
            if src.len() < 4 {
                // Not enough data to read length marker.
                return Ok(None);
            }

            // Read length marker.
            let mut length_bytes = [0u8; 4];
            length_bytes.copy_from_slice(&src[..4]);
            let length = u32::from_le_bytes(length_bytes) as usize;

            // Check that the length is not too large to avoid a denial of
            // service attack where the server runs out of memory.
            if length > MAX {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Frame of length {} is too large.", length),
                ));
            }

            if src.len() < 4 + length {
                // The full string has not yet arrived.
                src.reserve(4 + length - src.len());
                return Ok(None);
            }

            // Use advance to modify src such that it no longer contains
            // this frame.
            let data = src[4..4 + length].to_vec();
            src.advance(4 + length);

            // Convert the data to a string, or fail if it is not valid utf-8.
            match String::from_utf8(data) {
                Ok(string) => Ok(Some(string)),
                Err(utf8_error) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    utf8_error.utf8_error(),
                )),
            }
        }
    }

    fn frame(s: &str) -> Vec<u8> {
        let mut v = (s.len() as u32).to_le_bytes().to_vec();
        v.extend_from_slice(s.as_bytes());
        v
    }

    fn three_strings_input() -> Vec<u8> {
        let mut input = frame("hello");
        input.extend(frame("tokio"));
        input.extend(frame("world"));
        input
    }

    #[test]
    fn decodes_three_strings() {
        let mut decoder = MyStringDecoder {};
        let mut buf = BytesMut::from(&three_strings_input()[..]);

        assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("hello"));
        assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("tokio"));
        assert_eq!(decoder.decode(&mut buf).unwrap().as_deref(), Some("world"));
        assert!(buf.is_empty());
    }

    #[test]
    fn stream_to_stream_stops_after_each_object() {
        let input = three_strings_input();
        let mut source = SliceSource::new(&input);
        let mut adapter = TokioDecoder::new(MyStringDecoder {});

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
