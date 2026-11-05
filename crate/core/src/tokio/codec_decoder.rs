//! `CodecAsTokioDecoder`: a plain codec as a tokio `Decoder`.

use std::io;

use bytes::BytesMut;
use tokio_util::codec::Decoder;

use super::codec_support::{finish_all, process_all, to_io_error};
use crate::Codec;

/// Runs a [`Codec`] as a tokio `Decoder`. It works like tokio-util
/// `BytesCodec`: an item is the output that the codec made from the
/// bytes that are in the buffer now. The client gets it as soon as
/// there is output.
///
/// At the end of input, the decoder calls `finish` of the codec and
/// returns its output as the last item.
///
/// The error type is `std::io::Error`. The crate [`Error`](crate::Error)
/// is its source.
pub struct CodecAsTokioDecoder<C: Codec> {
    codec: C,
    finished: bool,
}

impl<C: Codec> CodecAsTokioDecoder<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            finished: false,
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
}

impl<C: Codec> Decoder for CodecAsTokioDecoder<C> {
    type Item = BytesMut;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        if src.is_empty() {
            return Ok(None);
        }
        let mut out = BytesMut::new();
        process_all(&mut out, src, |input, output| {
            self.codec.process(input, output).map(Into::into)
        })
        .map_err(to_io_error)?;
        src.clear();
        Ok((!out.is_empty()).then_some(out))
    }

    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        if let Some(item) = self.decode(src)? {
            return Ok(Some(item));
        }
        if self.finished {
            return Ok(None);
        }
        self.finished = true;
        let mut out = BytesMut::new();
        finish_all(&mut self.codec, &mut out).map_err(to_io_error)?;
        Ok((!out.is_empty()).then_some(out))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    use super::CodecAsTokioDecoder;
    use crate::codecs::rot13::Rot13;
    use crate::codecs::test_support::HoldsOutput;

    /// The trailer of `finish` comes as the last item, once.
    #[test]
    fn eof_returns_the_output_of_finish() {
        let codec = HoldsOutput {
            trailer: b"END",
            ..Default::default()
        };
        let mut decoder = CodecAsTokioDecoder::new(codec);
        let mut buf = BytesMut::from(&b"ab"[..]);

        assert_eq!(decoder.decode(&mut buf).unwrap(), None);
        assert!(buf.is_empty());
        let item = decoder.decode_eof(&mut buf).unwrap().unwrap();
        assert_eq!(&item[..], b"END");
        assert_eq!(decoder.decode_eof(&mut buf).unwrap(), None);
    }

    #[test]
    fn empty_buffer_gives_nothing() {
        let mut decoder = CodecAsTokioDecoder::new(Rot13);

        assert_eq!(decoder.decode(&mut BytesMut::new()).unwrap(), None);
    }
}
