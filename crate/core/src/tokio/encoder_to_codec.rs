//! `TokioEncoderAsBoundaryAwareCodec`: a tokio `Encoder` as a codec.

use core::mem::MaybeUninit;

use bytes::{Buf, BytesMut};
use tokio_util::codec::Encoder;

use crate::uninit::copy_to_uninit;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind,
};

/// Runs a tokio `Encoder` as a [`BoundaryAwareCodec`].
///
/// The codec ignores its input bytes. It encodes the items that the
/// client gave at construction, one at a time, and writes the bytes to
/// its output. When it has written the bytes of the last item, it
/// reports a boundary without consuming input. So a source stays
/// untouched, and an empty source works: then the output comes from
/// `finish`.
///
/// Input bytes that the codec gets are lost.
///
/// If the encoder fails, the codec reports
/// [`ErrorKind::CorruptStream`]. The error of the encoder stays in the
/// codec. Take it with [`take_error`](Self::take_error).
pub struct TokioEncoderAsBoundaryAwareCodec<E, I>
where
    I: Iterator,
    E: Encoder<I::Item>,
{
    encoder: E,
    items: I,
    /// Encoded bytes that the codec has not written yet.
    buf: BytesMut,
    error: Option<E::Error>,
}

impl<E, I> TokioEncoderAsBoundaryAwareCodec<E, I>
where
    I: Iterator,
    E: Encoder<I::Item>,
{
    /// Encode the items of an iterator, one after another.
    pub fn with_items<It: IntoIterator<IntoIter = I>>(encoder: E, items: It) -> Self {
        Self {
            encoder,
            items: items.into_iter(),
            buf: BytesMut::new(),
            error: None,
        }
    }

    /// Take the error of the encoder, after the codec reported a failure.
    pub fn take_error(&mut self) -> Option<E::Error> {
        self.error.take()
    }

    /// Write encoded bytes to `output` until it is full or the items
    /// end. Returns the number of bytes written and whether the items
    /// ended.
    fn emit(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<(usize, bool), Error> {
        let mut written = 0;
        while written < output.len() {
            if self.buf.is_empty() {
                let Some(item) = self.items.next() else {
                    return Ok((written, true));
                };
                if let Err(error) = self.encoder.encode(item, &mut self.buf) {
                    self.error = Some(error);
                    return Err(Error::new(ErrorKind::CorruptStream, 0, written));
                }
                // An item can give no bytes. Ask for the next one.
                continue;
            }
            let n = self.buf.len().min(output.len() - written);
            copy_to_uninit(&mut output[written..written + n], &self.buf[..n]);
            self.buf.advance(n);
            written += n;
        }
        Ok((written, false))
    }

    fn drain(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let (written, done) = self.emit(output)?;
        Ok(if done {
            DrainProgress::Done { written }
        } else {
            DrainProgress::OutputFilled
        })
    }
}

impl<E, T> TokioEncoderAsBoundaryAwareCodec<E, core::iter::Once<T>>
where
    E: Encoder<T>,
{
    /// Encode one item.
    pub fn with_item(encoder: E, item: T) -> Self {
        Self::with_items(encoder, core::iter::once(item))
    }
}

impl<E, I> DrainCodec for TokioEncoderAsBoundaryAwareCodec<E, I>
where
    I: Iterator,
    E: Encoder<I::Item>,
{
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain(output)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain(output)
    }
}

impl<E, I> BoundaryAwareCodec for TokioEncoderAsBoundaryAwareCodec<E, I>
where
    I: Iterator,
    E: Encoder<I::Item>,
{
    fn process_bounded(
        &mut self,
        _input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        let (written, done) = self.emit(output)?;
        Ok(if done {
            BoundaryAwareProgress::Boundary {
                consumed: 0,
                written,
            }
        } else {
            BoundaryAwareProgress::OutputFilled { consumed: 0 }
        })
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;
    use std::io;

    use bytes::BytesMut;
    use tokio_util::codec::Encoder;

    use super::TokioEncoderAsBoundaryAwareCodec;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::sources_and_sinks::vec::VecSink;
    use crate::tokio::test_support::{bar_delimited_codec, NullSource};
    use crate::{stream_to_stream, BoundaryAwareCodec, BoundaryAwareProgress, ErrorKind};

    struct FailingEncoder;

    impl<'a> Encoder<&'a str> for FailingEncoder {
        type Error = io::Error;

        fn encode(&mut self, _item: &'a str, _dst: &mut BytesMut) -> Result<(), io::Error> {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "no"))
        }
    }

    /// An output that is smaller than the item needs several calls.
    #[test]
    fn small_output_needs_several_calls() {
        let mut codec = TokioEncoderAsBoundaryAwareCodec::with_item(bar_delimited_codec(), "hello");
        let mut output = [MaybeUninit::<u8>::uninit(); 3];
        let mut bytes = Vec::new();

        for _ in 0..2 {
            let progress = codec.process_bounded(b"", &mut output).unwrap();
            assert_eq!(
                progress,
                BoundaryAwareProgress::OutputFilled { consumed: 0 }
            );
            bytes.extend(output.iter().map(|b| unsafe { b.assume_init() }));
        }
        let progress = codec.process_bounded(b"", &mut output).unwrap();

        assert_eq!(
            progress,
            BoundaryAwareProgress::Boundary {
                consumed: 0,
                written: 0
            }
        );
        assert_eq!(bytes, b"hello|");
    }

    /// With a source that has data, the codec reports the boundary
    /// and does not take a byte from the source.
    #[test]
    fn a_source_with_data_stays_untouched() {
        let mut source = SliceSource::new(b"abc");
        let mut sink = VecSink::default();
        let codec = TokioEncoderAsBoundaryAwareCodec::with_item(bar_delimited_codec(), "hello");

        stream_to_stream(&mut source, codec, &mut sink).unwrap();

        assert_eq!(sink.into_inner(), b"hello|");
        assert_eq!(source.consumed(), 0);
    }

    #[test]
    fn encoder_error_stays_in_the_codec() {
        let mut codec = TokioEncoderAsBoundaryAwareCodec::with_item(FailingEncoder, "hello");
        let mut output = [MaybeUninit::<u8>::uninit(); 16];

        let error = codec.process_bounded(b"", &mut output).unwrap_err();

        assert_eq!(error.kind, ErrorKind::CorruptStream);
        let original = codec.take_error().unwrap();
        assert_eq!(original.kind(), io::ErrorKind::InvalidInput);
        assert!(codec.take_error().is_none());
    }

    /// No items: the stream is empty.
    #[test]
    fn no_items_give_an_empty_stream() {
        let mut sink = VecSink::default();
        let codec = TokioEncoderAsBoundaryAwareCodec::with_items(
            bar_delimited_codec(),
            std::iter::empty::<&str>(),
        );

        stream_to_stream(&mut NullSource, codec, &mut sink).unwrap();

        assert!(sink.into_inner().is_empty());
    }
}
