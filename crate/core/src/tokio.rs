//! Adapter from `tokio_util::codec::Decoder` to this crate's codecs.
//!
//! Work in progress.

use core::mem::MaybeUninit;

use bytes::BytesMut;
use tokio_util::codec::Decoder;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainCodec, DrainProgress, Error, ErrorKind, Source,
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

/// A failure of [`TokioReader::next_item`].
#[derive(Debug)]
pub enum ReadError<ES, ED> {
    /// The source failed.
    Source(ES),
    /// The decoder failed.
    Decoder(ED),
    /// The source ended in the middle of an object.
    UnexpectedEnd,
}

impl<ES: core::fmt::Display, ED: core::fmt::Display> core::fmt::Display for ReadError<ES, ED> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Source(e) => e.fmt(f),
            Self::Decoder(e) => e.fmt(f),
            Self::UnexpectedEnd => f.write_str("the source ended in the middle of an object"),
        }
    }
}

impl<ES, ED> core::error::Error for ReadError<ES, ED>
where
    ES: core::error::Error + 'static,
    ED: core::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Source(e) => Some(e),
            Self::Decoder(e) => Some(e),
            Self::UnexpectedEnd => None,
        }
    }
}

type ReadResult<S, D> =
    Result<Option<<D as Decoder>::Item>, ReadError<<S as Source>::Error, <D as Decoder>::Error>>;

/// Reads the objects of a tokio `Decoder` from a [`Source`], one at a
/// time.
///
/// The reader owns the source. After an object is decoded, the bytes
/// that follow it stay unconsumed in the source, so the client can read
/// them with [`source_mut`](Self::source_mut) before asking for the next
/// object.
///
/// A byte is copied into the decoder's `BytesMut` once. The copy of the
/// not yet consumed tail of the source's chunk stays in the buffer
/// between objects, and the next call does not copy it again. Because
/// only this type can touch the source, that copy cannot go stale:
/// `source_mut` drops it.
///
/// The reader is also an [`Iterator`] over `Result<D::Item, _>`. After
/// the end of input or the first error, it yields nothing more.
pub struct TokioReader<S: Source, D: Decoder> {
    source: S,
    decoder: D,
    /// A copy of source input that the decoder has not finished with.
    /// It has two parts. The first part was consumed from the source,
    /// so only this buffer holds it. The last `lookahead_len` bytes
    /// are the lookahead: the source still holds them, unconsumed.
    input_copy: BytesMut,
    /// The current number of lookahead bytes at the end of
    /// `input_copy`, not a configured window size. It never exceeds
    /// `input_copy.len()`.
    lookahead_len: usize,
    done: bool,
}

impl<S: Source, D: Decoder> TokioReader<S, D> {
    pub fn new(source: S, decoder: D) -> Self {
        Self {
            source,
            decoder,
            input_copy: BytesMut::new(),
            lookahead_len: 0,
            done: false,
        }
    }

    /// Return the next object, or `None` at the end of input.
    pub fn next_item(&mut self) -> ReadResult<S, D> {
        if self.done {
            return Ok(None);
        }
        let result = self.read_item();
        if !matches!(result, Ok(Some(_))) {
            self.done = true;
        }
        result
    }

    fn read_item(&mut self) -> ReadResult<S, D> {
        loop {
            if !self.input_copy.is_empty() {
                let decoded = self.decoder.decode(&mut self.input_copy);
                self.settle_lookahead();
                if let Some(item) = decoded.map_err(ReadError::Decoder)? {
                    return Ok(Some(item));
                }
            }
            // The decoder needs more. The shared bytes are now consumed from the source, so only we hold them.
            self.source.consume(self.lookahead_len);
            self.lookahead_len = 0;
            match self.source.chunk().map_err(ReadError::Source)? {
                Some(chunk) => {
                    self.input_copy.extend_from_slice(chunk);
                    self.lookahead_len = chunk.len();
                }
                None => return self.finish_input(),
            }
        }
    }

    fn finish_input(&mut self) -> ReadResult<S, D> {
        match self.decoder.decode_eof(&mut self.input_copy) {
            Ok(None) if !self.input_copy.is_empty() => Err(ReadError::UnexpectedEnd),
            Ok(item) => Ok(item),
            Err(e) => Err(ReadError::Decoder(e)),
        }
    }

    /// The decoder removed bytes from the front of `input_copy`. If it went
    /// into the shared part, consume the same bytes from the source.
    fn settle_lookahead(&mut self) {
        let kept = self.lookahead_len.min(self.input_copy.len());
        self.source.consume(self.lookahead_len - kept);
        self.lookahead_len = kept;
    }

    /// Access the source, for example to read data between two objects.
    ///
    /// This drops the copy of the source's unconsumed bytes, so the next
    /// object is read from the source as the client left it.
    pub fn source_mut(&mut self) -> &mut S {
        self.input_copy
            .truncate(self.input_copy.len() - self.lookahead_len);
        self.lookahead_len = 0;
        &mut self.source
    }

    /// Return the source and the bytes that the reader consumed from it
    /// but did not decode yet.
    pub fn into_parts(mut self) -> (S, BytesMut) {
        self.source_mut();
        (self.source, self.input_copy)
    }
}

impl<S: Source, D: Decoder> Iterator for TokioReader<S, D> {
    type Item = Result<D::Item, ReadError<S::Error, D::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_item().transpose()
    }
}

#[cfg(test)]
mod tests {
    use bytes::{Buf, BytesMut};
    use tokio_util::codec::Decoder;

    use super::{ReadError, TokioDecoder, TokioReader};
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::sources_and_sinks::vec::VecSink;
    use crate::Source;
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

    #[test]
    fn reader_iterates_three_strings() {
        let input = three_strings_input();
        let reader = TokioReader::new(SliceSource::new(&input), MyStringDecoder {});

        let items: Vec<String> = reader.map(Result::unwrap).collect();

        assert_eq!(items, ["hello", "tokio", "world"]);
    }

    /// A source with small chunks, so objects span chunk boundaries.
    struct Chunked<'a>(SliceSource<'a>, usize);

    impl Source for Chunked<'_> {
        type Error = core::convert::Infallible;
        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            let n = self.1;
            Ok(self.0.chunk()?.map(|c| &c[..c.len().min(n)]))
        }
        fn consume(&mut self, amount: usize) {
            self.0.consume(amount)
        }
    }

    #[test]
    fn reader_handles_objects_across_chunks() {
        let input = three_strings_input();
        let reader = TokioReader::new(Chunked(SliceSource::new(&input), 3), MyStringDecoder {});

        let items: Vec<String> = reader.map(Result::unwrap).collect();

        assert_eq!(items, ["hello", "tokio", "world"]);
    }

    #[test]
    fn reader_leaves_the_rest_to_the_client() {
        let mut input = frame("hello");
        input.push(b'#');
        input.extend(frame("world"));
        let mut reader = TokioReader::new(SliceSource::new(&input), MyStringDecoder {});

        assert_eq!(reader.next_item().unwrap().as_deref(), Some("hello"));
        let source = reader.source_mut();
        assert_eq!(source.chunk().unwrap().unwrap()[0], b'#');
        source.consume(1);
        assert_eq!(reader.next_item().unwrap().as_deref(), Some("world"));
        assert_eq!(reader.next_item().unwrap(), None);
    }

    #[test]
    fn reader_reports_a_truncated_object_once() {
        let input = frame("hello");
        let input = &input[..input.len() - 1];
        let mut reader = TokioReader::new(SliceSource::new(input), MyStringDecoder {});

        assert!(matches!(
            reader.next_item(),
            Err(ReadError::Decoder(_) | ReadError::UnexpectedEnd)
        ));
        assert!(reader.next().is_none());
    }
}
