//! `TokioReader`: reads the objects of a tokio `Decoder` from a `Source`.

use bytes::BytesMut;
use tokio_util::codec::Decoder;

use super::ReadError;
use crate::Source;

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
    use super::{ReadError, TokioReader};
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::tokio::test_support::{frame, three_strings_input, Chunked, MyStringDecoder};
    use crate::Source;

    #[test]
    fn reader_iterates_three_strings() {
        let input = three_strings_input();
        let reader = TokioReader::new(SliceSource::new(&input), MyStringDecoder {});

        let items: Vec<String> = reader.map(Result::unwrap).collect();

        assert_eq!(items, ["hello", "tokio", "world"]);
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
