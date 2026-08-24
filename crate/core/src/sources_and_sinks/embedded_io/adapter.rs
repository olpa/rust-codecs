use embedded_io::{Read, Write};

use crate::{Sink, Source};

/// A `Source` over `embedded_io::Read`, reading into an owned scratch
/// buffer.
pub struct EmbeddedSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> EmbeddedSource<R, S> {
    /// Build an `EmbeddedSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte read
    /// from `inner`, so `chunk` could never return anything.
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedSource buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            pos: 0,
            len: 0,
            eof: false,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Reclaim the reader, discarding the scratch buffer and any
    /// buffered, unconsumed bytes (already read from `inner` into the
    /// buffer via `chunk`, but not yet passed to `consume`).
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Reclaim both the reader and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `EmbeddedSource`. Any
    /// buffered, unconsumed bytes (already read from `inner` into
    /// the buffer via `chunk`, but not yet passed to `consume`) are
    /// discarded along with them.
    pub fn into_parts(self) -> (R, S) {
        (self.inner, self.buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for EmbeddedSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Only refills once the current window is fully consumed; an
        // unconsumed remainder overlaps the previous call's chunk.
        if self.pos == self.len && !self.eof {
            self.len = self.inner.read(self.buffer.as_mut())?;
            self.pos = 0;
            self.eof = self.len == 0;
        }
        Ok((self.pos < self.len).then_some(&self.buffer.as_mut()[self.pos..self.len]))
    }

    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.len - self.pos);
        self.pos += amount;
    }
}

/// A `Sink` over `embedded_io::Write`, staging writes in an owned
/// scratch buffer.
pub struct EmbeddedSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> EmbeddedSink<W, S> {
    /// Build an `EmbeddedSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedSink buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            offered: 0,
        }
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Reclaim the writer, discarding the scratch buffer and any bytes
    /// staged in it via `spare` but not yet handed to `commit` — they
    /// are not written to `inner`.
    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Reclaim both the writer and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `EmbeddedSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for EmbeddedSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        let buf = self.buffer.as_mut();
        self.offered = buf.len();
        Ok(Some(buf))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        self.inner.write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{ErrorType, Read, Write};

    use super::{EmbeddedSink, EmbeddedSource};
    use crate::identity::identity;
    use crate::sources_and_sinks::shared_io::test_support::{CountingReader, RecordingWriter};
    use crate::stream_to_stream;
    use crate::{Sink, Source};

    impl<R: ErrorType> ErrorType for CountingReader<R> {
        type Error = R::Error;
    }

    impl<R: Read> Read for CountingReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            self.reads += 1;
            self.inner.read(buf)
        }
    }

    #[test]
    fn chunk_returns_none_at_genuine_eof() {
        let mut input = EmbeddedSource::new(&b""[..], [0u8; 4]);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn partial_consume_leaves_remainder_visible_on_next_chunk() {
        let reader = CountingReader {
            inner: &b"abcdef"[..],
            reads: 0,
        };
        let mut input = EmbeddedSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(1);
        // The unconsumed remainder reappears, overlapping the previous
        // chunk — no new bytes were pulled in to produce it.
        assert_eq!(input.chunk().unwrap(), Some(b"bcd".as_slice()));
        assert_eq!(input.get_ref().reads, 1);
    }

    #[test]
    fn full_consume_triggers_a_refill() {
        let reader = CountingReader {
            inner: &b"abcdef"[..],
            reads: 0,
        };
        let mut input = EmbeddedSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(4);
        assert_eq!(input.chunk().unwrap(), Some(b"ef".as_slice()));
        assert_eq!(input.get_ref().reads, 2);
    }

    #[test]
    fn repeated_chunk_without_consume_is_idempotent() {
        let reader = CountingReader {
            inner: &b"abcd"[..],
            reads: 0,
        };
        let mut input = EmbeddedSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.get_ref().reads, 1);
    }

    #[test]
    #[should_panic]
    fn consume_more_than_available_panics() {
        let mut input = EmbeddedSource::new(&b"ab"[..], [0u8; 4]);
        input.chunk().unwrap();
        input.consume(3);
    }

    #[test]
    fn spare_offers_the_whole_buffer() {
        let mut bytes = [0u8; 32];
        let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 6]);
        assert_eq!(output.spare().unwrap().unwrap().len(), 6);
    }

    #[test]
    fn spare_without_commit_is_reissuable() {
        let mut bytes = [0u8; 32];
        let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 6]);
        let first_len = output.spare().unwrap().unwrap().len();
        let second_len = output.spare().unwrap().unwrap().len();
        assert_eq!(first_len, second_len);
    }

    #[test]
    fn commit_writes_only_the_committed_prefix_through() {
        let mut bytes = [0u8; 32];
        let written = {
            let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 8]);
            let spare = output.spare().unwrap().unwrap();
            spare[..5].copy_from_slice(b"abcde");
            output.commit(3).unwrap();
            32 - output.into_inner().len()
        };
        assert_eq!(&bytes[..written], b"abc");
    }

    #[test]
    #[should_panic]
    fn commit_more_than_offered_panics() {
        let mut bytes = [0u8; 32];
        let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 4]);
        output.spare().unwrap();
        output.commit(5).unwrap();
    }

    impl ErrorType for RecordingWriter {
        type Error = core::convert::Infallible;
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn finish_flushes_the_inner_writer() {
        let mut output = EmbeddedSink::new(RecordingWriter { flushes: 0 }, [0u8; 4]);
        output.finish().unwrap();
        assert_eq!(output.get_ref().flushes, 1);
    }

    #[test]
    fn embedded_source_feeds_embedded_sink_end_to_end() {
        let mut input = EmbeddedSource::new(&b"embedded to embedded"[..], [0u8; 3]);
        let mut bytes = [0u8; 32];
        let written = {
            let mut output = EmbeddedSink::new(&mut bytes[..], [0u8; 3]);
            stream_to_stream(&mut input, identity(), &mut output).unwrap();
            32 - output.into_inner().len()
        };
        assert_eq!(&bytes[..written], b"embedded to embedded");
    }
}
