use embedded_io::{BufRead, Error as _, ErrorKind, Read, Write};

use crate::{Sink, Source};

/// A `Source` over `embedded_io::Read`, reading into an owned scratch
/// buffer.
pub struct EmbeddedSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
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
        if self.pos == self.len {
            self.len = loop {
                match self.inner.read(self.buffer.as_mut()) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            };
            self.pos = 0;
        }
        if self.pos < self.len {
            let unconsumed = &self.buffer.as_mut()[self.pos..self.len];
            Ok(Some(unconsumed))
        } else {
            Ok(None)
        }
    }

    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.len - self.pos);
        self.pos += amount;
    }
}

/// An `embedded_io::BufRead` used directly as an input stream, with no
/// scratch buffer of its own.
///
/// Unlike [`EmbeddedSource`], which owns a buffer and calls
/// `Read::read` into it, this forwards straight to `fill_buf`/`consume`
/// — `BufRead` is already a lending API with the same shape as
/// [`Source`], so a reader that already implements it can be adapted
/// with no extra copy.
pub struct BufReadSource<R> {
    inner: R,
}

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Return the wrapped reader. Unlike `EmbeddedSource::into_inner`,
    /// nothing is lost: `BufReadSource` owns no scratch buffer of its
    /// own, so any bytes `R` was still holding come back with it.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // There is a borrow-checker limitation that doesn't allow
        // returning the buffer from the loop. The solution is to
        // call `fill_buf` a second time.
        // That second call doesn't trigger a read: after a non-empty
        // `fill_buf`, the buffer stays available until `consume` is
        // called, so it just re-borrows the same still-unconsumed window.
        loop {
            match self.inner.fill_buf() {
                Ok([]) => return Ok(None),
                Ok(_) => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        self.inner.fill_buf().map(Some)
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
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
        let mut pos = 0;
        while pos < amount {
            match self.inner.write(&self.buffer.as_mut()[pos..amount]) {
                Ok(n) => pos += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{BufRead, ErrorKind, ErrorType, Read, Write};

    use super::{BufReadSource, EmbeddedSink, EmbeddedSource};
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

    /// An error that's either a genuine `Interrupted` or a wrapped
    /// inner error, so a test double can report `Interrupted` even
    /// when its wrapped reader's own `Error` (e.g. `Infallible` for
    /// `&[u8]`) can't express it.
    #[derive(Debug)]
    enum FlakyError<E> {
        Interrupted,
        Inner(E),
    }

    impl<E: core::fmt::Debug> core::fmt::Display for FlakyError<E> {
        fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
            write!(f, "{self:?}")
        }
    }

    impl<E: embedded_io::Error> core::error::Error for FlakyError<E> {}

    impl<E: embedded_io::Error> embedded_io::Error for FlakyError<E> {
        fn kind(&self) -> ErrorKind {
            match self {
                FlakyError::Interrupted => ErrorKind::Interrupted,
                FlakyError::Inner(e) => e.kind(),
            }
        }
    }

    /// Fails its first `read`/`fill_buf`/`write` with `Interrupted`,
    /// then delegates.
    struct FlakyOnce<R> {
        inner: R,
        failed: bool,
    }

    impl<R: ErrorType> ErrorType for FlakyOnce<R> {
        type Error = FlakyError<R::Error>;
    }

    impl<R: Read> Read for FlakyOnce<R> {
        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            if !self.failed {
                self.failed = true;
                return Err(FlakyError::Interrupted);
            }
            self.inner.read(buf).map_err(FlakyError::Inner)
        }
    }

    impl<R: BufRead> BufRead for FlakyOnce<R> {
        fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            if !self.failed {
                self.failed = true;
                return Err(FlakyError::Interrupted);
            }
            self.inner.fill_buf().map_err(FlakyError::Inner)
        }

        fn consume(&mut self, amt: usize) {
            self.inner.consume(amt);
        }
    }

    impl<W: Write> Write for FlakyOnce<W> {
        fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
            if !self.failed {
                self.failed = true;
                return Err(FlakyError::Interrupted);
            }
            self.inner.write(buf).map_err(FlakyError::Inner)
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.inner.flush().map_err(FlakyError::Inner)
        }
    }

    #[test]
    fn chunk_retries_an_interrupted_read() {
        let flaky = FlakyOnce {
            inner: &b"retry me"[..],
            failed: false,
        };
        let mut input = EmbeddedSource::new(flaky, [0u8; 8]);
        assert_eq!(input.chunk().unwrap(), Some(b"retry me".as_slice()));
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

    #[test]
    fn buf_read_source_forwards_to_fill_buf() {
        let mut input = BufReadSource::new(&b"hello"[..]);
        assert_eq!(input.chunk().unwrap(), Some(b"hello".as_slice()));
        input.consume(5);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn buf_read_source_leaves_unconsumed_remainder_visible() {
        let mut input = BufReadSource::new(&b"abcdef"[..]);
        assert_eq!(input.chunk().unwrap(), Some(b"abcdef".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), Some(b"cdef".as_slice()));
    }

    #[test]
    fn buf_read_source_retries_an_interrupted_fill() {
        let flaky = FlakyOnce {
            inner: &b"retry me too"[..],
            failed: false,
        };
        let mut input = BufReadSource::new(flaky);
        assert_eq!(input.chunk().unwrap(), Some(b"retry me too".as_slice()));
    }

    /// Yields `b"hi"`, then an empty fill, then `b"more"` — stands in
    /// for a transport whose "nothing right now" isn't forever (a
    /// growing file, a pipe), proving `chunk()` doesn't latch itself
    /// shut after a single empty fill. Unlike `std::io`'s
    /// `GrowsAfterAnEmptyRead`, which implements plain `Read` and
    /// relies on `BufReader` to cache `fill_buf`'s result until
    /// `consume`, this implements `BufRead` directly (`embedded_io`
    /// has no generic `Read`-to-`BufRead` adapter), so it does its own
    /// caching to honor that same contract.
    struct GrowsAfterAnEmptyRead {
        stage: usize,
        buf: &'static [u8],
    }

    impl ErrorType for GrowsAfterAnEmptyRead {
        type Error = core::convert::Infallible;
    }

    impl Read for GrowsAfterAnEmptyRead {
        fn read(&mut self, _buf: &mut [u8]) -> Result<usize, Self::Error> {
            unreachable!("BufReadSource never calls read(), only fill_buf()/consume()")
        }
    }

    impl BufRead for GrowsAfterAnEmptyRead {
        fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            if self.buf.is_empty() {
                self.buf = match self.stage {
                    0 => b"hi",
                    1 => b"",
                    2 => b"more",
                    _ => b"",
                };
                self.stage += 1;
            }
            Ok(self.buf)
        }

        fn consume(&mut self, amt: usize) {
            self.buf = &self.buf[amt..];
        }
    }

    #[test]
    fn buf_read_source_revisits_the_wrapped_reader_after_an_empty_fill() {
        let mut input = BufReadSource::new(GrowsAfterAnEmptyRead { stage: 0, buf: b"" });
        assert_eq!(input.chunk().unwrap(), Some(b"hi".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), None);
        assert_eq!(input.chunk().unwrap(), Some(b"more".as_slice()));
    }

    #[test]
    fn commit_retries_an_interrupted_write() {
        let mut bytes = [0u8; 32];
        let flaky = FlakyOnce {
            inner: &mut bytes[..],
            failed: false,
        };
        let written = {
            let mut output = EmbeddedSink::new(flaky, [0u8; 8]);
            let spare = output.spare().unwrap().unwrap();
            spare[..5].copy_from_slice(b"abcde");
            output.commit(5).unwrap();
            32 - output.into_inner().inner.len()
        };
        assert_eq!(&bytes[..written], b"abcde");
    }
}
