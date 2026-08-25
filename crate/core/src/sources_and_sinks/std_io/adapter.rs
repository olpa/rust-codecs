use std::io::{BufRead, Read, Write};

use crate::{Sink, Source};

/// A `Source` over `std::io::Read`, reading into an owned scratch
/// buffer.
pub struct StdSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
}

impl<R: Read, S: AsMut<[u8]>> StdSource<R, S> {
    /// Build a `StdSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte read
    /// from `inner`, so `chunk` could never return anything.
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSource buffer must be non-empty"
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
    /// the buffer's allocation for another `StdSource`. Any buffered,
    /// unconsumed bytes (already read from `inner` into the buffer via
    /// `chunk`, but not yet passed to `consume`) are discarded along
    /// with it.
    pub fn into_parts(self) -> (R, S) {
        (self.inner, self.buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for StdSource<R, S> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.pos == self.len {
            self.len = loop {
                match self.inner.read(self.buffer.as_mut()) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
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

/// A `std::io::BufRead` used directly as an input stream, with no
/// scratch buffer of its own.
///
/// Unlike [`StdSource`], which owns a buffer and calls `Read::read`
/// into it, this forwards straight to `fill_buf`/`consume` — `BufRead`
/// is already a lending API with the same shape as [`Source`], so a
/// reader that already implements it (`BufReader`, `&[u8]`,
/// `Cursor<Vec<u8>>`, `VecDeque<u8>`, ...) can be adapted with no extra
/// copy. Wrapping an `R` that doesn't already buffer (a raw `File` or
/// `TcpStream`) in a `BufReader` first, then in this, is the equivalent
/// of `StdSource` minus the double allocation.
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

    /// Return the wrapped reader. Unlike `StdSource::into_inner`,
    /// nothing is lost: `BufReadSource` owns no scratch buffer of its
    /// own, so any bytes `R` was still holding come back with it.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = std::io::Error;

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
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        }

        self.inner.fill_buf().map(Some)
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}

/// A `Sink` over `std::io::Write`, staging writes in an owned scratch
/// buffer.
pub struct StdSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> StdSink<W, S> {
    /// Build a `StdSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSink buffer must be non-empty"
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
    /// the buffer's allocation for another `StdSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for StdSink<W, S> {
    type Error = std::io::Error;

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
    use std::io::{BufReader, Cursor, Read, Write};

    use super::{BufReadSource, StdSink, StdSource};
    use crate::identity::identity;
    use crate::sources_and_sinks::shared_io::test_support::{CountingReader, RecordingWriter};
    use crate::stream_to_stream;
    use crate::{Sink, Source};

    impl<R: Read> Read for CountingReader<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.reads += 1;
            self.inner.read(buf)
        }
    }

    #[test]
    fn chunk_returns_none_at_genuine_eof() {
        let mut input = StdSource::new(Cursor::new(b"".as_slice()), [0u8; 4]);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn partial_consume_leaves_remainder_visible_on_next_chunk() {
        let reader = CountingReader {
            inner: Cursor::new(b"abcdef".as_slice()),
            reads: 0,
        };
        let mut input = StdSource::new(reader, [0u8; 4]);

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
            inner: Cursor::new(b"abcdef".as_slice()),
            reads: 0,
        };
        let mut input = StdSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(4);
        assert_eq!(input.chunk().unwrap(), Some(b"ef".as_slice()));
        assert_eq!(input.get_ref().reads, 2);
    }

    #[test]
    fn repeated_chunk_without_consume_is_idempotent() {
        let reader = CountingReader {
            inner: Cursor::new(b"abcd".as_slice()),
            reads: 0,
        };
        let mut input = StdSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.get_ref().reads, 1);
    }

    #[test]
    #[should_panic]
    fn consume_more_than_available_panics() {
        let mut input = StdSource::new(Cursor::new(b"ab".as_slice()), [0u8; 4]);
        input.chunk().unwrap();
        input.consume(3);
    }

    /// Fails its first `read` with `Interrupted`, then delegates.
    struct FlakyOnce<R> {
        inner: R,
        failed: bool,
    }

    impl<R: Read> Read for FlakyOnce<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if !self.failed {
                self.failed = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "eintr",
                ));
            }
            self.inner.read(buf)
        }
    }

    #[test]
    fn chunk_retries_an_interrupted_read() {
        let flaky = FlakyOnce {
            inner: Cursor::new(b"retry me".as_slice()),
            failed: false,
        };
        let mut input = StdSource::new(flaky, [0u8; 8]);
        assert_eq!(input.chunk().unwrap(), Some(b"retry me".as_slice()));
    }

    #[test]
    fn buf_read_source_forwards_to_fill_buf() {
        let mut input = BufReadSource::new(Cursor::new(b"hello".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"hello".as_slice()));
        input.consume(5);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn buf_read_source_leaves_unconsumed_remainder_visible() {
        let mut input = BufReadSource::new(Cursor::new(b"abcdef".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"abcdef".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), Some(b"cdef".as_slice()));
    }

    #[test]
    fn buf_read_source_retries_an_interrupted_fill() {
        // `BufReader::fill_buf` calls the wrapped `Read::read` directly
        // when its own buffer is empty, so `FlakyOnce`'s interruption
        // surfaces through `fill_buf` too.
        let flaky = FlakyOnce {
            inner: Cursor::new(b"retry me too".as_slice()),
            failed: false,
        };
        let mut input = BufReadSource::new(BufReader::new(flaky));
        assert_eq!(input.chunk().unwrap(), Some(b"retry me too".as_slice()));
    }

    /// Yields `b"hi"`, then an empty read, then `b"more"` — stands in
    /// for a transport whose "nothing right now" isn't forever (a
    /// growing file, a pipe), proving `chunk()` doesn't latch itself
    /// shut after a single empty fill.
    struct GrowsAfterAnEmptyRead {
        calls: usize,
    }

    impl Read for GrowsAfterAnEmptyRead {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            match self.calls {
                1 => {
                    buf[..2].copy_from_slice(b"hi");
                    Ok(2)
                }
                2 => Ok(0),
                3 => {
                    buf[..4].copy_from_slice(b"more");
                    Ok(4)
                }
                _ => Ok(0),
            }
        }
    }

    #[test]
    fn buf_read_source_revisits_the_wrapped_reader_after_an_empty_fill() {
        let mut input = BufReadSource::new(BufReader::new(GrowsAfterAnEmptyRead { calls: 0 }));
        assert_eq!(input.chunk().unwrap(), Some(b"hi".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), None);
        assert_eq!(input.chunk().unwrap(), Some(b"more".as_slice()));
    }

    #[test]
    fn spare_offers_the_whole_buffer() {
        let mut output = StdSink::new(Vec::new(), [0u8; 6]);
        assert_eq!(output.spare().unwrap().unwrap().len(), 6);
    }

    #[test]
    fn spare_without_commit_is_reissuable() {
        let mut output = StdSink::new(Vec::new(), [0u8; 6]);
        let first_len = output.spare().unwrap().unwrap().len();
        let second_len = output.spare().unwrap().unwrap().len();
        assert_eq!(first_len, second_len);
    }

    #[test]
    fn commit_writes_only_the_committed_prefix_through() {
        let mut output = StdSink::new(Vec::new(), [0u8; 8]);
        let spare = output.spare().unwrap().unwrap();
        spare[..5].copy_from_slice(b"abcde");
        output.commit(3).unwrap();
        assert_eq!(output.get_ref().as_slice(), b"abc");
    }

    #[test]
    #[should_panic]
    fn commit_more_than_offered_panics() {
        let mut output = StdSink::new(Vec::new(), [0u8; 4]);
        output.spare().unwrap();
        output.commit(5).unwrap();
    }

    impl Write for RecordingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn finish_flushes_the_inner_writer() {
        let mut output = StdSink::new(RecordingWriter { flushes: 0 }, [0u8; 4]);
        output.finish().unwrap();
        assert_eq!(output.get_ref().flushes, 1);
    }

    #[test]
    fn std_source_feeds_std_sink_end_to_end() {
        let mut input = StdSource::new(Cursor::new(b"std to std".as_slice()), [0u8; 3]);
        let mut output = StdSink::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to std");
    }
}
