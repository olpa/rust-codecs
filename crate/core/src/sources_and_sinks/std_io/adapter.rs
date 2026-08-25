use std::io::{BufRead, Read, Write};

use crate::sources_and_sinks::shared_io::{
    retry_fill_buf, retry_on_interrupted, retry_write_all, LendingSource, RetryingFillBuf,
    RetryingRead, RetryingWrite, ScratchSink, ScratchSource,
};
use crate::{Sink, Source};

fn is_interrupted(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::Interrupted
}

/// Wraps a `std::io::Read`, retrying `read` on `Interrupted` — the one
/// piece of backend-specific knowledge [`ScratchSource`] needs.
struct StdReader<R>(R);

impl<R: Read> RetryingRead for StdReader<R> {
    type Error = std::io::Error;

    fn retrying_read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        retry_on_interrupted(|| self.0.read(buf), is_interrupted)
    }
}

/// A `Source` over `std::io::Read`, reading into an owned scratch
/// buffer.
pub struct StdSource<R, S>(ScratchSource<StdReader<R>, S>);

impl<R: Read, S: AsMut<[u8]>> StdSource<R, S> {
    /// Build a `StdSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte read
    /// from `inner`, so `chunk` could never return anything.
    pub fn new(inner: R, buffer: S) -> Self {
        Self(ScratchSource::new(StdReader(inner), buffer))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Reclaim the reader, discarding the scratch buffer and any
    /// buffered, unconsumed bytes (already read from `inner` into the
    /// buffer via `chunk`, but not yet passed to `consume`).
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }

    /// Reclaim both the reader and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `StdSource`. Any buffered,
    /// unconsumed bytes (already read from `inner` into the buffer via
    /// `chunk`, but not yet passed to `consume`) are discarded along
    /// with it.
    pub fn into_parts(self) -> (R, S) {
        let (reader, buffer) = self.0.into_parts();
        (reader.0, buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for StdSource<R, S> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps a `std::io::BufRead`, retrying `fill_buf` on `Interrupted` —
/// the one piece of backend-specific knowledge [`LendingSource`] needs.
struct StdBufReader<R>(R);

impl<R: BufRead> RetryingFillBuf for StdBufReader<R> {
    type Error = std::io::Error;

    fn retrying_fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        retry_fill_buf(&mut self.0, R::fill_buf, is_interrupted)
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
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
pub struct BufReadSource<R>(LendingSource<StdBufReader<R>>);

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self(LendingSource::new(StdBufReader(inner)))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Return the wrapped reader. Unlike `StdSource::into_inner`,
    /// nothing is lost: `BufReadSource` owns no scratch buffer of its
    /// own, so any bytes `R` was still holding come back with it.
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps a `std::io::Write`, retrying `write` on `Interrupted` and on
/// a partial write — the one piece of backend-specific knowledge
/// [`ScratchSink`] needs.
///
/// `std::io::Write::write_all` already does this internally, so this
/// could just delegate straight to it — but driving `retry_write_all`
/// here too, the same as `embedded_io`'s writer does (whose
/// `write_all` doesn't retry), keeps one shared retry loop doing the
/// work for both backends instead of one backend trusting its native
/// `write_all` and the other hand-rolling it.
struct StdWriter<W>(W);

impl<W: Write> RetryingWrite for StdWriter<W> {
    type Error = std::io::Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        retry_write_all(&mut self.0, W::write, buf, is_interrupted, || {
            std::io::ErrorKind::WriteZero.into()
        })
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

/// A `Sink` over `std::io::Write`, staging writes in an owned scratch
/// buffer.
pub struct StdSink<W, S>(ScratchSink<StdWriter<W>, S>);

impl<W: Write, S: AsMut<[u8]>> StdSink<W, S> {
    /// Build a `StdSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, buffer: S) -> Self {
        Self(ScratchSink::new(StdWriter(inner), buffer))
    }

    pub fn get_ref(&self) -> &W {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.0.get_mut().0
    }

    /// Reclaim the writer, discarding the scratch buffer and any bytes
    /// staged in it via `spare` but not yet handed to `commit` — they
    /// are not written to `inner`.
    pub fn into_inner(self) -> W {
        self.0.into_inner().0
    }

    /// Reclaim both the writer and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `StdSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        let (writer, buffer) = self.0.into_parts();
        (writer.0, buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for StdSink<W, S> {
    type Error = std::io::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        self.0.spare()
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.0.commit(amount)
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.0.finish()
    }
}

// The buffer/`spare`/`commit` bookkeeping, EOF handling, panics, and
// interrupt-retry mechanics are all transport-independent and tested
// once against `ScratchSource`/`LendingSource`/`ScratchSink` in
// `shared_io::source`/`shared_io::sink`/`shared_io::retry`. What's
// left to test here is genuinely backend-specific: does `is_interrupted`
// recognize `std::io`'s own `Interrupted` kind, and does a real
// `std::io::Read`/`BufRead`/`Write` actually get driven correctly
// end to end through `StdSource`/`BufReadSource`/`StdSink`.
#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor, Read, Write};

    use super::{is_interrupted, BufReadSource, StdSink, StdSource};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::{Sink, Source};

    #[test]
    fn is_interrupted_recognizes_the_std_io_kind() {
        assert!(is_interrupted(&std::io::Error::new(
            std::io::ErrorKind::Interrupted,
            "eintr"
        )));
        assert!(!is_interrupted(&std::io::Error::other("boom")));
    }

    /// Fails its first `read`/`write` with `Interrupted`, then
    /// delegates.
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

    impl<W: Write> Write for FlakyOnce<W> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if !self.failed {
                self.failed = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "eintr",
                ));
            }
            self.inner.write(buf)
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
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

    #[test]
    fn commit_retries_an_interrupted_write() {
        let flaky = FlakyOnce {
            inner: Vec::new(),
            failed: false,
        };
        let mut output = StdSink::new(flaky, [0u8; 8]);
        let spare = output.spare().unwrap().unwrap();
        spare[..5].copy_from_slice(b"abcde");
        output.commit(5).unwrap();
        assert_eq!(output.into_inner().inner, b"abcde");
    }

    #[test]
    fn std_source_feeds_std_sink_end_to_end() {
        let mut input = StdSource::new(Cursor::new(b"std to std".as_slice()), [0u8; 3]);
        let mut output = StdSink::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to std");
    }
}
