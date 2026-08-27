use embedded_io::{BufRead, ErrorKind, Read, Write};

use crate::sources_and_sinks::shared_io::{
    retry_write_all, EintrFillBuf, EintrRead, LendingSource, RetryingWrite, ScratchSink,
    ScratchSource,
};
use crate::{Sink, Source};

fn is_interrupted<E: embedded_io::Error>(e: &E) -> bool {
    e.kind() == ErrorKind::Interrupted
}

/// Wraps an `embedded_io::Read`, recognizing `Interrupted` — the one
/// piece of backend-specific knowledge [`ScratchSource`] needs to
/// retry `read` itself.
struct EmbeddedReader<R>(R);

impl<R: Read> EintrRead for EmbeddedReader<R> {
    type Error = R::Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.0.read(buf)
    }

    fn is_interrupted(err: &Self::Error) -> bool {
        is_interrupted(err)
    }
}

/// A `Source` over `embedded_io::Read`, reading into an owned scratch
/// buffer.
pub struct EmbeddedSource<R, S>(ScratchSource<EmbeddedReader<R>, S>);

impl<R: Read, S: AsMut<[u8]>> EmbeddedSource<R, S> {
    /// Build an `EmbeddedSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte read
    /// from `inner`, so `chunk` could never return anything.
    pub fn new(inner: R, buffer: S) -> Self {
        Self(ScratchSource::new(EmbeddedReader(inner), buffer))
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
    /// the buffer's allocation for another `EmbeddedSource`. Any
    /// buffered, unconsumed bytes (already read from `inner` into
    /// the buffer via `chunk`, but not yet passed to `consume`) are
    /// discarded along with them.
    pub fn into_parts(self) -> (R, S) {
        let (reader, buffer) = self.0.into_parts();
        (reader.0, buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for EmbeddedSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps an `embedded_io::BufRead`, recognizing `Interrupted` — the
/// one piece of backend-specific knowledge [`LendingSource`] needs to
/// retry `fill_buf` itself.
struct EmbeddedBufReader<R>(R);

impl<R: BufRead> EintrFillBuf for EmbeddedBufReader<R> {
    type Error = R::Error;

    fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        self.0.fill_buf()
    }

    fn is_interrupted(err: &Self::Error) -> bool {
        is_interrupted(err)
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
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
pub struct BufReadSource<R>(LendingSource<EmbeddedBufReader<R>>);

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self(LendingSource::new(EmbeddedBufReader(inner)))
    }

    pub fn get_ref(&self) -> &R {
        &self.0.get_ref().0
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.0.get_mut().0
    }

    /// Return the wrapped reader. Unlike `EmbeddedSource::into_inner`,
    /// nothing is lost: `BufReadSource` owns no scratch buffer of its
    /// own, so any bytes `R` was still holding come back with it.
    pub fn into_inner(self) -> R {
        self.0.into_inner().0
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        self.0.chunk()
    }

    fn consume(&mut self, amount: usize) {
        self.0.consume(amount);
    }
}

/// Wraps an `embedded_io::Write`, retrying `write` on `Interrupted`
/// and on a partial write — the one piece of backend-specific
/// knowledge [`ScratchSink`] needs. Unlike `std::io::Write::write_all`,
/// `embedded_io::Write::write_all` doesn't do this itself, which is
/// why this backend uses the shared `retry_write_all` helper.
struct EmbeddedWriter<W>(W);

impl<W: Write> RetryingWrite for EmbeddedWriter<W> {
    type Error = W::Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
        retry_write_all(&mut self.0, W::write, buf, is_interrupted, || {
            unreachable!("embedded_io::Write::write must not return Ok(0) for non-empty input")
        })
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

/// A `Sink` over `embedded_io::Write`, staging writes in an owned
/// scratch buffer.
pub struct EmbeddedSink<W, S>(ScratchSink<EmbeddedWriter<W>, S>);

impl<W: Write, S: AsMut<[u8]>> EmbeddedSink<W, S> {
    /// Build an `EmbeddedSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, buffer: S) -> Self {
        Self(ScratchSink::new(EmbeddedWriter(inner), buffer))
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
    /// the buffer's allocation for another `EmbeddedSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        let (writer, buffer) = self.0.into_parts();
        (writer.0, buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for EmbeddedSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        self.0.spare()
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.0.commit(amount)
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.0.finish()
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.0.flush()
    }
}

// The buffer/`spare`/`commit` bookkeeping, EOF handling, panics, and
// interrupt-retry mechanics are all transport-independent and tested
// once against `ScratchSource`/`LendingSource`/`ScratchSink` in
// `shared_io::source`/`shared_io::sink`/`shared_io::retry`. What's
// left to test here is genuinely backend-specific: does `is_interrupted`
// recognize `embedded_io`'s own `Interrupted` kind, and does a real
// `embedded_io::Read`/`BufRead`/`Write` actually get driven correctly
// end to end through `EmbeddedSource`/`BufReadSource`/`EmbeddedSink`.
#[cfg(test)]
mod tests {
    use embedded_io::{BufRead, ErrorKind, ErrorType, Read, Write};

    use super::{is_interrupted, BufReadSource, EmbeddedSink, EmbeddedSource};
    use crate::identity::identity;
    use crate::stream_to_stream;
    use crate::{Sink, Source};

    #[test]
    fn is_interrupted_recognizes_the_embedded_io_kind() {
        assert!(is_interrupted(&ErrorKind::Interrupted));
        assert!(!is_interrupted(&ErrorKind::Other));
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
    fn buf_read_source_retries_an_interrupted_fill() {
        let flaky = FlakyOnce {
            inner: &b"retry me too"[..],
            failed: false,
        };
        let mut input = BufReadSource::new(flaky);
        assert_eq!(input.chunk().unwrap(), Some(b"retry me too".as_slice()));
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
