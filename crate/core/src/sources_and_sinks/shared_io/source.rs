use super::retry::{retry_fill_buf, retry_on_interrupted};
use crate::Source;

/// A backend's raw, unretried `read`, plus how that backend's error
/// says "interrupted" (`std::io::ErrorKind::Interrupted`,
/// `embedded_io::ErrorKind::Interrupted`, ...).
pub trait EintrRead {
    type Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;

    /// Whether `err` means "the call was interrupted, try again".
    fn is_interrupted(err: &Self::Error) -> bool;
}

/// A `Source` over any [`EintrRead`], reading into an owned scratch
/// buffer. The transport-independent core of `std_io::StdSource` /
/// `embedded_io::EmbeddedSource`.
pub struct ScratchSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
}

impl<R: EintrRead, S: AsMut<[u8]>> ScratchSource<R, S> {
    /// Build a `ScratchSource`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte read
    /// from `inner`, so `chunk` could never return anything.
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "ScratchSource buffer must be non-empty"
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
    /// the buffer's allocation for another `ScratchSource`. Any
    /// buffered, unconsumed bytes (already read from `inner` into
    /// the buffer via `chunk`, but not yet passed to `consume`) are
    /// discarded along with them.
    pub fn into_parts(self) -> (R, S) {
        (self.inner, self.buffer)
    }
}

impl<R: EintrRead, S: AsMut<[u8]>> Source for ScratchSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.pos == self.len {
            self.len =
                retry_on_interrupted(|| self.inner.read(self.buffer.as_mut()), R::is_interrupted)?;
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

/// A backend's raw, unretried `BufRead::fill_buf`, plus how that
/// backend's error says "interrupted" — the two pieces of
/// backend-specific knowledge [`LendingSource`] needs to retry a call
/// itself via [`retry_fill_buf`].
pub trait EintrFillBuf {
    type Error;

    /// Return the contents of the internal buffer, filling it with
    /// more data from the inner reader if it is empty, without
    /// retrying. An empty slice means EOF, same as
    /// `std::io::BufRead::fill_buf`/`embedded_io::BufRead::fill_buf`.
    fn fill_buf(&mut self) -> Result<&[u8], Self::Error>;

    /// Whether `err` means "the call was interrupted, try again".
    fn is_interrupted(err: &Self::Error) -> bool;

    /// Tell this buffer that `amount` bytes have been consumed.
    fn consume(&mut self, amount: usize);
}

/// A `Source` over any [`EintrFillBuf`], with no scratch buffer of
/// its own — the transport-independent core of `std_io::BufReadSource`
/// / `embedded_io::BufReadSource`.
///
/// Unlike [`ScratchSource`], which owns a buffer and calls
/// `EintrRead::read` into it, this forwards straight to
/// `fill_buf`/`consume` — `BufRead` is already a lending API with the
/// same shape as [`Source`], so a reader that already implements it
/// can be adapted with no extra copy.
pub struct LendingSource<R> {
    inner: R,
}

impl<R: EintrFillBuf> LendingSource<R> {
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Return the wrapped reader. Unlike `ScratchSource::into_inner`,
    /// nothing is lost: `LendingSource` owns no scratch buffer of its
    /// own, so any bytes `R` was still holding come back with it.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: EintrFillBuf> Source for LendingSource<R> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        let buf = retry_fill_buf(&mut self.inner, R::fill_buf, R::is_interrupted)?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}

#[cfg(test)]
mod tests {
    use super::{EintrFillBuf, EintrRead, LendingSource, ScratchSource};
    use crate::Source;
    use core::convert::Infallible;

    /// Wraps a reader, counting how many times `read` was actually
    /// called on it — lets a test prove `ScratchSource::chunk` didn't
    /// refill ahead of its consumed position (the `Source` contract
    /// point that new bytes must not be handed out until the old ones
    /// are released via `consume`).
    struct CountingReader<R> {
        inner: R,
        reads: usize,
    }

    impl<R: EintrRead> EintrRead for CountingReader<R> {
        type Error = R::Error;

        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            self.reads += 1;
            self.inner.read(buf)
        }

        fn is_interrupted(err: &Self::Error) -> bool {
            R::is_interrupted(err)
        }
    }

    /// A minimal [`EintrRead`]/[`EintrFillBuf`] over a borrowed
    /// byte slice — stands in for a real `std::io`/`embedded_io` reader
    /// when testing `ScratchSource`/`LendingSource`, which don't care
    /// which backend supplies bytes.
    struct SliceReader<'a> {
        bytes: &'a [u8],
    }

    impl<'a> EintrRead for SliceReader<'a> {
        type Error = Infallible;

        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            let n = buf.len().min(self.bytes.len());
            buf[..n].copy_from_slice(&self.bytes[..n]);
            self.bytes = &self.bytes[n..];
            Ok(n)
        }

        fn is_interrupted(_err: &Self::Error) -> bool {
            false
        }
    }

    impl<'a> EintrFillBuf for SliceReader<'a> {
        type Error = Infallible;

        fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            Ok(self.bytes)
        }

        fn is_interrupted(_err: &Self::Error) -> bool {
            false
        }

        fn consume(&mut self, amount: usize) {
            self.bytes = &self.bytes[amount..];
        }
    }

    /// A [`EintrFillBuf`] that yields `b"hi"`, then an empty fill,
    /// then `b"more"` — stands in for a transport whose "nothing right
    /// now" isn't forever (a growing file, a pipe), proving a
    /// `LendingSource` doesn't latch itself shut after a single empty
    /// fill.
    #[derive(Default)]
    struct GrowsAfterAnEmptyFill {
        stage: usize,
        buf: &'static [u8],
    }

    impl EintrFillBuf for GrowsAfterAnEmptyFill {
        type Error = Infallible;

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

        fn is_interrupted(_err: &Self::Error) -> bool {
            false
        }

        fn consume(&mut self, amount: usize) {
            self.buf = &self.buf[amount..];
        }
    }

    #[test]
    fn chunk_returns_none_at_genuine_eof() {
        let mut input = ScratchSource::new(SliceReader { bytes: b"" }, [0u8; 4]);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn partial_consume_leaves_remainder_visible_on_next_chunk() {
        let reader = CountingReader {
            inner: SliceReader { bytes: b"abcdef" },
            reads: 0,
        };
        let mut input = ScratchSource::new(reader, [0u8; 4]);

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
            inner: SliceReader { bytes: b"abcdef" },
            reads: 0,
        };
        let mut input = ScratchSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(4);
        assert_eq!(input.chunk().unwrap(), Some(b"ef".as_slice()));
        assert_eq!(input.get_ref().reads, 2);
    }

    #[test]
    fn repeated_chunk_without_consume_is_idempotent() {
        let reader = CountingReader {
            inner: SliceReader { bytes: b"abcd" },
            reads: 0,
        };
        let mut input = ScratchSource::new(reader, [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.get_ref().reads, 1);
    }

    #[test]
    #[should_panic]
    fn consume_more_than_available_panics() {
        let mut input = ScratchSource::new(SliceReader { bytes: b"ab" }, [0u8; 4]);
        input.chunk().unwrap();
        input.consume(3);
    }

    #[test]
    fn lending_source_forwards_to_fill_buf() {
        let mut input = LendingSource::new(SliceReader { bytes: b"hello" });
        assert_eq!(input.chunk().unwrap(), Some(b"hello".as_slice()));
        input.consume(5);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn lending_source_leaves_unconsumed_remainder_visible() {
        let mut input = LendingSource::new(SliceReader { bytes: b"abcdef" });
        assert_eq!(input.chunk().unwrap(), Some(b"abcdef".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), Some(b"cdef".as_slice()));
    }

    #[test]
    fn lending_source_revisits_the_wrapped_reader_after_an_empty_fill() {
        let mut input = LendingSource::new(GrowsAfterAnEmptyFill::default());
        assert_eq!(input.chunk().unwrap(), Some(b"hi".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), None);
        assert_eq!(input.chunk().unwrap(), Some(b"more".as_slice()));
    }
}
