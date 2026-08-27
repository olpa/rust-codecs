use super::retry::{retry_fill_buf, retry_on_interrupted};
use crate::Source;

/// A backend's raw, unretried `read`, plus how that backend's error
/// says "interrupted" (`std::io::ErrorKind::Interrupted`,
/// `embedded_io::ErrorKind::Interrupted`, ...).
pub trait EintrRead {
    type Error;

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;

    fn is_interrupted(err: &Self::Error) -> bool;
}

/// A `Source`, Eintr-reading into an owned scratch buffer.
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
    /// Panics on an empty `buffer`.
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
/// backend's error says "interrupted" (`std::io::ErrorKind::Interrupted`,
/// `embedded_io::ErrorKind::Interrupted`, ...).
pub trait EintrFillBuf {
    type Error;

    /// Like `std::io::BufRead::fill_buf`/`embedded_io::BufRead::fill_buf`.
    fn fill_buf(&mut self) -> Result<&[u8], Self::Error>;

    /// Whether `err` means "the call was interrupted, try again".
    fn is_interrupted(err: &Self::Error) -> bool;

    /// Like `std::io::BufRead::consume`/`embedded_io::BufRead::consume`.
    fn consume(&mut self, amount: usize);
}

/// A `Source`, Eintr-reading into the buffer of `inner`, so that
/// a `BufRead`-implementing reader can be adapted with no extra copy.
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

    /// A minimal [`EintrRead`]/[`EintrFillBuf`] over a borrowed
    /// byte slice — stands in for a real `std::io`/`embedded_io` reader
    /// when testing `ScratchSource`/`LendingSource`, which don't care
    /// which backend supplies bytes.
    struct SliceReader<'a>(&'a [u8]);

    impl<'a> EintrRead for SliceReader<'a> {
        type Error = std::io::Error;

        fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
            std::io::Read::read(&mut self.0, buf)
        }

        fn is_interrupted(err: &Self::Error) -> bool {
            err.kind() == std::io::ErrorKind::Interrupted
        }
    }

    impl<'a> EintrFillBuf for SliceReader<'a> {
        type Error = std::io::Error;

        fn fill_buf(&mut self) -> Result<&[u8], Self::Error> {
            std::io::BufRead::fill_buf(&mut self.0)
        }

        fn is_interrupted(err: &Self::Error) -> bool {
            err.kind() == std::io::ErrorKind::Interrupted
        }

        fn consume(&mut self, amount: usize) {
            std::io::BufRead::consume(&mut self.0, amount)
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
        let mut input = ScratchSource::new(SliceReader(b""), [0u8; 4]);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn partial_consume_leaves_remainder_visible_on_next_chunk() {
        let mut input = ScratchSource::new(SliceReader(b"abcdef"), [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(1);
        // The unconsumed remainder reappears, overlapping the previous
        // chunk — no new bytes were pulled in to produce it.
        assert_eq!(input.chunk().unwrap(), Some(b"bcd".as_slice()));
    }

    #[test]
    fn full_consume_triggers_a_refill() {
        let mut input = ScratchSource::new(SliceReader(b"abcdef"), [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        input.consume(4);
        assert_eq!(input.chunk().unwrap(), Some(b"ef".as_slice()));
    }

    #[test]
    fn repeated_chunk_without_consume_is_idempotent() {
        let mut input = ScratchSource::new(SliceReader(b"abcd"), [0u8; 4]);

        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
        assert_eq!(input.chunk().unwrap(), Some(b"abcd".as_slice()));
    }

    #[test]
    #[should_panic]
    fn consume_more_than_available_panics() {
        let mut input = ScratchSource::new(SliceReader(b"ab"), [0u8; 4]);
        input.chunk().unwrap();
        input.consume(3);
    }

    #[test]
    fn lending_source_forwards_to_fill_buf() {
        let mut input = LendingSource::new(SliceReader(b"hello"));
        assert_eq!(input.chunk().unwrap(), Some(b"hello".as_slice()));
        input.consume(5);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn lending_source_leaves_unconsumed_remainder_visible() {
        let mut input = LendingSource::new(SliceReader(b"abcdef"));
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
