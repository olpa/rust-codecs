//! Generic `Source` engines shared by every `std::io`/`embedded_io`-style
//! backend. The scratch-buffer bookkeeping ([`ScratchSource`]) and the
//! zero-copy `BufRead`-style forwarding ([`LendingSource`]) are
//! identical across backends — only how a single `read`/`fill_buf`
//! call is made, and what that backend's own retry-on-interruption
//! looks like, differs. That's the one thing a backend supplies, via
//! [`RetryingRead`]/[`RetryingFillBuf`].

use crate::Source;

/// A backend's `read`, already retrying internally on whatever that
/// backend calls "interrupted" (`std::io::ErrorKind::Interrupted`,
/// `embedded_io::ErrorKind::Interrupted`, ...). The one piece of
/// backend-specific knowledge [`ScratchSource`] needs — everything
/// else (the scratch buffer, the `pos`/`len` bookkeeping) is
/// transport-independent.
pub trait RetryingRead {
    type Error;

    /// Read some bytes into `buf`, retrying on an interrupted call.
    /// `Ok(0)` means EOF, same as `std::io::Read::read`/
    /// `embedded_io::Read::read`.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error>;
}

/// A `Source` over any [`RetryingRead`], reading into an owned scratch
/// buffer. The transport-independent core of `std_io::StdSource` /
/// `embedded_io::EmbeddedSource`.
pub struct ScratchSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
}

impl<R: RetryingRead, S: AsMut<[u8]>> ScratchSource<R, S> {
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

impl<R: RetryingRead, S: AsMut<[u8]>> Source for ScratchSource<R, S> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.pos == self.len {
            self.len = self.inner.read(self.buffer.as_mut())?;
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

/// A backend's `BufRead`, with `fill_buf` already retrying internally
/// on that backend's notion of "interrupted". The one piece of
/// backend-specific knowledge [`LendingSource`] needs.
pub trait RetryingFillBuf {
    type Error;

    /// Return the contents of the internal buffer, filling it with
    /// more data from the inner reader if it is empty, and retrying
    /// on an interrupted call. An empty slice means EOF, same as
    /// `std::io::BufRead::fill_buf`/`embedded_io::BufRead::fill_buf`.
    fn fill_buf(&mut self) -> Result<&[u8], Self::Error>;

    /// Tell this buffer that `amount` bytes have been consumed.
    fn consume(&mut self, amount: usize);
}

/// A `Source` over any [`RetryingFillBuf`], with no scratch buffer of
/// its own — the transport-independent core of `std_io::BufReadSource`
/// / `embedded_io::BufReadSource`.
///
/// Unlike [`ScratchSource`], which owns a buffer and calls
/// `RetryingRead::read` into it, this forwards straight to
/// `fill_buf`/`consume` — `BufRead` is already a lending API with the
/// same shape as [`Source`], so a reader that already implements it
/// can be adapted with no extra copy.
pub struct LendingSource<R> {
    inner: R,
}

impl<R: RetryingFillBuf> LendingSource<R> {
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

impl<R: RetryingFillBuf> Source for LendingSource<R> {
    type Error = R::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        let buf = self.inner.fill_buf()?;
        Ok((!buf.is_empty()).then_some(buf))
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}
