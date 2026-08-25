//! Generic `Sink` engine shared by every `std::io`/`embedded_io`-style
//! backend: the scratch-buffer/`spare`/`commit` bookkeeping
//! ([`ScratchSink`]) is identical across backends — only how a full
//! buffer actually gets written out, and what that backend's own
//! retry-on-interruption/partial-write looks like, differs. That's
//! the one thing a backend supplies, via [`RetryingWrite`].

use crate::Sink;

/// A backend's "write this whole buffer out", already retrying
/// internally on partial writes and on whatever that backend calls
/// "interrupted". The one piece of backend-specific knowledge
/// [`ScratchSink`] needs.
///
/// `std::io::Write::write_all` already retries on `Interrupted`
/// internally, so a `std::io` backend could delegate `write_all`
/// straight through; `embedded_io::Write::write_all` doesn't, so an
/// `embedded_io` backend's `retrying_write_all` must track its own
/// write position and retry the remainder itself. Both backends drive
/// the same shared `retry_write_all` helper regardless, so `commit`
/// below can trust this call already retries — hence the name.
pub trait RetryingWrite {
    type Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
}

/// A `Sink` over any [`RetryingWrite`], staging writes in an owned
/// scratch buffer. The transport-independent core of
/// `std_io::StdSink` / `embedded_io::EmbeddedSink`.
pub struct ScratchSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: RetryingWrite, S: AsMut<[u8]>> ScratchSink<W, S> {
    /// Build a `ScratchSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "ScratchSink buffer must be non-empty"
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
    /// the buffer's allocation for another `ScratchSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: RetryingWrite, S: AsMut<[u8]>> Sink for ScratchSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        let buf = self.buffer.as_mut();
        self.offered = buf.len();
        Ok(Some(buf))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        self.inner.retrying_write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}
