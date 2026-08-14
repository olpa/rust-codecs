//! Lending byte-stream endpoint contracts used by codec drivers.

/// A byte source which lends its current input chunk to the pump.
///
/// See `CREATING-IO-BACKENDS.md` in the repository root for how to
/// implement one for a new transport.
pub trait Source {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    ///
    /// "Current" is load-bearing: this is whatever hasn't been
    /// released by `consume` yet, not necessarily fresh bytes. A
    /// caller is never required to consume a whole chunk in one call
    /// (a codec may only take part of it, e.g. when output runs out
    /// first) — the unconsumed remainder is exactly what the next
    /// `chunk()` call returns, so consecutive chunks can overlap.
    /// Implementations must not hand out new bytes ahead of `pos`
    /// until the old ones are released.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Release the first `amount` bytes of the current chunk.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the pump.
///
/// See `CREATING-IO-BACKENDS.md` in the repository root for how to
/// implement one for a new transport.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    ///
    /// A caller is never required to commit any of it before calling
    /// `spare` again — an uncommitted call may simply be re-issued,
    /// returning the same (or an equivalent) span.
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}
