//! [`Carry`]: a helper for codec authors whose codec can generate more
//! output at once than the output buffer has room for.
//!
//! The `Codec` contract requires every `process`/`finish` call to
//! fully consume its input or fully fill its output. A codec is
//! never allowed to decline a buffer as "too small".
//!
//! So when a call generates more output than the buffer has room
//! left, the codec must still write what fits, then hold the rest
//! until the next call in `Carry`.
//!
//! Usage pattern inside a codec:
//!
//! 1. First thing in `process`/`finish`/`flush`:
//!    `out_pos += carry.drain(output)`. If the carry is still
//!    non-empty, the output is now full, return `OutputFilled`.
//! 2. To emit a chunk: render it into a scratch array, then
//!    `out_pos += carry.emit(&scratch, &mut output[out_pos..]).map_err(...)?`,
//!    mapping [`CarryError`] into the codec's own error type — both
//!    variants mean the codec itself is buggy, but a codec may run as
//!    a third-party plugin, and a host loading it has no way to
//!    recover from a panic. If the carry is non-empty afterward, the
//!    output is full — return
//!    `OutputFilled`.

/// Holds the tail of an output chunk that didn't fit the caller's
/// output buffer, to be delivered first on the next call.
#[derive(Debug, Clone)]
pub struct Carry<const N: usize> {
    buf: [u8; N],
    pos: usize,
    len: usize,
}

impl<const N: usize> Carry<N> {
    pub const fn new() -> Self {
        Self {
            buf: [0; N],
            pos: 0,
            len: 0,
        }
    }

    /// Whether nothing is currently held.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.len
    }

    /// Copy held bytes into the front of `out`; returns how many were
    /// copied. After this, either the carry is empty or `out` is full.
    pub fn drain(&mut self, out: &mut [u8]) -> usize {
        if self.is_empty() {
            return 0;
        }
        let take = (self.len - self.pos).min(out.len());
        out[..take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
        self.pos += take;
        take
    }

    /// Write `chunk` through `out`, holding whatever doesn't fit;
    /// returns how many bytes landed in `out`. After this, either the
    /// carry is empty (the chunk fit) or `out` is full.
    ///
    /// The carry must be empty on entry (drain it first), and `chunk`
    /// must be at most `N` bytes: both are codec bugs otherwise, and
    /// both are reported as [`CarryError`] rather than panicking.
    pub fn emit(&mut self, chunk: &[u8], out: &mut [u8]) -> Result<usize, CarryError> {
        if !self.is_empty() {
            return Err(CarryError::NotDrained);
        }
        if chunk.len() > N {
            return Err(CarryError::ChunkTooLarge);
        }
        let take = chunk.len().min(out.len());
        out[..take].copy_from_slice(&chunk[..take]);
        let rest = &chunk[take..];
        self.buf[..rest.len()].copy_from_slice(rest);
        self.pos = 0;
        self.len = rest.len();
        Ok(take)
    }
}

/// Why [`Carry::emit`] was refused — always a codec bug, never a
/// condition caused by the data being processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarryError {
    /// `emit` was called while a previous chunk was still held —
    /// `drain` must be called (and the carry found empty) first.
    NotDrained,
    /// `chunk` was longer than the carry's capacity `N`.
    ChunkTooLarge,
}

impl<const N: usize> Default for Carry<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use alloc::vec::Vec;

    use super::{Carry, CarryError};

    #[test]
    fn chunk_that_fits_leaves_carry_empty() {
        let mut carry = Carry::<4>::new();
        let mut out = [0u8; 8];
        let n = carry.emit(b"abcd", &mut out).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&out[..4], b"abcd");
        assert!(carry.is_empty());
    }

    #[test]
    fn chunk_spans_three_buffers() {
        let mut carry = Carry::<4>::new();
        let mut collected = Vec::new();

        let mut out = [0u8; 1];
        let n = carry.emit(b"abcd", &mut out).unwrap();
        assert_eq!(n, 1);
        collected.extend_from_slice(&out[..n]);
        assert!(!carry.is_empty());

        let mut out = [0u8; 2];
        let n = carry.drain(&mut out);
        assert_eq!(n, 2);
        collected.extend_from_slice(&out[..n]);
        assert!(!carry.is_empty());

        let mut out = [0u8; 8];
        let n = carry.drain(&mut out);
        assert_eq!(n, 1);
        collected.extend_from_slice(&out[..n]);
        assert!(carry.is_empty());

        assert_eq!(collected, b"abcd");
    }

    #[test]
    fn emit_into_empty_out_holds_everything() {
        let mut carry = Carry::<4>::new();
        let n = carry.emit(b"abcd", &mut []).unwrap();
        assert_eq!(n, 0);
        assert!(!carry.is_empty());
        let mut out = [0u8; 4];
        assert_eq!(carry.drain(&mut out), 4);
        assert_eq!(&out, b"abcd");
    }

    #[test]
    fn drain_on_empty_carry_is_a_no_op() {
        let mut carry = Carry::<4>::new();
        let mut out = [0u8; 4];
        assert_eq!(carry.drain(&mut out), 0);
        assert!(carry.is_empty());
    }

    #[test]
    fn emit_with_held_bytes_is_an_error() {
        let mut carry = Carry::<4>::new();
        carry.emit(b"abcd", &mut []).unwrap();
        assert_eq!(carry.emit(b"efgh", &mut []), Err(CarryError::NotDrained));
    }

    #[test]
    fn emit_chunk_larger_than_capacity_is_an_error() {
        let mut carry = Carry::<4>::new();
        assert_eq!(
            carry.emit(b"abcde", &mut []),
            Err(CarryError::ChunkTooLarge)
        );
    }
}
