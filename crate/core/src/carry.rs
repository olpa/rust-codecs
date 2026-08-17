//! [`Carry`]: a helper for codec authors whose codec can generate more
//! output at once than the output buffer has room for.
//!
//! Usage pattern inside a codec:
//!
//! 1. First thing in `process`/`finish`/`flush`:
//!    `out_pos += carry.drain(output)`. If the carry is still
//!    non-empty, the output is now full, return `OutputFilled`.
//! 2. Choose the largest prefix `input` of whole transform units for
//!    which encoding `input` fits in the remaining output. Encode it
//!    directly into that output.
//! 3. If encoding `input` underfilled the output, but encoding
//!    `input + one unit` would overfill it:
//!
//!    - Encode the extra unit into [`Carry::buffer`].
//!    - Call [`Carry::set_len`] with the rendered length.
//!    - Drain into the remaining output.
//!
//! For example, base64 encoding maps 3 input bytes to 4 output bytes.
//! With 10 output bytes available:
//!
//! - Encode two groups (6 input bytes to 8 output bytes) directly into
//!   `output`.
//! - Encode the next group into the 4-byte carry buffer.
//! - Drain 2 bytes into `output` and retain the last 2.

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

    /// Storage for rendering the next atomic output unit. The carry
    /// must have been completely drained first. After rendering, call
    /// [`set_len`](Self::set_len) with the number of bytes produced.
    pub fn buffer(&mut self) -> Result<&mut [u8], CarryError> {
        if !self.is_empty() {
            return Err(CarryError::NotDrained);
        }
        self.pos = 0;
        self.len = 0;
        Ok(&mut self.buf)
    }

    /// Make the first `len` rendered bytes available to [`Self::drain`].
    pub fn set_len(&mut self, len: usize) -> Result<(), CarryError> {
        if len > N {
            return Err(CarryError::OutputTooLarge);
        }
        self.len = len;
        Ok(())
    }
}

/// Why staging an output unit was refused — always a codec bug, never
/// a condition caused by the data being processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarryError {
    /// [`Carry::buffer`] was called while a previous unit was still held —
    /// `drain` must be called (and the carry found empty) first.
    NotDrained,
    /// [`Carry::set_len`] reported more output than capacity `N`.
    OutputTooLarge,
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

    fn fill(carry: &mut Carry<4>, bytes: &[u8]) -> Result<(), CarryError> {
        carry.buffer()?[..bytes.len()].copy_from_slice(bytes);
        carry.set_len(bytes.len())
    }

    #[test]
    fn chunk_that_fits_leaves_carry_empty() {
        let mut carry = Carry::<4>::new();
        let mut out = [0u8; 8];
        fill(&mut carry, b"abcd").unwrap();
        let n = carry.drain(&mut out);
        assert_eq!(n, 4);
        assert_eq!(&out[..4], b"abcd");
        assert!(carry.is_empty());
    }

    #[test]
    fn chunk_spans_three_buffers() {
        let mut carry = Carry::<4>::new();
        let mut collected = Vec::new();

        let mut out = [0u8; 1];
        fill(&mut carry, b"abcd").unwrap();
        let n = carry.drain(&mut out);
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
    fn empty_drain_holds_everything() {
        let mut carry = Carry::<4>::new();
        fill(&mut carry, b"abcd").unwrap();
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
    fn buffer_with_held_bytes_is_an_error() {
        let mut carry = Carry::<4>::new();
        fill(&mut carry, b"abcd").unwrap();
        assert!(matches!(carry.buffer(), Err(CarryError::NotDrained)));
    }

    #[test]
    fn committed_output_larger_than_capacity_is_an_error() {
        let mut carry = Carry::<4>::new();
        assert_eq!(carry.set_len(5), Err(CarryError::OutputTooLarge));
    }

    #[test]
    fn unused_buffer_leaves_carry_empty() {
        let mut carry = Carry::<4>::new();
        carry.buffer().unwrap()[0] = b'x';
        assert!(carry.is_empty());
    }
}
