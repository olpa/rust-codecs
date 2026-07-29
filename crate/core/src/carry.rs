//! [`Carry`]: the output-side counterpart of a codec's pending-input
//! buffer.
//!
//! The `Codec` contract requires every `process`/`finish` call to fully
//! consume its input or fully fill its output — a codec is never
//! allowed to decline a buffer as "too small". For codecs with a
//! minimum atomic output unit (base64 can only emit whole 4-byte
//! encoded groups) that means an emitted unit must be able to *span*
//! output buffers: write what fits now, hold the tail, deliver it at
//! the start of the next call. `Carry` is that hold, written once here
//! so each codec doesn't re-implement the suspend/resume bookkeeping.
//!
//! Usage pattern inside a codec (`N` = the codec's largest atomic
//! output unit, known statically):
//!
//! 1. First thing in `process`/`finish`/`flush`:
//!    `out_pos += carry.drain(output)`. If the carry is still
//!    non-empty, the output is now full — return `OutputFilled`.
//! 2. To emit a unit: render it into a scratch array, then
//!    `out_pos += carry.emit(&scratch, &mut output[out_pos..])`. If
//!    the carry is non-empty afterward, the output is full — return
//!    `OutputFilled`.

/// Holds the tail of an atomic output unit that didn't fit the
/// caller's output buffer, to be delivered first on the next call.
#[derive(Debug, Clone)]
pub struct Carry<const N: usize> {
    buf: [u8; N],
    pos: usize,
    len: usize,
}

impl<const N: usize> Carry<N> {
    pub const fn new() -> Self {
        Self { buf: [0; N], pos: 0, len: 0 }
    }

    /// Whether nothing is currently held.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.len
    }

    /// Copy held bytes into the front of `out`; returns how many were
    /// copied. After this, either the carry is empty or `out` is full.
    pub fn drain(&mut self, out: &mut [u8]) -> usize {
        let take = (self.len - self.pos).min(out.len());
        out[..take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
        self.pos += take;
        if self.pos >= self.len {
            self.pos = 0;
            self.len = 0;
        }
        take
    }

    /// Write `unit` through `out`, holding whatever doesn't fit;
    /// returns how many bytes landed in `out`. After this, either the
    /// carry is empty (the unit fit) or `out` is full.
    ///
    /// The carry must be empty on entry (drain it first), and `unit`
    /// must be at most `N` bytes — both are codec bugs otherwise, and
    /// both panic.
    pub fn emit(&mut self, unit: &[u8], out: &mut [u8]) -> usize {
        assert!(self.is_empty(), "Carry::emit called with bytes still held");
        let take = unit.len().min(out.len());
        out[..take].copy_from_slice(&unit[..take]);
        let rest = &unit[take..];
        self.buf[..rest.len()].copy_from_slice(rest);
        self.pos = 0;
        self.len = rest.len();
        take
    }
}

impl<const N: usize> Default for Carry<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::Carry;

    #[test]
    fn unit_that_fits_leaves_carry_empty() {
        let mut carry = Carry::<4>::new();
        let mut out = [0u8; 8];
        let n = carry.emit(b"abcd", &mut out);
        assert_eq!(n, 4);
        assert_eq!(&out[..4], b"abcd");
        assert!(carry.is_empty());
    }

    #[test]
    fn unit_spans_three_buffers() {
        let mut carry = Carry::<4>::new();
        let mut collected = Vec::new();

        let mut out = [0u8; 1];
        let n = carry.emit(b"abcd", &mut out);
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
        let n = carry.emit(b"abcd", &mut []);
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
    #[should_panic(expected = "bytes still held")]
    fn emit_with_held_bytes_panics() {
        let mut carry = Carry::<4>::new();
        carry.emit(b"abcd", &mut []);
        carry.emit(b"efgh", &mut []);
    }
}
