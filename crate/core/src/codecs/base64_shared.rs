//! Constants and helpers shared by [`super::base64_enc`] and
//! [`super::base64_dec`].
//!
//! [`PendingInput`] and [`PendingOutput`] are a symmetric pair, one for
//! each side of the transform: `PendingOutput` holds output that was
//! produced but didn't fit the caller's buffer, dribbling it out over
//! later calls; `PendingInput` holds input that arrived but wasn't
//! enough to form a whole transform unit yet, topped up over later
//! calls until there's enough to consume as one group.
//!
//! Going through either is the exception, not the main path: the bulk
//! transform in `process` reads straight from the caller's
//! `input`/`output` slices without touching them. Only when a call
//! starts (or ends) with a leftover partial group does `PendingInput`
//! come into play — check [`PendingInput::is_empty`] first, and only
//! call [`PendingInput::fill`]/[`PendingInput::take`] when it isn't.
//! `PendingOutput` is checked the same way, via
//! [`PendingOutput::is_empty`].

// 3 bytes (24 bits) = four 6-bit groups, always — this ratio is part
// of the base64 algorithm itself, not a detail of any one alphabet or
// padding config. It's also a documented requirement of every `Engine`
// impl (see `Engine::internal_decode`'s doc: "each complete 4-byte
// chunk of encoded data decodes to 3 bytes"), so these constants hold
// no matter which `Engine` a caller plugs in via `with_engine`.
pub(super) const GROUP: usize = 3;
pub(super) const ENCODED_GROUP: usize = 4;

/// Holds a transform-unit's worth of input that arrived incomplete, to
/// be topped up and consumed on a later call. See the module docs for
/// how this relates to [`PendingOutput`].
#[derive(Debug, Clone)]
pub(super) struct PendingInput<const N: usize> {
    buf: [u8; N],
    len: usize,
}

impl<const N: usize> PendingInput<N> {
    pub(super) const fn new() -> Self {
        Self {
            buf: [0; N],
            len: 0,
        }
    }

    /// Whether nothing is currently held. The common case (no leftover
    /// from the previous call) never touches the rest of this type —
    /// callers check this first and only reach for `fill`/`take` when
    /// it's `false`.
    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Whether a full unit (`N` bytes) is currently held.
    pub(super) fn is_full(&self) -> bool {
        self.len == N
    }

    /// The bytes held so far, for inspection (e.g. the decoder checking
    /// for padding) before deciding whether to [`Self::take`] them.
    pub(super) fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }

    /// Copy as much of `input` as fits into the remaining room,
    /// advance the held length, and return how many bytes were taken
    /// so the caller can advance its own `in_pos`.
    pub(super) fn fill(&mut self, input: &[u8]) -> usize {
        let take = (N - self.len).min(input.len());
        self.buf[self.len..self.len + take].copy_from_slice(&input[..take]);
        self.len += take;
        take
    }

    /// Replace the held bytes with `tail` (0 to `N` bytes) for the
    /// next call. Used both for a genuine leftover shorter than `N`
    /// and for a full `N`-byte group the decoder must defer (pending a
    /// padding/end-of-stream check).
    pub(super) fn set(&mut self, tail: &[u8]) {
        debug_assert!(tail.len() <= N);
        self.buf[..tail.len()].copy_from_slice(tail);
        self.len = tail.len();
    }

    /// Take the held bytes as `[u8; N]` and reset to empty. Callers
    /// only do this once [`Self::is_full`] holds — a codec bug
    /// otherwise, so this debug-asserts rather than returning `Result`.
    pub(super) fn take(&mut self) -> [u8; N] {
        debug_assert!(self.is_full());
        self.len = 0;
        self.buf
    }

    /// Take whatever is currently held (0 to `N` bytes) as an owned
    /// buffer paired with how many of its leading bytes are valid, and
    /// reset to empty. Unlike [`Self::take`], valid on a partial (not
    /// full) hold — used by `finish` to encode/decode a final short
    /// group without holding a borrow of `self` across the call.
    pub(super) fn take_partial(&mut self) -> ([u8; N], usize) {
        let len = self.len;
        self.len = 0;
        (self.buf, len)
    }
}

/// Holds the tail of an output chunk that didn't fit the caller's
/// output buffer, to be delivered first on the next call. See the
/// module docs for how this relates to [`PendingInput`].
///
/// Usage pattern inside `process`/`finish`/`flush`:
///
/// 1. First thing: `out_pos += pending_output.drain(output)`. If it's
///    still non-empty, the output is now full — return `OutputFilled`.
/// 2. Choose the largest prefix of `input` made of whole transform
///    units for which encoding it fits in the remaining output. Encode
///    it directly into that output.
/// 3. If that underfilled the output, but encoding one more unit would
///    overfill it: encode the extra unit into
///    [`PendingOutput::buffer`], call [`PendingOutput::set_len`] with
///    the rendered length, then drain into the remaining output.
#[derive(Debug, Clone)]
pub(super) struct PendingOutput<const N: usize> {
    buf: [u8; N],
    pos: usize,
    len: usize,
}

impl<const N: usize> PendingOutput<N> {
    pub(super) const fn new() -> Self {
        Self {
            buf: [0; N],
            pos: 0,
            len: 0,
        }
    }

    /// Whether nothing is currently held.
    pub(super) fn is_empty(&self) -> bool {
        self.pos >= self.len
    }

    /// Copy held bytes into the front of `out`; returns how many were
    /// copied. After this, either this is empty or `out` is full.
    pub(super) fn drain(&mut self, out: &mut [u8]) -> usize {
        if self.is_empty() {
            return 0;
        }
        let take = (self.len - self.pos).min(out.len());
        out[..take].copy_from_slice(&self.buf[self.pos..self.pos + take]);
        self.pos += take;
        take
    }

    /// Storage for rendering the next atomic output unit. Must have
    /// been completely drained first. After rendering, call
    /// [`set_len`](Self::set_len) with the number of bytes produced.
    pub(super) fn buffer(&mut self) -> Result<&mut [u8], PendingOutputError> {
        if !self.is_empty() {
            return Err(PendingOutputError::NotDrained);
        }
        self.pos = 0;
        self.len = 0;
        Ok(&mut self.buf)
    }

    /// Make the first `len` rendered bytes available to [`Self::drain`].
    pub(super) fn set_len(&mut self, len: usize) -> Result<(), PendingOutputError> {
        if len > N {
            return Err(PendingOutputError::OutputTooLarge);
        }
        self.len = len;
        Ok(())
    }
}

/// Why staging an output unit was refused — always a codec bug, never
/// a condition caused by the data being processed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingOutputError {
    /// [`PendingOutput::buffer`] was called while a previous unit was
    /// still held — `drain` must be called (and found empty) first.
    NotDrained,
    /// [`PendingOutput::set_len`] reported more output than capacity `N`.
    OutputTooLarge,
}

impl<const N: usize> Default for PendingOutput<N> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::{PendingOutput, PendingOutputError};

    fn fill(pending: &mut PendingOutput<4>, bytes: &[u8]) -> Result<(), PendingOutputError> {
        pending.buffer()?[..bytes.len()].copy_from_slice(bytes);
        pending.set_len(bytes.len())
    }

    #[test]
    fn drains_across_output_buffers() {
        let mut pending = PendingOutput::<4>::new();
        let mut output = [0u8; 4];

        fill(&mut pending, b"abcd").unwrap();
        assert_eq!(pending.drain(&mut []), 0);
        assert_eq!(pending.drain(&mut output[..1]), 1);
        assert!(!pending.is_empty());
        assert_eq!(pending.drain(&mut output[1..3]), 2);
        assert!(!pending.is_empty());
        assert_eq!(pending.drain(&mut output[3..]), 1);
        assert!(pending.is_empty());
        assert_eq!(&output, b"abcd");
    }

    #[test]
    fn buffer_with_held_bytes_is_an_error() {
        let mut pending = PendingOutput::<4>::new();
        fill(&mut pending, b"abcd").unwrap();
        assert!(matches!(
            pending.buffer(),
            Err(PendingOutputError::NotDrained)
        ));
    }

    #[test]
    fn committed_output_larger_than_capacity_is_an_error() {
        let mut pending = PendingOutput::<4>::new();
        assert_eq!(pending.set_len(5), Err(PendingOutputError::OutputTooLarge));
    }
}
