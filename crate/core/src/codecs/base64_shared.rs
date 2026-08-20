//! Constants and helpers shared by [`super::base64_enc`] and
//! [`super::base64_dec`].
//!
//! [`PendingInput`] mirrors [`crate::Carry`] but for the opposite side
//! of the transform: `Carry` holds output that was produced but didn't
//! fit the caller's buffer, dribbling it out over later calls;
//! `PendingInput` holds input that arrived but wasn't enough to form a
//! whole transform unit yet, topped up over later calls until there's
//! enough to consume as one group.
//!
//! Unlike `Carry`, going through `PendingInput` is the exception, not
//! the main path: the bulk transform below reads straight from the
//! caller's `input`/`output` slices without touching it. Only when a
//! call starts (or ends) with a leftover partial group does
//! `PendingInput` come into play — check [`PendingInput::is_empty`]
//! first, and only call [`PendingInput::fill`]/[`PendingInput::take`]
//! when it isn't.

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
/// how this relates to [`crate::Carry`].
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
