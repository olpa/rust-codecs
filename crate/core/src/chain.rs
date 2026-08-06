//! [`Chain`]: compose two [`Codec`]s into one.
//!
//! Bytes flow `first` → staging buffer → `second`. Composition happens
//! at the `Codec` level (`Chain` is itself a `Codec`), so every driver in
//! `io` (or a client's own) gets chaining for free without knowing
//! anything about it.

use crate::transfer::{transfer, ProgressEnd};
use crate::{Codec, Drain, Error, ErrorKind, Progress};

/// Composes `A` (encodes/decodes into `staging`) and `B` (reads out of
/// `staging`) into a single [`Codec`].
///
/// `staging` is caller-provided, `S: AsMut<[u8]>` — same convention as
/// the `io` adapters — so it can be a borrowed `&mut [u8]`, an inline
/// `[u8; N]`, or a `Vec<u8>` depending on the environment. `Chain` is
/// itself a `Codec`, so chains of chains work.
///
/// # Corner cases
///
/// The interesting behavior of a chain lives in its corners. All of
/// them are tested; this list is the contract.
///
/// **`first` ends its stream early.** Some formats can say "I am
/// finished" in the middle of the input. When `first` does this, no
/// more data can ever reach `second` — so `process` itself finishes
/// `second` (its final bytes, e.g. base64 padding, come out right
/// there) and then reports `StreamEnd` for the whole chain. The
/// composed codec is self-terminating exactly when `first` is. The
/// unread rest of the input stays unconsumed, and the `StreamEnd`
/// counts say so — those bytes belong to whatever comes after the
/// stream, not to this codec. If the output buffer fills while
/// `second` is being finished, the call reports `OutputFilled` and the
/// next `process` call continues from that point.
///
/// **`second` ends its stream early.** The chain ends too — but only
/// cleanly if `second` consumed everything it was actually handed.
/// Bytes still inside `first` (never even staged) are fine to drop:
/// that's the same policy as unread input, bytes past the end of a
/// stream are not the stream's to deliver. But bytes that *were*
/// staged and offered to `second`, which it then left unconsumed
/// before ending, would be silently lost rather than merely unread —
/// that's [`ErrorKind::UnexpectedEnd`](crate::ErrorKind::UnexpectedEnd),
/// not a `StreamEnd`, and it stays reported on every later call too.
/// Note one honest wrinkle in the clean case: the reported `consumed`
/// counts what `first` took from your input, even if some of the
/// resulting bytes died on the way to the output.
///
/// **`second` ends during `finish` or `flush`.** Both simply report
/// `Done`: nothing more can ever come out, so the operation is
/// complete by definition — again, only if `second` consumed
/// everything staged; otherwise it's `UnexpectedEnd` there too.
///
/// **Calling again after the end.** Once the chain has ended — `second`
/// itself ended, or `first` ended and `second.finish` then reached
/// `Done` — that's latched: every later `process`/`finish`/`flush`
/// call reports the end with zero counts, without touching `first` or
/// `second` again at all.
///
/// **Interrupted `flush`.** A flush that returns `OutputFilled` is
/// resumed by calling `flush` again with fresh output room. If
/// `first` hasn't ended, it's asked to flush again on resume — the
/// `Codec` contract requires that to be a no-op once `first` already
/// reached `Done` for this sync point, so no second sync marker comes
/// out. (If `first` *has* already ended — via an earlier `process` or
/// `finish` — there's nothing left to flush from it, so it's skipped
/// entirely rather than relying on that no-op.) If you call `process`
/// between the two flush calls, `first` sees new input and is expected
/// to treat the next `flush` as a fresh one: new input opens a new
/// sync boundary.
///
/// **Return-clean.** When `process` returns, the staging buffer holds
/// only bytes that `second` refused because your output was full. The
/// chain never keeps back bytes it could have delivered — an
/// interactive caller sees a typed line travel the whole chain in the
/// same call. (A codec may still hold a partial unit *inside itself*,
/// as its format requires; that is what `flush` and `finish` drain.)
///
/// **Staging size is a performance knob, not a correctness knob.**
/// Any non-empty staging buffer works, even a single byte — the
/// `Codec` contract guarantees progress into any non-empty buffer.
/// An empty staging buffer can never work, so `new` panics on it.
///
/// **Empty output buffer.** `process` with zero-length output always
/// reports `OutputFilled`, since a zero-length window is trivially
/// full — `written` never advances. `first` may still be pulled from
/// into staging if there's room, but nothing can ever reach `output`.
/// It is not an error, but a driver looping on that call gets nowhere
/// for output — give it room instead.
///
/// **Misbehaving codecs.** The byte counts reported by both inner
/// codecs are checked on every call; an overclaimed count surfaces as
/// [`ErrorKind::ContractViolation`](crate::ErrorKind::ContractViolation)
/// instead of corrupting the staging indices. Error counts are always
/// chain-level — bytes consumed from *your* input and written to
/// *your* output in this call — never the inner codec's own numbers.
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,
    /// Bytes in `staging[..stage_pos]` are valid, produced by `first`
    /// and not yet all drained by `second` — `second` is always fed
    /// from offset 0, so a partial drain is compacted to the front
    /// (and `stage_pos` shrunk to match) before `first` appends more.
    stage_pos: usize,
    end: EndState,
}

/// Which side of the chain has permanently ended, so `process`/
/// `finish`/`flush` know when a side no longer needs to be called.
#[derive(Clone, Copy, PartialEq, Eq)]
enum EndState {
    /// Neither side has ended; `first` and `second` are both called
    /// normally.
    Normal,
    /// `first` has permanently ended, reported via `Progress::StreamEnd`
    /// from `process` — never set from `finish` reaching `Drain::Done`,
    /// since that only guarantees `finish` itself is idempotent
    /// (contract point 3), not that every later call of any method is
    /// pinned to reporting the end (that's point 4, and only
    /// `StreamEnd` carries it). Once set, `first` is never called
    /// again. `staging` may still hold bytes it already produced,
    /// waiting to drain through `second`.
    FirstEnded,
    /// `second` has permanently ended — via `Progress::StreamEnd` from
    /// `second.process`, or by `process` itself finishing `second`
    /// after `first` ended and, in doing so, reporting the chain's own
    /// `Progress::StreamEnd` (point 4 then applies to the chain as a
    /// `Codec` in its own right, same as `FirstEnded`). Never set from
    /// `finish`'s or `flush`'s own final call to `second.finish`/
    /// `second.flush` reaching `Done`: that's governed by point 3
    /// only, not point 4 — it doesn't pin every later call the way a
    /// real `StreamEnd` does. So the whole chain is over: neither side
    /// is called again, every method just reports the end. There's no
    /// separate `BothEnded` arm: once `second` has ended, the chain's
    /// behavior no longer depends on whether `first` also ended, so
    /// tracking that distinction would be dead state.
    SecondEnded,
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Chain<A, B, S> {
    /// Build a `Chain`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `staging` buffer: it could never hold a byte
    /// for `second` to drain, so the chain could never make progress —
    /// a caller bug, not a runtime condition.
    pub fn new(first: A, second: B, mut staging: S) -> Self {
        assert!(!staging.as_mut().is_empty(), "Chain staging buffer must be non-empty");
        Self {
            first,
            second,
            staging,
            stage_pos: 0,
            end: EndState::Normal,
        }
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        // The chain is already over: report it without touching
        // either side again. A non-empty `staging` here means `second`
        // ended while bytes it had already been offered were still
        // unconsumed — see the `UnexpectedEnd` branch below — and that
        // stays reported on every later call too.
        if self.end == EndState::SecondEnded {
            if self.stage_pos != 0 {
                return Err(Error::new(ErrorKind::UnexpectedEnd, 0, 0));
            }
            return Ok(Progress::StreamEnd { consumed: 0, written: 0 });
        }

        let mut in_pos = 0;
        let mut out_pos = 0;

        // Each turn is one pass — `first` appends to staging, then
        // staging drains into `output` — followed by a check of the
        // `Codec` contract's three outcomes: input fully consumed,
        // output fully filled, or the stream ended. Only when none of
        // those hold (staging came up empty but both `input` and
        // `output` still have room) does another pass run.
        loop {
            // `first` is only called while it's still `Normal`, and
            // only while there's input to offer it — once `input` is
            // exhausted, calling it again would just feed it an empty
            // slice.
            if self.end == EndState::Normal && in_pos < input.len() {
                let staging = self.staging.as_mut();
                let moved = transfer(&mut self.first, &input[in_pos..], &mut staging[self.stage_pos..])
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
                in_pos += moved.consumed;
                self.stage_pos += moved.written;
                if moved.end == ProgressEnd::StreamEnd {
                    self.end = EndState::FirstEnded;
                }
            }

            // `second` always reads staging from offset 0. A partial
            // drain is compacted to the front so the invariant holds
            // for the next pass (or the next call). Skipped entirely
            // when there's nothing staged — an empty-input call to
            // `second` can't do anything a caller needs to see.
            if self.stage_pos > 0 {
                let staging = self.staging.as_mut();
                let moved = transfer(&mut self.second, &staging[..self.stage_pos], &mut output[out_pos..])
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
                out_pos += moved.written;
                let leftover = self.stage_pos - moved.consumed;
                if leftover > 0 {
                    let staging = self.staging.as_mut();
                    staging.copy_within(moved.consumed..self.stage_pos, 0);
                }
                self.stage_pos = leftover;
                if moved.end == ProgressEnd::StreamEnd {
                    self.end = EndState::SecondEnded;
                    // `second` ending is only clean if it consumed
                    // everything it was offered. Bytes it left
                    // unconsumed would otherwise be silently lost —
                    // that's an error, not the normal "unread input
                    // past the end" case (which is about *your* input,
                    // not already-staged bytes `second` was actually
                    // handed).
                    if leftover > 0 {
                        return Err(Error::new(ErrorKind::UnexpectedEnd, in_pos, out_pos));
                    }
                    return Ok(Progress::StreamEnd { consumed: in_pos, written: out_pos });
                }
            }

            // Case: `first`'s stream ended in-band and everything it
            // had already staged has passed through `second` — no more
            // input will ever reach `second`, exactly what `finish`
            // expresses. Drive it here so the composed codec is
            // self-terminating exactly when `first` is: `second` being
            // done reports `StreamEnd`, which leaves the rest of
            // `input` unconsumed and reported — it's simply not this
            // stream's to read.
            if self.end == EndState::FirstEnded && self.stage_pos == 0 {
                return match self
                    .second
                    .finish(&mut output[out_pos..])
                    .and_then(|d| d.validated(output.len() - out_pos))
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => Ok(Progress::OutputFilled { consumed: in_pos }),
                    Drain::Done { written } => {
                        self.end = EndState::SecondEnded;
                        Ok(Progress::StreamEnd { consumed: in_pos, written: out_pos + written })
                    }
                };
            }
            // Case: input fully consumed, and nothing is left waiting
            // in staging for `second`.
            if in_pos == input.len() && self.stage_pos == 0 {
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            // Case: output fully filled.
            if out_pos == output.len() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
            // Otherwise staging came up empty but neither `input` nor
            // `output` is finished — run another pass.
        }
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        if self.end == EndState::SecondEnded {
            if self.stage_pos != 0 {
                return Err(Error::new(ErrorKind::UnexpectedEnd, 0, 0));
            }
            return Ok(Drain::Done { written: 0 });
        }

        let mut out_pos = 0;
        // Persists across passes *within this call* (not stored in
        // `self.end` — see the note below on why that would overreach).
        // Once `first.finish` reaches `Done`, there is no need to ask
        // it again on a later pass of this same loop.
        let mut nothing_more_from_first = self.end == EndState::FirstEnded;

        // Same pass structure as `process`: one pass calls `first`,
        // then `second`, then checks the exit conditions. Pulling
        // from `first.finish` instead of `first.process` — but unlike
        // `process`, reaching `Drain::Done` here does *not* set
        // `EndState::FirstEnded`: that variant means `first` is pinned
        // to reporting its end on *any* later call, of any method
        // (contract point 4), a guarantee only `Progress::StreamEnd`
        // carries. `finish` reaching `Done` only guarantees `finish`
        // itself is idempotent from here (point 3) — calling `process`
        // or `flush` afterward is undefined (point 6), so `first` may
        // legitimately still expect to be called normally on a *later
        // call* to `finish`. If `first` is already `FirstEnded` (a
        // genuine prior `StreamEnd`), it's skipped — that skip is
        // backed by point 4, not assumed here.
        loop {
            if !nothing_more_from_first {
                let staging = self.staging.as_mut();
                match self
                    .first
                    .finish(&mut staging[self.stage_pos..])
                    .and_then(|d| d.validated(staging.len() - self.stage_pos))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => {
                        self.stage_pos = staging.len();
                    }
                    Drain::Done { written } => {
                        self.stage_pos += written;
                        nothing_more_from_first = true;
                    }
                }
            }

            if self.stage_pos > 0 {
                let staging = self.staging.as_mut();
                let moved = transfer(&mut self.second, &staging[..self.stage_pos], &mut output[out_pos..])
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?;
                out_pos += moved.written;
                let leftover = self.stage_pos - moved.consumed;
                if leftover > 0 {
                    let staging = self.staging.as_mut();
                    staging.copy_within(moved.consumed..self.stage_pos, 0);
                }
                self.stage_pos = leftover;
                if moved.end == ProgressEnd::StreamEnd {
                    self.end = EndState::SecondEnded;
                    if leftover > 0 {
                        return Err(Error::new(ErrorKind::UnexpectedEnd, 0, out_pos));
                    }
                    return Ok(Drain::Done { written: out_pos });
                }
            }

            if nothing_more_from_first && self.stage_pos == 0 {
                // `first` is fully drained through `second`; finish
                // `second`. Unlike `process`'s equivalent step, this
                // doesn't set `EndState::SecondEnded` on `Done`: this
                // `Drain::Done` is governed by contract point 3
                // (finish/flush idempotent against repeats of
                // themselves), not point 4 (process pinned forever on
                // `StreamEnd`) — `nothing_more_from_first` can be true
                // here purely because `first.finish` just returned
                // `Done` this call, not because `first` ever reported
                // a genuine `StreamEnd`, so neither side is provably
                // exhausted forever. A later `process` call is free to
                // feed `first` normally again (point 6); latching
                // `SecondEnded` here would wrongly foreclose on that.
                return match self
                    .second
                    .finish(&mut output[out_pos..])
                    .and_then(|d| d.validated(output.len() - out_pos))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => Ok(Drain::OutputFilled),
                    Drain::Done { written } => Ok(Drain::Done { written: out_pos + written }),
                };
            }
            if out_pos == output.len() {
                return Ok(Drain::OutputFilled);
            }
            // Otherwise staging came up empty but `first` isn't `Done`
            // yet — run another pass.
        }
    }

    fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        if self.end == EndState::SecondEnded {
            if self.stage_pos != 0 {
                return Err(Error::new(ErrorKind::UnexpectedEnd, 0, 0));
            }
            return Ok(Drain::Done { written: 0 });
        }

        let mut out_pos = 0;
        // Persists across passes *within this call* only — once
        // `first.flush` reaches `Done` there's no need to ask it again
        // on a later pass of this same loop. Not stored in `self.end`:
        // a completed `flush` only means a sync point was reached, the
        // stream stays open, so `first` must still be called on the
        // *next* `flush`/`process` call. If `first` is already
        // `FirstEnded` (from an earlier `process` or `finish`), there's
        // nothing left to flush from it, so it starts out skipped.
        let mut nothing_more_from_first = self.end == EndState::FirstEnded;

        // Same pass structure as `finish`, pulling from `first.flush`
        // instead of `first.finish`.
        loop {
            if !nothing_more_from_first {
                let staging = self.staging.as_mut();
                match self
                    .first
                    .flush(&mut staging[self.stage_pos..])
                    .and_then(|d| d.validated(staging.len() - self.stage_pos))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => {
                        self.stage_pos = staging.len();
                    }
                    Drain::Done { written } => {
                        self.stage_pos += written;
                        nothing_more_from_first = true;
                    }
                }
            }

            if self.stage_pos > 0 {
                let staging = self.staging.as_mut();
                let moved = transfer(&mut self.second, &staging[..self.stage_pos], &mut output[out_pos..])
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?;
                out_pos += moved.written;
                let leftover = self.stage_pos - moved.consumed;
                if leftover > 0 {
                    let staging = self.staging.as_mut();
                    staging.copy_within(moved.consumed..self.stage_pos, 0);
                }
                self.stage_pos = leftover;
                if moved.end == ProgressEnd::StreamEnd {
                    // `second` ended in-band mid-flush: nothing more
                    // can ever come out, so the flush is trivially
                    // complete — unless bytes it was offered went
                    // unconsumed, which is `UnexpectedEnd` rather than
                    // a clean end.
                    self.end = EndState::SecondEnded;
                    if leftover > 0 {
                        return Err(Error::new(ErrorKind::UnexpectedEnd, 0, out_pos));
                    }
                    return Ok(Drain::Done { written: out_pos });
                }
            }

            if nothing_more_from_first && self.stage_pos == 0 {
                // Everything `first` owed has passed through `second`;
                // now `second`'s own flush.
                return match self
                    .second
                    .flush(&mut output[out_pos..])
                    .and_then(|d| d.validated(output.len() - out_pos))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => Ok(Drain::OutputFilled),
                    Drain::Done { written } => Ok(Drain::Done { written: out_pos + written }),
                };
            }
            if out_pos == output.len() {
                return Ok(Drain::OutputFilled);
            }
            // Otherwise staging came up empty but `first` isn't `Done`
            // flushing yet — run another pass.
        }
    }
}

#[cfg(all(
    test,
    feature = "alloc",
    feature = "identity",
    feature = "rot13",
    feature = "base64"
))]
mod tests {
    use alloc::{boxed::Box, vec, vec::Vec};

    use super::Chain;
    use crate::base64::{base64_dec, base64_enc};
    use crate::identity::identity;
    use crate::{stream_to_stream, DriveError};
    use crate::sources_and_sinks::vec::{VecSource, VecSink};
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Progress};

    const INPUT: &[u8] = b"Hello, World! 123";

    fn collect(codec: impl Codec, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        let mut input = VecSource::new(bytes.to_vec());
        let mut output = VecSink::default();
        stream_to_stream(&mut input, codec, &mut output)
            .map_err(|error| match error {
                DriveError::Codec(error) => error,
                _ => unreachable!("infallible Vec adapter"),
            })?;
        Ok(output.into_inner())
    }

    /// A test-only codec that copies bytes 1:1, like `Identity`, but
    /// self-terminates: once `limit` bytes have been written, `process`
    /// reports `StreamEnd` even with more input still in the caller's
    /// slice. Models a self-describing format that ends before the
    /// input stream does.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl Codec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Progress::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn rot13_then_rot13_is_identity() {
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_enc_then_base64_dec_round_trip() {
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 64]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_staging_buffer_forces_partial_progress() {
        // A 1-byte staging buffer forces `first` and `second` to hand
        // off one byte at a time internally.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 1]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_round_trip_through_one_byte_staging() {
        // The carry contract means even base64's 4-byte groups squeeze
        // through a 1-byte staging buffer — impossible before, when a
        // buffer below the atomic unit was a hard error.
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 1]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_output_buffer_forces_partial_progress() {
        // A single `process` call with a 1-byte *caller* output buffer:
        // rot13-then-rot13 is the identity, so one byte of `INPUT`
        // should come straight back out, with plenty of input left
        // over to report `OutputFilled` rather than `InputConsumed`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 8]);
        let mut out = [0u8; 1];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert!(matches!(outcome, Progress::OutputFilled { .. }));
        assert_eq!(out[0], INPUT[0]);
    }

    #[test]
    fn return_clean_no_hoarding_across_calls() {
        // With generous input and output room in a single call, every
        // byte `second` can produce must come out of *this* call — nothing
        // held back to surface only on a later call or on `finish`.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: INPUT.len() });
        assert_eq!(&out[..INPUT.len()], INPUT);
    }

    #[test]
    fn repeated_one_byte_output_calls_drive_to_completion() {
        // Chain state must survive un-normalized across call
        // boundaries: with 4-byte staging (exactly one base64 encoded
        // group) and a 1-byte caller output, calls routinely return
        // with staging exactly drained (`drained == filled`, not
        // reset) — the next call's entry normalization is what makes
        // the following refill start at the top of the buffer.
        let expected = collect(rot13(), &collect(base64_enc(), INPUT).unwrap()).unwrap();
        let mut chain = Chain::new(base64_enc(), rot13(), vec![0u8; 4]);
        let mut collected = Vec::new();
        let mut in_pos = 0;
        while in_pos < INPUT.len() {
            let mut out = [0u8; 1];
            match chain.process(&INPUT[in_pos..], &mut out).unwrap() {
                Progress::InputConsumed { written } => {
                    collected.extend_from_slice(&out[..written]);
                    in_pos = INPUT.len();
                }
                Progress::OutputFilled { consumed } => {
                    collected.extend_from_slice(&out);
                    in_pos += consumed;
                }
                Progress::StreamEnd { .. } => unreachable!("base64∘rot13 never self-terminates"),
            }
        }
        loop {
            let mut out = [0u8; 1];
            match chain.finish(&mut out).unwrap() {
                Drain::OutputFilled => collected.extend_from_slice(&out),
                Drain::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, expected);
    }

    #[test]
    fn finish_drains_first_through_second() {
        // base64_enc's finish() emits the padding `=`; chained into
        // rot13, that padding must come out rot13'd too (not appended
        // raw after Chain's own finish), so an adapter-driven round trip
        // against the independently-computed expected bytes covers it.
        let expected = collect(rot13(), &collect(base64_enc(), INPUT).unwrap()).unwrap();
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 64]);
        assert_eq!(collect(chain, INPUT).unwrap(), expected);
    }

    #[test]
    fn first_ends_early_mid_stream() {
        // `first` self-terminates after 3 bytes; `Chain` must latch
        // that, stop feeding `first` the rest of the input, and still
        // finish cleanly through `second` (here, identity).
        let chain = Chain::new(EarlyEnd { limit: 3, done: 0 }, identity(), vec![0u8; 64]);
        assert_eq!(collect(chain, b"Hello World").unwrap(), b"Hel");
    }

    #[test]
    fn first_ending_ends_the_chain_and_leaves_input_unconsumed() {
        // The composed codec is self-terminating exactly when `first`
        // is: `first`'s in-band end surfaces as the chain's own
        // `StreamEnd`, with the unread tail of the input reported as
        // unconsumed rather than silently swallowed.
        let mut chain = Chain::new(EarlyEnd { limit: 3, done: 0 }, identity(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(b"Hello World", &mut out).unwrap();
        assert_eq!(outcome, Progress::StreamEnd { consumed: 3, written: 3 });
        assert_eq!(&out[..3], b"Hel");
    }

    #[test]
    fn first_ending_finishes_second_inside_process() {
        // `first`'s end means no input will ever reach `second` again,
        // so `process` itself drives `second.finish`: base64_enc holds
        // one leftover byte internally, and its padded trailer group
        // must arrive without the caller ever calling finish().
        let expected = collect(base64_enc(), b"Hell").unwrap();
        let mut chain = Chain::new(EarlyEnd { limit: 4, done: 0 }, base64_enc(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(b"Hello World", &mut out).unwrap();
        assert_eq!(outcome, Progress::StreamEnd { consumed: 4, written: expected.len() });
        assert_eq!(&out[..expected.len()], expected.as_slice());
    }

    #[test]
    fn dyn_composition_compiles_and_runs() {
        let first: Box<dyn Codec> = Box::new(rot13());
        let second: Box<dyn Codec> = Box::new(rot13());
        let chain: Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>> =
            Chain::new(first, second, vec![0u8; 64]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn nested_chain_three_codecs() {
        // rot13 ∘ rot13 ∘ identity == identity, stacked three deep.
        let inner = Chain::new(rot13(), identity(), vec![0u8; 32]);
        let outer = Chain::new(rot13(), inner, vec![0u8; 32]);
        assert_eq!(collect(outer, INPUT).unwrap(), INPUT);
    }

    #[test]
    #[should_panic(expected = "staging buffer must be non-empty")]
    fn empty_staging_buffer_panics() {
        let _ = Chain::new(rot13(), rot13(), Vec::<u8>::new());
    }

    /// A block-buffering codec: hoards all input internally and emits
    /// it only on `flush`/`finish` — the codec class `Codec::flush`
    /// exists for (deflate-style sync boundaries).
    #[derive(Default)]
    struct Hoarder {
        buf: Vec<u8>,
    }

    impl Hoarder {
        fn emit(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            let n = self.buf.len().min(output.len());
            output[..n].copy_from_slice(&self.buf[..n]);
            self.buf.drain(..n);
            if self.buf.is_empty() {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    impl Codec for Hoarder {
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            self.buf.extend_from_slice(input);
            Ok(Progress::InputConsumed { written: 0 })
        }

        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            self.emit(output)
        }

        fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            self.emit(output)
        }
    }

    #[test]
    fn flush_drains_a_hoarding_first_through_second() {
        // `first` withholds everything until flushed; `Chain::flush`
        // must pull it out *through* `second`, so the bytes arrive
        // transformed — and the stream stays open for more input.
        let expected = collect(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.flush(&mut out).unwrap();
        assert_eq!(drain, Drain::Done { written: expected.len() });
        assert_eq!(&out[..expected.len()], expected.as_slice());
    }

    #[test]
    fn flush_drains_a_hoarding_second() {
        // `second` withholds; `Chain::flush` must invoke `second`'s
        // own flush after `first`'s.
        let expected = collect(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(rot13(), Hoarder::default(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, &mut out).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.flush(&mut out).unwrap();
        assert_eq!(drain, Drain::Done { written: expected.len() });
        assert_eq!(&out[..expected.len()], expected.as_slice());
    }

    #[test]
    fn interrupted_flush_resumes_and_the_stream_stays_open() {
        // Drive a flush through 1-byte outputs: each OutputFilled
        // return must resume where it left off (never re-flushing
        // `first`), and after Done the chain must accept new input
        // and flush it too — proving `flushing_second` resets.
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]);
        let mut big = [0u8; 64];
        chain.process(INPUT, &mut big).unwrap();

        let expected = collect(rot13(), INPUT).unwrap();
        let mut collected = Vec::new();
        loop {
            let mut out = [0u8; 1];
            match chain.flush(&mut out).unwrap() {
                Drain::OutputFilled => collected.extend_from_slice(&out),
                Drain::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, expected);

        // Second round: new input after a completed flush.
        chain.process(b"abc", &mut big).unwrap();
        let expected2 = collect(rot13(), b"abc").unwrap();
        let drain = chain.flush(&mut big).unwrap();
        assert_eq!(drain, Drain::Done { written: expected2.len() });
        assert_eq!(&big[..expected2.len()], expected2.as_slice());
    }

    /// Claims to have consumed more input than it was given.
    struct Overclaimer;

    impl Codec for Overclaimer {
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(Progress::OutputFilled { consumed: input.len() + 1 })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn lying_inner_codec_is_an_error_not_index_corruption() {
        // Unchecked, the overclaimed consumed-count would push
        // `drained` past `filled` and corrupt the staging indices
        // (panicking on a later slice at best). The trust-boundary
        // validation turns it into a ContractViolation error.
        let chain = Chain::new(rot13(), Overclaimer, vec![0u8; 8]);
        let result = collect(chain, INPUT);
        assert_eq!(result.unwrap_err().kind, crate::ErrorKind::ContractViolation);
    }

    #[test]
    fn second_ending_with_unconsumed_staged_bytes_is_unexpected_end() {
        // `identity` stages all of `INPUT` (11 bytes) in one pass;
        // `EarlyEnd { limit: 3 }` is then offered all 11 but only
        // consumes 3 before ending — the other 8 would be silently
        // lost, which is `UnexpectedEnd`, not a clean `StreamEnd`.
        let mut chain = Chain::new(identity(), EarlyEnd { limit: 3, done: 0 }, vec![0u8; 64]);
        let mut out = [0u8; 64];
        let error = chain.process(INPUT, &mut out).unwrap_err();
        assert_eq!(error.kind, crate::ErrorKind::UnexpectedEnd);

        // The error is latched: a later call reports the same thing
        // without touching `first` or `second` again.
        let error = chain.process(INPUT, &mut out).unwrap_err();
        assert_eq!(error.kind, crate::ErrorKind::UnexpectedEnd);
        assert_eq!(error.consumed, 0);
        assert_eq!(error.written, 0);
    }
}
