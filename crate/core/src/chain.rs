//! [`Chain`]: compose two [`Codec`]s into one.
//!
//! Bytes flow `first` → staging buffer → `second`. Composition happens
//! at the `Codec` level (`Chain` is itself a `Codec`), so every driver in
//! `io` (or a client's own) gets chaining for free without knowing
//! anything about it.

use crate::transfer::{transfer, TransferEnd};
use crate::{Codec, Drain, Error, Outcome};

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
/// **`second` ends its stream early.** The chain ends too. Bytes that
/// `first` had already produced but `second` never read — waiting in
/// the staging buffer, or still inside `first` — are dropped. This is
/// the same policy as for unread input: bytes past the end of a
/// stream are not the stream's to deliver. Note one honest wrinkle:
/// the reported `consumed` counts what `first` took from your input,
/// even if some of the resulting bytes died on the way to the output.
///
/// **`second` ends during `finish` or `flush`.** Both simply report
/// `Done`: nothing more can ever come out, so the operation is
/// complete by definition.
///
/// **Calling again after the end.** Once the chain has reported
/// `StreamEnd`, a later `process` call consumes nothing and reports
/// `StreamEnd` with zero counts (it re-runs `second.finish`, which a
/// well-behaved codec answers with "done, zero bytes").
///
/// **Interrupted `flush`.** A flush that returns `OutputFilled` is
/// resumed by calling `flush` again with fresh output room. The
/// resume continues where it stopped: once `first.flush` has
/// completed, it is not started a second time — a deflate-style codec
/// would emit a second sync marker. But if you call `process` between
/// the two flush calls, the half-done flush is forgotten and the next
/// flush starts from `first` again: new input opens a new sync
/// boundary.
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
/// **Empty output buffer.** `process` with zero-length output reports
/// `OutputFilled` without progress when data is waiting. It is not an
/// error, but a driver looping on that call gets nowhere — give it
/// room instead.
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
    /// Bytes in `staging` written by `first`, not yet all drained by `second`.
    filled: usize,
    /// Of `filled`, how many `second` has already consumed. The pair is
    /// normalized (both zero once equal) at the top of every
    /// `process`/`finish` turn, not at each mutation site — between
    /// turns `drained == filled` may linger un-reset.
    drained: usize,
    /// `first` reported `StreamEnd` (or was finished) — stop feeding it.
    first_ended: bool,
    /// A `flush` interrupted by `OutputFilled` resumes where it left
    /// off: once `first.flush` has reported done, later `flush` calls
    /// skip straight to `second` — re-flushing `first` would make a
    /// deflate-style codec emit a second sync marker. Cleared when new
    /// input arrives (`process`) or the flush completes.
    flushing_second: bool,
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
            filled: 0,
            drained: 0,
            first_ended: false,
            flushing_second: false,
        }
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;
        // New input invalidates a half-completed flush's phase
        // tracking; the next flush starts from `first` again.
        self.flushing_second = false;

        loop {
            // Normalized once per turn: everything staged has been
            // drained, so refills start from the top of the buffer.
            // The arms below only advance counts; none of them resets.
            if self.drained == self.filled {
                self.drained = 0;
                self.filled = 0;
            }

            // Drain whatever's already staged into the caller's output
            // before asking `first` for more — biggest possible chunks,
            // invisible from outside a single call.
            if self.drained < self.filled {
                let staging = self.staging.as_mut();
                let moved = transfer(
                    &mut self.second,
                    &staging[self.drained..self.filled],
                    &mut output[out_pos..],
                )
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
                self.drained += moved.consumed;
                out_pos += moved.written;
                match moved.end {
                    TransferEnd::InputExhausted => {
                        continue;
                    }
                    TransferEnd::OutputExhausted => {
                        return Ok(Outcome::OutputFilled { consumed: in_pos });
                    }
                    TransferEnd::StreamEnd => {
                        return Ok(Outcome::StreamEnd { consumed: in_pos, written: out_pos });
                    }
                }
            }

            // Staging is clean (guaranteed above whenever we get here).
            // Return-clean: don't withhold anything `second` could
            // take — only stop when there's genuinely nothing left to
            // feed `first`.
            if self.first_ended {
                // `first`'s stream ended in-band, so no more input
                // will ever reach `second` — exactly what `finish`
                // expresses. Drive it here so the composed codec is
                // self-terminating exactly when `first` is: once
                // `second` is done, report `StreamEnd`, which (unlike
                // the `InputConsumed` this branch used to return)
                // leaves the rest of `input` unconsumed and reported —
                // it's simply not this stream's to read.
                debug_assert!(self.drained == self.filled, "staging must be drained before finishing second");
                return match self
                    .second
                    .finish(&mut output[out_pos..])
                    .and_then(|d| d.validated(output.len() - out_pos))
                    .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => Ok(Outcome::OutputFilled { consumed: in_pos }),
                    Drain::Done { written } => {
                        Ok(Outcome::StreamEnd { consumed: in_pos, written: out_pos + written })
                    }
                };
            }
            if in_pos == input.len() {
                return Ok(Outcome::InputConsumed { written: out_pos });
            }
            if out_pos == output.len() {
                // Output is exactly full with staging clean and input
                // remaining: report the bottleneck rather than churn
                // `first`'s bytes into staging that `second` couldn't
                // move anywhere.
                return Ok(Outcome::OutputFilled { consumed: in_pos });
            }

            let staging = self.staging.as_mut();
            let moved = transfer(
                &mut self.first,
                &input[in_pos..],
                &mut staging[self.filled..],
            )
                .map_err(|e| Error { consumed: in_pos, written: out_pos, ..e })?;
            in_pos += moved.consumed;
            self.filled += moved.written;
            if moved.end == TransferEnd::StreamEnd {
                self.first_ended = true;
            }
            // Loop around: drain what was just staged.
        }
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = 0;

        loop {
            // Same per-turn normalization as `process`.
            if self.drained == self.filled {
                self.drained = 0;
                self.filled = 0;
            }

            // Any leftover staged bytes (from a prior `process`/`finish`
            // call that left the caller's output full) go first.
            if self.drained < self.filled {
                let staging = self.staging.as_mut();
                let moved = transfer(
                    &mut self.second,
                    &staging[self.drained..self.filled],
                    &mut output[out_pos..],
                )
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?;
                self.drained += moved.consumed;
                out_pos += moved.written;
                match moved.end {
                    TransferEnd::InputExhausted => {
                        continue;
                    }
                    TransferEnd::OutputExhausted => {
                        return Ok(Drain::OutputFilled);
                    }
                    TransferEnd::StreamEnd => {
                        return Ok(Drain::Done { written: out_pos });
                    }
                }
            }

            if !self.first_ended {
                let staging = self.staging.as_mut();
                match self
                    .first
                    .finish(&mut staging[self.filled..])
                    .and_then(|d| d.validated(staging.len() - self.filled))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => {
                        self.filled = staging.len();
                    }
                    Drain::Done { written } => {
                        self.filled += written;
                        self.first_ended = true;
                    }
                }
                continue;
            }

            // `first` is fully drained through `second`; finish `second`.
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
    }

    fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = 0;

        loop {
            // Same per-turn normalization as `process`/`finish`.
            if self.drained == self.filled {
                self.drained = 0;
                self.filled = 0;
            }

            // Staged bytes (leftovers, or what `first.flush` below just
            // produced) go through `second` first — `second.flush` may
            // only run once everything staged has passed through its
            // `process`.
            if self.drained < self.filled {
                let staging = self.staging.as_mut();
                let moved = transfer(
                    &mut self.second,
                    &staging[self.drained..self.filled],
                    &mut output[out_pos..],
                )
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?;
                self.drained += moved.consumed;
                out_pos += moved.written;
                match moved.end {
                    TransferEnd::InputExhausted => {
                        continue;
                    }
                    TransferEnd::OutputExhausted => {
                        return Ok(Drain::OutputFilled);
                    }
                    TransferEnd::StreamEnd => {
                        // `second` ended in-band mid-flush: nothing
                        // more can ever come out, so the flush is
                        // trivially complete.
                        return Ok(Drain::Done { written: out_pos });
                    }
                }
            }

            if !self.flushing_second {
                if self.first_ended {
                    // Nothing left in `first` to flush.
                    self.flushing_second = true;
                    continue;
                }
                let staging = self.staging.as_mut();
                match self
                    .first
                    .flush(&mut staging[self.filled..])
                    .and_then(|d| d.validated(staging.len() - self.filled))
                    .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
                {
                    Drain::OutputFilled => {
                        self.filled = staging.len();
                    }
                    Drain::Done { written } => {
                        self.filled += written;
                        self.flushing_second = true;
                    }
                }
                continue;
            }

            // Everything `first` owed has passed through `second`; now
            // `second`'s own flush.
            return match self
                .second
                .flush(&mut output[out_pos..])
                .and_then(|d| d.validated(output.len() - out_pos))
                .map_err(|e| Error { consumed: 0, written: out_pos, ..e })?
            {
                // `flushing_second` stays set: an interrupted flush
                // resumes here, not at `first`.
                Drain::OutputFilled => Ok(Drain::OutputFilled),
                Drain::Done { written } => {
                    self.flushing_second = false;
                    Ok(Drain::Done { written: out_pos + written })
                }
            };
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
    use crate::io::{stream_to_stream, CopyError, VecInput, VecOutput};
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Outcome};

    const INPUT: &[u8] = b"Hello, World! 123";

    fn collect(codec: impl Codec, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        let mut input = VecInput::new(bytes.to_vec());
        let mut output = VecOutput::default();
        stream_to_stream(&mut input, codec, &mut output)
            .map_err(|error| match error {
                CopyError::Codec(error) => error,
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
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Outcome::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Outcome::InputConsumed { written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
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
        assert!(matches!(outcome, Outcome::OutputFilled { .. }));
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
        assert_eq!(outcome, Outcome::InputConsumed { written: INPUT.len() });
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
                Outcome::InputConsumed { written } => {
                    collected.extend_from_slice(&out[..written]);
                    in_pos = INPUT.len();
                }
                Outcome::OutputFilled { consumed } => {
                    collected.extend_from_slice(&out);
                    in_pos += consumed;
                }
                Outcome::StreamEnd { .. } => unreachable!("base64∘rot13 never self-terminates"),
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
        assert_eq!(outcome, Outcome::StreamEnd { consumed: 3, written: 3 });
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
        assert_eq!(outcome, Outcome::StreamEnd { consumed: 4, written: expected.len() });
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
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            self.buf.extend_from_slice(input);
            Ok(Outcome::InputConsumed { written: 0 })
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
        assert_eq!(outcome, Outcome::InputConsumed { written: 0 });
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
        assert_eq!(outcome, Outcome::InputConsumed { written: 0 });
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
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(Outcome::OutputFilled { consumed: input.len() + 1 })
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
}
