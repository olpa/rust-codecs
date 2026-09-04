//! [`Chain`]: compose two [`Codec`]s into one.
//!
//! Bytes flow `first` → staging buffer → `second`. Composition happens
//! at the `Codec` level (`Chain` is itself a `Codec`), so every driver in
//! `io` (or a client's own) gets chaining for free without knowing
//! anything about it.
//!
//! Both members are bound to [`Codec`], not [`BoundaryAwareCodec`](crate::BoundaryAwareCodec):
//! neither can ever report an in-band end, so `Chain` doesn't need a
//! policy for propagating one, finalizing `second` early, or discarding
//! bytes an ended `second` never got to. A future terminating
//! composition would need its own, separate design.

use core::mem::MaybeUninit;

use crate::step::{codec_step, DrainOp};
use crate::uninit::as_uninit_mut;
use crate::{Codec, DrainProgress, DrainCodec, Error, Progress};

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
/// **Interrupted `flush`.** A flush that returns `OutputFilled` is
/// resumed by calling `flush` again with fresh output room — `first` is
/// asked to flush again on resume, and the `Codec` contract requires
/// that to be a no-op once `first` already reached `Done` for this sync
/// point, so no second sync marker comes out. If you call `process`
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
/// [`ErrorKind::ByteCountClaim`](crate::ErrorKind::ByteCountClaim)
/// instead of corrupting the staging indices. Error counts are always
/// chain-level — bytes consumed from *your* input and written to
/// *your* output in this call — never the inner codec's own numbers.
///
/// # `Chain` vs. wrapping input and output separately
///
/// `stream_to_stream(input, Chain::new(a, b, staging), output)` and
/// `io::copy` over an `a`-wrapped reader into a `b`-wrapped writer
/// have the same effect only for well-behaved, whole-stream codecs.
/// The complexity difference comes from `Chain` promising substantially
/// stronger semantics than that `Read`/`Write` composition.
///
/// With `CodecReader(a)` → `io::copy` → `CodecWriter(b)`:
///
/// - `io::copy` is the intermediate buffer and scheduler.
/// - EOF naturally finalizes `a`.
/// - Explicitly finishing the writer finalizes `b` — and *only*
///   explicitly: nothing about `Read`/`Write` tells `io::copy` when
///   the input is exhausted for good, so this step doesn't happen on
///   its own. Forget it, even for a plain, well-behaved whole stream,
///   and a stateful codec's trailer/checksum/padding never gets
///   written — the output is silently truncated. `Write::flush()`
///   doesn't cover for this either, since it's a resumable sync point,
///   not a permanent end.
/// - Each wrapper only drives one codec, in one direction.
///
/// `Chain`, however, must behave as one correct `Codec` during every
/// individual call. That means it must:
///
/// - Track partial consumption on both sides.
/// - Retain and compact intermediate output.
/// - Translate two codecs' progress into one `Progress`.
/// - Propagate exact chain-level consumed/written counts.
/// - Support interrupted and repeated `process`, `flush`, and `finish`
///   calls.
/// - Preserve the `Codec` lifecycle contract even when nested in
///   another `Chain`.
/// - Validate both codecs' reported counts.
///
/// `Read` → `copy` → `Write` composes complete stream drivers; `Chain`
/// composes resumable state machines while exposing a single
/// state-machine interface. Those are equivalent at the "transform
/// this ordinary finite stream" level, but not operationally. The
/// wrappers delegate most orchestration to `std::io`; `Chain` has to
/// implement that orchestration itself and make it resumable inside
/// one `Codec`.
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,
    /// Bytes in `staging[..stage_pos]` are valid, produced by `first`
    /// and not yet all drained by `second` — `second` is always fed
    /// from offset 0, so a partial drain is compacted to the front
    /// (and `stage_pos` shrunk to match) before `first` appends more.
    stage_pos: usize,
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
        assert!(
            !staging.as_mut().is_empty(),
            "Chain staging buffer must be non-empty"
        );
        Self {
            first,
            second,
            staging,
            stage_pos: 0,
        }
    }

    /// Reclaim both codecs and the staging buffer — e.g. to read state
    /// one of them holds (a checksum, a digest) once the stream is
    /// done, or to reuse the buffer's allocation for another `Chain`.
    /// Any bytes `first` had produced but `second` hadn't yet drained
    /// are still physically present at the front of the returned
    /// buffer, but `stage_pos` isn't returned alongside it, so there's
    /// no way to tell how many (if any) — treat them as lost.
    pub fn into_parts(self) -> (A, B, S) {
        (self.first, self.second, self.staging)
    }

    /// Offer everything currently staged to `second`, advancing
    /// `out_pos` by what it wrote and compacting whatever it left
    /// unconsumed to the front of `staging`. Shared by `process` (where
    /// an error reports `in_pos` as `consumed`) and `drain_through`
    /// (where it's always 0, since draining doesn't consume caller
    /// input). A no-op when nothing is staged.
    fn drain_staging_into(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        out_pos: &mut usize,
        consumed_on_error: usize,
    ) -> Result<(), Error> {
        if self.stage_pos == 0 {
            return Ok(());
        }
        let staging = self.staging.as_mut();
        let staged = self.stage_pos;
        let output_len = output.len() - *out_pos;
        let moved = match codec_step(
            &mut self.second,
            &staging[..staged],
            &mut output[*out_pos..],
        ) {
            Ok(moved) => moved,
            Err(error) => {
                let error = match error.validated(staged, output_len) {
                    Ok(error) => error,
                    Err(error) => {
                        return Err(Error {
                            consumed: consumed_on_error,
                            written: *out_pos,
                            ..error
                        });
                    }
                };
                *out_pos += error.written;
                let leftover = staged - error.consumed;
                if leftover > 0 {
                    let staging = self.staging.as_mut();
                    staging.copy_within(error.consumed..staged, 0);
                }
                self.stage_pos = leftover;
                return Err(Error {
                    consumed: consumed_on_error,
                    written: *out_pos,
                    ..error
                });
            }
        };
        *out_pos += moved.written;
        let leftover = self.stage_pos - moved.consumed;
        if leftover > 0 {
            let staging = self.staging.as_mut();
            staging.copy_within(moved.consumed..self.stage_pos, 0);
        }
        self.stage_pos = leftover;
        Ok(())
    }

    /// Shared engine behind `finish` and `sync_flush`: both drive `first`
    /// through staging into `second`, one pass at a time (same pass
    /// structure as `process`), and differ only in which operation —
    /// `op` — runs on each side.
    fn drain_through(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        op: DrainOp,
    ) -> Result<DrainProgress, Error> {
        let mut out_pos = 0;
        let mut nothing_more_from_first = false;

        loop {
            if !nothing_more_from_first {
                let staging = self.staging.as_mut();
                let available = staging.len() - self.stage_pos;
                let moved = match op.step(
                    &mut self.first,
                    as_uninit_mut(&mut staging[self.stage_pos..]),
                ) {
                    Ok(moved) => moved,
                    Err(error) => {
                        let error = error
                            .validated(0, available)
                            .unwrap_or_else(|violation| violation);
                        self.stage_pos += error.written;
                        return Err(Error {
                            consumed: 0,
                            written: out_pos,
                            ..error
                        });
                    }
                };
                match moved {
                    DrainProgress::OutputFilled => self.stage_pos += available,
                    DrainProgress::Done { written } => {
                        self.stage_pos += written;
                        nothing_more_from_first = true;
                    }
                }
            }

            self.drain_staging_into(output, &mut out_pos, 0)?;

            if nothing_more_from_first && self.stage_pos == 0 {
                // `first` is fully drained through `second`; run
                // `second`'s own step.
                let available = output.len() - out_pos;
                let moved = match op.step(&mut self.second, &mut output[out_pos..]) {
                    Ok(moved) => moved,
                    Err(error) => {
                        let error = error
                            .validated(0, available)
                            .unwrap_or_else(|violation| violation);
                        return Err(Error {
                            consumed: 0,
                            written: out_pos + error.written,
                            ..error
                        });
                    }
                };
                return Ok(match moved {
                    DrainProgress::OutputFilled => DrainProgress::OutputFilled,
                    DrainProgress::Done { written } => DrainProgress::Done {
                        written: out_pos + written,
                    },
                });
            }
            if out_pos == output.len() {
                return Ok(DrainProgress::OutputFilled);
            }
            // Otherwise staging came up empty but `first` isn't done
            // yet — run another pass.
        }
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> DrainCodec for Chain<A, B, S> {
    fn sync_flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain_through(output, DrainOp::SyncFlush)
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain_through(output, DrainOp::Finish)
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        // Each turn is one pass — `first` appends to staging, then
        // staging drains into `output` — followed by a check of the
        // `Codec` contract's two outcomes: input fully consumed, or
        // output fully filled. Only when neither holds (staging came
        // up empty but both `input` and `output` still have room)
        // does another pass run.
        loop {
            // `first` is only called while there's input to offer it —
            // once `input` is exhausted, calling it again would just
            // feed it an empty slice.
            if in_pos < input.len() {
                let staging = self.staging.as_mut();
                let input_len = input.len() - in_pos;
                let output_len = staging.len() - self.stage_pos;
                let moved = match codec_step(
                    &mut self.first,
                    &input[in_pos..],
                    as_uninit_mut(&mut staging[self.stage_pos..]),
                ) {
                    Ok(moved) => moved,
                    Err(error) => {
                        let error = error
                            .validated(input_len, output_len)
                            .unwrap_or_else(|violation| violation);
                        in_pos += error.consumed;
                        self.stage_pos += error.written;
                        return Err(Error {
                            consumed: in_pos,
                            written: out_pos,
                            ..error
                        });
                    }
                };
                in_pos += moved.consumed;
                self.stage_pos += moved.written;
            }

            // `second` always reads staging from offset 0. A partial
            // drain is compacted to the front so the invariant holds
            // for the next pass (or the next call). Skipped entirely
            // when there's nothing staged — an empty-input call to
            // `second` can't do anything a caller needs to see.
            self.drain_staging_into(output, &mut out_pos, in_pos)?;

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
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use core::mem::MaybeUninit;

    use alloc::{boxed::Box, vec, vec::Vec};

    use super::Chain;
    #[cfg(feature = "base64")]
    use crate::base64_dec::base64_dec;
    use crate::base64_enc::base64_enc;
    use crate::identity::identity;
    use crate::rot13::rot13;
    use crate::sources_and_sinks::vec::{VecSink, VecSource};
    use crate::uninit::as_uninit_mut;
    use crate::{stream_to_stream, DriveError};
    use crate::{Codec, DrainProgress, DrainCodec, Error, Progress};

    const INPUT: &[u8] = b"Hello, World! 123";

    fn collect(codec: impl Codec, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        let mut input = VecSource::new(bytes.to_vec());
        let mut output = VecSink::default();
        stream_to_stream(&mut input, codec, &mut output).map_err(|error| match error {
            DriveError::Codec(error) => error,
            _ => unreachable!("infallible Vec adapter"),
        })?;
        Ok(output.into_inner())
    }

    #[test]
    fn rot13_then_rot13_is_identity() {
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        assert_eq!(collect(chain, INPUT).unwrap(), INPUT);
    }

    #[cfg(feature = "base64")]
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

    #[cfg(feature = "base64")]
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
        let outcome = chain.process(INPUT, as_uninit_mut(&mut out)).unwrap();
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
        let outcome = chain.process(INPUT, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            outcome,
            Progress::InputConsumed {
                written: INPUT.len()
            }
        );
        assert_eq!(&out[..INPUT.len()], INPUT);
    }

    #[cfg(feature = "base64")]
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
            match chain
                .process(&INPUT[in_pos..], as_uninit_mut(&mut out))
                .unwrap()
            {
                Progress::InputConsumed { written } => {
                    collected.extend_from_slice(&out[..written]);
                    in_pos = INPUT.len();
                }
                Progress::OutputFilled { consumed } => {
                    collected.extend_from_slice(&out);
                    in_pos += consumed;
                }
            }
        }
        loop {
            let mut out = [0u8; 1];
            match chain.finish(as_uninit_mut(&mut out)).unwrap() {
                DrainProgress::OutputFilled => collected.extend_from_slice(&out),
                DrainProgress::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, expected);
    }

    #[cfg(feature = "base64")]
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
    /// it only on `sync_flush`/`finish` — the codec class
    /// `DrainCodec::sync_flush` exists for (deflate-style sync
    /// boundaries).
    #[derive(Default)]
    struct Hoarder {
        buf: Vec<u8>,
    }

    impl Hoarder {
        fn emit(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            let n = self.buf.len().min(output.len());
            output[..n].write_copy_of_slice(&self.buf[..n]);
            self.buf.drain(..n);
            if self.buf.is_empty() {
                Ok(DrainProgress::Done { written: n })
            } else {
                Ok(DrainProgress::OutputFilled)
            }
        }
    }

    impl DrainCodec for Hoarder {
        fn sync_flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.emit(output)
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.emit(output)
        }
    }

    impl Codec for Hoarder {
        fn process(
            &mut self,
            input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            self.buf.extend_from_slice(input);
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn flush_drains_a_hoarding_first_through_second() {
        // `first` withholds everything until flushed; `Chain::sync_flush`
        // must pull it out *through* `second`, so the bytes arrive
        // transformed — and the stream stays open for more input.
        let expected = collect(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.sync_flush(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
        assert_eq!(&out[..expected.len()], expected.as_slice());
    }

    #[test]
    fn flush_drains_a_hoarding_second() {
        // `second` withholds; `Chain::sync_flush` must invoke `second`'s
        // own sync_flush after `first`'s.
        let expected = collect(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(rot13(), Hoarder::default(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain.process(INPUT, as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.sync_flush(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
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
        chain.process(INPUT, as_uninit_mut(&mut big)).unwrap();

        let expected = collect(rot13(), INPUT).unwrap();
        let mut collected = Vec::new();
        loop {
            let mut out = [0u8; 1];
            match chain.sync_flush(as_uninit_mut(&mut out)).unwrap() {
                DrainProgress::OutputFilled => collected.extend_from_slice(&out),
                DrainProgress::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, expected);

        // Second round: new input after a completed flush.
        chain.process(b"abc", as_uninit_mut(&mut big)).unwrap();
        let expected2 = collect(rot13(), b"abc").unwrap();
        let drain = chain.sync_flush(as_uninit_mut(&mut big)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected2.len()
            }
        );
        assert_eq!(&big[..expected2.len()], expected2.as_slice());
    }

    /// Claims to have consumed more input than it was given.
    struct Overclaimer;

    impl DrainCodec for Overclaimer {
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for Overclaimer {
        fn process(
            &mut self,
            input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            Ok(Progress::OutputFilled {
                consumed: input.len() + 1,
            })
        }
    }

    #[test]
    fn lying_inner_codec_is_an_error_not_index_corruption() {
        // Unchecked, the overclaimed consumed-count would push
        // `drained` past `filled` and corrupt the staging indices
        // (panicking on a later slice at best). The trust-boundary
        // validation turns it into a ByteCountClaim error.
        let chain = Chain::new(rot13(), Overclaimer, vec![0u8; 8]);
        let result = collect(chain, INPUT);
        assert_eq!(
            result.unwrap_err().kind,
            crate::ErrorKind::ByteCountClaim
        );
    }

    struct FirstFailsOnce {
        failed: bool,
    }

    impl DrainCodec for FirstFailsOnce {
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for FirstFailsOnce {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            if !self.failed {
                self.failed = true;
                output[..2].write_copy_of_slice(b"xy");
                return Err(Error::new(crate::ErrorKind::CorruptStream, 1, 2));
            }
            let n = input.len().min(output.len());
            output[..n].write_copy_of_slice(&input[..n]);
            if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }
    }

    #[test]
    fn first_codec_error_progress_is_retained_in_staging() {
        let mut chain = Chain::new(FirstFailsOnce { failed: false }, identity(), vec![0; 8]);
        let mut output = [0; 8];

        let error = chain
            .process(b"abc", as_uninit_mut(&mut output))
            .unwrap_err();
        assert_eq!(error, Error::new(crate::ErrorKind::CorruptStream, 1, 0));

        let progress = chain.process(b"bc", as_uninit_mut(&mut output)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 4 });
        assert_eq!(&output[..4], b"xybc");
    }

    struct SecondFailsOnce {
        failed: bool,
    }

    impl DrainCodec for SecondFailsOnce {
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for SecondFailsOnce {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            if !self.failed {
                self.failed = true;
                output[..1].write_copy_of_slice(&input[..1]);
                return Err(Error::new(crate::ErrorKind::CorruptStream, 1, 1));
            }
            let n = input.len().min(output.len());
            output[..n].write_copy_of_slice(&input[..n]);
            if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }
    }

    #[test]
    fn second_codec_error_progress_compacts_staging() {
        let mut chain = Chain::new(identity(), SecondFailsOnce { failed: false }, vec![0; 8]);
        let mut output = [0; 8];

        let error = chain
            .process(b"abc", as_uninit_mut(&mut output))
            .unwrap_err();
        assert_eq!(error, Error::new(crate::ErrorKind::CorruptStream, 3, 1));
        assert_eq!(&output[..1], b"a");

        let progress = chain.process(b"", as_uninit_mut(&mut output)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 2 });
        assert_eq!(&output[..2], b"bc");
    }
}
