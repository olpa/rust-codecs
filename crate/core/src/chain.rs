//! [`Chain`]: compose two [`Codec`]s into one [`Codec`].
//!
//! Bytes flow `first` -> staging buffer -> `second`.
//!
//! [`BoundaryAwareCodec`](crate::BoundaryAwareCodec) is not
//! supported. A terminating composition would need its own design.

use core::mem::MaybeUninit;

use crate::step::{codec_step, DrainOp};
use crate::uninit::as_uninit_mut;
use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

/// Composes `A` (encodes/decodes into `staging`) and `B` (reads out of
/// `staging`) into a single [`Codec`].
///
/// `staging` is caller-provided (`S: AsMut<[u8]>`): a borrowed `&mut
/// [u8]`, an inline `[u8; N]`, or a `Vec<u8>`. `Chain` is itself a
/// `Codec`, so chains of chains work.
///
/// # Corner cases
///
/// **Interrupted `flush`.** A flush that returns `OutputFilled` is
/// resumed by calling `flush` again with fresh output room. `first`
/// is flushed again, but by the `Codec` contract that is a no-op once
/// it already reached `Done` for this sync point. A `process` call
/// between the two flushes opens a new sync boundary.
///
/// **Return-clean.** When `process` returns, `staging` holds only
/// bytes `second` refused because output was full. The chain never
/// withholds bytes it could have delivered.
///
/// **Staging size.** Affects performance, not correctness: any
/// non-empty buffer works, even one byte. `new` panics on an empty
/// one, since it could never hold a byte.
///
/// **Empty output buffer.** `process` always reports `OutputFilled`
/// for a zero-length `output`, since it's trivially full. Not an
/// error, but a driver looping on it makes no progress.
///
/// **Misbehaving codecs.** Byte counts from both inner codecs are
/// checked on every call; an overclaim becomes
/// [`ErrorKind::ByteCountClaim`](crate::ErrorKind::ByteCountClaim)
/// instead of corrupting the staging indices. Reported counts are
/// always chain-level, never the inner codec's own numbers.
///
/// # `Chain` vs. wrapping input and output separately
///
/// `stream_to_stream(input, Chain::new(a, b, staging), output)` and
/// `io::copy` between an `a`-wrapped reader and a `b`-wrapped writer
/// agree only for well-behaved, whole-stream codecs. `io::copy` gives
/// `b` no signal that input is exhausted for good (`Write::flush` is
/// a resumable sync point, not a permanent end), so a stateful
/// codec's trailer, checksum, or padding never gets written unless
/// `b` is finalized explicitly.
///
/// `Chain` instead behaves as one correct `Codec` on every call: it
/// tracks partial consumption on both sides, retains and compacts
/// intermediate output, translates two codecs' progress into one
/// `Progress`, and validates both codecs' reported counts.
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,
    /// Bytes in `staging[..stage_pos]` are valid: produced by `first`,
    /// not yet fully drained by `second`. `second` always reads from
    /// offset 0, so a partial drain is compacted to the front before
    /// `first` appends more.
    stage_pos: usize,
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Chain<A, B, S> {
    /// Build a `Chain`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `staging` buffer: it could never hold a byte
    /// for `second` to drain, so the chain could never make progress.
    /// This is a caller bug, not a runtime condition.
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

    /// Reclaim both codecs and the staging buffer, for example to read
    /// state one holds (a checksum, a digest) or to reuse the
    /// buffer's allocation. Any bytes `first` produced but `second`
    /// had not yet drained are still in the buffer, but `stage_pos` is
    /// not returned with it — treat them as lost.
    pub fn into_parts(self) -> (A, B, S) {
        (self.first, self.second, self.staging)
    }

    /// Rebase a validated failure from `first` onto the chain's
    /// position from just before this step ran. `pre_in_pos` grows by
    /// the real input `first` consumed. `out_pos` stays the same:
    /// whatever `first` wrote landed in `staging`, not in `output`.
    fn rebase_first_failed(error: Error, pre_in_pos: usize, out_pos: usize) -> Error {
        Error::new(error.kind, pre_in_pos + error.consumed, out_pos)
    }

    /// Mirrors `rebase_first_failed`, but for `second`.
    fn rebase_second_failed(error: Error, consumed: usize, pre_out_pos: usize) -> Error {
        Error::new(error.kind, consumed, pre_out_pos + error.written)
    }

    /// Offer everything currently staged to `second`, advancing
    /// `out_pos` by what it wrote and compacting the unconsumed
    /// remainder to the front of `staging`. `process` passes its own
    /// `in_pos` as `consumed_on_error`; `drain_through` always passes
    /// 0, since draining consumes no caller input. A no-op when
    /// nothing is staged.
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
                let error = error
                    .validated(staged, output_len)
                    .unwrap_or_else(|violation| violation);
                let leftover = staged - error.consumed;
                if leftover > 0 {
                    let staging = self.staging.as_mut();
                    staging.copy_within(error.consumed..staged, 0);
                }
                self.stage_pos = leftover;
                return Err(Self::rebase_second_failed(
                    error,
                    consumed_on_error,
                    *out_pos,
                ));
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

    /// Shared engine behind `finish` and `sync_flush`: both drive
    /// `first` through staging into `second`, one pass at a time,
    /// differing only in which operation `op` runs on each side.
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
                        return Err(Self::rebase_first_failed(error, 0, out_pos));
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
                        return Err(Self::rebase_second_failed(error, 0, out_pos));
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

        // Each turn is one pass: `first` appends to staging, then
        // staging drains into `output`. A pass ends the loop once
        // input is fully consumed or output is fully filled;
        // otherwise it runs again.
        loop {
            // `first` runs only while input remains; once `input` is
            // exhausted, calling it again would just feed an empty
            // slice.
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
                        self.stage_pos += error.written;
                        return Err(Self::rebase_first_failed(error, in_pos, out_pos));
                    }
                };
                in_pos += moved.consumed;
                self.stage_pos += moved.written;
            }

            // `second` always reads staging from offset 0. A partial
            // drain is compacted to the front for the next pass.
            // Skipped when nothing is staged, since an empty-input
            // call to `second` produces nothing.
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
    use core::convert::Infallible;
    use core::mem::MaybeUninit;

    use alloc::{boxed::Box, vec, vec::Vec};

    use super::Chain;
    use crate::base64_dec::base64_dec;
    use crate::base64_enc::base64_enc;
    use crate::identity::identity;
    use crate::rot13::rot13;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::sources_and_sinks::vec::{encode_string, EncodeError};
    use crate::uninit::as_uninit_mut;
    use crate::{stream_to_stream, Codec, DrainCodec, DrainProgress, Error, Progress, Sink};

    const INPUT: &str = "Hello, World! 123";
    // rot13(base64_enc(INPUT)): $ echo -n "Hello, World! 123" | base64 | rot13
    const ROT13_OF_BASE64_INPUT: &str = "FTIfoT8fVSqipzkxVFNkZwZ=";

    // ----
    // Round-trip basics
    // ----

    #[test]
    fn smoke_test_using_rot13() {
        // rot13 then rot13 is identity.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_round_trip_through_one_byte_staging() {
        // Even base64's 4-byte groups fit through a 1-byte staging
        // buffer, thanks to the internal buffer of the codec
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 1]);
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn nested_chain_three_codecs() {
        // rot13 ∘ rot13 ∘ identity == identity, stacked three deep.
        let inner = Chain::new(rot13(), identity(), vec![0u8; 32]);
        let outer = Chain::new(rot13(), inner, vec![0u8; 32]);
        assert_eq!(encode_string(outer, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn dyn_composition_compiles_and_runs() {
        // Identity logic is already covered by `smoke_test_using_rot13`;
        // this test exists only to check that `Chain` accepts `Box<dyn Codec>`.
        let first: Box<dyn Codec> = Box::new(rot13());
        let second: Box<dyn Codec> = Box::new(rot13());
        let chain: Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>> =
            Chain::new(first, second, vec![0u8; 64]);
        encode_string(chain, INPUT).unwrap();
    }

    #[test]
    fn finish_drains_first_through_second() {
        // base64_enc's finish() emits padding `=`; chained into rot13,
        // that padding must come out rot13'd too, not appended raw.
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 64]);
        assert_eq!(encode_string(chain, INPUT).unwrap(), ROT13_OF_BASE64_INPUT);
    }

    // ----
    // Buffer-size edge cases
    // ----

    #[test]
    fn tiny_staging_buffer_forces_partial_progress() {
        // A 1-byte staging buffer forces `first` and `second` to
        // hand off one byte at a time internally.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 1]);
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_output_buffer_forces_partial_progress() {
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 8]);
        let mut out = [0u8; 1];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        // 8-byte staging caps `first` at 8 bytes consumed, even though
        // `output` only fits 1 of those through `second`.
        assert_eq!(outcome, Progress::OutputFilled { consumed: 8 });
        assert_eq!(out[0], INPUT.as_bytes()[0]);
    }

    #[test]
    fn return_clean_no_hoarding_across_calls() {
        // With generous room on both sides, every byte `second` can
        // produce must come out of this call — nothing held back for
        // later.
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(
            outcome,
            Progress::InputConsumed {
                written: INPUT.len()
            }
        );
        assert_eq!(&out[..INPUT.len()], INPUT.as_bytes());
    }

    /// A `Sink` that never offers more than one byte of spare room per
    /// call, unlike `VecSink`, whose growth is capacity-driven and can
    /// jump to many bytes at once.
    struct OneByteAtATimeSink {
        bytes: Vec<u8>,
    }

    impl Sink for OneByteAtATimeSink {
        type Error = Infallible;

        fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
            self.bytes.reserve_exact(1);
            Ok(Some(&mut self.bytes.spare_capacity_mut()[..1]))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            let amount = amount.min(1);
            unsafe { self.bytes.set_len(self.bytes.len() + amount) };
            Ok(())
        }
    }

    #[test]
    fn repeated_one_byte_output_calls_drive_to_completion() {
        // Chain state must survive un-normalized across calls: drive
        // it through a sink that only ever offers one byte at a time.
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 4]);
        let mut input = SliceSource::new(INPUT.as_bytes());
        let mut output = OneByteAtATimeSink { bytes: Vec::new() };
        stream_to_stream(&mut input, chain, &mut output).unwrap();
        assert_eq!(output.bytes, ROT13_OF_BASE64_INPUT.as_bytes());
    }

    // ----
    // `sync_flush`/`finish` draining
    // ----

    /// A block-buffering codec: hoards all input and emits it only on
    /// `sync_flush`/`finish`, the class `DrainCodec::sync_flush`
    /// exists for (deflate-style sync boundaries).
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
        // `first` withholds everything until flushed;
        // `Chain::sync_flush` must pull it out through `second` so
        // the bytes arrive transformed, and the stream stays open.
        let expected = encode_string(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]);
        let mut out = [0u8; 64];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.sync_flush(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
        assert_eq!(&out[..expected.len()], expected.as_bytes());
    }

    #[test]
    fn flush_drains_a_hoarding_second() {
        // `second` withholds; `Chain::sync_flush` must invoke
        // `second`'s own sync_flush after `first`'s.
        let expected = encode_string(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(rot13(), Hoarder::default(), vec![0u8; 64]);
        let mut out = [0u8; 64];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.sync_flush(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
        assert_eq!(&out[..expected.len()], expected.as_bytes());
    }

    #[test]
    fn interrupted_flush_resumes_and_the_stream_stays_open() {
        // Drive a flush through 1-byte outputs: each OutputFilled
        // must resume where it left off without re-flushing `first`,
        // and after Done the chain must accept and flush new input
        // too.
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]);
        let mut big = [0u8; 64];
        chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut big))
            .unwrap();

        let expected = encode_string(rot13(), INPUT).unwrap();
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
        assert_eq!(collected, expected.as_bytes());

        // Second round: new input after a completed flush.
        chain.process(b"abc", as_uninit_mut(&mut big)).unwrap();
        let expected2 = encode_string(rot13(), "abc").unwrap();
        let drain = chain.sync_flush(as_uninit_mut(&mut big)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected2.len()
            }
        );
        assert_eq!(&big[..expected2.len()], expected2.as_bytes());
    }

    // ----
    // Error handling
    // ----

    /// Claims to consume more input than it was given.
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
        // Unchecked, the overclaimed count would push the staging
        // indices out of bounds. Validation turns it into a
        // ByteCountClaim error instead.
        let chain = Chain::new(rot13(), Overclaimer, vec![0u8; 8]);
        match encode_string(chain, INPUT).unwrap_err() {
            EncodeError::Codec(error) => {
                assert_eq!(error.kind, crate::ErrorKind::ByteCountClaim);
            }
            other => panic!("expected a codec error, got {other:?}"),
        }
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

    #[test]
    #[should_panic(expected = "staging buffer must be non-empty")]
    fn empty_staging_buffer_panics() {
        let _ = Chain::new(rot13(), rot13(), Vec::<u8>::new());
    }
}
