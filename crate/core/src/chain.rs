//! [`Chain`]: compose two [`Codec`]s into one [`Codec`].
//!
//! [`BoundaryAwareCodec`](crate::BoundaryAwareCodec) is not
//! supported. A terminating composition would need its own design.

use core::mem::MaybeUninit;

use crate::step::{codec_step, finish_step, flush_step};
use crate::uninit::as_uninit_mut;
use crate::{Codec, DrainCodec, DrainProgress, EmptyBufferError, Error, ErrorKind, Progress};

/// Compose two codecs into a single [`Codec`].
///
/// Bytes flow `first` -> staging buffer -> `second`.
///
/// `Chain` is itself a `Codec`, so chains of chains work.
///
/// # `Chain` vs. wrapping `first` and `second` separately
///
/// Prefer [`stream_to_stream`](crate::stream_to_stream) in library code.
///
/// ```text
/// let chain = Chain::new(first, second, staging).unwrap();
/// stream_to_stream(input, chain, output);
/// ```
///
/// and
///
/// ```text
/// let mut reader = CodecReader::new(input, first, inbuf);
/// let mut writer = CodecWriter::new(output, second, outbuf);
/// io::copy(&mut reader, &mut writer);
/// // reader.finish(); automatically on eof
/// writer.finish();
/// ```
///
/// do the same work, but `stream_to_stream` calls `finish` automatically.
///
/// Also, the first form needs no I/O backend at all. Its parameters
/// are the [`Source`](crate::Source) and [`Sink`](crate::Sink) traits,
/// not `std::io` or `embedded_io`.
/// `CodecReader` and `CodecWriter` exist for those two backends, but a
/// custom `Source`/`Sink` implementation might not have one.
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,
    /// Bytes in `staging[stage_start..stage_end]` are valid: produced
    /// by `first`, not drained by `second`.
    stage_start: usize,
    stage_end: usize,
    /// Set when `first.finish` reports `Done`. Then `flush` and
    /// `finish` skip `first`. Does not affect `process`: the
    /// `Codec` contract does not specify `process` after `finish`,
    /// so we skip that work.
    first_done: bool,
    /// Set when `first` fails. The valid bytes that `first` wrote
    /// before the failure are in `staging`. The chain passes them
    /// through `second` first. It returns this error only when
    /// `staging` is empty. If `second` fails on these bytes, its error
    /// comes first in the data, and the chain returns it instead.
    first_error: Option<ErrorKind>,
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Chain<A, B, S> {
    /// Build a `Chain`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `staging` buffer.
    pub fn new(first: A, second: B, mut staging: S) -> Result<Self, EmptyBufferError> {
        if staging.as_mut().is_empty() {
            return Err(EmptyBufferError);
        }
        Ok(Self {
            first,
            second,
            staging,
            stage_start: 0,
            stage_end: 0,
            first_done: false,
            first_error: None,
        })
    }

    /// Reclaim both codecs and the staging buffer. If the buffer
    /// holds undrained bytes, treat them as lost.
    pub fn into_parts(self) -> (A, B, S) {
        (self.first, self.second, self.staging)
    }

    /// The error of `first`, when it is time to return it: `first`
    /// failed, and `staging` is empty. `consumed` and `out_pos` are the
    /// bytes that this call has consumed and written.
    fn first_error_if_staging_is_empty(&self, consumed: usize, out_pos: usize) -> Option<Error> {
        if !self.is_staging_empty() {
            return None;
        }
        self.first_error
            .map(|kind| Error::new(kind, consumed, out_pos))
    }

    /// Mirrors `rebase_first_failed`, but for `second`.
    fn rebase_second_failed(error: Error, consumed: usize, pre_out_pos: usize) -> Error {
        Error::new(error.kind, consumed, pre_out_pos + error.written)
    }

    fn is_staging_empty(&self) -> bool {
        self.stage_start == self.stage_end
    }

    /// Drop the first `consumed` valid bytes of `staging`. If none are
    /// left, reset both indexes to 0. `consumed` must not be more than
    /// the number of valid bytes.
    fn drop_staged(&mut self, consumed: usize) {
        self.stage_start += consumed;
        debug_assert!(self.stage_start <= self.stage_end);
        if self.stage_start == self.stage_end {
            self.stage_start = 0;
            self.stage_end = 0;
        }
    }

    /// Convert as much data as possible from `staging` to `output`.
    /// Keep the remaining bytes where they are. The next pass or the
    /// next call drains them before `first` runs again.
    /// Return the number of bytes written to `output`.
    /// Do nothing if `staging` is empty.
    /// `caller_consumed_so_far` is used for the error position rebasing.
    ///
    /// Postcondition: `staging` ends up empty, or `output[out_pos..]`
    /// ends up completely filled. This follows from
    /// [`Codec::process`]'s contract.
    fn drain_staging_into(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        out_pos: usize,
        caller_consumed_so_far: usize,
    ) -> Result<usize, Error> {
        if self.is_staging_empty() {
            return Ok(0);
        }
        let staging = self.staging.as_mut();
        let staged_len = self.stage_end - self.stage_start;
        let output_len = output.len() - out_pos;
        let moved = match codec_step(
            &mut self.second,
            &staging[self.stage_start..self.stage_end],
            &mut output[out_pos..],
        ) {
            Ok(moved) => moved,
            Err(error) => {
                let error = error
                    .validated(staged_len, output_len)
                    .unwrap_or_else(|violation| violation);
                self.drop_staged(error.consumed);
                return Err(Self::rebase_second_failed(
                    error,
                    caller_consumed_so_far,
                    out_pos,
                ));
            }
        };
        self.drop_staged(moved.consumed);
        Ok(moved.written)
    }

    /// Engine behind `flush` and `finish`.
    ///
    /// Finishing the chain means finishing both inner codecs, in
    /// pipeline order. Each one may still hold buffered bytes after
    /// the caller stops feeding input. So this drains `staging` into
    /// `second`. Then it finishes `first` into the empty `staging`.
    /// It repeats these steps. It runs the finish of `second` only
    /// when `first` and `staging` are both empty. `second` must not
    /// finish while data is still upstream of it.
    ///
    /// Flushing the chain follows the same order, with `flush` instead
    /// of `finish`.
    fn drain_through(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        mode: DrainMode,
    ) -> Result<DrainProgress, Error> {
        let mut out_pos = 0;
        let mut first_done = self.first_done;

        loop {
            //
            // Drain `staging` into `output`.
            //
            // A drain has no input, so the input consumed so far is 0.
            out_pos += self.drain_staging_into(output, out_pos, 0)?;

            if let Some(error) = self.first_error_if_staging_is_empty(0, out_pos) {
                return Err(error);
            }

            if first_done && self.is_staging_empty() {
                //
                // At this point, `first` is done and `staging` is empty.
                // Drain `second`.
                //
                let moved = match mode.step(&mut self.second, &mut output[out_pos..]) {
                    Ok(moved) => moved,
                    Err(error) => {
                        let error = error
                            .validated(0, output.len() - out_pos)
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

            if out_pos == output.len() && !self.is_staging_empty() {
                return Ok(DrainProgress::OutputFilled);
            }
            // `staging` is empty here. There are two cases:
            // - `out_pos != output.len()`. Then the postcondition of
            //   `drain_staging_into` says that `staging` is empty.
            // - `out_pos == output.len()`. Then the check above did not
            //   return, so `staging` was already empty.
            // `first` is not done yet.
            debug_assert_eq!(self.stage_start, 0);
            debug_assert_eq!(self.stage_end, 0);

            //
            // Drain `first` into `staging`.
            //
            let staging = self.staging.as_mut();
            let moved = match mode.step(&mut self.first, as_uninit_mut(staging)) {
                Ok(moved) => moved,
                Err(error) => {
                    let error = error
                        .validated(0, staging.len())
                        .unwrap_or_else(|violation| violation);
                    // The next pass drains the bytes of `first`. Then
                    // it returns the error.
                    self.stage_end = error.written;
                    self.first_error = Some(error.kind);
                    continue;
                }
            };
            match moved {
                DrainProgress::OutputFilled => self.stage_end = staging.len(),
                DrainProgress::Done { written } => {
                    self.stage_end = written;
                    first_done = true;
                    if mode == DrainMode::Finish {
                        self.first_done = true;
                    }
                }
            }
        }
    }
}

/// Selects the `DrainCodec` method that `Chain::drain_through` calls.
#[derive(Clone, Copy, PartialEq, Eq)]
enum DrainMode {
    Flush,
    Finish,
}

impl DrainMode {
    fn step<C: DrainCodec>(
        self,
        codec: &mut C,
        output: &mut [MaybeUninit<u8>],
    ) -> Result<DrainProgress, Error> {
        match self {
            DrainMode::Flush => flush_step(codec, output),
            DrainMode::Finish => finish_step(codec, output),
        }
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> DrainCodec for Chain<A, B, S> {
    /// Flush `first` through `staging` to `second`, then flush
    /// `second`.
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain_through(output, DrainMode::Flush)
    }

    /// Finish `first` through `staging` to `second`, then finish
    /// `second`.
    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        self.drain_through(output, DrainMode::Finish)
    }
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> {
    /// Push `input` through the chain into `output`.
    ///
    /// Repeat these steps:
    ///
    /// 1. Drain `staging` through `second` into `output`.
    /// 2. Push `input` through `first` into the empty `staging`.
    ///
    /// `second` sets the pace: `first` runs only after `second` has
    /// taken all the bytes in `staging`.
    ///
    /// If `first` fails, the chain still passes the bytes that `first`
    /// wrote before the failure through `second`. It returns the error
    /// of `first` after that, even if this needs several calls. If
    /// `second` fails on these bytes, the chain returns the error of
    /// `second`.
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        // A pass drains `staging` first. The loop stops when all of
        // `input` is consumed or `output` is full. If not, `first`
        // runs, and the loop starts a new pass.
        loop {
            //
            // Drain `staging` into `output`.
            //
            out_pos += self.drain_staging_into(output, out_pos, in_pos)?;

            // Case: `first` failed, and `second` has taken all of its
            // bytes. The error of `first` comes last in the data.
            if let Some(error) = self.first_error_if_staging_is_empty(in_pos, out_pos) {
                return Err(error);
            }
            // Case: input fully consumed, and nothing is left waiting
            // in staging for `second`.
            if in_pos == input.len() && self.is_staging_empty() {
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            // Case: output fully filled, and `second` still has bytes
            // waiting in staging.
            if out_pos == output.len() && !self.is_staging_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
            // `staging` is empty here. There are two cases:
            // - `out_pos != output.len()`. Then the postcondition of
            //   `drain_staging_into` says that `staging` is empty.
            // - `out_pos == output.len()`. Then the check above did
            //   not return, so `staging` was already empty.
            // The check for consumed input did not return either, so
            // `input` is not finished.
            //
            // With a full `output`, `first` still runs. The bytes
            // that it writes into `staging` go to `second` in the next
            // pass. `second` may take them without producing output.
            debug_assert_eq!(self.stage_start, 0);
            debug_assert_eq!(self.stage_end, 0);

            //
            // Push `input` through `first` into `staging`.
            //
            let staging = self.staging.as_mut();
            let input_len = input.len() - in_pos;
            let output_len = staging.len();
            let moved = match codec_step(&mut self.first, &input[in_pos..], as_uninit_mut(staging))
            {
                Ok(moved) => moved,
                Err(error) => {
                    let error = error
                        .validated(input_len, output_len)
                        .unwrap_or_else(|violation| violation);
                    // The next pass drains the bytes of `first`. Then
                    // it returns the error. The error reports the
                    // input that `first` consumed.
                    self.stage_end = error.written;
                    self.first_error = Some(error.kind);
                    in_pos += error.consumed;
                    continue;
                }
            };
            in_pos += moved.consumed;
            self.stage_end = moved.written;
        }
    }
}

#[cfg(all(test, feature = "alloc", feature = "base64"))]
mod tests {
    use core::convert::Infallible;
    use core::mem::MaybeUninit;

    use alloc::{boxed::Box, vec, vec::Vec};

    use super::{Chain, DrainMode};
    use crate::base64_dec::base64_dec;
    use crate::base64_enc::base64_enc;
    use crate::codecs::test_support::{
        DropEverything, FailsWithOverclaim, HoldsOutput, SplitSource,
    };
    use crate::identity::{identity, Identity};
    use crate::rot13::rot13;
    use crate::sources_and_sinks::slice::{SliceSink, SliceSource};
    use crate::sources_and_sinks::vec::{encode_string, EncodeError};
    use crate::uninit::{as_uninit_mut, copy_to_uninit};
    use crate::{
        stream_to_stream, Codec, DrainCodec, DrainProgress, DriveError, DriveErrorKind,
        EmptyBufferError, Error, ErrorKind, Progress, Pump, Sink,
    };

    const INPUT: &str = "Hello, World! 123";
    // rot13(base64_enc(INPUT)): $ echo -n "Hello, World! 123" | base64 | rot13
    const ROT13_OF_BASE64_INPUT: &str = "FTIfoT8fVSqipzkxVFNkZwZ=";

    // ----
    // Round-trip basics
    // ----

    #[test]
    fn smoke_test_using_rot13() {
        // rot13 then rot13 is identity.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 64]).unwrap();
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn base64_round_trip_through_one_byte_staging() {
        // Even base64's 4-byte groups fit through a 1-byte staging
        // buffer, thanks to the internal buffer of the codec
        let chain = Chain::new(base64_enc(), base64_dec(), vec![0u8; 1]).unwrap();
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn nested_chain_three_codecs() {
        // rot13 ∘ rot13 ∘ identity == identity, stacked three deep.
        let inner = Chain::new(rot13(), identity(), vec![0u8; 32]).unwrap();
        let outer = Chain::new(rot13(), inner, vec![0u8; 32]).unwrap();
        assert_eq!(encode_string(outer, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn dyn_composition_compiles_and_runs() {
        // Identity logic is already covered by `smoke_test_using_rot13`;
        // this test exists only to check that `Chain` accepts `Box<dyn Codec>`.
        let first: Box<dyn Codec> = Box::new(rot13());
        let second: Box<dyn Codec> = Box::new(rot13());
        let chain: Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>> =
            Chain::new(first, second, vec![0u8; 64]).unwrap();
        encode_string(chain, INPUT).unwrap();
    }

    #[test]
    fn finish_drains_first_through_second() {
        // base64_enc's finish() emits padding `=`; chained into rot13,
        // that padding must come out rot13'd too, not appended raw.
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 64]).unwrap();
        assert_eq!(encode_string(chain, INPUT).unwrap(), ROT13_OF_BASE64_INPUT);
    }

    // ----
    // Buffer-size edge cases
    // ----

    #[test]
    fn tiny_staging_buffer_forces_partial_progress() {
        // A 1-byte staging buffer forces `first` and `second` to
        // hand off one byte at a time internally.
        let chain = Chain::new(rot13(), rot13(), vec![0u8; 1]).unwrap();
        assert_eq!(encode_string(chain, INPUT).unwrap(), INPUT);
    }

    #[test]
    fn tiny_output_buffer_forces_partial_progress() {
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 8]).unwrap();
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
        let mut chain = Chain::new(rot13(), rot13(), vec![0u8; 64]).unwrap();
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
        let chain = Chain::new(base64_enc(), rot13(), vec![0u8; 4]).unwrap();
        let mut input = SliceSource::new(INPUT.as_bytes());
        let mut output = OneByteAtATimeSink { bytes: Vec::new() };
        stream_to_stream(&mut input, chain, &mut output).unwrap();
        assert_eq!(output.bytes, ROT13_OF_BASE64_INPUT.as_bytes());
    }

    // ----
    // `finish` draining
    // ----

    /// A block-buffering codec: hoards all input and emits it only on
    /// `finish`.
    #[derive(Default)]
    struct Hoarder {
        buf: Vec<u8>,
    }

    impl DrainCodec for Hoarder {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            let n = self.buf.len().min(output.len());
            copy_to_uninit(&mut output[..n], &self.buf[..n]);
            self.buf.drain(..n);
            if self.buf.is_empty() {
                Ok(DrainProgress::Done { written: n })
            } else {
                Ok(DrainProgress::OutputFilled)
            }
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
    fn finish_drains_a_hoarding_first_through_second() {
        // `first` withholds everything until finished; `Chain::finish`
        // must pull it out through `second` so the bytes arrive
        // transformed.
        let expected = encode_string(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(Hoarder::default(), rot13(), vec![0u8; 4]).unwrap();
        let mut out = [0u8; 64];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.finish(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
        assert_eq!(&out[..expected.len()], expected.as_bytes());
    }

    #[test]
    fn finish_drains_a_hoarding_second() {
        // `second` withholds; `Chain::finish` must invoke `second`'s
        // own finish after `first`'s.
        let expected = encode_string(rot13(), INPUT).unwrap();
        let mut chain = Chain::new(rot13(), Hoarder::default(), vec![0u8; 64]).unwrap();
        let mut out = [0u8; 64];
        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = chain.finish(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(
            drain,
            DrainProgress::Done {
                written: expected.len()
            }
        );
        assert_eq!(&out[..expected.len()], expected.as_bytes());
    }

    #[derive(Default)]
    struct CountingFinisher {
        inner: Identity,
        finish_calls: usize,
    }

    impl DrainCodec for CountingFinisher {
        fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.inner.flush(output)
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.finish_calls += 1;
            self.inner.finish(output)
        }
    }

    impl Codec for CountingFinisher {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            self.inner.process(input, output)
        }
    }

    #[test]
    fn finish_does_not_call_a_completed_first_codec_again() {
        let mut chain = Chain::new(
            CountingFinisher::default(),
            Hoarder::default(),
            vec![0u8; 8],
        )
        .unwrap();
        let mut output = [0u8; 8];

        let outcome = chain
            .process(INPUT.as_bytes(), as_uninit_mut(&mut output))
            .unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });

        let mut small_output = [0u8; 1];
        let mut outer_finish_calls = 0;
        loop {
            outer_finish_calls += 1;
            match chain.finish(as_uninit_mut(&mut small_output)).unwrap() {
                DrainProgress::OutputFilled => continue,
                DrainProgress::Done { .. } => break,
            }
        }
        assert!(
            outer_finish_calls > 1,
            "the test setup must force several outer finish calls, or it does not exercise the latch"
        );

        let (first, _second, _staging) = chain.into_parts();
        assert_eq!(
            first.finish_calls, 1,
            "first's finish must run exactly once, no matter how many outer finish calls it took"
        );
    }

    // ----
    // `flush` draining
    // ----

    /// Call `flush` with a 1-byte output until it reports `Done`.
    /// Return the collected output.
    fn flush_one_byte_at_a_time(codec: &mut impl DrainCodec) -> Vec<u8> {
        let mut collected = Vec::new();
        loop {
            let mut out = [0u8; 1];
            match codec.flush(as_uninit_mut(&mut out)).unwrap() {
                DrainProgress::OutputFilled => collected.push(out[0]),
                DrainProgress::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    return collected;
                }
            }
        }
    }

    #[test]
    fn flush_drains_a_holding_first_through_second() {
        // `first` holds "XXX", and rot13 turns each 'X' into 'K'.
        //
        // `flush` and `finish` share one drain loop. In that loop, it
        // is not immediately clear when it calls `finish`. If `Chain`
        // calls `finish` on `first` by mistake, the output also gets
        // the trailer "T".
        let first = HoldsOutput {
            held: 3,
            trailer: b"T",
            ..Default::default()
        };
        let mut chain = Chain::new(first, rot13(), vec![0u8; 1]).unwrap();
        assert_eq!(flush_one_byte_at_a_time(&mut chain), b"KKK");
    }

    #[test]
    fn flush_drains_a_holding_second() {
        // The same as `flush_drains_a_holding_first_through_second`,
        // but here `second` holds the output: "XX".
        let second = HoldsOutput {
            held: 2,
            trailer: b"T",
            ..Default::default()
        };
        let mut chain = Chain::new(identity(), second, vec![0u8; 1]).unwrap();
        assert_eq!(flush_one_byte_at_a_time(&mut chain), b"XX");
    }

    // ----
    // Error handling
    // ----

    /// Claims to consume more input than it was given.
    struct Overclaimer;

    impl DrainCodec for Overclaimer {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }

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
        let chain = Chain::new(rot13(), Overclaimer, vec![0u8; 8]).unwrap();
        match encode_string(chain, INPUT).unwrap_err() {
            EncodeError::Codec(error) => {
                assert_eq!(error.kind, crate::ErrorKind::ByteCountClaim);
            }
            other => panic!("expected a codec error, got {other:?}"),
        }
    }

    /// Claims to have written more than the buffer it was handed.
    struct DrainOverclaimer;

    impl DrainCodec for DrainOverclaimer {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done {
                written: output.len() + 1,
            })
        }
    }

    impl Codec for DrainOverclaimer {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn lying_drain_codec_is_an_error_not_index_corruption() {
        // Same property as `lying_inner_codec_is_an_error_not_index_corruption`,
        // but for a `finish` overclaim instead of a `process` overclaim:
        // `finish_step` validates the count before `drain_through`
        // ever uses it to set `stage_end`.
        let mut chain = Chain::new(DrainOverclaimer, identity(), vec![0u8; 4]).unwrap();
        let mut output = [0u8; 8];
        let error = chain.finish(as_uninit_mut(&mut output)).unwrap_err();
        assert_eq!(error.kind, crate::ErrorKind::ByteCountClaim);
    }

    #[test]
    fn an_overclaim_in_an_error_of_first_is_rejected() {
        let violation = Error::new(ErrorKind::ByteCountClaim, 0, 0);
        let mut output = [0u8; 8];

        let mut chain = Chain::new(FailsWithOverclaim, identity(), vec![0u8; 4]).unwrap();
        let error = chain
            .process(b"abc", as_uninit_mut(&mut output))
            .unwrap_err();
        assert_eq!(error, violation);

        let mut chain = Chain::new(FailsWithOverclaim, identity(), vec![0u8; 4]).unwrap();
        let error = chain.finish(as_uninit_mut(&mut output)).unwrap_err();
        assert_eq!(error, violation);
    }

    #[test]
    fn an_overclaim_in_an_error_of_second_is_rejected() {
        let mut output = [0u8; 8];

        // `first` moves the 3 input bytes to staging. So the chain
        // reports them as consumed, but `second` writes nothing.
        let mut chain = Chain::new(identity(), FailsWithOverclaim, vec![0u8; 4]).unwrap();
        let error = chain
            .process(b"abc", as_uninit_mut(&mut output))
            .unwrap_err();
        assert_eq!(error, Error::new(ErrorKind::ByteCountClaim, 3, 0));

        let mut chain = Chain::new(identity(), FailsWithOverclaim, vec![0u8; 4]).unwrap();
        let error = chain.finish(as_uninit_mut(&mut output)).unwrap_err();
        assert_eq!(error, Error::new(ErrorKind::ByteCountClaim, 0, 0));
    }

    /// Fails once, echoing 1 byte of real input in the error, then
    /// behaves like a plain `Identity`. Shared by both positions:
    /// the exact byte count it fakes on failure doesn't matter to
    /// either test, only that `Chain` rebases and keeps or drops bytes in
    /// staging correctly around it.
    struct FailsOnce {
        failed: bool,
        inner: Identity,
    }

    impl DrainCodec for FailsOnce {
        fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.inner.flush(output)
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.inner.finish(output)
        }
    }

    impl Codec for FailsOnce {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            if !self.failed {
                self.failed = true;
                copy_to_uninit(&mut output[..1], &input[..1]);
                return Err(Error::new(crate::ErrorKind::CorruptStream, 1, 1));
            }
            self.inner.process(input, output)
        }
    }

    /// Copies the input up to the first `marker`. At the marker, it
    /// fails with `kind` and reports the copied bytes, like
    /// `Base64Dec` does at a corrupt group. Tests use different kinds
    /// to tell the error of `first` from the error of `second`.
    struct FailsAtByte {
        marker: u8,
        kind: ErrorKind,
    }

    impl FailsAtByte {
        fn corrupt_at(marker: u8) -> Self {
            Self {
                marker,
                kind: ErrorKind::CorruptStream,
            }
        }

        fn unexpected_end_at(marker: u8) -> Self {
            Self {
                marker,
                kind: ErrorKind::UnexpectedEnd,
            }
        }
    }

    impl DrainCodec for FailsAtByte {
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }

        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for FailsAtByte {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            let good = input
                .iter()
                .position(|&b| b == self.marker)
                .unwrap_or(input.len());
            let n = good.min(output.len());
            copy_to_uninit(&mut output[..n], &input[..n]);
            if n == good && n < input.len() {
                return Err(Error::new(self.kind, n, n));
            }
            if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }
    }

    /// Writes `text` in `flush` or in `finish`, then fails in the call
    /// that writes the last byte. It reports the bytes of that call,
    /// like `Base64Dec` does at a corrupt end.
    struct FailsWhileDraining {
        text: &'static [u8],
        emitted: usize,
    }

    impl FailsWhileDraining {
        fn drain(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            let left = &self.text[self.emitted..];
            let n = left.len().min(output.len());
            copy_to_uninit(&mut output[..n], &left[..n]);
            self.emitted += n;
            if self.emitted == self.text.len() {
                return Err(Error::new(ErrorKind::CorruptStream, 0, n));
            }
            Ok(DrainProgress::OutputFilled)
        }
    }

    impl DrainCodec for FailsWhileDraining {
        fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.drain(output)
        }

        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            self.drain(output)
        }
    }

    impl Codec for FailsWhileDraining {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    const DRAIN_MODES: [(DrainMode, &str); 2] =
        [(DrainMode::Flush, "flush"), (DrainMode::Finish, "finish")];

    /// Drives the chain until it fails. `sink` takes one byte per call.
    /// So the chain needs several calls to deliver the bytes, and must
    /// keep the error until then.
    ///
    /// The tests use `Pump` only to save code: it already calls the
    /// codec in a loop, commits the bytes of an error and sums the
    /// counts. The tests do not check `Pump`.
    fn process_until_error<A: Codec, B: Codec>(
        chain: Chain<A, B, Vec<u8>>,
        input: &[u8],
        sink: &mut OneByteAtATimeSink,
    ) -> DriveError<Infallible, Infallible> {
        Pump::new(chain)
            .transfer_from(&mut SliceSource::new(input), sink)
            .unwrap_err()
    }

    /// As `process_until_error`, but the chain is flushed or finished.
    /// `Pump` is used only to save code here too.
    fn drain_until_error<A: Codec, B: Codec>(
        chain: Chain<A, B, Vec<u8>>,
        mode: DrainMode,
        sink: &mut OneByteAtATimeSink,
    ) -> DriveError<Infallible, Infallible> {
        let mut pump = Pump::new(chain);
        match mode {
            DrainMode::Flush => pump.flush_to(sink).unwrap_err(),
            DrainMode::Finish => pump.finish_to(sink).unwrap_err(),
        }
    }

    fn codec_error_kind(error: &DriveError<Infallible, Infallible>) -> ErrorKind {
        match &error.kind {
            DriveErrorKind::Codec(error) => error.kind,
            other => panic!("not a codec error: {other:?}"),
        }
    }

    // The four tests that follow cover two cases (an error of `first`,
    // and an error of `second` that comes before it) on two code paths
    // of `Chain` (`process`, and `flush`/`finish`). The paths are
    // separate, so each has its own test.
    //
    // In all four, `staging` holds all the valid bytes, but the sink
    // takes one byte per call.

    /// `first` fails in `process` after it wrote valid bytes. The chain
    /// passes these bytes through `second` before it reports the error.
    #[test]
    fn process_passes_output_of_first_through_before_the_error_of_first() {
        let chain = Chain::new(FailsAtByte::corrupt_at(b'!'), identity(), vec![0u8; 64]).unwrap();
        let mut sink = OneByteAtATimeSink { bytes: Vec::new() };
        let error = process_until_error(chain, b"abcde!!!!", &mut sink);
        assert_eq!(codec_error_kind(&error), ErrorKind::CorruptStream);
        assert_eq!(sink.bytes, b"abcde");
        assert_eq!(error.moved.consumed, 5);
    }

    /// Like `process_passes_output_of_first_through_before_the_error_of_first`,
    /// but `first` fails in `flush` or `finish`.
    #[test]
    fn drain_passes_output_of_first_through_before_the_error_of_first() {
        for (mode, name) in DRAIN_MODES {
            let first = FailsWhileDraining {
                text: b"01234",
                emitted: 0,
            };
            let chain = Chain::new(first, identity(), vec![0u8; 64]).unwrap();
            let mut sink = OneByteAtATimeSink { bytes: Vec::new() };
            let error = drain_until_error(chain, mode, &mut sink);
            assert_eq!(codec_error_kind(&error), ErrorKind::CorruptStream, "{name}");
            assert_eq!(sink.bytes, b"01234", "{name}");
            assert_eq!(error.moved.consumed, 0, "{name}");
        }
    }

    /// `second` fails in `process` on the bytes of `first`, before the
    /// point where `first` fails. The error that comes first in the
    /// data wins: the chain reports the error of `second`, and the
    /// output holds the bytes that `second` wrote before it failed.
    #[test]
    fn process_reports_the_error_of_second_when_it_comes_before_the_error_of_first() {
        let second = FailsAtByte::unexpected_end_at(b'e');
        let chain = Chain::new(FailsAtByte::corrupt_at(b'!'), second, vec![0u8; 64]).unwrap();
        let mut sink = OneByteAtATimeSink { bytes: Vec::new() };
        let error = process_until_error(chain, b"abcdef!!!!", &mut sink);
        assert_eq!(codec_error_kind(&error), ErrorKind::UnexpectedEnd);
        assert_eq!(sink.bytes, b"abcd");
    }

    /// Like
    /// `process_reports_the_error_of_second_when_it_comes_before_the_error_of_first`,
    /// but the errors come in `flush` or `finish`.
    #[test]
    fn drain_reports_the_error_of_second_when_it_comes_before_the_error_of_first() {
        for (mode, name) in DRAIN_MODES {
            let first = FailsWhileDraining {
                text: b"012345",
                emitted: 0,
            };
            let second = FailsAtByte::unexpected_end_at(b'4');
            let chain = Chain::new(first, second, vec![0u8; 64]).unwrap();
            let mut sink = OneByteAtATimeSink { bytes: Vec::new() };
            let error = drain_until_error(chain, mode, &mut sink);
            assert_eq!(codec_error_kind(&error), ErrorKind::UnexpectedEnd, "{name}");
            assert_eq!(sink.bytes, b"0123", "{name}");
        }
    }

    #[test]
    fn second_codec_error_progress_is_dropped_from_staging() {
        let mut chain = Chain::new(
            identity(),
            FailsOnce {
                failed: false,
                inner: identity(),
            },
            vec![0; 8],
        )
        .unwrap();
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
    fn finish_into_empty_output_is_done_when_nothing_is_pending() {
        // A plain codec answers `Done` to `finish(&mut [])` when it
        // holds nothing. A chain must give the same answer.
        assert_eq!(
            identity().finish(&mut []),
            Ok(DrainProgress::Done { written: 0 })
        );
        let mut chain = Chain::new(identity(), identity(), vec![0u8; 16]).unwrap();
        assert_eq!(
            chain.finish(&mut []),
            Ok(DrainProgress::Done { written: 0 })
        );
    }

    #[test]
    fn flush_into_empty_output_is_done_when_nothing_is_pending() {
        // A plain codec answers `Done` to `flush(&mut [])` when it
        // holds nothing. A chain must give the same answer.
        assert_eq!(
            identity().flush(&mut []),
            Ok(DrainProgress::Done { written: 0 })
        );
        let mut chain = Chain::new(identity(), identity(), vec![0u8; 16]).unwrap();
        assert_eq!(chain.flush(&mut []), Ok(DrainProgress::Done { written: 0 }));
    }

    #[test]
    fn flush_into_empty_output_is_done_when_second_dropped_the_input() {
        // `second` takes the bytes of `first` without producing
        // output. So nothing is pending, and `flush` needs no room.
        let mut chain = Chain::new(identity(), DropEverything, vec![0u8; 16]).unwrap();
        assert_eq!(
            chain.process(b"abc", &mut []),
            Ok(Progress::InputConsumed { written: 0 })
        );
        assert_eq!(chain.flush(&mut []), Ok(DrainProgress::Done { written: 0 }));
    }

    #[test]
    fn a_sink_that_the_output_fills_exactly_is_not_exhausted() {
        let input = b"hello";
        let mut buffer = [0u8; 5];
        let chain = Chain::new(identity(), identity(), vec![0u8; 16]).unwrap();
        let counts = stream_to_stream(
            &mut SliceSource::new(input),
            chain,
            &mut SliceSink::new(&mut buffer),
        )
        .unwrap();
        assert_eq!(counts.written, 5);
        assert_eq!(&buffer, input);
    }

    #[test]
    fn process_consumes_input_that_needs_no_output_when_output_is_empty() {
        // A plain `DropEverything` accepts the input with no room in
        // `output`. A chain with `DropEverything` as `first` must too.
        let mut chain = Chain::new(DropEverything, identity(), vec![0u8; 16]).unwrap();
        assert_eq!(
            chain.process(b"abc", &mut []),
            Ok(Progress::InputConsumed { written: 0 })
        );
    }

    #[test]
    fn process_consumes_input_that_second_drops_when_output_is_empty() {
        // `DropEverything` is `second`. `first` is `identity`, so it
        // writes the input into `staging`. `second` takes those bytes
        // and writes no output, so it needs no room in `output`. The
        // chain must consume the input.
        let mut chain = Chain::new(identity(), DropEverything, vec![0u8; 16]).unwrap();
        assert_eq!(
            chain.process(b"abc", &mut []),
            Ok(Progress::InputConsumed { written: 0 })
        );
    }

    #[test]
    fn a_full_sink_does_not_exhaust_a_chain_that_needs_no_output() {
        let chain = Chain::new(DropEverything, identity(), vec![0u8; 16]).unwrap();
        let counts = stream_to_stream(
            &mut SplitSource {
                bytes: b"abcdef",
                pos: 0,
                chunk_size: 2,
            },
            chain,
            &mut SliceSink::new(&mut []),
        )
        .unwrap();
        assert_eq!(counts.consumed, 6);
        assert_eq!(counts.written, 0);
    }

    #[test]
    fn finish_into_empty_output_is_output_filled_when_first_holds_a_trailer() {
        let first = HoldsOutput {
            trailer: b"END",
            ..HoldsOutput::default()
        };
        let mut chain = Chain::new(first, identity(), vec![0u8; 16]).unwrap();
        assert_eq!(chain.finish(&mut []), Ok(DrainProgress::OutputFilled));
    }

    #[test]
    fn a_sink_too_small_for_the_trailer_of_first_is_exhausted() {
        let first = HoldsOutput {
            trailer: b"END",
            ..HoldsOutput::default()
        };
        let chain = Chain::new(first, identity(), vec![0u8; 16]).unwrap();
        let mut buffer = [0u8; 2];
        let error = stream_to_stream(
            &mut SliceSource::new(b"hello"),
            chain,
            &mut SliceSink::new(&mut buffer),
        )
        .unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::SinkExhausted));
    }

    #[test]
    fn empty_staging_buffer_errors() {
        let result = Chain::new(rot13(), rot13(), Vec::<u8>::new());
        assert_eq!(result.err(), Some(EmptyBufferError));
    }
}
