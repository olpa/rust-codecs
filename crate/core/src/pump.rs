//! Bufferless codec lifecycle and the lending stream driver
//! ([`stream_to_stream`]) that ties a codec to a pair of endpoints.
//!
//! Endpoint adapters retain the buffers dictated by their direction:
//! readers own input storage, writers own output storage, and chunked
//! frontends lend both current windows. [`Pump`] owns only codec
//! lifecycle, so using it never introduces a byte copy.
//!
//! [`TerminatingCodec::process`](crate::TerminatingCodec::process)
//! reports only the counts not already implied by its outcome: all
//! input was consumed, all output was filled, or the stream ended.
//! [`crate::step::terminating_step`] validates that report and
//! normalizes it into exact progress on both sides — the trust
//! boundary every `Pump::process` call and [`stream_to_stream`] call
//! goes through.

use crate::step::{terminating_step, DrainOp, DrainStop, TerminatingStep, TerminatingStepEnd};
use crate::{Error, TerminatingCodec};

/// A byte source which lends its current input chunk to the pump.
///
/// See `CREATING-IO-BACKENDS.md` in the repository root for how to
/// implement one for a new transport.
pub trait Source {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    ///
    /// "Current" is load-bearing: this is whatever hasn't been
    /// released by `consume` yet, not necessarily fresh bytes. A
    /// caller is never required to consume a whole chunk in one call
    /// (a codec may only take part of it, e.g. when output runs out
    /// first) — the unconsumed remainder is exactly what the next
    /// `chunk()` call returns, so consecutive chunks can overlap.
    /// Implementations must not hand out new bytes ahead of `pos`
    /// until the old ones are released.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Release the first `amount` bytes of the current chunk.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the pump.
///
/// See `CREATING-IO-BACKENDS.md` in the repository root for how to
/// implement one for a new transport.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// How much moved through [`stream_to_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    pub consumed: usize,
    pub written: usize,
}

/// Why [`stream_to_stream`] stopped before the codec finished its stream.
#[derive(Debug)]
pub enum DriveError<EI, EO> {
    Source(EI),
    Sink(EO),
    Codec(crate::Error),
    SinkExhausted,
    /// A call moved zero bytes on both sides without ending the
    /// stream — the pump refuses to spin forever on a stalled
    /// codec/endpoint pair.
    NoProgress,
}

/// Drive one codec between any pair of lending stream adapters.
pub fn stream_to_stream<I, O, C>(
    input: &mut I,
    codec: C,
    output: &mut O,
) -> Result<Totals, DriveError<I::Error, O::Error>>
where
    I: Source,
    O: Sink,
    C: TerminatingCodec,
{
    let mut pump = Pump::new(codec);
    let mut totals = Totals { consumed: 0, written: 0 };

    let moved = pump.transfer_from(input, output)?;
    totals.consumed += moved.consumed;
    totals.written += moved.written;
    match moved.end {
        PumpEnd::End => {
            output.finish().map_err(DriveError::Sink)?;
            return Ok(totals);
        }
        PumpEnd::SinkExhausted => return Err(DriveError::SinkExhausted),
        PumpEnd::SourceExhausted => {}
    }

    let drained = pump.finish_to(output).map_err(|error| match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => DriveError::Sink(error),
        DriveError::Codec(error) => DriveError::Codec(error),
        DriveError::SinkExhausted => DriveError::SinkExhausted,
        DriveError::NoProgress => DriveError::NoProgress,
    })?;
    totals.written += drained.written;
    match drained.end {
        DrainEnd::Done => {
            output.finish().map_err(DriveError::Sink)?;
            Ok(totals)
        }
        DrainEnd::SinkExhausted => Err(DriveError::SinkExhausted),
    }
}

/// Exact progress and boundary of one validated `finish` or `flush`
/// call, as reported by [`Pump::finish`]/[`Pump::flush`] — renames
/// [`DrainStop`] to `SinkExhausted`/`Done`, since at this layer
/// `output` is always exactly one [`Sink::spare`] slice, so "the
/// buffer filled" and "the sink ran out of room this round" coincide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainStep {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainEnd {
    SinkExhausted,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpTransfer {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: PumpEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpEnd {
    SourceExhausted,
    SinkExhausted,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrain {
    pub(crate) written: usize,
    pub(crate) end: DrainEnd,
}

/// A bufferless lifecycle wrapper around a codec.
///
/// Public so a third-party `Source`/`Sink` backend can build its own
/// `Read`/`Write`-style wrapper on top of it, the same way this
/// crate's own `std_io`/`embedded_io` backends do: hold a `Pump<C>`
/// alongside your adapter, and drive it with
/// [`sources_and_sinks::shared_io`](crate::sources_and_sinks::shared_io)'s
/// `pump_read`/`pump_write`/`pump_finish`/`pump_flush` rather than
/// calling its own methods directly — those stay crate-private.
pub struct Pump<C> {
    codec: C,
    done: bool,
}

impl<C: TerminatingCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self { codec, done: false }
    }

    #[cfg_attr(not(feature = "std"), allow(dead_code))]
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<TerminatingStep, Error> {
        if self.done {
            return Ok(TerminatingStep {
                consumed: 0,
                written: 0,
                end: TerminatingStepEnd::End,
            });
        }
        let moved = terminating_step(&mut self.codec, input, output)?;
        if moved.end == TerminatingStepEnd::End {
            self.done = true;
        }
        Ok(moved)
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint has no more room/data to
    /// offer.
    ///
    /// Each loop iteration fetches one chunk from `input`, then one
    /// spare slice from `output`, calls [`Pump::process`] once, then
    /// feeds the result back (`input.consume`, `output.commit`).
    ///
    /// Returns once one of three things happens: the source is
    /// exhausted (`PumpEnd::SourceExhausted`), the sink has no more
    /// spare space (`PumpEnd::SinkExhausted`), or the codec itself
    /// signals the end of the stream (`PumpEnd::End`). A call
    /// that moves zero bytes on both sides without ending the stream
    /// is a stall — the sink offered an empty (but `Some`) slice and
    /// the codec couldn't do anything with it — and is reported as
    /// `DriveError::NoProgress`, the same as a codec error, both of
    /// which abort the loop after committing zero bytes to the sink.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        loop {
            // Invariants at top of loop: `consumed`/`written` are the
            // committed totals from prior iterations only (this
            // iteration hasn't moved anything yet); the codec hasn't
            // reported `End` (that returns immediately); neither
            // `input` nor `output` holds a chunk/spare left over from a
            // prior iteration (each iteration either resolves what it
            // borrowed via `process`, or commits 0 before returning).
            let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::SourceExhausted });
            };
            let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::SinkExhausted });
            };
            // A call may make progress with an empty `spare` (e.g. a
            // codec that only ever consumes input, never writing
            // anything), so instead of rejecting an empty slice up
            // front, progress is judged as part of the call's own
            // result, right alongside the error case: zero bytes moved
            // on both sides, without ending the stream, means this
            // pair genuinely can't advance.
            let moved = match self.process(chunk, spare) {
                Ok(moved)
                    if moved.consumed > 0
                        || moved.written > 0
                        || moved.end == TerminatingStepEnd::End =>
                {
                    moved
                }
                Ok(_) => {
                    output.commit(0).map_err(DriveError::Sink)?;
                    return Err(DriveError::NoProgress);
                }
                Err(error) => {
                    output.commit(0).map_err(DriveError::Sink)?;
                    return Err(DriveError::Codec(error));
                }
            };
            // `moved.consumed` may be less than `chunk.len()` (output
            // ran out first); the unconsumed remainder isn't lost —
            // it reappears (overlapping this chunk) on the next
            // `input.chunk()` call.
            input.consume(moved.consumed);
            output.commit(moved.written).map_err(DriveError::Sink)?;
            consumed += moved.consumed;
            written += moved.written;
            if moved.end == TerminatingStepEnd::End {
                return Ok(PumpTransfer { consumed, written, end: PumpEnd::End });
            }
        }
    }

    /// Drain the codec's trailing output by repeatedly calling
    /// [`Pump::finish`] against spare space taken from `output`,
    /// until the codec reports `Done`.
    ///
    /// This is the finalizing counterpart to `transfer_from`: it is
    /// called once the source is exhausted, to flush whatever bytes
    /// the codec still owes (e.g. block padding, trailers) with no
    /// further input. It shares its loop with `flush_to` via
    /// `drain_to(output, DrainOp::Finish)`, which selects `finish`
    /// (permanently ends the stream) over `flush` (may be called
    /// again later).
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Finish)
    }

    #[cfg_attr(not(any(feature = "std", feature = "embedded-io")), allow(dead_code))]
    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Flush)
    }

    /// Shared loop behind `finish_to`/`flush_to`: repeatedly obtain
    /// spare space from `output` and hand it to [`Pump::finish`] (if
    /// `op` is `DrainOp::Finish`) or [`Pump::flush`] (otherwise),
    /// committing what was written, until the codec reports `Done`.
    ///
    /// When `output.spare()` returns `None`, the sink has no room —
    /// but rather than immediately reporting `SinkExhausted`, one more
    /// `finish`/`flush` call is made against an empty slice (`&mut
    /// []`) to ask the codec whether it was actually done regardless
    /// (e.g. nothing left to write, or it errors/rejects the empty
    /// buffer). If that call reports `Done`, the drain is genuinely
    /// complete; otherwise it really is blocked on sink space, and
    /// `SinkExhausted` is returned. This avoids conflating "sink is
    /// full but the codec had nothing left anyway" with "sink is full
    /// and blocking real progress".
    ///
    /// A call that writes nothing and doesn't reach `Done` is a stall
    /// (`DriveError::NoProgress`), judged as part of the call's own
    /// result right alongside the error case, not by rejecting an
    /// empty spare slice up front — so a codec that happens to have
    /// nothing left to write against an empty buffer still completes
    /// normally. A codec error commits zero bytes to the sink before
    /// propagating.
    fn drain_to<O: Sink>(
        &mut self,
        output: &mut O,
        op: DrainOp,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let moved = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match self.finish_or_flush(spare, op) {
                    Ok(moved) if moved.written > 0 || moved.end == DrainEnd::Done => Ok(moved),
                    Ok(_) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::NoProgress);
                    }
                    Err(error) => {
                        output.commit(0).map_err(DriveError::Sink)?;
                        return Err(DriveError::Codec(error));
                    }
                },
                None => {
                    let moved = self.finish_or_flush(&mut [], op).map_err(DriveError::Codec)?;
                    return Ok(PumpDrain { written, end: if moved.end == DrainEnd::Done {
                        DrainEnd::Done
                    } else {
                        DrainEnd::SinkExhausted
                    }});
                }
            }
            .map_err(DriveError::Codec)?;
            output.commit(moved.written).map_err(DriveError::Sink)?;
            written += moved.written;
            if moved.end == DrainEnd::Done {
                return Ok(PumpDrain { written, end: DrainEnd::Done });
            }
        }
    }

    /// Dispatch to [`Pump::finish`] or [`Pump::flush`] by `op` — the
    /// `drain_to` loop's counterpart to [`DrainOp::step`] dispatching
    /// to `DrainCodec::finish`/`DrainCodec::flush`, one layer up.
    fn finish_or_flush(&mut self, output: &mut [u8], op: DrainOp) -> Result<DrainStep, Error> {
        match op {
            DrainOp::Finish => self.finish(output),
            DrainOp::Flush => self.flush(output),
        }
    }

    pub(crate) fn finish(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        self.drain(output, DrainOp::Finish)
    }

    pub(crate) fn flush(&mut self, output: &mut [u8]) -> Result<DrainStep, Error> {
        self.drain(output, DrainOp::Flush)
    }

    /// Shared engine behind [`Pump::finish`]/[`Pump::flush`]:
    /// `self.done` is only ever set by [`Pump::process`] reaching a
    /// genuine `TerminatingProgress::End` (contract point 4 — pinned
    /// forever, across every method). Reaching `Drain::Done` here is
    /// governed by point 3 instead: `finish` is idempotent against
    /// repeats of itself, but that doesn't license skipping `process`
    /// or `flush` afterward (point 6), so a `Done` from `self.codec`
    /// is never latched into `self.done` — only reported.
    fn drain(&mut self, output: &mut [u8], op: DrainOp) -> Result<DrainStep, Error> {
        if self.done {
            return Ok(DrainStep {
                written: 0,
                end: DrainEnd::Done,
            });
        }
        let step = op.step(&mut self.codec, output)?;
        Ok(DrainStep {
            written: step.written,
            end: match step.stop {
                DrainStop::OutputFilled => DrainEnd::SinkExhausted,
                DrainStop::Done => DrainEnd::Done,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{DrainEnd, Pump, TerminatingStepEnd, TerminatingStep, PumpEnd};
    use crate::step::terminating_step;
    use crate::{
        Codec, Drain, DrainCodec, DriveError, Error, ErrorKind, Progress, Sink, Source,
        TerminatingCodec, TerminatingProgress,
    };

    struct Scripted {
        process: TerminatingProgress,
        drain: Drain,
    }

    impl DrainCodec for Scripted {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }

        fn flush(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(self.drain)
        }
    }

    impl TerminatingCodec for Scripted {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<TerminatingProgress, Error> {
            Ok(self.process)
        }
    }

    /// A `Source` over a plain byte slice.
    struct SliceSource<'a> {
        bytes: &'a [u8],
        pos: usize,
    }

    impl Source for SliceSource<'_> {
        type Error = core::convert::Infallible;

        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..]))
        }

        fn consume(&mut self, amount: usize) {
            self.pos += amount;
        }
    }

    /// A `Sink` that always offers a zero-length slice and never
    /// reports exhaustion — stands in for an endpoint a codec doesn't
    /// actually need output room from.
    struct NullSink;

    impl Sink for NullSink {
        type Error = core::convert::Infallible;

        fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
            Ok(Some(&mut []))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            assert_eq!(amount, 0, "NullSink never offers room to write into");
            Ok(())
        }
    }

    /// Consumes everything, writes nothing — e.g. a hash/checksum
    /// pass with no output stream at all.
    struct DropEverything;

    impl DrainCodec for DropEverything {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl Codec for DropEverything {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn codec_that_only_consumes_input_needs_no_output_room() {
        let mut input = SliceSource { bytes: b"abcdef", pos: 0 };
        let mut output = NullSink;
        let mut pump = Pump::new(DropEverything);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 6);
        assert_eq!(moved.written, 0);
        assert_eq!(moved.end, PumpEnd::SourceExhausted);
    }

    #[test]
    fn a_pair_that_truly_cannot_progress_reports_no_progress() {
        let mut input = SliceSource { bytes: b"x", pos: 0 };
        let mut output = NullSink;
        let mut pump = Pump::new(Scripted {
            process: TerminatingProgress::OutputFilled { consumed: 0 },
            drain: Drain::Done { written: 0 },
        });
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    #[test]
    fn process_uses_the_shared_step() {
        let mut pump = Pump::new(Scripted {
            process: TerminatingProgress::OutputFilled { consumed: 2 },
            drain: Drain::Done { written: 0 },
        });
        let moved = pump.process(b"abc", &mut [0; 4]).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, TerminatingStepEnd::OutputExhausted);
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut pump = Pump::new(Scripted {
            process: TerminatingProgress::End {
                consumed: 1,
                written: 2,
            },
            drain: Drain::OutputFilled,
        });
        pump.process(b"abc", &mut [0; 4]).unwrap();
        assert!(pump.is_done());
        assert_eq!(pump.finish(&mut []).unwrap().end, DrainEnd::Done);
        let repeated = pump.process(b"trailing", &mut [0; 4]).unwrap();
        assert_eq!(repeated.consumed, 0);
        assert_eq!(repeated.written, 0);
        assert_eq!(repeated.end, TerminatingStepEnd::End);
    }

    #[test]
    fn finish_normalizes_output_progress_without_latching_done() {
        let mut filled = Pump::new(Scripted {
            process: TerminatingProgress::InputConsumed { written: 0 },
            drain: Drain::OutputFilled,
        });
        let moved = filled.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, DrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        // `finish` reaching `Done` is only point-3 self-idempotency,
        // not a point-4 pin — `process` must still be free to run
        // normally afterward (point 6), so `is_done()` stays false and
        // the codec, not a synthetic `End`, answers the next
        // `process` call.
        let mut done = Pump::new(Scripted {
            process: TerminatingProgress::InputConsumed { written: 5 },
            drain: Drain::Done { written: 2 },
        });
        let moved = done.finish(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!done.is_done());
        let resumed = done.process(b"abc", &mut [0; 8]).unwrap();
        assert_eq!(resumed.written, 5);
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut pump = Pump::new(Scripted {
            process: TerminatingProgress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = pump.flush(&mut [0; 3]).unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, DrainEnd::Done);
        assert!(!pump.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut pump = Pump::new(Scripted {
            process: TerminatingProgress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 4 },
        });
        assert_eq!(
            pump.finish(&mut [0; 3]),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
    }

    struct Reports(TerminatingProgress);

    impl DrainCodec for Reports {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl TerminatingCodec for Reports {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<TerminatingProgress, Error> {
            Ok(self.0)
        }
    }

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut codec = Reports(TerminatingProgress::InputConsumed { written: 2 });
        let mut output = [0; 5];

        assert_eq!(
            terminating_step(&mut codec, b"abc", &mut output),
            Ok(TerminatingStep {
                consumed: 3,
                written: 2,
                end: TerminatingStepEnd::InputExhausted,
            })
        );
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut codec = Reports(TerminatingProgress::OutputFilled { consumed: 2 });
        let mut output = [0; 5];

        assert_eq!(
            terminating_step(&mut codec, b"abc", &mut output),
            Ok(TerminatingStep {
                consumed: 2,
                written: 5,
                end: TerminatingStepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut codec = Reports(TerminatingProgress::End {
            consumed: 2,
            written: 4,
        });
        let mut output = [0; 5];

        assert_eq!(
            terminating_step(&mut codec, b"abc", &mut output),
            Ok(TerminatingStep {
                consumed: 2,
                written: 4,
                end: TerminatingStepEnd::End
            })
        );
    }

    #[test]
    fn degenerate_windows_remain_well_defined() {
        let mut input_done = Reports(TerminatingProgress::InputConsumed { written: 0 });
        assert_eq!(
            terminating_step(&mut input_done, b"", &mut []),
            Ok(TerminatingStep {
                consumed: 0,
                written: 0,
                end: TerminatingStepEnd::InputExhausted,
            })
        );

        let mut output_done = Reports(TerminatingProgress::OutputFilled { consumed: 0 });
        assert_eq!(
            terminating_step(&mut output_done, b"abc", &mut []),
            Ok(TerminatingStep {
                consumed: 0,
                written: 0,
                end: TerminatingStepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ContractViolation, 0, 0);

        let mut input_done = Reports(TerminatingProgress::InputConsumed { written: 6 });
        assert_eq!(
            terminating_step(&mut input_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut output_done = Reports(TerminatingProgress::OutputFilled { consumed: 4 });
        assert_eq!(
            terminating_step(&mut output_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut ended = Reports(TerminatingProgress::End {
            consumed: 4,
            written: 6,
        });
        assert_eq!(terminating_step(&mut ended, b"abc", &mut [0; 5]), Err(violation));
    }

    struct Fails;

    impl DrainCodec for Fails {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl Codec for Fails {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        }
    }

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            terminating_step(&mut Fails, b"abc", &mut [0; 5]),
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        );
    }
}
