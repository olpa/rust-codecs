//! Bufferless codec lifecycle and the lending stream driver
//! ([`stream_to_stream`]) that ties a codec to a pair of endpoints.
//!
//! Endpoint adapters retain the buffers dictated by their direction:
//! readers own input storage, writers own output storage, and chunked
//! frontends lend both current windows. [`Pump`] owns only codec
//! lifecycle, so using it never introduces a byte copy.
//!
//! [`EndCapableCodec::process`](crate::EndCapableCodec::process)
//! reports only the counts not already implied by its outcome: all
//! input was consumed, all output was filled, or the stream ended.
//! [`crate::step::end_capable_step`] validates that report and
//! normalizes it into exact progress on both sides — the trust
//! boundary every `Pump::latched_step` call and [`stream_to_stream`] call
//! goes through.

use crate::step::{end_capable_step, DrainOp, DrainStop, EndCapableStep, EndCapableStepEnd};
use crate::{EndCapableCodec, Error, Sink, Source};

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
    C: EndCapableCodec,
{
    let mut pump = Pump::new(codec);
    let mut totals = Totals {
        consumed: 0,
        written: 0,
    };

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
        PumpDrainEnd::Done => {
            output.finish().map_err(DriveError::Sink)?;
            Ok(totals)
        }
        PumpDrainEnd::SinkExhausted => Err(DriveError::SinkExhausted),
    }
}

/// Result of one whole [`Pump::transfer_from`] run: accumulated
/// `consumed`/`written` totals and which of the three ways it
/// stopped.
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

/// Result of one [`Pump::transfer_step`] call — the single-step
/// counterpart to [`PumpTransfer`]/[`PumpEnd`], which cover a whole
/// [`Pump::transfer_from`] run. Distinct from `PumpTransfer` because
/// [`PumpStepEnd`] has a fourth case, [`PumpStepEnd::Progressed`], that
/// `PumpEnd` deliberately has no room for: `transfer_from`'s loop
/// consumes it internally (keep looping) and never lets it escape as
/// its own `PumpEnd::End`/`SourceExhausted`/`SinkExhausted` result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpStep {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: PumpStepEnd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpStepEnd {
    SourceExhausted,
    SinkExhausted,
    /// The step completed normally (the codec fully consumed its
    /// input window or fully filled its output window) without
    /// exhausting `input` or `output` themselves — more may still be
    /// available from either on the next step.
    Progressed,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpDrainEnd {
    SinkExhausted,
    Done,
}

/// Result of one whole `drain_to` run (behind [`Pump::finish_to`]/
/// [`Pump::flush_to`]) — the [`PumpTransfer`] counterpart for the
/// drain side: `written` accumulates across every call `drain_to`
/// made, not one call's contribution, kept as a distinct type so the
/// two can't be mixed up at a call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrainTransfer {
    pub(crate) written: usize,
    pub(crate) end: PumpDrainEnd,
}

/// Exact progress and boundary of one validated `finish` or `flush`
/// call, as reported by [`Pump::finish_or_flush_step`] — the
/// single-call counterpart to [`PumpDrainTransfer`], which covers a
/// whole `drain_to` run. Renames [`DrainStop`] to
/// `SinkExhausted`/`Done`, since at this layer `output` is always
/// exactly one [`Sink::spare`] slice, so "the buffer filled" and "the
/// sink ran out of room this round" coincide.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrainStep {
    pub(crate) written: usize,
    pub(crate) end: PumpDrainEnd,
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
///
/// `C` is generic, not fixed to `EndCapableCodec` itself, because a
/// trait isn't a sized type a field can hold directly. The obvious
/// fix, `Box<dyn EndCapableCodec>`, would require `alloc`
/// unconditionally, breaking this crate's `no_std`-without-`alloc`
/// support. Callers who want that trade-off can still get it without
/// any change here, via `Pump<Box<dyn Codec>>`.
pub struct Pump<C> {
    codec: C,
    done: bool,
}

impl<C: EndCapableCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self { codec, done: false }
    }

    /// Reach the wrapped codec, e.g. to read state a `EndCapableCodec`
    /// call doesn't expose (a checksum, a digest) once the stream has
    /// ended.
    pub fn get_ref(&self) -> &C {
        &self.codec
    }

    /// Mutable counterpart to [`Pump::get_ref`].
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    /// Unwrap the codec back out, discarding `Pump`'s lifecycle state.
    pub fn into_inner(self) -> C {
        self.codec
    }

    /// Lets a caller like `shared_io::pump_read` short-circuit to a
    /// no-op once the stream has ended, instead of re-entering
    /// `transfer_from`/`finish_to` on every repeated call past EOF.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn latched_step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
    ) -> Result<EndCapableStep, Error> {
        if self.done {
            return Ok(EndCapableStep {
                consumed: 0,
                written: 0,
                end: EndCapableStepEnd::End,
            });
        }
        let moved = end_capable_step(&mut self.codec, input, output)?;
        if moved.end == EndCapableStepEnd::End {
            self.done = true;
        }
        Ok(moved)
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint has no more room/data to
    /// offer.
    ///
    /// Each loop iteration is one [`Pump::transfer_step`] call; this
    /// just accumulates its `consumed`/`written` counts and keeps
    /// looping past [`PumpStepEnd::Progressed`].
    ///
    /// Returns once one of three things happens: the source is
    /// exhausted (`PumpEnd::SourceExhausted`), the sink has no more
    /// spare space (`PumpEnd::SinkExhausted`), or the codec itself
    /// signals the end of the stream (`PumpEnd::End`). A call
    /// that moves zero bytes on both sides without ending the stream
    /// is a stall — the sink offered an empty (but `Some`) slice and
    /// the codec couldn't do anything with it — and is reported as
    /// `DriveError::NoProgress`, the same as a codec error, both of
    /// which abort the loop without committing anything to the sink;
    /// its uncommitted `spare` is simply left for the next caller to
    /// re-request.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        loop {
            // Invariant at top of loop: `consumed`/`written` are the
            // committed totals from prior iterations only (this
            // iteration hasn't moved anything yet).
            let step = self.transfer_step(input, output)?;
            consumed += step.consumed;
            written += step.written;
            let end = match step.end {
                PumpStepEnd::SourceExhausted => PumpEnd::SourceExhausted,
                PumpStepEnd::SinkExhausted => PumpEnd::SinkExhausted,
                PumpStepEnd::End => PumpEnd::End,
                PumpStepEnd::Progressed => continue,
            };
            return Ok(PumpTransfer {
                consumed,
                written,
                end,
            });
        }
    }

    /// Attempt exactly one [`Pump::latched_step`] between `input` and
    /// `output`: pull at most one chunk from `input`, hand it to the
    /// codec along with one spare slice from `output`, and feed the
    /// result back (`input.consume`, `output.commit`) — then return,
    /// without looping back for more input the way
    /// [`Pump::transfer_from`] does.
    ///
    /// This is the single-step primitive `transfer_from` loops on, and
    /// is also driven directly by
    /// [`crate::sources_and_sinks::shared_io::pump_read`] under
    /// [`crate::sources_and_sinks::shared_io::ReadGranularity::SingleRead`],
    /// so a `Read::read` call returns as soon as the wrapped source's
    /// own read produced anything, instead of coalescing multiple
    /// source reads into one call. That's the interactive-application
    /// case `SingleRead` exists for: a handler downstream of the
    /// `Read` should see each unit of input as soon as it arrives,
    /// not only once enough of them have piled up to fill some
    /// buffer it can't see into.
    ///
    /// Same stall/error handling as `transfer_from`'s loop body: a call
    /// that moves zero bytes on both sides without ending the stream is
    /// `DriveError::NoProgress`; a codec error still commits whatever
    /// progress it validly reported before returning it.
    pub(crate) fn transfer_step<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpStep, DriveError<I::Error, O::Error>> {
        let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
            return Ok(PumpStep {
                consumed: 0,
                written: 0,
                end: PumpStepEnd::SourceExhausted,
            });
        };
        let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
            return Ok(PumpStep {
                consumed: 0,
                written: 0,
                end: PumpStepEnd::SinkExhausted,
            });
        };
        // A call may make progress with an empty `spare` (e.g. a
        // codec that only ever consumes input, never writing
        // anything), so instead of rejecting an empty slice up
        // front, progress is judged as part of the call's own
        // result, right alongside the error case: zero bytes moved
        // on both sides, without ending the stream, means this
        // pair genuinely can't advance.
        let moved = match self.latched_step(chunk, spare) {
            Ok(moved)
                if moved.consumed > 0
                    || moved.written > 0
                    || moved.end == EndCapableStepEnd::End =>
            {
                moved
            }
            Ok(_) => return Err(DriveError::NoProgress),
            Err(error) => {
                let error = error
                    .validated(chunk.len(), spare.len())
                    .unwrap_or_else(|violation| violation);
                if error.consumed > 0 {
                    input.consume(error.consumed);
                }
                if error.written > 0 {
                    output.commit(error.written).map_err(DriveError::Sink)?;
                }
                return Err(DriveError::Codec(error));
            }
        };
        // `moved.consumed` may be less than `chunk.len()` (output ran
        // out first); the unconsumed remainder isn't lost — it
        // reappears (overlapping this chunk) on the next
        // `input.chunk()` call.
        if moved.consumed > 0 {
            input.consume(moved.consumed);
        }
        if moved.written > 0 {
            output.commit(moved.written).map_err(DriveError::Sink)?;
        }
        Ok(PumpStep {
            consumed: moved.consumed,
            written: moved.written,
            end: if moved.end == EndCapableStepEnd::End {
                PumpStepEnd::End
            } else {
                PumpStepEnd::Progressed
            },
        })
    }

    /// Drain the codec's trailing output by repeatedly calling
    /// [`Pump::finish_or_flush_step`] against spare space taken from
    /// `output`, until the codec reports `Done`.
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
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Finish)
    }

    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Flush)
    }

    /// Shared loop behind `finish_to`/`flush_to`: repeatedly obtain
    /// spare space from `output` and hand it to
    /// [`Pump::finish_or_flush_step`], committing what was written,
    /// until the codec reports `Done`.
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
    /// normally. Neither case commits anything to the sink before
    /// propagating; the uncommitted `spare` is simply left for the
    /// next caller to re-request.
    fn drain_to<O: Sink>(
        &mut self,
        output: &mut O,
        op: DrainOp,
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let moved = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match self.finish_or_flush_step(spare, op) {
                    Ok(moved) if moved.written > 0 || moved.end == PumpDrainEnd::Done => Ok(moved),
                    Ok(_) => return Err(DriveError::NoProgress),
                    Err(error) => {
                        let error = error
                            .validated(0, spare.len())
                            .unwrap_or_else(|violation| violation);
                        if error.written > 0 {
                            output.commit(error.written).map_err(DriveError::Sink)?;
                        }
                        return Err(DriveError::Codec(error));
                    }
                },
                None => {
                    let moved = self
                        .finish_or_flush_step(&mut [], op)
                        .map_err(DriveError::Codec)?;
                    return Ok(PumpDrainTransfer {
                        written,
                        end: if moved.end == PumpDrainEnd::Done {
                            PumpDrainEnd::Done
                        } else {
                            PumpDrainEnd::SinkExhausted
                        },
                    });
                }
            }
            .map_err(DriveError::Codec)?;
            if moved.written > 0 {
                output.commit(moved.written).map_err(DriveError::Sink)?;
            }
            written += moved.written;
            if moved.end == PumpDrainEnd::Done {
                return Ok(PumpDrainTransfer {
                    written,
                    end: PumpDrainEnd::Done,
                });
            }
        }
    }

    /// Run one `finish`/`flush` call against `output`, selecting which
    /// by `op` — the `drain_to` loop's counterpart to [`DrainOp::step`]
    /// dispatching to `DrainCodec::finish`/`DrainCodec::flush`, one
    /// layer up.
    ///
    /// `self.done` is only ever set by [`Pump::latched_step`] reaching
    /// a genuine `EndCapableProgress::End` (contract point 4 — pinned
    /// forever, across every method). Reaching `Drain::Done` here is
    /// governed by point 3 instead: this call is idempotent against
    /// repeats of itself, but that doesn't license skipping
    /// `latched_step` or a call with the other `op` afterward (point
    /// 6), so a `Done` from `self.codec` is never latched into
    /// `self.done` — only reported.
    fn finish_or_flush_step(
        &mut self,
        output: &mut [u8],
        op: DrainOp,
    ) -> Result<PumpDrainStep, Error> {
        if self.done {
            return Ok(PumpDrainStep {
                written: 0,
                end: PumpDrainEnd::Done,
            });
        }
        let step = op.step(&mut self.codec, output)?;
        Ok(PumpDrainStep {
            written: step.written,
            end: match step.stop {
                DrainStop::OutputFilled => PumpDrainEnd::SinkExhausted,
                DrainStop::Done => PumpDrainEnd::Done,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{EndCapableStep, EndCapableStepEnd, Pump, PumpDrainEnd, PumpEnd};
    use crate::step::{end_capable_step, DrainOp};
    use crate::{
        Codec, Drain, DrainCodec, DriveError, EndCapableCodec, EndCapableProgress, Error,
        ErrorKind, Progress, Sink, Source,
    };

    struct Scripted {
        process: EndCapableProgress,
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

    impl EndCapableCodec for Scripted {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [u8],
        ) -> Result<EndCapableProgress, Error> {
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

    struct RecordingSink {
        bytes: [u8; 8],
        written: usize,
    }

    impl Sink for RecordingSink {
        type Error = core::convert::Infallible;

        fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
            Ok(Some(&mut self.bytes[self.written..]))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            self.written += amount;
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
        let mut input = SliceSource {
            bytes: b"abcdef",
            pos: 0,
        };
        let mut output = NullSink;
        let mut pump = Pump::new(DropEverything);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 6);
        assert_eq!(moved.written, 0);
        assert_eq!(moved.end, PumpEnd::SourceExhausted);
    }

    struct FailsAfterProgress;

    impl DrainCodec for FailsAfterProgress {
        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            output[0] = b'!';
            Err(Error::new(ErrorKind::Corrupt, 0, 1))
        }
    }

    impl Codec for FailsAfterProgress {
        fn process(&mut self, _input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
            output[..2].copy_from_slice(b"ok");
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        }
    }

    #[test]
    fn process_error_progress_is_applied_to_endpoints() {
        let mut input = SliceSource {
            bytes: b"abc",
            pos: 0,
        };
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsAfterProgress);

        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();

        assert_eq!(input.pos, 1);
        assert_eq!(output.written, 2);
        assert_eq!(&output.bytes[..2], b"ok");
        assert!(matches!(
            error,
            DriveError::Codec(Error {
                kind: ErrorKind::Corrupt,
                consumed: 1,
                written: 2
            })
        ));
    }

    #[test]
    fn finish_error_progress_is_committed() {
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsAfterProgress);

        let error = pump.finish_to(&mut output).unwrap_err();

        assert_eq!(output.written, 1);
        assert_eq!(output.bytes[0], b'!');
        assert!(matches!(
            error,
            DriveError::Codec(Error {
                kind: ErrorKind::Corrupt,
                consumed: 0,
                written: 1
            })
        ));
    }

    #[test]
    #[cfg(feature = "alloc")]
    fn pump_accepts_a_boxed_trait_object_codec() {
        let mut input = SliceSource {
            bytes: b"abcdef",
            pos: 0,
        };
        let mut output = NullSink;
        let boxed: alloc::boxed::Box<dyn Codec> = alloc::boxed::Box::new(DropEverything);
        let mut pump: Pump<alloc::boxed::Box<dyn Codec>> = Pump::new(boxed);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 6);
        assert_eq!(moved.written, 0);
        assert_eq!(moved.end, PumpEnd::SourceExhausted);
    }

    #[test]
    fn a_pair_that_truly_cannot_progress_reports_no_progress() {
        let mut input = SliceSource {
            bytes: b"x",
            pos: 0,
        };
        let mut output = NullSink;
        let mut pump = Pump::new(Scripted {
            process: EndCapableProgress::OutputFilled { consumed: 0 },
            drain: Drain::Done { written: 0 },
        });
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    #[test]
    fn process_uses_the_shared_step() {
        let mut pump = Pump::new(Scripted {
            process: EndCapableProgress::OutputFilled { consumed: 2 },
            drain: Drain::Done { written: 0 },
        });
        let moved = pump.latched_step(b"abc", &mut [0; 4]).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, EndCapableStepEnd::OutputExhausted);
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut pump = Pump::new(Scripted {
            process: EndCapableProgress::End {
                consumed: 1,
                written: 2,
            },
            drain: Drain::OutputFilled,
        });
        pump.latched_step(b"abc", &mut [0; 4]).unwrap();
        assert!(pump.is_done());
        assert_eq!(
            pump.finish_or_flush_step(&mut [], DrainOp::Finish)
                .unwrap()
                .end,
            PumpDrainEnd::Done
        );
        let repeated = pump.latched_step(b"trailing", &mut [0; 4]).unwrap();
        assert_eq!(repeated.consumed, 0);
        assert_eq!(repeated.written, 0);
        assert_eq!(repeated.end, EndCapableStepEnd::End);
    }

    #[test]
    fn finish_normalizes_output_progress_without_latching_done() {
        let mut filled = Pump::new(Scripted {
            process: EndCapableProgress::InputConsumed { written: 0 },
            drain: Drain::OutputFilled,
        });
        let moved = filled
            .finish_or_flush_step(&mut [0; 3], DrainOp::Finish)
            .unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, PumpDrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        // `finish_or_flush_step` reaching `Done` is only point-3 self-idempotency,
        // not a point-4 pin — `latched_step` must still be free to run
        // normally afterward (point 6), so `is_done()` stays false and
        // the codec, not a synthetic `End`, answers the next
        // `latched_step` call.
        let mut done = Pump::new(Scripted {
            process: EndCapableProgress::InputConsumed { written: 5 },
            drain: Drain::Done { written: 2 },
        });
        let moved = done
            .finish_or_flush_step(&mut [0; 3], DrainOp::Finish)
            .unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, PumpDrainEnd::Done);
        assert!(!done.is_done());
        let resumed = done.latched_step(b"abc", &mut [0; 8]).unwrap();
        assert_eq!(resumed.written, 5);
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut pump = Pump::new(Scripted {
            process: EndCapableProgress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 2 },
        });
        let moved = pump
            .finish_or_flush_step(&mut [0; 3], DrainOp::Flush)
            .unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, PumpDrainEnd::Done);
        assert!(!pump.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut pump = Pump::new(Scripted {
            process: EndCapableProgress::InputConsumed { written: 0 },
            drain: Drain::Done { written: 4 },
        });
        assert_eq!(
            pump.finish_or_flush_step(&mut [0; 3], DrainOp::Finish),
            Err(Error::new(ErrorKind::ContractViolation, 0, 0))
        );
    }

    struct Reports(EndCapableProgress);

    impl DrainCodec for Reports {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl EndCapableCodec for Reports {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [u8],
        ) -> Result<EndCapableProgress, Error> {
            Ok(self.0)
        }
    }

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut codec = Reports(EndCapableProgress::InputConsumed { written: 2 });
        let mut output = [0; 5];

        assert_eq!(
            end_capable_step(&mut codec, b"abc", &mut output),
            Ok(EndCapableStep {
                consumed: 3,
                written: 2,
                end: EndCapableStepEnd::InputExhausted,
            })
        );
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut codec = Reports(EndCapableProgress::OutputFilled { consumed: 2 });
        let mut output = [0; 5];

        assert_eq!(
            end_capable_step(&mut codec, b"abc", &mut output),
            Ok(EndCapableStep {
                consumed: 2,
                written: 5,
                end: EndCapableStepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut codec = Reports(EndCapableProgress::End {
            consumed: 2,
            written: 4,
        });
        let mut output = [0; 5];

        assert_eq!(
            end_capable_step(&mut codec, b"abc", &mut output),
            Ok(EndCapableStep {
                consumed: 2,
                written: 4,
                end: EndCapableStepEnd::End
            })
        );
    }

    #[test]
    fn degenerate_windows_remain_well_defined() {
        let mut input_done = Reports(EndCapableProgress::InputConsumed { written: 0 });
        assert_eq!(
            end_capable_step(&mut input_done, b"", &mut []),
            Ok(EndCapableStep {
                consumed: 0,
                written: 0,
                end: EndCapableStepEnd::InputExhausted,
            })
        );

        let mut output_done = Reports(EndCapableProgress::OutputFilled { consumed: 0 });
        assert_eq!(
            end_capable_step(&mut output_done, b"abc", &mut []),
            Ok(EndCapableStep {
                consumed: 0,
                written: 0,
                end: EndCapableStepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ContractViolation, 0, 0);

        let mut input_done = Reports(EndCapableProgress::InputConsumed { written: 6 });
        assert_eq!(
            end_capable_step(&mut input_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut output_done = Reports(EndCapableProgress::OutputFilled { consumed: 4 });
        assert_eq!(
            end_capable_step(&mut output_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut ended = Reports(EndCapableProgress::End {
            consumed: 4,
            written: 6,
        });
        assert_eq!(
            end_capable_step(&mut ended, b"abc", &mut [0; 5]),
            Err(violation)
        );
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
            end_capable_step(&mut Fails, b"abc", &mut [0; 5]),
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        );
    }
}
