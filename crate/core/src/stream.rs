//! [`stream_to_stream`] drives a codec from an input source to an
//! output sink. [`Pump`], the helper behind it, is also used directly
//! by `std_io`/`embedded_io` wrappers.

use core::mem::MaybeUninit;

use crate::step::DrainOp;
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainProgress, Error, Sink, Source, TransferCounts,
};

/// Why [`stream_to_stream`] stopped before the codec finished its stream.
#[derive(Debug)]
pub enum DriveError<EI, EO> {
    Source(EI),
    Sink(EO),
    Codec(crate::Error),
    /// The sink had no more room (`Sink::spare` returned `None`)
    /// before the codec reached the end of its stream.
    SinkExhausted,
    /// The call moved zero bytes on both sides without ending the
    /// stream. The pump does not spin forever on a stalled codec or
    /// endpoint.
    NoProgress,
}

/// Drive the codec from the input source to the output sink.
pub fn stream_to_stream<I, O, C>(
    input: &mut I,
    codec: C,
    output: &mut O,
) -> Result<TransferCounts, DriveError<I::Error, O::Error>>
where
    I: Source,
    O: Sink,
    C: BoundaryAwareCodec,
{
    let mut pump = Pump::new(codec);
    let mut totals = TransferCounts::default();

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

/// Result of one whole [`Pump::transfer_from`] run: the accumulated
/// `consumed`/`written` totals, and how it stopped.
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

/// Result of one [`Pump::transfer_step`] call, the single-step
/// counterpart to [`PumpTransfer`]. Its [`PumpStepEnd`] has one more
/// case than [`PumpEnd`]: [`PumpStepEnd::Progressed`], which
/// `transfer_from` consumes internally and never returns as a
/// [`PumpEnd`].
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
    /// The step completed normally, without exhausting `input` or
    /// `output`. More may be available from either on the next step.
    Progressed,
    End,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpDrainEnd {
    SinkExhausted,
    Done,
}

/// Result of one whole `drain_to` run (behind [`Pump::finish_to`]/
/// [`Pump::sync_flush_to`]), the [`PumpTransfer`] counterpart for the
/// drain side. `written` accumulates across every call `drain_to`
/// made, not just the last one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrainTransfer {
    pub(crate) written: usize,
    pub(crate) end: PumpDrainEnd,
}

/// Exact progress of one validated `finish`/`sync_flush` call, as
/// reported by [`Pump::finish_or_sync_flush_step`] — the single-call
/// counterpart to [`PumpDrainTransfer`]. Here `output` is always one
/// [`Sink::spare`] slice, so a filled buffer and an exhausted sink
/// are the same event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PumpDrainStep {
    pub(crate) written: usize,
    pub(crate) end: PumpDrainEnd,
}

/// A bufferless lifecycle wrapper around a codec.
///
/// Public so a third-party `Source`/`Sink` backend can build its own
/// `Read`/`Write`-style wrapper on top of it, the way this crate's
/// own `std_io`/`embedded_io` backends do. Drive it through
/// [`sources_and_sinks::shared_io`](crate::sources_and_sinks::shared_io);
/// its own methods stay crate-private.
///
/// `C` is generic rather than fixed to `BoundaryAwareCodec`, since a
/// trait is not a sized type a field can hold. A caller who wants a
/// boxed codec can still use `Pump<Box<dyn Codec>>`, without forcing
/// `alloc` on everyone else.
pub struct Pump<C> {
    codec: C,
    done: bool,
}

impl<C: BoundaryAwareCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self { codec, done: false }
    }

    /// Access the wrapped codec, for example to read state it does
    /// not expose through `BoundaryAwareCodec`, such as a checksum,
    /// once the stream has ended.
    pub fn get_ref(&self) -> &C {
        &self.codec
    }

    /// Mutable counterpart to [`Pump::get_ref`].
    pub fn get_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    /// Unwrap the codec, discarding `Pump`'s lifecycle state.
    pub fn into_inner(self) -> C {
        self.codec
    }

    /// Tell a caller such as `shared_io::boundary_aware_pump_read`
    /// that the stream has ended, so it can skip
    /// `transfer_from`/`finish_to` on repeated calls past EOF.
    pub(crate) fn is_done(&self) -> bool {
        self.done
    }

    pub(crate) fn latched_step(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        if self.done {
            return Ok(BoundaryAwareProgress::Boundary {
                consumed: 0,
                written: 0,
            });
        }
        let progress = self
            .codec
            .process(input, output)?
            .validated(input.len(), output.len())?;
        if matches!(progress, BoundaryAwareProgress::Boundary { .. }) {
            self.done = true;
        }
        Ok(progress)
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint runs out of room or data.
    ///
    /// Each loop iteration runs one [`Pump::transfer_step`] call and
    /// accumulates its counts.
    ///
    /// Returns when one of three things happens: the source is
    /// exhausted, the sink has no more spare space, or the codec
    /// signals the end of the stream. A call that moves zero bytes on
    /// both sides without ending the stream is a stall, reported as
    /// `DriveError::NoProgress`. Like a codec error, it aborts the
    /// loop without committing anything to the sink.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        loop {
            // `consumed`/`written` hold only totals from prior
            // iterations here.
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

    /// Run exactly one [`Pump::latched_step`] between `input` and
    /// `output`: pull at most one chunk from `input`, hand it to the
    /// codec with one spare slice from `output`, and commit the
    /// result. Unlike [`Pump::transfer_from`], it does not loop back
    /// for more input.
    ///
    /// `transfer_from` loops on this primitive. It is also driven
    /// directly by
    /// [`crate::sources_and_sinks::shared_io::boundary_aware_pump_read`],
    /// so a `Read::read` call returns as soon as the source produced
    /// anything, instead of coalescing several source reads into one
    /// call — useful for an interactive caller that wants to see each
    /// unit of input as soon as it arrives.
    ///
    /// Same stall and error handling as `transfer_from`: a call that
    /// moves zero bytes on both sides without ending the stream is
    /// `DriveError::NoProgress`. A codec error still commits whatever
    /// progress it validly reported.
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
        // A call may still progress with an empty `spare` (e.g. a
        // codec that only consumes input). So an empty slice is not
        // rejected up front — zero bytes moved on both sides without
        // ending the stream is what marks a genuine stall.
        let progress = match self.latched_step(chunk, spare) {
            Ok(progress) => progress,
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
        let (moved, boundary) = match progress {
            BoundaryAwareProgress::InputConsumed { written } => (
                TransferCounts {
                    consumed: chunk.len(),
                    written,
                },
                false,
            ),
            BoundaryAwareProgress::OutputFilled { consumed } => (
                TransferCounts {
                    consumed,
                    written: spare.len(),
                },
                false,
            ),
            BoundaryAwareProgress::Boundary { consumed, written } => {
                (TransferCounts { consumed, written }, true)
            }
        };
        if moved.consumed == 0 && moved.written == 0 && !boundary {
            return Err(DriveError::NoProgress);
        }
        // `moved.consumed` may be less than `chunk.len()` if output
        // ran out first. The unconsumed remainder is not lost: it
        // reappears on the next `input.chunk()` call.
        if moved.consumed > 0 {
            input.consume(moved.consumed);
        }
        if moved.written > 0 {
            output.commit(moved.written).map_err(DriveError::Sink)?;
        }
        Ok(PumpStep {
            consumed: moved.consumed,
            written: moved.written,
            end: if boundary {
                PumpStepEnd::End
            } else {
                PumpStepEnd::Progressed
            },
        })
    }

    /// Drain the codec's trailing output by repeatedly calling
    /// [`Pump::finish_or_sync_flush_step`] against spare space from
    /// `output`, until the codec reports `Done`.
    ///
    /// Call this once the source is exhausted, to flush whatever
    /// bytes the codec still owes (e.g. padding, a trailer). It
    /// shares its loop with `sync_flush_to` via
    /// `drain_to(output, DrainOp::Finish)`, which selects `finish`
    /// (ends the stream for good) over `sync_flush` (resumable).
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Finish)
    }

    pub(crate) fn sync_flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::SyncFlush)
    }

    /// Shared loop behind `finish_to`/`sync_flush_to`: repeatedly get
    /// spare space from `output` and hand it to
    /// [`Pump::finish_or_sync_flush_step`], committing what was
    /// written, until the codec reports `Done`.
    ///
    /// When `output.spare()` returns `None`, the sink has no room.
    /// One more call is made against an empty slice, to check whether
    /// the codec was actually done regardless. If that reports
    /// `Done`, the drain is complete; otherwise it really is blocked
    /// on sink space, and `SinkExhausted` is returned. This tells "the
    /// sink is full but the codec had nothing left" apart from "the
    /// sink is full and blocking real progress".
    ///
    /// A call that writes nothing and does not reach `Done` is a
    /// stall (`DriveError::NoProgress`). Neither case commits
    /// anything to the sink before propagating; the uncommitted
    /// `spare` stays available for the next caller.
    fn drain_to<O: Sink>(
        &mut self,
        output: &mut O,
        op: DrainOp,
    ) -> Result<PumpDrainTransfer, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let moved = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match self.finish_or_sync_flush_step(spare, op) {
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
                        .finish_or_sync_flush_step(&mut [], op)
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

    /// Run one `finish`/`sync_flush` call against `output`, chosen by
    /// `op` — the `drain_to` loop's counterpart to [`DrainOp::step`].
    ///
    /// `self.done` is set only by [`Pump::latched_step`] reaching a
    /// genuine `BoundaryAwareProgress::Boundary`; once set, `Pump`
    /// treats it as permanent. `DrainProgress::Done` from `self.codec`
    /// is different: `finish`/`sync_flush` may still be called again
    /// later, so `Done` is reported here but never latched into
    /// `self.done`.
    fn finish_or_sync_flush_step(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        op: DrainOp,
    ) -> Result<PumpDrainStep, Error> {
        if self.done {
            return Ok(PumpDrainStep {
                written: 0,
                end: PumpDrainEnd::Done,
            });
        }
        Ok(match op.step(&mut self.codec, output)? {
            DrainProgress::OutputFilled => PumpDrainStep {
                written: output.len(),
                end: PumpDrainEnd::SinkExhausted,
            },
            DrainProgress::Done { written } => PumpDrainStep {
                written,
                end: PumpDrainEnd::Done,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::{Pump, PumpDrainEnd, PumpEnd};
    use crate::step::DrainOp;
    use crate::{
        BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, DriveError,
        Error, ErrorKind, Progress, Sink, Source,
    };

    struct Scripted {
        process: BoundaryAwareProgress,
        drain: DrainProgress,
    }

    impl DrainCodec for Scripted {
        fn sync_flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(self.drain)
        }

        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(self.drain)
        }
    }

    impl BoundaryAwareCodec for Scripted {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<BoundaryAwareProgress, Error> {
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
    /// reports exhaustion. Stands in for an endpoint that needs no
    /// output room.
    struct NullSink;

    impl Sink for NullSink {
        type Error = core::convert::Infallible;

        fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
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

        fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
            Ok(Some(crate::uninit::as_uninit_mut(
                &mut self.bytes[self.written..],
            )))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            self.written += amount;
            Ok(())
        }
    }

    /// Consumes everything, writes nothing, like a checksum pass
    /// with no output stream.
    struct DropEverything;

    impl DrainCodec for DropEverything {
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for DropEverything {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
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
        fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            output[0].write(b'!');
            Err(Error::new(ErrorKind::CorruptStream, 0, 1))
        }
    }

    impl Codec for FailsAfterProgress {
        fn process(
            &mut self,
            _input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            output[..2].write_copy_of_slice(b"ok");
            Err(Error::new(ErrorKind::CorruptStream, 1, 2))
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
                kind: ErrorKind::CorruptStream,
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
                kind: ErrorKind::CorruptStream,
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
            process: BoundaryAwareProgress::OutputFilled { consumed: 0 },
            drain: DrainProgress::Done { written: 0 },
        });
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    #[test]
    fn latched_step_validates_progress() {
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 2 },
            drain: DrainProgress::Done { written: 0 },
        });
        let progress = pump
            .latched_step(b"abc", &mut [MaybeUninit::uninit(); 4])
            .unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::OutputFilled { consumed: 2 }
        );
    }

    #[test]
    fn in_band_end_latches_completion() {
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::Boundary {
                consumed: 1,
                written: 2,
            },
            drain: DrainProgress::OutputFilled,
        });
        pump.latched_step(b"abc", &mut [MaybeUninit::uninit(); 4])
            .unwrap();
        assert!(pump.is_done());
        assert_eq!(
            pump.finish_or_sync_flush_step(&mut [], DrainOp::Finish)
                .unwrap()
                .end,
            PumpDrainEnd::Done
        );
        let repeated = pump
            .latched_step(b"trailing", &mut [MaybeUninit::uninit(); 4])
            .unwrap();
        assert_eq!(
            repeated,
            BoundaryAwareProgress::Boundary {
                consumed: 0,
                written: 0
            }
        );
    }

    #[test]
    fn finish_normalizes_output_progress_without_latching_done() {
        let mut filled = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 0 },
            drain: DrainProgress::OutputFilled,
        });
        let moved = filled
            .finish_or_sync_flush_step(&mut [MaybeUninit::uninit(); 3], DrainOp::Finish)
            .unwrap();
        assert_eq!(moved.written, 3);
        assert_eq!(moved.end, PumpDrainEnd::SinkExhausted);
        assert!(!filled.is_done());

        // Reaching `Done` here does not latch `self.done`; only an
        // in-band `End` from `latched_step` does that. So
        // `is_done()` stays false, and the codec — not a synthetic
        // `End` — answers the next `latched_step` call.
        let mut done = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 5 },
            drain: DrainProgress::Done { written: 2 },
        });
        let moved = done
            .finish_or_sync_flush_step(&mut [MaybeUninit::uninit(); 3], DrainOp::Finish)
            .unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, PumpDrainEnd::Done);
        assert!(!done.is_done());
        let resumed = done
            .latched_step(b"abc", &mut [MaybeUninit::uninit(); 8])
            .unwrap();
        assert_eq!(resumed, BoundaryAwareProgress::InputConsumed { written: 5 });
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 0 },
            drain: DrainProgress::Done { written: 2 },
        });
        let moved = pump
            .finish_or_sync_flush_step(&mut [MaybeUninit::uninit(); 3], DrainOp::SyncFlush)
            .unwrap();
        assert_eq!(moved.written, 2);
        assert_eq!(moved.end, PumpDrainEnd::Done);
        assert!(!pump.is_done());
    }

    #[test]
    fn drain_overclaims_are_contract_violations() {
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 0 },
            drain: DrainProgress::Done { written: 4 },
        });
        assert_eq!(
            pump.finish_or_sync_flush_step(&mut [MaybeUninit::uninit(); 3], DrainOp::Finish),
            Err(Error::new(ErrorKind::ByteCountClaim, 0, 0))
        );
    }

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut input = SliceSource {
            bytes: b"abc",
            pos: 0,
        };
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 2 },
            drain: DrainProgress::Done { written: 0 },
        });

        let moved = pump.transfer_step(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 3);
        assert_eq!(moved.written, 2);
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut input = SliceSource {
            bytes: b"abc",
            pos: 0,
        };
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 3,
        };
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 2 },
            drain: DrainProgress::Done { written: 0 },
        });

        let moved = pump.transfer_step(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 5);
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut input = SliceSource {
            bytes: b"abc",
            pos: 0,
        };
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::Boundary {
                consumed: 2,
                written: 4,
            },
            drain: DrainProgress::Done { written: 0 },
        });

        let moved = pump.transfer_step(&mut input, &mut output).unwrap();
        assert_eq!(moved.consumed, 2);
        assert_eq!(moved.written, 4);
        assert_eq!(moved.end, super::PumpStepEnd::End);
    }

    #[test]
    fn degenerate_windows_remain_well_defined() {
        let input_done = BoundaryAwareProgress::InputConsumed { written: 0 }
            .validated(0, 0)
            .unwrap();
        assert_eq!(
            input_done,
            BoundaryAwareProgress::InputConsumed { written: 0 }
        );

        let output_done = BoundaryAwareProgress::OutputFilled { consumed: 0 }
            .validated(3, 0)
            .unwrap();
        assert_eq!(
            output_done,
            BoundaryAwareProgress::OutputFilled { consumed: 0 }
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ByteCountClaim, 0, 0);

        let mut input_done = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 6 },
            drain: DrainProgress::Done { written: 0 },
        });
        assert_eq!(
            input_done.latched_step(b"abc", &mut [MaybeUninit::uninit(); 5]),
            Err(violation)
        );

        let mut output_done = Pump::new(Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 4 },
            drain: DrainProgress::Done { written: 0 },
        });
        assert_eq!(
            output_done.latched_step(b"abc", &mut [MaybeUninit::uninit(); 5]),
            Err(violation)
        );

        let mut ended = Pump::new(Scripted {
            process: BoundaryAwareProgress::Boundary {
                consumed: 4,
                written: 6,
            },
            drain: DrainProgress::Done { written: 0 },
        });
        assert_eq!(
            ended.latched_step(b"abc", &mut [MaybeUninit::uninit(); 5]),
            Err(violation)
        );
    }

    struct Fails;

    impl DrainCodec for Fails {
        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for Fails {
        fn process(
            &mut self,
            _input: &[u8],
            _output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            Err(Error::new(ErrorKind::CorruptStream, 1, 2))
        }
    }

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            Pump::new(Fails).latched_step(b"abc", &mut [MaybeUninit::uninit(); 5]),
            Err(Error::new(ErrorKind::CorruptStream, 1, 2))
        );
    }
}
