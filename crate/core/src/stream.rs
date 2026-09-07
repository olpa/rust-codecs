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

    match pump.transfer_from(input, output)? {
        PumpTransfer::End(moved) => {
            totals.consumed += moved.consumed;
            totals.written += moved.written;
            output.finish().map_err(DriveError::Sink)?;
            return Ok(totals);
        }
        PumpTransfer::SinkExhausted(_) => return Err(DriveError::SinkExhausted),
        PumpTransfer::SourceExhausted(moved) => {
            totals.consumed += moved.consumed;
            totals.written += moved.written;
        }
        PumpTransfer::Progressed(_) => unreachable!("transfer_from consumes progress internally"),
    }

    let drained = pump.finish_to(output).map_err(|error| match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => DriveError::Sink(error),
        DriveError::Codec(error) => DriveError::Codec(error),
        DriveError::SinkExhausted => DriveError::SinkExhausted,
        DriveError::NoProgress => DriveError::NoProgress,
    })?;
    match drained {
        PumpDrain::Done { written } => {
            totals.written += written;
            output.finish().map_err(DriveError::Sink)?;
            Ok(totals)
        }
        PumpDrain::SinkExhausted { .. } => Err(DriveError::SinkExhausted),
    }
}

/// Result of one pump transfer, with the counts moved before it stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpTransfer {
    SourceExhausted(TransferCounts),
    SinkExhausted(TransferCounts),
    /// The step completed normally, without exhausting `input` or
    /// `output`. More may be available from either on the next step.
    Progressed(TransferCounts),
    End(TransferCounts),
}

/// Result of one pump drain, with the bytes written before it stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpDrain {
    SinkExhausted { written: usize },
    Done { written: usize },
}

/// A bufferless lifecycle wrapper around a codec: processes input
/// until the stream ends, then drains what's owed.
///
/// Used by [`stream_to_stream`] and by I/O backend wrappers.
///
/// `Pump` is public only because third-party I/O backends are built
/// on top of
/// [`sources_and_sinks::shared_io`](crate::sources_and_sinks::shared_io),
/// whose functions must name `Pump` in their signatures.
///
/// `C` is generic rather than fixed to [`BoundaryAwareCodec`], since a
/// trait is not a sized type a field can hold. A caller who wants a
/// boxed codec can still use `Pump<Box<dyn Codec>>`.
pub struct Pump<C> {
    codec: C,
    ended_in_band: bool,
}

impl<C: BoundaryAwareCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            ended_in_band: false,
        }
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
    /// that the stream has ended.
    pub(crate) fn is_done(&self) -> bool {
        self.ended_in_band
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint runs out of room or data.
    ///
    /// Returns when one of three things happens:
    /// - the source is exhausted
    /// - the sink has no more spare space
    /// - the codec signals the end of the stream
    ///
    /// A call that moves zero bytes on both sides without ending the
    /// stream is a stall, reported as `DriveError::NoProgress`.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let mut consumed = 0;
        let mut written = 0;
        loop {
            let step = self.transfer_step(input, output)?;
            let total = |moved: TransferCounts| TransferCounts {
                consumed: consumed + moved.consumed,
                written: written + moved.written,
            };
            match step {
                PumpTransfer::Progressed(moved) => {
                    consumed += moved.consumed;
                    written += moved.written;
                }
                PumpTransfer::SourceExhausted(moved) => {
                    return Ok(PumpTransfer::SourceExhausted(total(moved)));
                }
                PumpTransfer::SinkExhausted(moved) => {
                    return Ok(PumpTransfer::SinkExhausted(total(moved)));
                }
                PumpTransfer::End(moved) => return Ok(PumpTransfer::End(total(moved))),
            }
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
    ///
    /// Same stall and error handling as `transfer_from`: a call that
    /// moves zero bytes on both sides without ending the stream is
    /// `DriveError::NoProgress`. A codec error still commits whatever
    /// progress it validly reported.
    pub(crate) fn transfer_step<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        let Some(chunk) = input.chunk().map_err(DriveError::Source)? else {
            return Ok(PumpTransfer::SourceExhausted(TransferCounts::default()));
        };
        let Some(spare) = output.spare().map_err(DriveError::Sink)? else {
            return Ok(PumpTransfer::SinkExhausted(TransferCounts::default()));
        };
        // A call may still progress with an empty `spare` (e.g. a
        // codec that only consumes input). So an empty slice is not
        // rejected up front, zero bytes moved on both sides without
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
        Ok(if boundary {
            PumpTransfer::End(moved)
        } else {
            PumpTransfer::Progressed(moved)
        })
    }

    pub(crate) fn latched_step(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<BoundaryAwareProgress, Error> {
        if self.ended_in_band {
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
            self.ended_in_band = true;
        }
        Ok(progress)
    }

    /// Drain the codec's trailing output.
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::Finish)
    }

    /// Let deflate/zlib and similar codecs write a sync marker mid-stream.
    pub(crate) fn sync_flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_to(output, DrainOp::SyncFlush)
    }

    /// Shared loop behind `finish_to`/`sync_flush_to`: repeatedly get
    /// spare space from `output` and hand it to
    /// [`Pump::latched_finish_or_sync_flush_step`], committing what was
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
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let (step_written, done) = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match self.latched_finish_or_sync_flush_step(spare, op) {
                    Ok(DrainProgress::Done { written }) => (written, true),
                    Ok(DrainProgress::OutputFilled) if !spare.is_empty() => (spare.len(), false),
                    Ok(DrainProgress::OutputFilled) => return Err(DriveError::NoProgress),
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
                        .latched_finish_or_sync_flush_step(&mut [], op)
                        .map_err(DriveError::Codec)?;
                    return Ok(match moved {
                        DrainProgress::Done { .. } => PumpDrain::Done { written },
                        DrainProgress::OutputFilled => PumpDrain::SinkExhausted { written },
                    });
                }
            };
            if step_written > 0 {
                output.commit(step_written).map_err(DriveError::Sink)?;
            }
            written += step_written;
            if done {
                return Ok(PumpDrain::Done { written });
            }
        }
    }

    /// Run one `finish`/`sync_flush` call, chosen by `op`.
    ///
    /// If the codec already ended in-band, [`BoundaryAwareCodec::process`]'s
    /// contract already required it to finish itself first. So this
    /// function skips the call. It reports a permanent `Done` and
    /// never touches the codec again.
    fn latched_finish_or_sync_flush_step(
        &mut self,
        output: &mut [MaybeUninit<u8>],
        op: DrainOp,
    ) -> Result<DrainProgress, Error> {
        if self.ended_in_band {
            return Ok(DrainProgress::Done { written: 0 });
        }
        op.step(&mut self.codec, output)
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::{Pump, PumpTransfer};
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::step::DrainOp;
    use crate::{
        BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, DriveError,
        Error, ErrorKind, Progress, Sink, TransferCounts,
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

    // ----
    // Pump::transfer_from
    // ----

    #[test]
    fn codec_that_only_consumes_input_needs_no_output_room() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = NullSink;
        let mut pump = Pump::new(DropEverything);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(
            moved,
            PumpTransfer::SourceExhausted(TransferCounts {
                consumed: 6,
                written: 0,
            })
        );
    }

    #[test]
    fn process_error_progress_is_applied_to_endpoints() {
        let mut input = SliceSource::new(b"abc");
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsAfterProgress);

        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();

        assert_eq!(input.consumed(), 1);
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
    #[cfg(feature = "alloc")]
    fn pump_accepts_a_boxed_trait_object_codec() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = NullSink;
        let boxed: alloc::boxed::Box<dyn Codec> = alloc::boxed::Box::new(DropEverything);
        let mut pump: Pump<alloc::boxed::Box<dyn Codec>> = Pump::new(boxed);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(
            moved,
            PumpTransfer::SourceExhausted(TransferCounts {
                consumed: 6,
                written: 0,
            })
        );
    }

    #[test]
    fn a_pair_that_truly_cannot_progress_reports_no_progress() {
        let mut input = SliceSource::new(b"x");
        let mut output = NullSink;
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 0 },
            drain: DrainProgress::Done { written: 0 },
        });
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::NoProgress));
    }

    // ----
    // Pump::finish_to
    // ----

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

    // ----
    // Pump::latched_step
    // ----

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
            pump.latched_finish_or_sync_flush_step(&mut [], DrainOp::Finish),
            Ok(DrainProgress::Done { written: 0 })
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

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            Pump::new(FailsAfterProgress).latched_step(b"abc", &mut [MaybeUninit::uninit(); 5]),
            Err(Error::new(ErrorKind::CorruptStream, 1, 2))
        );
    }

    // ----
    // Pump::transfer_step
    // ----

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut input = SliceSource::new(b"abc");
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 2 },
            drain: DrainProgress::Done { written: 0 },
        });

        assert_eq!(
            pump.transfer_step(&mut input, &mut output).unwrap(),
            PumpTransfer::Progressed(TransferCounts {
                consumed: 3,
                written: 2,
            })
        );
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut input = SliceSource::new(b"abc");
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 3,
        };
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 2 },
            drain: DrainProgress::Done { written: 0 },
        });

        assert_eq!(
            pump.transfer_step(&mut input, &mut output).unwrap(),
            PumpTransfer::Progressed(TransferCounts {
                consumed: 2,
                written: 5,
            })
        );
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut input = SliceSource::new(b"abc");
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

        assert_eq!(
            pump.transfer_step(&mut input, &mut output).unwrap(),
            PumpTransfer::End(TransferCounts {
                consumed: 2,
                written: 4,
            })
        );
    }

    // ----
    // The rest
    // ----

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
}
