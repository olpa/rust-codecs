//! [`stream_to_stream`] drives a codec from an input source to an
//! output sink. [`Pump`], the helper behind it, is also used directly
//! by `std_io`/`embedded_io` wrappers.

use core::mem::MaybeUninit;

use crate::step::{boundary_aware_step, finish_step, flush_step};
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainProgress, Error, ErrorKind, Sink, Source,
    TransferCounts,
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

impl<EO> DriveError<core::convert::Infallible, EO> {
    /// Widen `EI` from `Infallible` to any type.
    ///
    /// `flush_to`/`finish_to` never touch a `Source`, so their result
    /// carries `Infallible` in this slot. A caller with a real
    /// `Source` error type uses this to line its `DriveError` up with
    /// its own, so both share one `?`-friendly error type.
    pub(crate) fn widen_source<EI>(self) -> DriveError<EI, EO> {
        match self {
            DriveError::Source(never) => match never {},
            DriveError::Sink(error) => DriveError::Sink(error),
            DriveError::Codec(error) => DriveError::Codec(error),
            DriveError::SinkExhausted => DriveError::SinkExhausted,
            DriveError::NoProgress => DriveError::NoProgress,
        }
    }
}

impl<EI: core::fmt::Display, EO: core::fmt::Display> core::fmt::Display for DriveError<EI, EO> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Source(_) => f.write_str("source error"),
            Self::Sink(_) => f.write_str("sink error"),
            Self::Codec(_) => f.write_str("codec error"),
            Self::SinkExhausted => f.write_str("sink has no more room"),
            Self::NoProgress => f.write_str("no progress on input or output"),
        }
    }
}

impl<EI, EO> core::error::Error for DriveError<EI, EO>
where
    EI: core::error::Error + 'static,
    EO: core::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Source(error) => Some(error),
            Self::Sink(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::SinkExhausted | Self::NoProgress => None,
        }
    }
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
        PumpTransfer::SinkExhausted(_) => return Err(DriveError::SinkExhausted),
        PumpTransfer::SourceExhausted(moved) | PumpTransfer::InputEnded(moved) => {
            totals.consumed += moved.consumed;
            totals.written += moved.written;
        }
        PumpTransfer::Progressed(_) => unreachable!("transfer_from consumes progress internally"),
    }

    let drained = pump.finish_to(output).map_err(DriveError::widen_source)?;
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
    /// The codec reported an in-band end. The input of this logical
    /// stream ended. The driver must call `finish_to` next.
    InputEnded(TransferCounts),
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
    /// Persistent flag for `finish`.
    ///
    /// Does not guard transfer steps. The `Codec` contract allows
    /// `process` after `finish`, so we skip that extra work.
    ended_by_finish: bool,
    /// Latches when `finish_to` is entered.
    ///
    /// `Read`-style interfaces have no `finish` call. A wrapper like
    /// `boundary_aware_pump_read` integrates `finish` into `read`. This
    /// flag selects the mode. `false` means `read` pulls more data
    /// from the source. `true` means `read` drives the codec to its
    /// end.
    finishing: bool,
    /// Latches the codec error.
    ///
    /// After `Err`, the codec state is not defined. So the pump does
    /// not call the codec again. It returns this error instead.
    failed: Option<ErrorKind>,
}

impl<C: BoundaryAwareCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            ended_by_finish: false,
            finishing: false,
            failed: None,
        }
    }

    pub fn get_ref(&self) -> &C {
        &self.codec
    }

    pub fn get_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    /// Unwrap the codec, discarding `Pump`'s lifecycle state.
    pub fn into_inner(self) -> C {
        self.codec
    }

    /// Whether `finish` has reported `Done`.
    pub(crate) fn is_done(&self) -> bool {
        self.ended_by_finish
    }

    /// See [`Self::finishing`].
    pub(crate) fn is_finishing(&self) -> bool {
        self.finishing
    }

    /// The latched codec error, if any. See [`Self::failed`].
    ///
    /// The counts are zero: they describe the call that returns the
    /// error, and that call moves no bytes.
    pub(crate) fn failure(&self) -> Option<Error> {
        self.failed.map(|kind| Error::new(kind, 0, 0))
    }

    /// Return the latched codec error, if any, before a call to the
    /// codec.
    fn check_not_failed<EI, EO>(&self) -> Result<(), DriveError<EI, EO>> {
        match self.failure() {
            Some(error) => Err(DriveError::Codec(error)),
            None => Ok(()),
        }
    }

    /// Latch a codec error from `result`. Other errors pass through
    /// without a latch: a source or sink error can be temporary.
    fn latch_failure<T, EI, EO>(
        &mut self,
        result: Result<T, DriveError<EI, EO>>,
    ) -> Result<T, DriveError<EI, EO>> {
        if let Err(DriveError::Codec(error)) = &result {
            self.failed = Some(error.kind);
        }
        result
    }

    /// Drive the codec by repeatedly pulling chunks from `input` and
    /// pushing processed bytes into `output`, until the codec reaches
    /// its stream end or either endpoint runs out of room or data.
    ///
    /// Returns when one of three things happens:
    /// - the source is exhausted
    /// - the sink has no more spare space
    /// - the codec signals the end of its input in-band
    ///
    /// This method must not return for partial progress alone.
    /// `stream_to_stream` treats that case as `unreachable!()`.
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
                PumpTransfer::InputEnded(moved) => {
                    return Ok(PumpTransfer::InputEnded(total(moved)));
                }
            }
        }
    }

    /// Run exactly one [`boundary_aware_step`] between `input` and
    /// `output`: pull at most one chunk from `input`, hand it to the
    /// codec with one spare slice from `output`, and commit the
    /// result. Unlike [`Pump::transfer_from`], it does not loop back
    /// for more input.
    ///
    /// `transfer_from` loops on this primitive. It is also driven
    /// directly by
    /// [`crate::sources_and_sinks::shared_io::boundary_aware_pump_read`].
    ///
    /// Same stall and error handling as `transfer_from`: a call that
    /// moves zero bytes on both sides without ending the stream is
    /// `DriveError::NoProgress`. A codec error still commits whatever
    /// progress it validly reported.
    ///
    /// After a codec error, the pump latches it. Later calls return
    /// the error and do not call the codec.
    pub(crate) fn transfer_step<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        self.check_not_failed()?;
        let result = self.unlatched_transfer_step(input, output);
        self.latch_failure(result)
    }

    /// The body of [`Pump::transfer_step`], without the codec error
    /// latch.
    fn unlatched_transfer_step<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        // AI review agents detect a potential problem here:
        //
        // > A blocking `Source` transport can stall here. Drain
        // > available codec output before requesting more input.
        //
        // Two independent cases produce "available codec output":
        //
        // - The `output` buffer is smaller than `input`. Should not
        //   happen: `Source::chunk`'s doc tells implementors to
        //   return available data instead of reading more.
        // - The codec buffers bytes internally, for example atomic
        //   units in the base64 codec. With large buffers, the worst
        //   case is a one-read delay at each atomic-unit boundary.
        //   This delay is tolerable. We do not plan to fix it.
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
        let progress = match boundary_aware_step(&mut self.codec, chunk, spare) {
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
        // reappears on the next `input.chunk()` call, possibly
        // together with newly arrived input.
        if moved.consumed > 0 {
            input.consume(moved.consumed);
        }
        if moved.written > 0 {
            output.commit(moved.written).map_err(DriveError::Sink)?;
        }
        Ok(if boundary {
            PumpTransfer::InputEnded(moved)
        } else {
            PumpTransfer::Progressed(moved)
        })
    }

    /// Flush the codec into `output`. The codec decides how much of its
    /// held output to write. The codec stream does not end.
    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.drain_loop(output, Self::latched_flush_step)
    }

    /// Drain all the bytes that the codec must still write, and end
    /// the codec stream.
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.finishing = true;
        self.drain_loop(output, Self::latched_finish_step)
    }

    /// Move the bytes that `step` writes into `output`.
    /// The codec gets no new input.
    ///
    /// Repeatedly get spare space from `output` and hand it to `step`,
    /// committing what was written, until `step` reports `Done`.
    ///
    /// When `output.spare()` returns `None`, the sink has no room.
    /// One more call is made against an empty slice, to check whether
    /// the codec was actually done regardless:
    /// - `Done` means the drain is complete, despite the full sink
    /// - `OutputFilled` means the sink is genuinely blocking progress,
    ///   so `SinkExhausted` is returned
    ///
    /// A call that writes nothing and does not reach `Done` is a
    /// stall (`DriveError::NoProgress`). A codec error still commits
    /// whatever progress it validly reported.
    ///
    /// After a codec error, the pump latches it. Later calls return
    /// the error and do not call the codec.
    fn drain_loop<O: Sink>(
        &mut self,
        output: &mut O,
        step: impl FnMut(&mut Self, &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error>,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.check_not_failed()?;
        let result = self.unlatched_drain_loop(output, step);
        self.latch_failure(result)
    }

    /// The body of [`Pump::drain_loop`], without the codec error
    /// latch.
    fn unlatched_drain_loop<O: Sink>(
        &mut self,
        output: &mut O,
        mut step: impl FnMut(&mut Self, &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error>,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        let mut written = 0;
        loop {
            let (step_written, done) = match output.spare().map_err(DriveError::Sink)? {
                Some(spare) => match step(self, spare) {
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
                    let moved = step(self, &mut []).map_err(DriveError::Codec)?;
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

    /// With [`Pump::drain_loop`], flushes the codec.
    ///
    /// If `finish` already reported `Done`, the codec stream has ended,
    /// and nothing is left to flush. Then this function does not call
    /// the codec. It returns the latched `Done`.
    fn latched_flush_step(
        &mut self,
        output: &mut [MaybeUninit<u8>],
    ) -> Result<DrainProgress, Error> {
        if self.is_done() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        flush_step(&mut self.codec, output)
    }

    /// Run one `finish` call, latched.
    ///
    /// With [`Pump::drain_loop`], drains all the bytes that the codec
    /// must still write. The trailer is part of these bytes, if the
    /// format has one. Then the codec stream ends.
    ///
    /// Skips the call and reports a permanent `Done` if `finish`
    /// already reported `Done`. The [`DrainCodec`](crate::DrainCodec)
    /// contract does not specify a repeat call. We skip it for
    /// defensive programming: a repeat call could replay a trailer.
    fn latched_finish_step(
        &mut self,
        output: &mut [MaybeUninit<u8>],
    ) -> Result<DrainProgress, Error> {
        if self.is_done() {
            return Ok(DrainProgress::Done { written: 0 });
        }
        let progress = finish_step(&mut self.codec, output)?;
        if matches!(progress, DrainProgress::Done { .. }) {
            self.ended_by_finish = true;
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::{Pump, PumpDrain, PumpTransfer};
    use crate::codecs::test_support::{EndsAtBar, FailsAfterInner, HoldsOutput, Scripted};
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::{
        BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, DriveError, Error, ErrorKind,
        Progress, Sink, TransferCounts,
    };

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

    /// Like `RecordingSink`, but each `spare` call offers a maximum
    /// of 1 byte.
    struct OneByteWindowSink {
        bytes: [u8; 8],
        written: usize,
    }

    impl Sink for OneByteWindowSink {
        type Error = core::convert::Infallible;

        fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
            let end = (self.written + 1).min(self.bytes.len());
            Ok((self.written < end)
                .then(|| crate::uninit::as_uninit_mut(&mut self.bytes[self.written..end])))
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
        fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }

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
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                held: 2,
                ..Default::default()
            },
            ..Default::default()
        });

        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();

        assert_eq!(input.consumed(), 3);
        assert_eq!(output.written, 2);
        assert_eq!(&output.bytes[..2], b"XX");
        assert!(matches!(
            error,
            DriveError::Codec(Error {
                kind: ErrorKind::CorruptStream,
                consumed: 3,
                written: 2
            })
        ));
    }

    #[test]
    fn a_process_error_is_latched() {
        let mut input = SliceSource::new(b"abc");
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                held: 2,
                ..Default::default()
            },
            ..Default::default()
        });
        pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(input.consumed(), 3);
        assert_eq!(output.written, 2);

        // The codec state is not defined after `Err`. A buggy pump
        // would call the codec again and write more bytes.
        let latched = Error::new(ErrorKind::CorruptStream, 0, 0);
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error, DriveError::Codec(e) if e == latched));
        let error = pump.flush_to(&mut output).unwrap_err();
        assert!(matches!(error, DriveError::Codec(e) if e == latched));
        let error = pump.finish_to(&mut output).unwrap_err();
        assert!(matches!(error, DriveError::Codec(e) if e == latched));

        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(input.consumed(), 3);
        assert_eq!(output.written, 2);
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
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                trailer: b"!",
                ..Default::default()
            },
            ..Default::default()
        });

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
    fn a_finish_error_is_latched() {
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                trailer: b"!",
                ..Default::default()
            },
            ..Default::default()
        });
        pump.finish_to(&mut output).unwrap_err();
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 1);

        let error = pump.finish_to(&mut output).unwrap_err();
        assert!(matches!(
            error,
            DriveError::Codec(Error {
                kind: ErrorKind::CorruptStream,
                consumed: 0,
                written: 0
            })
        ));
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 1);
    }

    #[test]
    fn finish_to_must_not_run_a_completed_codec_again() {
        // `Scripted` always answers the same `Done { written: 1 }`,
        // whether or not it was already called before. So a second
        // `finish_to` call that still writes a byte means `finish`
        // ran again after it already reported `Done`.
        let mut pump = Pump::new(Scripted {
            process: BoundaryAwareProgress::InputConsumed { written: 0 },
            drain: DrainProgress::Done { written: 1 },
        });
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };

        let first = pump.finish_to(&mut output).unwrap();
        assert_eq!(first, PumpDrain::Done { written: 1 });

        output.written = 0;
        let second = pump.finish_to(&mut output).unwrap();
        assert_eq!(
            second,
            PumpDrain::Done { written: 0 },
            "finish ran again after it already reported Done"
        );
    }

    // ----
    // Pump::flush_to
    // ----

    #[test]
    fn flush_to_keeps_draining_across_output_windows() {
        let mut pump = Pump::new(HoldsOutput {
            held: 3,
            trailer: b"F",
            ..Default::default()
        });
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };

        // Each window holds 1 byte, so the 3 held bytes need 3 codec
        // calls. `flush_to` must not stop after the first window.
        // Also, `flush_to` must not finish the codec. If it does, the
        // output gets an 'F' marker.
        let drained = pump.flush_to(&mut output).unwrap();

        assert_eq!(drained, PumpDrain::Done { written: 3 });
        assert_eq!(&output.bytes[..output.written], b"XXX");
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
            PumpTransfer::InputEnded(TransferCounts {
                consumed: 2,
                written: 4,
            })
        );
    }

    #[test]
    fn stream_to_stream_drains_the_tail_after_an_in_band_end() {
        let mut input = SliceSource::new(b"ab|cd");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };
        let codec = EndsAtBar {
            inner: HoldsOutput {
                per_input: 1,
                trailer: b"TAIL",
                ..Default::default()
            },
            ..Default::default()
        };

        let counts = super::stream_to_stream(&mut input, codec, &mut output).unwrap();

        assert_eq!(&output.bytes[..output.written], b"XXTAIL");
        assert_eq!(
            counts,
            TransferCounts {
                consumed: 3,
                written: 6,
            }
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
