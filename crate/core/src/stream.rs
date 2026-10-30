//! [`stream_to_stream`] drives a codec from an input source to an
//! output sink. [`Pump`], the helper behind it, is also used directly
//! by `std_io`/`embedded_io` wrappers.

use core::mem::MaybeUninit;

use crate::step::{boundary_aware_step, finish_step, flush_step, FinishPhase};
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainProgress, Error, ErrorKind, Sink, Source,
    TransferCounts,
};

/// Why the drive stopped before the end of the stream. See
/// [`DriveError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DriveErrorKind<EI, EO> {
    Source(EI),
    Sink(EO),
    /// The codec failed. The `consumed` and `written` fields of the
    /// [`Error`] count only the codec call that failed.
    /// [`DriveError::moved`] counts the bytes that the whole stream moved.
    Codec(crate::Error),
    /// The sink had no more room (`Sink::spare` returned `None`)
    /// before the codec reached the end of its stream.
    SinkExhausted,
    /// The call moved zero bytes on both sides without ending the
    /// stream. The pump does not spin forever on a stalled codec or
    /// endpoint.
    NoProgress,
}

impl<EI, EO> core::fmt::Display for DriveErrorKind<EI, EO> {
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

/// An error from a drive, and the bytes that the drive moved before
/// the error.
///
/// `moved` counts the whole drive: all the bytes that the [`Pump`]
/// moved since its creation, up to and including the step that failed.
/// Also, with [`DriveErrorKind::Codec`], the `consumed` and `written`
/// fields of the inner [`Error`] count only the codec step that failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriveError<EI, EO> {
    pub kind: DriveErrorKind<EI, EO>,
    pub moved: TransferCounts,
}

impl<EI, EO> DriveError<EI, EO> {
    pub(crate) fn new(kind: DriveErrorKind<EI, EO>, moved: TransferCounts) -> Self {
        Self { kind, moved }
    }
}

impl<EO> DriveError<core::convert::Infallible, EO> {
    /// Change `EI` from `Infallible` to any type.
    ///
    /// `flush_to` and `finish_to` do not use a `Source`, so `EI` is
    /// `Infallible` in their errors. A caller with a `Source` uses
    /// this method to get the same error type as its own errors.
    /// Then `?` works for both.
    pub(crate) fn widen_source<EI>(self) -> DriveError<EI, EO> {
        let kind = match self.kind {
            DriveErrorKind::Source(never) => match never {},
            DriveErrorKind::Sink(error) => DriveErrorKind::Sink(error),
            DriveErrorKind::Codec(error) => DriveErrorKind::Codec(error),
            DriveErrorKind::SinkExhausted => DriveErrorKind::SinkExhausted,
            DriveErrorKind::NoProgress => DriveErrorKind::NoProgress,
        };
        DriveError::new(kind, self.moved)
    }
}

impl<EI, EO> core::fmt::Display for DriveError<EI, EO> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "{} (after {} input bytes, {} output bytes)",
            self.kind, self.moved.consumed, self.moved.written
        )
    }
}

impl<EI, EO> core::error::Error for DriveError<EI, EO>
where
    EI: core::error::Error + 'static,
    EO: core::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match &self.kind {
            DriveErrorKind::Source(error) => Some(error),
            DriveErrorKind::Sink(error) => Some(error),
            DriveErrorKind::Codec(error) => Some(error),
            DriveErrorKind::SinkExhausted | DriveErrorKind::NoProgress => None,
        }
    }
}

/// Drive the codec from the input source to the output sink.
///
/// Returns the bytes consumed from `input` and written to `output`.
/// On failure, the [`DriveError`] has these counts too.
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
    if let PumpStop::SinkExhausted = pump.transfer_from(input, output)? {
        return Err(pump.drive_error(DriveErrorKind::SinkExhausted));
    }
    match pump.finish_to(output).map_err(DriveError::widen_source)? {
        PumpDrain::Done => Ok(pump.total()),
        PumpDrain::SinkExhausted => Err(pump.drive_error(DriveErrorKind::SinkExhausted)),
    }
}

/// Result of one pump transfer. The bytes it moved are in
/// [`Pump::total`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpTransfer {
    SourceExhausted,
    SinkExhausted,
    /// The step ended normally. It did not exhaust `input` or `output`.
    /// The next step may move more bytes.
    Progressed,
    /// The codec found the end of the logical stream in the input.
    InputEnded,
}

/// A version of [`PumpTransfer`] without `Progressed`, for
/// [`Pump::transfer_from`]. That method does not return for partial
/// progress alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpStop {
    SourceExhausted,
    SinkExhausted,
    InputEnded,
}

/// Result of one pump drain. The bytes it wrote are in
/// [`Pump::total`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpDrain {
    SinkExhausted,
    Done,
}

/// A bufferless lifecycle wrapper around a codec: processes input
/// until the stream ends, then drains what's owed.
///
/// After `pump_finish`, `pump_write` and `pump_flush` fail with
/// [`ErrorKind::AfterFinish`].
///
/// Used by [`stream_to_stream`] and by I/O backend wrappers.
///
/// `Pump` is public only because third-party I/O backends are built
/// on top of
/// [`sources_and_sinks::shared_io`](crate::sources_and_sinks::shared_io),
/// whose functions must name `Pump` in their signatures.
//
// Design note: `C` is generic instead of `dyn BoundaryAwareCodec`,
// because a trait has no fixed size, so a field cannot hold it. A caller
// who activated the `alloc` feature can generalize with a box:
// `Pump<Box<dyn Codec>>`.
#[derive(Debug)]
pub struct Pump<C> {
    codec: C,
    /// How far the pump is in `finish`.
    finish_phase: FinishPhase,
    /// Latches the codec error.
    ///
    /// After `Err`, the codec state is not defined. So the pump does
    /// not call the codec again. It returns this error instead.
    failed: Option<ErrorKind>,
    /// The bytes that the pump moved since its creation. The pump counts a
    /// byte after the source or the sink has taken it.
    total: TransferCounts,
}

impl<C: BoundaryAwareCodec> Pump<C> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            finish_phase: FinishPhase::Open,
            failed: None,
            total: TransferCounts::default(),
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
        self.finish_phase.is_finished()
    }

    /// Whether `finish_to` was called.
    pub(crate) fn is_finishing(&self) -> bool {
        !self.finish_phase.is_open()
    }

    /// The latched codec error, if any. See [`Self::failed`].
    ///
    /// The counts are zero: they describe the call that returns the
    /// error, and that call moves no bytes.
    pub(crate) fn failure(&self) -> Option<Error> {
        self.failed.map(|kind| Error::new(kind, 0, 0))
    }

    /// The bytes that the pump moved since its creation. See [`Self::total`].
    pub fn total(&self) -> TransferCounts {
        self.total
    }

    /// An error of the pump, with the bytes that the pump moved in its
    /// life.
    pub(crate) fn drive_error<EI, EO>(&self, kind: DriveErrorKind<EI, EO>) -> DriveError<EI, EO> {
        DriveError::new(kind, self.total)
    }

    fn count_consumed(&mut self, amount: usize) {
        self.total.consumed = self.total.consumed.wrapping_add(amount);
    }

    fn count_written(&mut self, amount: usize) {
        self.total.written = self.total.written.wrapping_add(amount);
    }

    /// Return the latched codec error, if any, before a call to the
    /// codec.
    fn check_not_failed<EI, EO>(&self) -> Result<(), DriveError<EI, EO>> {
        match self.failure() {
            Some(error) => Err(self.drive_error(DriveErrorKind::Codec(error))),
            None => Ok(()),
        }
    }

    /// Fail if `finish_to` was called. The error is not latched.
    fn check_open<EI, EO>(&self) -> Result<(), DriveError<EI, EO>> {
        self.finish_phase
            .check_open()
            .map_err(|error| self.drive_error(DriveErrorKind::Codec(error)))
    }

    /// Latch a codec error. Later calls return it and do not call the
    /// codec. Each place that receives a codec error must call this
    /// before it does anything that can fail. Source and sink errors
    /// are not latched: they can be temporary.
    fn latch_failure(&mut self, error: &Error) {
        self.failed = Some(error.kind);
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
    /// A call that moves zero bytes on both sides without ending the
    /// stream is a stall, reported as `DriveErrorKind::NoProgress`.
    pub(crate) fn transfer_from<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpStop, DriveError<I::Error, O::Error>> {
        loop {
            match self.transfer_step(input, output)? {
                PumpTransfer::Progressed => {}
                PumpTransfer::SourceExhausted => return Ok(PumpStop::SourceExhausted),
                PumpTransfer::SinkExhausted => return Ok(PumpStop::SinkExhausted),
                PumpTransfer::InputEnded => return Ok(PumpStop::InputEnded),
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
    /// `DriveErrorKind::NoProgress`. A codec error still commits whatever
    /// progress it validly reported.
    ///
    /// After a codec error, the pump latches it. Later calls return
    /// the error and do not call the codec.
    ///
    /// After `finish_to`, fails with [`ErrorKind::AfterFinish`].
    pub(crate) fn transfer_step<I: Source, O: Sink>(
        &mut self,
        input: &mut I,
        output: &mut O,
    ) -> Result<PumpTransfer, DriveError<I::Error, O::Error>> {
        self.check_not_failed()?;
        self.check_open()?;
        // Design note: this step must call `input.chunk()` before
        // `output.spare()`. Only the source can tell end of input from
        // more input. If the sink were checked first, a sink that the
        // output fills exactly could report `SinkExhausted` instead of
        // success, or it could allocate memory that nobody uses.
        //
        // The cost: a blocking `Source` transport can stall here while
        // the output side already decides the result.
        //
        // - The sink is full. The drive waits for one read, then
        //   reports `SinkExhausted`. No data is lost.
        // - The `output` buffer is smaller than `input`. Should not
        //   happen: `Source::chunk`'s doc tells implementors to
        //   return available data instead of reading more.
        // - The codec buffers bytes internally, for example atomic
        //   units in the base64 codec. With large buffers, the worst
        //   case is a one-read delay at each atomic-unit boundary.
        //
        // These delays are tolerable. We do not plan to fix them.
        let Some(chunk) = input
            .chunk()
            .map_err(|error| self.drive_error(DriveErrorKind::Source(error)))?
        else {
            return Ok(PumpTransfer::SourceExhausted);
        };
        let spare = output
            .spare()
            .map_err(|error| self.drive_error(DriveErrorKind::Sink(error)))?;
        // A call may still progress with an empty `spare` (e.g. a
        // codec that only consumes input). So an empty slice is not
        // rejected up front, zero bytes moved on both sides without
        // ending the stream is what marks a genuine stall.
        let sink_full = spare.is_none();
        let spare = spare.unwrap_or(&mut []);
        let progress = match boundary_aware_step(&mut self.codec, chunk, spare) {
            Ok(progress) => progress,
            Err(error) => {
                let error = error
                    .validated(chunk.len(), spare.len())
                    .unwrap_or_else(|violation| violation);
                self.latch_failure(&error);
                if error.consumed > 0 {
                    input.consume(error.consumed);
                    self.count_consumed(error.consumed);
                }
                if error.written > 0 {
                    // If the commit fails, no bytes count as written.
                    output
                        .commit(error.written)
                        .map_err(|sink_error| self.drive_error(DriveErrorKind::Sink(sink_error)))?;
                    self.count_written(error.written);
                }
                return Err(self.drive_error(DriveErrorKind::Codec(error)));
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
            if sink_full {
                return Ok(PumpTransfer::SinkExhausted);
            }
            return Err(self.drive_error(DriveErrorKind::NoProgress));
        }
        // `moved.consumed` may be less than `chunk.len()` if output
        // ran out first. The unconsumed remainder is not lost: it
        // reappears on the next `input.chunk()` call, possibly
        // together with newly arrived input.
        if moved.consumed > 0 {
            input.consume(moved.consumed);
            self.count_consumed(moved.consumed);
        }
        if moved.written > 0 {
            // If the commit fails, no bytes count as written.
            output
                .commit(moved.written)
                .map_err(|error| self.drive_error(DriveErrorKind::Sink(error)))?;
            self.count_written(moved.written);
        }
        Ok(if boundary {
            PumpTransfer::InputEnded
        } else {
            PumpTransfer::Progressed
        })
    }

    /// Flush the codec into `output`. The codec decides how much of its
    /// held output to write. The codec stream does not end.
    ///
    /// After `finish_to`, fails with [`ErrorKind::AfterFinish`].
    pub(crate) fn flush_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.check_not_failed()?;
        self.check_open()?;
        self.drain_loop(output, |pump, output| flush_step(&mut pump.codec, output))
    }

    /// Drain all the bytes that the codec must still write, and end
    /// the codec stream.
    pub(crate) fn finish_to<O: Sink>(
        &mut self,
        output: &mut O,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.finish_phase.start_finish();
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
    /// stall (`DriveErrorKind::NoProgress`). A codec error still commits
    /// whatever progress it validly reported.
    ///
    /// After a codec error, the pump latches it. Later calls return
    /// the error and do not call the codec.
    fn drain_loop<O: Sink>(
        &mut self,
        output: &mut O,
        mut step: impl FnMut(&mut Self, &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error>,
    ) -> Result<PumpDrain, DriveError<core::convert::Infallible, O::Error>> {
        self.check_not_failed()?;
        loop {
            let spare = output
                .spare()
                .map_err(|error| self.drive_error(DriveErrorKind::Sink(error)))?;
            let (step_written, done) = match spare {
                Some(spare) => match step(self, spare) {
                    Ok(DrainProgress::Done { written }) => (written, true),
                    Ok(DrainProgress::OutputFilled) if !spare.is_empty() => (spare.len(), false),
                    Ok(DrainProgress::OutputFilled) => {
                        return Err(self.drive_error(DriveErrorKind::NoProgress));
                    }
                    Err(error) => {
                        let error = error
                            .validated(0, spare.len())
                            .unwrap_or_else(|violation| violation);
                        self.latch_failure(&error);
                        if error.written > 0 {
                            output.commit(error.written).map_err(|sink_error| {
                                self.drive_error(DriveErrorKind::Sink(sink_error))
                            })?;
                            self.count_written(error.written);
                        }
                        return Err(self.drive_error(DriveErrorKind::Codec(error)));
                    }
                },
                None => {
                    let progress = match step(self, &mut []) {
                        Ok(progress) => progress,
                        Err(error) => {
                            let error = error.validated(0, 0).unwrap_or_else(|violation| violation);
                            self.latch_failure(&error);
                            return Err(self.drive_error(DriveErrorKind::Codec(error)));
                        }
                    };
                    return Ok(match progress {
                        DrainProgress::Done { .. } => PumpDrain::Done,
                        DrainProgress::OutputFilled => PumpDrain::SinkExhausted,
                    });
                }
            };
            if step_written > 0 {
                output
                    .commit(step_written)
                    .map_err(|error| self.drive_error(DriveErrorKind::Sink(error)))?;
                self.count_written(step_written);
            }
            if done {
                return Ok(PumpDrain::Done);
            }
        }
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
            self.finish_phase.finish_done();
        }
        Ok(progress)
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::{Pump, PumpDrain, PumpStop, PumpTransfer};
    use crate::codecs::test_support::{
        assert_latched, assert_overclaim_rejected, DropEverything, EndsAtBar, FailsAfterInner,
        FailsWithOverclaim, HoldsOutput, Scripted, SplitSource,
    };
    use crate::identity::identity;
    use crate::sources_and_sinks::slice::{SliceSink, SliceSource};
    use crate::{
        BoundaryAwareProgress, DrainProgress, DriveErrorKind, Error, ErrorKind, Sink,
        TransferCounts,
    };

    /// A `Sink` that always gives an empty slice and never reports a
    /// full sink. This sink does not obey the `Sink::spare` contract.
    /// The tests use this sink to cause a stall.
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

    /// Like `OneByteWindowSink`, but each `spare` call offers a
    /// maximum of 2 bytes. It accepts `ok_commits` commits, then fails
    /// every commit.
    struct FailsOnCommit {
        bytes: [u8; 8],
        written: usize,
        ok_commits: usize,
    }

    impl Sink for FailsOnCommit {
        type Error = ();

        fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
            let end = (self.written + 2).min(self.bytes.len());
            Ok((self.written < end)
                .then(|| crate::uninit::as_uninit_mut(&mut self.bytes[self.written..end])))
        }

        fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
            if self.ok_commits == 0 {
                return Err(());
            }
            self.ok_commits -= 1;
            self.written += amount;
            Ok(())
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
        assert_eq!(moved, PumpStop::SourceExhausted);
        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 6,
                written: 0,
            }
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
            error.kind,
            DriveErrorKind::Codec(Error {
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
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert_latched(&error.kind);
        let error = pump.flush_to(&mut output).unwrap_err();
        assert_latched(&error.kind);
        let error = pump.finish_to(&mut output).unwrap_err();
        assert_latched(&error.kind);

        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(input.consumed(), 3);
        assert_eq!(output.written, 2);
    }

    #[test]
    fn a_process_error_is_latched_when_its_commit_fails() {
        let mut input = SliceSource::new(b"abc");
        let mut output = FailsOnCommit {
            bytes: [0; 8],
            written: 0,
            ok_commits: 0,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                held: 2,
                ..Default::default()
            },
            ..Default::default()
        });

        // The codec fails with 2 bytes of output. The commit of these
        // bytes fails too, so the call reports the sink error.
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::Sink(())));
        assert_eq!(pump.get_ref().calls, 1);

        // The sink recovers. The codec error must still be latched.
        output.ok_commits = 8;
        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 0);
    }

    #[test]
    fn a_process_error_with_an_overclaim_moves_no_bytes() {
        let mut input = SliceSource::new(b"abc");
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsWithOverclaim);

        let error = pump.transfer_from(&mut input, &mut output).unwrap_err();

        assert_overclaim_rejected(&error.kind);
        assert_eq!(error.moved, TransferCounts::default());
        assert_eq!(input.consumed(), 0);
        assert_eq!(output.written, 0);
    }

    // The codec passes three 4-byte chunks and fails on the fourth. The
    // first `transfer_from` call moves two chunks. The second call moves
    // one chunk and fails on the next. The failing chunk counts too.
    #[test]
    fn total_counts_the_bytes_of_earlier_transfers() {
        let mut pump = Pump::new(FailsAfterInner {
            inner: identity(),
            ok_calls: 3,
            calls: 0,
        });
        let mut bytes = [0u8; 16];
        let mut output = SliceSink::new(&mut bytes);
        let mut first = SplitSource {
            bytes: b"01234567",
            pos: 0,
            chunk_size: 4,
        };
        let mut second = SplitSource {
            bytes: b"89012345",
            pos: 0,
            chunk_size: 4,
        };

        pump.transfer_from(&mut first, &mut output).unwrap();
        assert_eq!(pump.failure(), None);
        pump.transfer_from(&mut second, &mut output).unwrap_err();

        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 16,
                written: 16,
            }
        );
        assert_eq!(
            pump.failure(),
            Some(Error::new(ErrorKind::CorruptStream, 0, 0))
        );
    }

    #[test]
    #[cfg(feature = "alloc")]
    fn pump_accepts_a_boxed_trait_object_codec() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = NullSink;
        let boxed: alloc::boxed::Box<dyn crate::Codec> = alloc::boxed::Box::new(DropEverything);
        let mut pump: Pump<alloc::boxed::Box<dyn crate::Codec>> = Pump::new(boxed);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(moved, PumpStop::SourceExhausted);
        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 6,
                written: 0,
            }
        );
    }

    #[test]
    fn stream_to_stream_accepts_a_borrowed_codec() {
        let mut input = SliceSource::new(b"ab");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut codec = HoldsOutput {
            per_input: 1,
            trailer: b"TAIL",
            ..Default::default()
        };

        super::stream_to_stream(&mut input, &mut codec, &mut output).unwrap();

        // The caller still owns the codec and can read its state.
        assert_eq!(&output.bytes[..output.written], b"XXTAIL");
        assert_eq!(codec.trailer_written, 4);
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
        assert!(matches!(error.kind, DriveErrorKind::NoProgress));
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
            error.kind,
            DriveErrorKind::Codec(Error {
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
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 1);
    }

    #[test]
    fn a_finish_error_is_latched_when_its_commit_fails() {
        let mut output = FailsOnCommit {
            bytes: [0; 8],
            written: 0,
            ok_commits: 0,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                trailer: b"!",
                ..Default::default()
            },
            ..Default::default()
        });

        // The codec fails with 1 byte of output. The commit of this
        // byte fails too, so the call reports the sink error.
        let error = pump.finish_to(&mut output).unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::Sink(())));
        assert_eq!(pump.get_ref().calls, 1);

        // The sink recovers. The codec error must still be latched.
        output.ok_commits = 8;
        let error = pump.finish_to(&mut output).unwrap_err();
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 0);
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
        assert_eq!(first, PumpDrain::Done);
        let after_first = pump.total();
        assert_eq!(after_first.written, 1);

        output.written = 0;
        let second = pump.finish_to(&mut output).unwrap();
        assert_eq!(second, PumpDrain::Done);
        assert_eq!(
            pump.total(),
            after_first,
            "finish ran again after it already reported Done"
        );
    }

    // The codec holds output from the first chunk and releases it with
    // the second. Two chunks make 8 bytes in and 4 bytes out. The
    // drain then releases 4 more bytes and fails. The failure must
    // count the bytes of the transfer and of the drain.
    fn pump_that_fails_in_the_drain() -> Pump<FailsAfterInner<HoldsOutput>> {
        Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                per_input: 1,
                ..Default::default()
            },
            ok_calls: 2,
            calls: 0,
        })
    }

    #[test]
    fn total_counts_the_bytes_before_a_failing_finish() {
        let mut pump = pump_that_fails_in_the_drain();
        let mut bytes = [0u8; 16];
        let mut output = SliceSink::new(&mut bytes);
        let mut input = SplitSource {
            bytes: b"01234567",
            pos: 0,
            chunk_size: 4,
        };

        pump.transfer_from(&mut input, &mut output).unwrap();
        assert!(pump.finish_to(&mut output).is_err());

        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 8,
                written: 8,
            }
        );
        assert_eq!(
            pump.failure(),
            Some(Error::new(ErrorKind::CorruptStream, 0, 0))
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

        assert_eq!(drained, PumpDrain::Done);
        assert_eq!(pump.total().written, 3);
        assert_eq!(&output.bytes[..output.written], b"XXX");
    }

    #[test]
    fn a_flush_error_is_latched_when_the_sink_has_no_room() {
        // The sink is full, so the codec gets an empty slice.
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 8,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput::default(),
            ..Default::default()
        });

        let error = pump.flush_to(&mut output).unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::Codec(_)));
        assert_eq!(pump.get_ref().calls, 1);

        let error = pump.flush_to(&mut output).unwrap_err();
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
    }

    #[test]
    fn a_flush_error_is_latched_when_its_commit_fails() {
        let mut output = FailsOnCommit {
            bytes: [0; 8],
            written: 0,
            ok_commits: 0,
        };
        let mut pump = Pump::new(FailsAfterInner {
            inner: HoldsOutput {
                held: 1,
                ..Default::default()
            },
            ..Default::default()
        });

        // The codec fails with 1 byte of output. The commit of this
        // byte fails too, so the call reports the sink error.
        let error = pump.flush_to(&mut output).unwrap_err();
        assert!(matches!(error.kind, DriveErrorKind::Sink(())));
        assert_eq!(pump.get_ref().calls, 1);

        // The sink recovers. The codec error must still be latched.
        output.ok_commits = 8;
        let error = pump.flush_to(&mut output).unwrap_err();
        assert_latched(&error.kind);
        assert_eq!(pump.get_ref().calls, 1);
        assert_eq!(output.written, 0);
    }

    #[test]
    fn a_flush_error_with_an_overclaim_is_rejected_when_the_sink_has_room() {
        let mut output = RecordingSink {
            bytes: [0; 8],
            written: 0,
        };
        let mut pump = Pump::new(FailsWithOverclaim);

        let error = pump.flush_to(&mut output).unwrap_err();

        assert_overclaim_rejected(&error.kind);
        assert_eq!(error.moved, TransferCounts::default());
        assert_eq!(output.written, 0);
    }

    #[test]
    fn a_flush_error_with_an_overclaim_is_rejected_when_the_sink_has_no_room() {
        // The sink is full, so the codec gets an empty slice.
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 8,
        };
        let mut pump = Pump::new(FailsWithOverclaim);

        let error = pump.flush_to(&mut output).unwrap_err();

        assert_overclaim_rejected(&error.kind);
        assert_eq!(error.moved, TransferCounts::default());
    }

    #[test]
    fn total_counts_the_bytes_before_a_failing_flush() {
        let mut pump = pump_that_fails_in_the_drain();
        let mut bytes = [0u8; 16];
        let mut output = SliceSink::new(&mut bytes);
        let mut input = SplitSource {
            bytes: b"01234567",
            pos: 0,
            chunk_size: 4,
        };

        pump.transfer_from(&mut input, &mut output).unwrap();
        assert!(pump.flush_to(&mut output).is_err());

        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 8,
                written: 8,
            }
        );
        assert_eq!(
            pump.failure(),
            Some(Error::new(ErrorKind::CorruptStream, 0, 0))
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
            PumpTransfer::Progressed
        );
        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 3,
                written: 2,
            }
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
            PumpTransfer::Progressed
        );
        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 2,
                written: 5,
            }
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
            PumpTransfer::InputEnded
        );
        assert_eq!(
            pump.total(),
            TransferCounts {
                consumed: 2,
                written: 4,
            }
        );
    }

    // ----
    // stream_to_stream
    // ----

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

        // `EndsAtBar` stops at "|" and consumes it. `finish` writes the
        // held "XX" and the trailer "TAIL". "cd" stays in the source.
        assert_eq!(&output.bytes[..output.written], b"XXTAIL");
        assert_eq!(
            counts,
            TransferCounts {
                consumed: 3,
                written: 6,
            }
        );
    }

    #[test]
    fn stream_to_stream_finds_an_in_band_end_when_the_output_fills_the_sink() {
        for chunk_size in [3, 2] {
            let mut input = SplitSource {
                bytes: b"ab|",
                pos: 0,
                chunk_size,
            };
            let mut bytes = [0; 2];
            let mut output = SliceSink::new(&mut bytes);
            let codec = EndsAtBar {
                inner: identity(),
                finish_calls: 0,
            };

            let counts = super::stream_to_stream(&mut input, codec, &mut output)
                .unwrap_or_else(|error| panic!("chunk_size {chunk_size}: {error:?}"));

            // "ab" fills the sink. The "|" needs no room in the sink,
            // so the result must not depend on where the chunk ends.
            assert_eq!(
                counts,
                TransferCounts {
                    consumed: 3,
                    written: 2,
                }
            );
            assert_eq!(&bytes, b"ab");
        }
    }

    #[test]
    fn stream_to_stream_reports_the_counts_when_the_sink_is_full_in_process() {
        let mut input = SliceSource::new(b"abcdefghij");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };

        let error = super::stream_to_stream(&mut input, identity(), &mut output).unwrap_err();

        // Each step copies 1 byte. After 8 steps the sink is full, and
        // "ij" stays in the source.
        assert!(matches!(error.kind, DriveErrorKind::SinkExhausted));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 8,
                written: 8,
            }
        );
        assert_eq!(&output.bytes, b"abcdefgh");
    }

    #[test]
    fn stream_to_stream_reports_the_counts_on_a_codec_error_in_process() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };
        let codec = FailsAfterInner {
            inner: identity(),
            ok_calls: 2,
            ..Default::default()
        };

        let error = super::stream_to_stream(&mut input, codec, &mut output).unwrap_err();

        // Steps 1 and 2 copy "a" and "b". Step 3 copies "c", then
        // fails. The error counts include "c".
        assert!(matches!(error.kind, DriveErrorKind::Codec(_)));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 3,
                written: 3,
            }
        );
        assert_eq!(&output.bytes[..output.written], b"abc");
    }

    #[test]
    fn stream_to_stream_reports_the_counts_on_a_sink_error_in_process() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = FailsOnCommit {
            bytes: [0; 8],
            written: 0,
            ok_commits: 1,
        };

        let error = super::stream_to_stream(&mut input, identity(), &mut output).unwrap_err();

        // Step 1 copies "ab". Step 2 uses "cd", but its commit fails,
        // so its output does not reach the sink. See issue #20.
        assert!(matches!(error.kind, DriveErrorKind::Sink(())));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 4,
                written: 2,
            }
        );
        assert_eq!(&output.bytes[..output.written], b"ab");
    }

    #[test]
    fn stream_to_stream_reports_the_counts_when_the_sink_is_full_in_finish() {
        let mut input = SliceSource::new(b"ab");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };
        let codec = HoldsOutput {
            per_input: 1,
            trailer: b"TAILTAIL",
            ..Default::default()
        };

        let error = super::stream_to_stream(&mut input, codec, &mut output).unwrap_err();

        // `process` consumes "ab" and holds "XX". `finish` writes "XX"
        // and "TAILTA", 1 byte in each step. Then the sink is full.
        assert!(matches!(error.kind, DriveErrorKind::SinkExhausted));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 2,
                written: 8,
            }
        );
        assert_eq!(&output.bytes, b"XXTAILTA");
    }

    #[test]
    fn stream_to_stream_reports_the_counts_on_a_codec_error_in_finish() {
        let mut input = SliceSource::new(b"ab");
        let mut output = OneByteWindowSink {
            bytes: [0; 8],
            written: 0,
        };
        let codec = FailsAfterInner {
            inner: HoldsOutput {
                per_input: 1,
                trailer: b"TAIL",
                ..Default::default()
            },
            ok_calls: 3,
            ..Default::default()
        };

        let error = super::stream_to_stream(&mut input, codec, &mut output).unwrap_err();

        // `process` consumes "ab" and holds "XX". `finish` writes "X"
        // and "X", then fails after it writes "T".
        assert!(matches!(error.kind, DriveErrorKind::Codec(_)));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 2,
                written: 3,
            }
        );
        assert_eq!(&output.bytes[..output.written], b"XXT");
    }

    #[test]
    fn stream_to_stream_reports_the_counts_on_a_sink_error_in_finish() {
        let mut input = SliceSource::new(b"ab");
        let mut output = FailsOnCommit {
            bytes: [0; 8],
            written: 0,
            ok_commits: 2,
        };
        let codec = HoldsOutput {
            per_input: 1,
            trailer: b"TAIL",
            ..Default::default()
        };

        let error = super::stream_to_stream(&mut input, codec, &mut output).unwrap_err();

        // `process` consumes "ab" and holds "XX". `finish` writes "XX"
        // and "TA". The commit of "IL" fails.
        assert!(matches!(error.kind, DriveErrorKind::Sink(())));
        assert_eq!(
            error.moved,
            TransferCounts {
                consumed: 2,
                written: 4,
            }
        );
        assert_eq!(&output.bytes[..output.written], b"XXTA");
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
