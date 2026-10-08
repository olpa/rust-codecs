//! [`stream_to_stream`] drives a codec from an input source to an
//! output sink. [`Pump`], the helper behind it, is also used directly
//! by `std_io`/`embedded_io` wrappers.

use core::mem::MaybeUninit;

use crate::step::{boundary_aware_step, finish_step, flush_step};
use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, DrainProgress, Error, ErrorKind, Sink, Source,
    TransferCounts,
};

/// Why the drive stopped before the end of the stream. See
/// [`DriveError`].
#[derive(Debug)]
pub enum DriveErrorKind<EI, EO> {
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

impl<EI: core::fmt::Display, EO: core::fmt::Display> core::fmt::Display for DriveErrorKind<EI, EO> {
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
/// `moved` counts the whole drive. With [`DriveErrorKind::Codec`],
/// the inner [`Error`] counts only the codec step that failed.
#[derive(Debug)]
pub struct DriveError<EI, EO> {
    pub kind: DriveErrorKind<EI, EO>,
    pub moved: TransferCounts,
}

impl<EI, EO> DriveError<EI, EO> {
    pub(crate) fn new(kind: DriveErrorKind<EI, EO>, moved: TransferCounts) -> Self {
        Self { kind, moved }
    }

    /// Add the counts of the earlier steps of the drive.
    ///
    /// One step of the drive creates the error, and that step knows
    /// only its own counts. The caller adds the counts of the steps
    /// before it. Then `moved` counts the whole drive.
    pub(crate) fn after(self, earlier: TransferCounts) -> Self {
        let moved = TransferCounts {
            consumed: earlier.consumed + self.moved.consumed,
            written: earlier.written + self.moved.written,
        };
        Self::new(self.kind, moved)
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

impl<EI: core::fmt::Display, EO: core::fmt::Display> core::fmt::Display for DriveError<EI, EO> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        self.kind.fmt(f)
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
    let transferred = match pump.transfer_from(input, output)? {
        PumpStop::SinkExhausted(moved) => {
            return Err(DriveError::new(DriveErrorKind::SinkExhausted, moved));
        }
        PumpStop::SourceExhausted(moved) | PumpStop::InputEnded(moved) => moved,
    };
    let drained = pump
        .finish_to(output)
        .map_err(|error| error.widen_source().after(transferred))?;
    let (PumpDrain::Done { written } | PumpDrain::SinkExhausted { written }) = drained;
    let moved = TransferCounts {
        consumed: transferred.consumed,
        written: transferred.written + written,
    };
    match drained {
        PumpDrain::Done { .. } => {
            output
                .finish()
                .map_err(|error| DriveError::new(DriveErrorKind::Sink(error), moved))?;
            Ok(moved)
        }
        PumpDrain::SinkExhausted { .. } => {
            Err(DriveError::new(DriveErrorKind::SinkExhausted, moved))
        }
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

/// A version of [`PumpTransfer`] without `Progressed`, for
/// [`Pump::transfer_from`]. That method does not return for partial
/// progress alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PumpStop {
    SourceExhausted(TransferCounts),
    SinkExhausted(TransferCounts),
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
    /// Does not guard transfer steps, because a guard is extra work and
    /// nothing requires it. The `Codec` contract leaves `process` after
    /// `finish` to the codec. It can continue, or it can return `Err`.
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
            Some(error) => Err(DriveError::new(
                DriveErrorKind::Codec(error),
                TransferCounts::default(),
            )),
            None => Ok(()),
        }
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
        let mut consumed = 0;
        let mut written = 0;
        loop {
            let total = |moved: TransferCounts| TransferCounts {
                consumed: consumed + moved.consumed,
                written: written + moved.written,
            };
            let step = self
                .transfer_step(input, output)
                .map_err(|error| error.after(total(TransferCounts::default())))?;
            match step {
                PumpTransfer::Progressed(moved) => {
                    consumed += moved.consumed;
                    written += moved.written;
                }
                PumpTransfer::SourceExhausted(moved) => {
                    return Ok(PumpStop::SourceExhausted(total(moved)));
                }
                PumpTransfer::SinkExhausted(moved) => {
                    return Ok(PumpStop::SinkExhausted(total(moved)));
                }
                PumpTransfer::InputEnded(moved) => {
                    return Ok(PumpStop::InputEnded(total(moved)));
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
    /// `DriveErrorKind::NoProgress`. A codec error still commits whatever
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
        // Design note: this step reads input before it drains codec
        // output. A blocking `Source` transport can stall here. Two
        // independent cases produce "available codec output":
        //
        // - The `output` buffer is smaller than `input`. Should not
        //   happen: `Source::chunk`'s doc tells implementors to
        //   return available data instead of reading more.
        // - The codec buffers bytes internally, for example atomic
        //   units in the base64 codec. With large buffers, the worst
        //   case is a one-read delay at each atomic-unit boundary.
        //   This delay is tolerable. We do not plan to fix it.
        let Some(chunk) = input.chunk().map_err(|error| {
            DriveError::new(DriveErrorKind::Source(error), TransferCounts::default())
        })?
        else {
            return Ok(PumpTransfer::SourceExhausted(TransferCounts::default()));
        };
        let Some(spare) = output.spare().map_err(|error| {
            DriveError::new(DriveErrorKind::Sink(error), TransferCounts::default())
        })?
        else {
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
                self.latch_failure(&error);
                if error.consumed > 0 {
                    input.consume(error.consumed);
                }
                if error.written > 0 {
                    output.commit(error.written).map_err(|sink_error| {
                        // The commit failed, so no bytes count as written.
                        DriveError::new(
                            DriveErrorKind::Sink(sink_error),
                            TransferCounts::only_consumed(error.consumed),
                        )
                    })?;
                }
                let moved = TransferCounts {
                    consumed: error.consumed,
                    written: error.written,
                };
                return Err(DriveError::new(DriveErrorKind::Codec(error), moved));
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
            return Err(DriveError::new(
                DriveErrorKind::NoProgress,
                TransferCounts::default(),
            ));
        }
        // `moved.consumed` may be less than `chunk.len()` if output
        // ran out first. The unconsumed remainder is not lost: it
        // reappears on the next `input.chunk()` call, possibly
        // together with newly arrived input.
        if moved.consumed > 0 {
            input.consume(moved.consumed);
        }
        if moved.written > 0 {
            output.commit(moved.written).map_err(|error| {
                // The commit failed, so no bytes count as written.
                DriveError::new(
                    DriveErrorKind::Sink(error),
                    TransferCounts::only_consumed(moved.consumed),
                )
            })?;
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
        let mut written = 0;
        loop {
            let spare = output.spare().map_err(|error| {
                DriveError::new(
                    DriveErrorKind::Sink(error),
                    TransferCounts::only_written(written),
                )
            })?;
            let (step_written, done) = match spare {
                Some(spare) => match step(self, spare) {
                    Ok(DrainProgress::Done { written }) => (written, true),
                    Ok(DrainProgress::OutputFilled) if !spare.is_empty() => (spare.len(), false),
                    Ok(DrainProgress::OutputFilled) => {
                        return Err(DriveError::new(
                            DriveErrorKind::NoProgress,
                            TransferCounts::only_written(written),
                        ));
                    }
                    Err(error) => {
                        let error = error
                            .validated(0, spare.len())
                            .unwrap_or_else(|violation| violation);
                        self.latch_failure(&error);
                        if error.written > 0 {
                            output.commit(error.written).map_err(|sink_error| {
                                DriveError::new(
                                    DriveErrorKind::Sink(sink_error),
                                    TransferCounts::only_written(written),
                                )
                            })?;
                        }
                        let all = TransferCounts::only_written(written + error.written);
                        return Err(DriveError::new(DriveErrorKind::Codec(error), all));
                    }
                },
                None => {
                    let progress = match step(self, &mut []) {
                        Ok(progress) => progress,
                        Err(error) => {
                            self.latch_failure(&error);
                            return Err(DriveError::new(
                                DriveErrorKind::Codec(error),
                                TransferCounts::only_written(written),
                            ));
                        }
                    };
                    return Ok(match progress {
                        DrainProgress::Done { .. } => PumpDrain::Done { written },
                        DrainProgress::OutputFilled => PumpDrain::SinkExhausted { written },
                    });
                }
            };
            if step_written > 0 {
                output.commit(step_written).map_err(|error| {
                    DriveError::new(
                        DriveErrorKind::Sink(error),
                        TransferCounts::only_written(written),
                    )
                })?;
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

    use super::{Pump, PumpDrain, PumpStop, PumpTransfer};
    use crate::codecs::test_support::{
        assert_latched, EndsAtBar, FailsAfterInner, HoldsOutput, Scripted,
    };
    use crate::identity::identity;
    use crate::sources_and_sinks::slice::SliceSource;
    use crate::{
        BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, DriveErrorKind, Error, ErrorKind,
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
        assert_eq!(
            moved,
            PumpStop::SourceExhausted(TransferCounts {
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
    #[cfg(feature = "alloc")]
    fn pump_accepts_a_boxed_trait_object_codec() {
        let mut input = SliceSource::new(b"abcdef");
        let mut output = NullSink;
        let boxed: alloc::boxed::Box<dyn Codec> = alloc::boxed::Box::new(DropEverything);
        let mut pump: Pump<alloc::boxed::Box<dyn Codec>> = Pump::new(boxed);
        let moved = pump.transfer_from(&mut input, &mut output).unwrap();
        assert_eq!(
            moved,
            PumpStop::SourceExhausted(TransferCounts {
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
