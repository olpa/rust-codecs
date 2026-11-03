//! Validates a codec's reported progress against the buffers it
//! received. [`codec_step`] also converts the result into exact
//! counts. [`FinishPhase`] tracks how far a wrapper is in `finish`.
//! [`Pump`](crate::stream::Pump) and [`Chain`](crate::Chain)
//! share this module.

use core::mem::MaybeUninit;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, ErrorKind,
    Progress, TransferCounts,
};

/// Run one step of an ordinary [`Codec`]. Validate the result against
/// the buffers it received.
pub(crate) fn codec_step<C: Codec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [MaybeUninit<u8>],
) -> Result<TransferCounts, Error> {
    let input_len = input.len();
    let output_len = output.len();
    let outcome = codec
        .process(input, output)?
        .validated(input_len, output_len)?;

    Ok(match outcome {
        Progress::InputConsumed { written } => TransferCounts {
            consumed: input_len,
            written,
        },
        Progress::OutputFilled { consumed } => TransferCounts {
            consumed,
            written: output_len,
        },
    })
}

/// Run one step of a [`BoundaryAwareCodec`]. Validate the result
/// against the buffers it received.
pub(crate) fn boundary_aware_step<C: BoundaryAwareCodec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [MaybeUninit<u8>],
) -> Result<BoundaryAwareProgress, Error> {
    codec
        .process_bounded(input, output)?
        .validated(input.len(), output.len())
}

/// Run one `flush` step against `codec` and validate the result.
pub(crate) fn flush_step<C: DrainCodec + ?Sized>(
    codec: &mut C,
    output: &mut [MaybeUninit<u8>],
) -> Result<DrainProgress, Error> {
    codec.flush(output)?.validated(output.len())
}

/// Run one `finish` step against `codec` and validate the result.
pub(crate) fn finish_step<C: DrainCodec + ?Sized>(
    codec: &mut C,
    output: &mut [MaybeUninit<u8>],
) -> Result<DrainProgress, Error> {
    codec.finish(output)?.validated(output.len())
}

/// How far a wrapper of a codec, such as `Chain` or `Pump`, is in
/// `finish`. It moves only forward: `Open` -> `Finishing` -> `Finished`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum FinishPhase {
    /// `finish` was not called.
    #[default]
    Open,
    /// `finish` was called, but it did not report `Done` yet.
    Finishing,
    /// `finish` reported `Done`.
    Finished,
}

impl FinishPhase {
    /// Fail if `finish` was called. Call it before `process` and
    /// `flush`.
    ///
    /// The counts of the error are zero: the call moves no bytes.
    pub(crate) fn check_open(self) -> Result<(), Error> {
        match self {
            Self::Open => Ok(()),
            Self::Finishing | Self::Finished => Err(Error::new(ErrorKind::AfterFinish, 0, 0)),
        }
    }

    pub(crate) fn is_open(self) -> bool {
        self == Self::Open
    }

    pub(crate) fn is_finished(self) -> bool {
        self == Self::Finished
    }

    /// Record a `finish` call. A repeated call does not change the
    /// phase.
    pub(crate) fn start_finish(&mut self) {
        if *self == Self::Open {
            *self = Self::Finishing;
        }
    }

    /// Record that `finish` reported `Done`. Call it only after
    /// [`start_finish`](Self::start_finish), and only once.
    pub(crate) fn finish_done(&mut self) {
        *self = Self::Finished;
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::{boundary_aware_step, FinishPhase};
    use crate::codecs::test_support::{FailsAfterInner, HoldsOutput, Scripted};
    use crate::{BoundaryAwareProgress, DrainProgress, Error, ErrorKind};

    #[test]
    fn boundary_aware_step_validates_progress() {
        let mut codec = Scripted {
            process: BoundaryAwareProgress::OutputFilled { consumed: 2 },
            drain: DrainProgress::Done { written: 0 },
        };
        let progress =
            boundary_aware_step(&mut codec, b"abc", &mut [MaybeUninit::uninit(); 4]).unwrap();
        assert_eq!(
            progress,
            BoundaryAwareProgress::OutputFilled { consumed: 2 }
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ByteCountClaim, 0, 0);
        let overclaims = [
            BoundaryAwareProgress::InputConsumed { written: 6 },
            BoundaryAwareProgress::OutputFilled { consumed: 4 },
            BoundaryAwareProgress::Boundary {
                consumed: 4,
                written: 6,
            },
        ];
        for process in overclaims {
            let mut codec = Scripted {
                process,
                drain: DrainProgress::Done { written: 0 },
            };
            assert_eq!(
                boundary_aware_step(&mut codec, b"abc", &mut [MaybeUninit::uninit(); 5]),
                Err(violation),
                "{process:?}"
            );
        }
    }

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            boundary_aware_step(
                &mut FailsAfterInner {
                    inner: HoldsOutput {
                        held: 2,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                b"abc",
                &mut [MaybeUninit::uninit(); 5]
            ),
            Err(Error::new(ErrorKind::CorruptStream, 3, 2))
        );
    }

    #[test]
    fn check_open_fails_after_the_first_finish_call() {
        let after_finish = Err(Error::new(ErrorKind::AfterFinish, 0, 0));
        let mut phase = FinishPhase::default();

        phase.start_finish();
        assert_eq!(phase.check_open(), after_finish);
        phase.finish_done();
        assert_eq!(phase.check_open(), after_finish);
    }
}
