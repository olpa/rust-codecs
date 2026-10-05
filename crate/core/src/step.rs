//! Validates a codec's reported progress against the buffers it
//! received. [`codec_step`] also converts the result into exact
//! counts. [`Pump`](crate::stream::Pump) and [`Chain`](crate::Chain)
//! share this module.

use core::mem::MaybeUninit;

use crate::{
    BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainCodec, DrainProgress, Error, Progress,
    TransferCounts,
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
        .process(input, output)?
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

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;

    use super::boundary_aware_step;
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
}
