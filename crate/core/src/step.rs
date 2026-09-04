//! Checks a codec's reported progress against the buffers it received.
//! Converts the result into exact counts. [`Pump`](crate::stream::Pump)
//! and [`Chain`](crate::Chain) share this module.

use core::mem::MaybeUninit;

use crate::{Codec, DrainCodec, DrainProgress, Error, Progress, TransferCounts};

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

/// Selects which of [`DrainCodec`]'s two draining operations
/// [`DrainOp::step`] runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOp {
    Finish,
    SyncFlush,
}

impl DrainOp {
    /// Run this operation once against `codec` and validate the result.
    pub(crate) fn step<C: DrainCodec + ?Sized>(
        self,
        codec: &mut C,
        output: &mut [MaybeUninit<u8>],
    ) -> Result<DrainProgress, Error> {
        let result = match self {
            DrainOp::Finish => codec.finish(output),
            DrainOp::SyncFlush => codec.sync_flush(output),
        };
        result?.validated(output.len())
    }
}
