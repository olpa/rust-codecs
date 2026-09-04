//! Checks a codec's reported progress against the buffers it received.
//! Converts the result into exact counts. [`Pump`](crate::stream::Pump)
//! and [`Chain`](crate::Chain) share this module.

use core::mem::MaybeUninit;

use crate::{BoundaryAwareCodec, BoundaryAwareProgress, Codec, DrainProgress, DrainCodec, Error, Progress};

/// Why one step of an ordinary [`Codec::process`] call stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodecStepEnd {
    /// The call consumed the complete input window.
    InputExhausted,
    /// The call filled the complete output window.
    OutputExhausted,
}

/// Exact progress made by one validated [`Codec::process`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodecStep {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: CodecStepEnd,
}

/// Run one step of an ordinary [`Codec`]. Validate the result against
/// the buffers it received.
pub(crate) fn codec_step<C: Codec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [MaybeUninit<u8>],
) -> Result<CodecStep, Error> {
    let input_len = input.len();
    let output_len = output.len();
    let outcome = codec
        .process(input, output)?
        .validated(input_len, output_len)?;

    Ok(match outcome {
        Progress::InputConsumed { written } => CodecStep {
            consumed: input_len,
            written,
            end: CodecStepEnd::InputExhausted,
        },
        Progress::OutputFilled { consumed } => CodecStep {
            consumed,
            written: output_len,
            end: CodecStepEnd::OutputExhausted,
        },
    })
}

/// Why one step of a [`BoundaryAwareCodec::process`] call stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BoundaryAwareStepEnd {
    /// The call consumed the complete input window.
    InputExhausted,
    /// The call filled the complete output window.
    OutputExhausted,
    /// The codec ended its stream in-band.
    Boundary,
}

/// Exact progress made by one validated [`BoundaryAwareCodec::process`]
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BoundaryAwareStep {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: BoundaryAwareStepEnd,
}

/// Run one step of a [`BoundaryAwareCodec`]. Validate the result
/// against the buffers it received.
pub(crate) fn boundary_aware_step<C: BoundaryAwareCodec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [MaybeUninit<u8>],
) -> Result<BoundaryAwareStep, Error> {
    let input_len = input.len();
    let output_len = output.len();
    let outcome = codec
        .process(input, output)?
        .validated(input_len, output_len)?;

    Ok(match outcome {
        BoundaryAwareProgress::InputConsumed { written } => BoundaryAwareStep {
            consumed: input_len,
            written,
            end: BoundaryAwareStepEnd::InputExhausted,
        },
        BoundaryAwareProgress::OutputFilled { consumed } => BoundaryAwareStep {
            consumed,
            written: output_len,
            end: BoundaryAwareStepEnd::OutputExhausted,
        },
        BoundaryAwareProgress::Boundary { consumed, written } => BoundaryAwareStep {
            consumed,
            written,
            end: BoundaryAwareStepEnd::Boundary,
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

/// Why one [`DrainOp::step`] call stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainStop {
    /// The call filled all of `output`. More output remains.
    OutputFilled,
    /// The call delivered everything owed. `written` in the enclosing
    /// [`DrainStep`] holds the final count, at most `output.len()`.
    Done,
}

/// Exact progress and boundary of one validated `finish`/`sync_flush`
/// call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct DrainStep {
    pub(crate) written: usize,
    pub(crate) stop: DrainStop,
}

impl DrainOp {
    /// Run this operation once against `codec`. Validate the result
    /// and normalize it into the uniform `{ written, stop }` shape.
    pub(crate) fn step<C: DrainCodec + ?Sized>(
        self,
        codec: &mut C,
        output: &mut [MaybeUninit<u8>],
    ) -> Result<DrainStep, Error> {
        let output_len = output.len();
        let result = match self {
            DrainOp::Finish => codec.finish(output),
            DrainOp::SyncFlush => codec.sync_flush(output),
        };
        Ok(match result?.validated(output_len)? {
            DrainProgress::OutputFilled => DrainStep {
                written: output_len,
                stop: DrainStop::OutputFilled,
            },
            DrainProgress::Done { written } => DrainStep {
                written,
                stop: DrainStop::Done,
            },
        })
    }
}
