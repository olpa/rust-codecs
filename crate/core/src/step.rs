//! Codec-call normalization shared by [`Pump`](crate::stream::Pump) and
//! [`Chain`](crate::Chain): validates a codec's reported progress
//! against the buffers it was actually given, and turns it into exact
//! counts callers can use without re-deriving them. Neither `Pump` nor
//! `Chain` owns this — both depend on it, which is why it lives in its
//! own module rather than inside `pump`.
//!
//! [`codec_step`]/[`boundary_aware_step`] are the `process`-side halves:
//! two entry points, not one generic function, because `Chain` accepts
//! only [`Codec`] members. A single `BoundaryAwareCodec`-bound `step`
//! would still work for `Chain` — every `Codec` is one via the blanket
//! implementation — but its result type could represent an in-band
//! `End` that `Chain`'s members can never actually produce. Keeping
//! [`CodecStep`] end-less makes that impossibility a type-level fact
//! instead of a runtime one.
//!
//! [`DrainOp`] is the `finish`/`sync_flush`-side half, shared as-is:
//! neither caller needs a narrower type here, since `Drain` has no
//! in-band-end counterpart to begin with.

use core::mem::MaybeUninit;

use crate::{BoundaryAwareCodec, BoundaryAwareProgress, Codec, Drain, DrainCodec, Error, Progress};

/// Why one step of an ordinary [`Codec::process`] call stopped. No
/// in-band end is possible — see [`BoundaryAwareStepEnd`] for the
/// [`BoundaryAwareCodec`] counterpart that has one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CodecStepEnd {
    /// The complete input window was consumed.
    InputExhausted,
    /// The complete output window was filled.
    OutputExhausted,
}

/// Exact progress made by one validated [`Codec::process`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CodecStep {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: CodecStepEnd,
}

/// Run one step of an ordinary [`Codec`], validated against the
/// buffers it was given.
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
    /// The complete input window was consumed.
    InputExhausted,
    /// The complete output window was filled.
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

/// Run one step of a [`BoundaryAwareCodec`], validated against the
/// buffers it was given.
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

/// Which of a [`DrainCodec`]'s two draining operations [`DrainOp::step`]
/// should run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOp {
    Finish,
    SyncFlush,
}

/// Why one [`DrainOp::step`] call stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainStop {
    /// All of `output` was filled and there is more to come.
    OutputFilled,
    /// Everything owed was delivered; `written` (in the enclosing
    /// [`DrainStep`]) is the final count, at most `output.len()`.
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
    /// Run this operation once against `codec`, validated
    /// (`Drain::validated`, which rejects a `Done { written }` that
    /// overclaims past `output.len()` as `ErrorKind::ContractViolation`)
    /// and normalized: `Drain` only tells you *how* the call stopped —
    /// it doesn't carry the amount written for the filled case, since
    /// by contract that must be the whole buffer. This fills that in,
    /// so both variants collapse into the uniform `{ written, stop }`
    /// shape callers work with.
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
            Drain::OutputFilled => DrainStep {
                written: output_len,
                stop: DrainStop::OutputFilled,
            },
            Drain::Done { written } => DrainStep {
                written,
                stop: DrainStop::Done,
            },
        })
    }
}
