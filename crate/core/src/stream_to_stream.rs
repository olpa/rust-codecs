//! The shared pump for lending input and output stream adapters.

use crate::pump::{DrainEnd, Pump, PumpEnd};
use crate::Codec;

/// A byte source which lends its current input chunk to the pump.
pub trait Source {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    ///
    /// "Current" is load-bearing: this is whatever hasn't been
    /// released by `consume` yet, not necessarily fresh bytes. A
    /// caller is never required to consume a whole chunk in one call
    /// (a codec may only take part of it, e.g. when output runs out
    /// first) — the unconsumed remainder is exactly what the next
    /// `chunk()` call returns, so consecutive chunks can overlap.
    /// Implementations must not hand out new bytes ahead of `pos`
    /// until the old ones are released.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Release the first `amount` bytes of the current chunk.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the pump.
pub trait Sink {
    type Error;

    /// Return writable space, or `None` when the destination is full.
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error>;

    /// Commit the first `amount` bytes of the space returned by `spare`.
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;

    /// Complete the destination after the codec stream has ended.
    fn finish(&mut self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// How much moved through [`stream_to_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    pub consumed: usize,
    pub written: usize,
}

/// Why [`stream_to_stream`] stopped before the codec finished its stream.
#[derive(Debug)]
pub enum DriveError<EI, EO> {
    Source(EI),
    Sink(EO),
    Codec(crate::Error),
    SinkExhausted,
    /// A call moved zero bytes on both sides without ending the
    /// stream — the pump refuses to spin forever on a stalled
    /// codec/endpoint pair.
    NoProgress,
}

/// Drive one codec between any pair of lending stream adapters.
pub fn stream_to_stream<I, O, C>(
    input: &mut I,
    codec: C,
    output: &mut O,
) -> Result<Totals, DriveError<I::Error, O::Error>>
where
    I: Source,
    O: Sink,
    C: Codec,
{
    let mut pump = Pump::new(codec);
    let mut totals = Totals { consumed: 0, written: 0 };

    let moved = pump.transfer_from(input, output)?;
    totals.consumed += moved.consumed;
    totals.written += moved.written;
    match moved.end {
        PumpEnd::StreamEnd => {
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
        DrainEnd::Done => {
            output.finish().map_err(DriveError::Sink)?;
            Ok(totals)
        }
        DrainEnd::SinkExhausted => Err(DriveError::SinkExhausted),
    }
}
