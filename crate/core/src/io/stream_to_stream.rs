//! The shared driver for lending input and output stream adapters.

use crate::driver::{DrainEnd, Driver};
use crate::transfer::TransferEnd;
use crate::Codec;

/// A byte source which lends its current input chunk to the driver.
pub trait Input {
    type Error;

    /// Return the current non-empty chunk, or `None` at end of input.
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;

    /// Release the first `amount` bytes of the current chunk.
    fn consume(&mut self, amount: usize);
}

/// A byte destination which lends writable space to the driver.
pub trait Output {
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
pub enum CopyError<EI, EO> {
    Input(EI),
    Output(EO),
    Codec(crate::Error),
    OutputExhausted,
    EmptySlot,
}

/// Drive one codec between any pair of lending stream adapters.
pub fn stream_to_stream<I, O, C>(
    input: &mut I,
    codec: C,
    output: &mut O,
) -> Result<Totals, CopyError<I::Error, O::Error>>
where
    I: Input,
    O: Output,
    C: Codec,
{
    let mut driver = Driver::new(codec);
    let mut totals = Totals { consumed: 0, written: 0 };

    loop {
        let moved = {
            let Some(chunk) = input.chunk().map_err(CopyError::Input)? else {
                break;
            };
            let spare = output
                .spare()
                .map_err(CopyError::Output)?
                .ok_or(CopyError::OutputExhausted)?;
            if spare.is_empty() {
                return Err(CopyError::EmptySlot);
            }
            driver.process(chunk, spare).map_err(CopyError::Codec)?
        };
        input.consume(moved.consumed);
        output.commit(moved.written).map_err(CopyError::Output)?;
        totals.consumed += moved.consumed;
        totals.written += moved.written;

        if moved.end == TransferEnd::StreamEnd {
            output.finish().map_err(CopyError::Output)?;
            return Ok(totals);
        }
    }

    let moved = driver.finish(&mut []).map_err(CopyError::Codec)?;
    if moved.end == DrainEnd::Done {
        output.finish().map_err(CopyError::Output)?;
        return Ok(totals);
    }

    loop {
        let moved = {
            let spare = output
                .spare()
                .map_err(CopyError::Output)?
                .ok_or(CopyError::OutputExhausted)?;
            if spare.is_empty() {
                return Err(CopyError::EmptySlot);
            }
            driver.finish(spare).map_err(CopyError::Codec)?
        };
        output.commit(moved.written).map_err(CopyError::Output)?;
        totals.written += moved.written;
        if moved.end == DrainEnd::Done {
            output.finish().map_err(CopyError::Output)?;
            return Ok(totals);
        }
    }
}
