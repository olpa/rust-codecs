use core::convert::Infallible;

use crate::pump::{Pump, PumpEnd};
use crate::sources_and_sinks::slice::SliceSink;
use crate::{DriveError, Source, TerminatingCodec};

/// Drive `pump` against `input`, filling `buf` with transformed bytes —
/// the transport-independent core shared by every `sources_and_sinks`
/// reader wrapper's `Read::read`. Ends the codec's stream (running
/// `finish`) once `input` reports exhaustion, so the caller never has
/// to invoke `finish` itself.
pub(crate) fn pump_read<I: Source, C: TerminatingCodec>(
    pump: &mut Pump<C>,
    input: &mut I,
    buf: &mut [u8],
) -> Result<usize, DriveError<I::Error, Infallible>> {
    if buf.is_empty() || pump.is_done() {
        return Ok(0);
    }
    let mut output = SliceSink::new(buf);
    let moved = pump.transfer_from(input, &mut output)?;
    if moved.end == PumpEnd::SourceExhausted {
        pump.finish_to(&mut output).map_err(widen)?;
    }
    Ok(output.written())
}

/// `finish_to`'s error is `DriveError<Infallible, Infallible>` (its
/// input side never runs and its output side is the same `SliceSink`
/// as `transfer_from`'s); widen it to line up with `pump_read`'s
/// return type so both calls share one `?`-friendly error type.
fn widen<T>(error: DriveError<Infallible, Infallible>) -> DriveError<T, Infallible> {
    match error {
        DriveError::Source(never) | DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => DriveError::Codec(error),
        DriveError::SinkExhausted => DriveError::SinkExhausted,
        DriveError::NoProgress => DriveError::NoProgress,
    }
}
