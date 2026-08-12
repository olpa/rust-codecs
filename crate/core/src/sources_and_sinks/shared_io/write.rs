use core::convert::Infallible;

use crate::pump::Pump;
use crate::sources_and_sinks::slice::SliceSource;
use crate::{Codec, DriveError, Sink};

/// Drive `pump` from `buf`, writing transformed bytes into `output` —
/// the transport-independent core shared by every `sources_and_sinks`
/// writer wrapper's `Write::write`. Returns the number of bytes
/// consumed from `buf`.
pub(crate) fn pump_write<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
    buf: &[u8],
) -> Result<usize, DriveError<Infallible, O::Error>> {
    let mut input = SliceSource::new(buf);
    pump.transfer_from(&mut input, output)?;
    Ok(input.consumed())
}

/// Drain `pump`'s trailing output into `output`, then finalize
/// `output` itself — the transport-independent core shared by every
/// `sources_and_sinks` writer wrapper's `finish`. Stops at the first
/// failure, before finalizing `output` if `pump` didn't fully drain.
pub(crate) fn pump_finish<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    pump.finish_to(output)?;
    output.finish().map_err(DriveError::Sink)?;
    Ok(())
}

/// Drain `pump`'s trailing output into `output` at a sync point,
/// without ending the stream — the transport-independent core shared
/// by every `sources_and_sinks` writer wrapper's `flush`.
pub(crate) fn pump_flush<O: Sink, C: Codec>(
    pump: &mut Pump<C>,
    output: &mut O,
) -> Result<(), DriveError<Infallible, O::Error>> {
    pump.flush_to(output)?;
    Ok(())
}
