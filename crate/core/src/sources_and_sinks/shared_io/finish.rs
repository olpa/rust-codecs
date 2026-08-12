use core::convert::Infallible;

use crate::pump::Pump;
use crate::{Codec, DriveError, Sink};

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
