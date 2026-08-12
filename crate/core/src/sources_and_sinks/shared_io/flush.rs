use core::convert::Infallible;

use crate::pump::Pump;
use crate::{Codec, DriveError, Sink};

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
