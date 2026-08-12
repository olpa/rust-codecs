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
