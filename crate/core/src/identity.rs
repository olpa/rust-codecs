//! Example [`Codec`]: passes bytes through unchanged.

use crate::{Codec, Error, Progress, Status};

/// A no-op codec: output is identical to input.
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl Codec for Identity {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let n = input.len().min(output.len());
        output[..n].copy_from_slice(&input[..n]);
        let status = if n == input.len() { Status::InputEmpty } else { Status::OutputFull };
        Ok((Progress { consumed: n, written: n }, status))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }
}

/// Build an [`Identity`] codec. Encoding and decoding are the same
/// operation, so there's only one constructor.
pub fn identity() -> Identity {
    Identity
}
