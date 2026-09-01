//! Passes bytes through unchanged.

use core::mem::MaybeUninit;

use crate::{Codec, Drain, DrainCodec, Error, Progress};

/// Output is identical to input.
#[derive(Debug, Clone, Copy, Default)]
pub struct Identity;

impl DrainCodec for Identity {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl Codec for Identity {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let n = input.len().min(output.len());
        output[..n].write_copy_of_slice(&input[..n]);
        if n == input.len() {
            Ok(Progress::InputConsumed { written: n })
        } else {
            Ok(Progress::OutputFilled { consumed: n })
        }
    }
}

/// Build an [`Identity`] codec.
pub fn identity() -> Identity {
    Identity
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::identity;
    use crate::sources_and_sinks::vec::encode_string;

    #[test]
    fn round_trip() {
        let input = "Hello, world!";
        assert_eq!(encode_string(identity(), input).unwrap(), input);
    }
}
