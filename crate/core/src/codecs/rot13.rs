//! Example [`Codec`]: ROT13 letter substitution.
//!
//! Behind the `rot13` feature flag.

use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

/// Rotate an ASCII letter by 13 places. Keep all other bytes.
fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

/// A stateless, self-inverse ROT13 codec.
#[derive(Debug, Clone, Copy, Default)]
pub struct Rot13;

impl DrainCodec for Rot13 {
    fn flush(&mut self, _output: &mut [u8]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        for (out, &inp) in output.iter_mut().zip(input) {
            *out = rot13_byte(inp);
        }
        if input.len() <= output.len() {
            Ok(Progress::InputConsumed { written: input.len() })
        } else {
            Ok(Progress::OutputFilled { consumed: output.len() })
        }
    }
}

/// Build a [`Rot13`] codec.
pub fn rot13() -> Rot13 {
    Rot13
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use super::rot13;
    use crate::sources_and_sinks::vec::encode_string;

    #[test]
    fn round_trip() {
        let input = "Hello, World! 123";
        let rot13d = "Uryyb, Jbeyq! 123";
        assert_eq!(encode_string(rot13(), input).unwrap(), rot13d);
        assert_eq!(encode_string(rot13(), rot13d).unwrap(), input);
    }
}
