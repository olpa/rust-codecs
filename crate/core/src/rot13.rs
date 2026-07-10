//! Example [`Codec`]: ROT13 letter substitution.

use crate::{Codec, Error, Progress, Status};

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

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            *out = rot13_byte(inp);
        }
        let status = if n == input.len() { Status::InputEmpty } else { Status::OutputFull };
        Ok((Progress { consumed: n, written: n }, status))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }
}

/// Build a [`Rot13`] codec. ROT13 is its own inverse, so there's only one
/// constructor.
pub fn rot13() -> Rot13 {
    Rot13
}
