//! Example [`Codec`]: ROT13 letter substitution.

use core::mem::MaybeUninit;

use crate::{Codec, Drain, DrainCodec, Error, Progress};

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
    fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }

    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let n = input.len().min(output.len());
        let out_iter = output[..n].iter_mut();
        let in_iter = input[..n].iter();
        for (out, &inp) in out_iter.zip(in_iter) {
            // `write` compiles to a plain store even in debug builds (no
            // call), and release builds autovectorize this whole loop.
            out.write(rot13_byte(inp));
        }
        if n == input.len() {
            Ok(Progress::InputConsumed { written: n })
        } else {
            Ok(Progress::OutputFilled { consumed: n })
        }
    }
}

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
