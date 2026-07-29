//! Example [`Codec`]: ROT13 letter substitution.

use crate::{Codec, Drain, Error, Outcome};

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
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            *out = rot13_byte(inp);
        }
        // A 1:1 codec satisfies the fully-consume-or-fully-fill
        // contract for free: `n` exhausts whichever side is shorter.
        if n == input.len() {
            Ok(Outcome::InputConsumed { written: n })
        } else {
            Ok(Outcome::OutputFilled { consumed: n })
        }
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

/// Build a [`Rot13`] codec. ROT13 is its own inverse, so there's only one
/// constructor.
pub fn rot13() -> Rot13 {
    Rot13
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::rot13;
    use crate::io::{to_vec, CodecReader, CodecWriter};

    const INPUT: &[u8] = b"Hello, World! 123";
    const ROT13D: &[u8] = b"Uryyb, Jbeyq! 123";

    #[test]
    fn to_vec_round_trip() {
        assert_eq!(to_vec(rot13(), INPUT).unwrap(), ROT13D);
        assert_eq!(to_vec(rot13(), ROT13D).unwrap(), INPUT);
    }

    #[test]
    fn reader_with_small_output_buffer() {
        let mut reader = CodecReader::new(Cursor::new(INPUT), rot13(), vec![0u8; 3]);
        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, ROT13D);
    }

    #[test]
    fn writer_finish_reaches_done() {
        let mut writer = CodecWriter::new(Vec::new(), rot13(), vec![0u8; 64]);
        for chunk in INPUT.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, ROT13D);
    }
}
