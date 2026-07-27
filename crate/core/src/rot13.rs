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
        let mut reader = CodecReader::new(Cursor::new(INPUT), rot13(), vec![0u8; 3]).unwrap();
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
    fn writer_finish_reaches_stream_end() {
        let mut writer = CodecWriter::new(Vec::new(), rot13(), vec![0u8; 64]).unwrap();
        for chunk in INPUT.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, ROT13D);
    }
}
