//! Example [`Codec`]: ROT13 letter substitution.

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
    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            *out = rot13_byte(inp);
        }
        // A 1:1 codec satisfies the fully-consume-or-fully-fill
        // contract for free: `n` exhausts whichever side is shorter.
        if n == input.len() {
            Ok(Progress::InputConsumed { written: n })
        } else {
            Ok(Progress::OutputFilled { consumed: n })
        }
    }
}

/// Build a [`Rot13`] codec. ROT13 is its own inverse, so there's only one
/// constructor.
pub fn rot13() -> Rot13 {
    Rot13
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "std")]
    use std::io::{Cursor, Read, Write};

    #[cfg(feature = "alloc")]
    use super::rot13;
    #[cfg(feature = "alloc")]
    use alloc::vec::Vec;
    #[cfg(feature = "alloc")]
    use crate::stream_to_stream;
    #[cfg(feature = "std")]
    use crate::sources_and_sinks::std_io::{CodecReader, CodecWriter};
    #[cfg(feature = "alloc")]
    use crate::sources_and_sinks::vec::{VecSource, VecSink};

    #[cfg(feature = "alloc")]
    fn collect(codec: impl crate::Codec, bytes: &[u8]) -> Vec<u8> {
        let mut input = VecSource::new(bytes.to_vec());
        let mut output = VecSink::default();
        stream_to_stream(&mut input, codec, &mut output).unwrap();
        output.into_inner()
    }

    #[cfg(feature = "alloc")]
    const INPUT: &[u8] = b"Hello, World! 123";
    #[cfg(feature = "alloc")]
    const ROT13D: &[u8] = b"Uryyb, Jbeyq! 123";

    #[cfg(feature = "alloc")]
    #[test]
    fn vec_adapter_round_trip() {
        assert_eq!(collect(rot13(), INPUT), ROT13D);
        assert_eq!(collect(rot13(), ROT13D), INPUT);
    }

    #[cfg(feature = "std")]
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

    #[cfg(feature = "std")]
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
