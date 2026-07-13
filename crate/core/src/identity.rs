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

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::identity;
    use crate::io::{to_vec, CodecReader, CodecWriter};

    const INPUT: &[u8] = b"Hello, world!";

    #[test]
    fn to_vec_round_trip() {
        assert_eq!(to_vec(identity(), INPUT).unwrap(), INPUT);
    }

    #[test]
    fn reader_with_small_output_buffer() {
        let mut reader = CodecReader::new(Cursor::new(INPUT), identity());
        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, INPUT);
    }

    #[test]
    fn writer_finish_reaches_stream_end() {
        let mut writer = CodecWriter::new(Vec::new(), identity());
        for chunk in INPUT.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, INPUT);
    }
}
