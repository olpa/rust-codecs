//! Example [`Codec`]s: standard base64 encode/decode, built on the
//! `base64` crate (<https://docs.rs/base64/>).
//!
//! Both codecs buffer at most one incomplete group between `process`
//! calls: up to 2 leftover bytes for the encoder, up to 3 leftover
//! base64 characters for the decoder.

use base64::engine::{general_purpose::STANDARD, Engine};

use crate::{Codec, Error, Progress, Status};

const GROUP: usize = 3;
const ENCODED_GROUP: usize = 4;

/// Standard base64 encoder.
#[derive(Debug, Clone, Default)]
pub struct B64Enc {
    buf: [u8; GROUP],
    buf_len: usize,
}

impl Codec for B64Enc {
    // Dummy: discards input, writes nothing. Replaced by the real
    // encoder in the next commit.
    fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress { consumed: input.len(), written: 0 }, Status::InputEmpty))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }
}

/// Build a [`B64Enc`] codec.
pub fn b64_enc() -> B64Enc {
    B64Enc::default()
}

/// Standard base64 decoder.
#[derive(Debug, Clone, Default)]
pub struct B64Dec {
    buf: [u8; ENCODED_GROUP],
    buf_len: usize,
}

impl Codec for B64Dec {
    // Dummy: discards input, writes nothing. Replaced by the real
    // decoder in the next commit.
    fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress { consumed: input.len(), written: 0 }, Status::InputEmpty))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }
}

/// Build a [`B64Dec`] codec.
pub fn b64_dec() -> B64Dec {
    B64Dec::default()
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::{b64_dec, b64_enc};
    use crate::io::{to_vec, CodecReader, CodecWriter};

    const INPUT: &[u8] = b"Hello, World! 123";
    const ENCODED: &[u8] = b"SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn encode_to_vec() {
        assert_eq!(to_vec(b64_enc(), INPUT).unwrap(), ENCODED);
    }

    #[test]
    fn decode_to_vec() {
        assert_eq!(to_vec(b64_dec(), ENCODED).unwrap(), INPUT);
    }

    #[test]
    fn encode_reader_with_small_output_buffer() {
        let mut reader = CodecReader::new(Cursor::new(INPUT), b64_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, ENCODED);
    }

    #[test]
    fn decode_reader_with_small_output_buffer() {
        let mut reader = CodecReader::new(Cursor::new(ENCODED), b64_dec());
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
    fn encode_writer_finish_reaches_stream_end() {
        let mut writer = CodecWriter::new(Vec::new(), b64_enc());
        for chunk in INPUT.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, ENCODED);
    }

    #[test]
    fn decode_writer_finish_reaches_stream_end() {
        let mut writer = CodecWriter::new(Vec::new(), b64_dec());
        for chunk in ENCODED.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, INPUT);
    }

    #[test]
    fn round_trip_small_input_chunks() {
        let encoded = to_vec(b64_enc(), INPUT).unwrap();
        assert_eq!(encoded, ENCODED);
        let decoded = to_vec(b64_dec(), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }
}
