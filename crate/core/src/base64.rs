//! Example [`Codec`]s: standard base64 encode/decode, built on the
//! `base64` crate (<https://docs.rs/base64/>).
//!
//! Both codecs carry a `pending_group` of at most one incomplete group
//! between `process` calls: up to 2 leftover bytes for the encoder, up
//! to 3 leftover base64 characters for the decoder.

use base64::engine::{general_purpose::STANDARD, Engine};

use crate::{Codec, Error, Progress, Status};

const GROUP: usize = 3;
const ENCODED_GROUP: usize = 4;

/// Standard base64 encoder.
#[derive(Debug, Clone, Default)]
pub struct B64Enc {
    pending_group: [u8; GROUP],
    len: usize,
}

impl Codec for B64Enc {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        // Top up a pending partial group with fresh input.
        if self.len > 0 {
            let need = GROUP - self.len;
            let take = need.min(input.len());
            self.pending_group[self.len..self.len + take].copy_from_slice(&input[..take]);
            self.len += take;
            in_pos += take;

            if self.len < GROUP {
                return Ok((Progress { consumed: in_pos, written: 0 }, Status::InputEmpty));
            }
            if output.len() < ENCODED_GROUP {
                if in_pos == 0 {
                    // Buffer was already full on entry (a previous call
                    // topped it up but couldn't fit the output) and this
                    // call took no new input either — no progress at
                    // all, so retrying with the same buffer would spin
                    // forever.
                    return Err(Error::OutputTooSmall);
                }
                return Ok((Progress { consumed: in_pos, written: 0 }, Status::OutputFull));
            }
            out_pos += STANDARD
                .encode_slice(&self.pending_group[..], &mut output[..ENCODED_GROUP])
                .map_err(|_| Error::Corrupt)?;
            self.len = 0;
        }

        // Bulk-encode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / GROUP).min(remaining_out / ENCODED_GROUP);
        if groups > 0 {
            let in_bytes = groups * GROUP;
            let out_bytes = groups * ENCODED_GROUP;
            out_pos += STANDARD
                .encode_slice(&input[in_pos..in_pos + in_bytes], &mut output[out_pos..out_pos + out_bytes])
                .map_err(|_| Error::Corrupt)?;
            in_pos += in_bytes;
        }

        // A full group's worth of unconsumed input means the output
        // buffer ran out before a bulk group could be encoded — leave
        // it for the caller to retry with more output space, rather
        // than overflowing the leftover buffer (which only ever holds
        // < GROUP bytes).
        let remaining = input.len() - in_pos;
        if remaining >= GROUP {
            if out_pos == 0 {
                // Nothing was written this call and the output buffer
                // is smaller than one encoded group (4 bytes) — this
                // codec has a minimum atomic output size, so retrying
                // with the same buffer would spin forever.
                return Err(Error::OutputTooSmall);
            }
            return Ok((Progress { consumed: in_pos, written: out_pos }, Status::OutputFull));
        }

        // Buffer any leftover < GROUP bytes for the next call.
        if remaining > 0 {
            self.pending_group[..remaining].copy_from_slice(&input[in_pos..]);
            self.len = remaining;
            in_pos = input.len();
        }

        let status = if in_pos == input.len() { Status::InputEmpty } else { Status::OutputFull };
        Ok((Progress { consumed: in_pos, written: out_pos }, status))
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        if self.len == 0 {
            return Ok((Progress::default(), Status::StreamEnd));
        }
        if output.len() < ENCODED_GROUP {
            return Ok((Progress::default(), Status::OutputFull));
        }
        let written = STANDARD
            .encode_slice(&self.pending_group[..self.len], &mut output[..ENCODED_GROUP])
            .map_err(|_| Error::Corrupt)?;
        self.len = 0;
        Ok((Progress { consumed: 0, written }, Status::StreamEnd))
    }
}

/// Build a [`B64Enc`] codec.
pub fn b64_enc() -> B64Enc {
    B64Enc::default()
}

/// Standard base64 decoder.
#[derive(Debug, Clone, Default)]
pub struct B64Dec {
    pending_group: [u8; ENCODED_GROUP],
    len: usize,
}

impl Codec for B64Dec {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;

        // Top up a pending partial group with fresh input.
        if self.len > 0 {
            let need = ENCODED_GROUP - self.len;
            let take = need.min(input.len());
            self.pending_group[self.len..self.len + take].copy_from_slice(&input[..take]);
            self.len += take;
            in_pos += take;

            if self.len < ENCODED_GROUP {
                return Ok((Progress { consumed: in_pos, written: 0 }, Status::InputEmpty));
            }
            if output.len() < GROUP {
                if in_pos == 0 {
                    return Err(Error::OutputTooSmall);
                }
                return Ok((Progress { consumed: in_pos, written: 0 }, Status::OutputFull));
            }
            out_pos += STANDARD
                .decode_slice(&self.pending_group[..], &mut output[..GROUP])
                .map_err(|_| Error::Corrupt)?;
            self.len = 0;
        }

        // Bulk-decode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / ENCODED_GROUP).min(remaining_out / GROUP);
        if groups > 0 {
            let in_bytes = groups * ENCODED_GROUP;
            let out_bytes = groups * GROUP;
            out_pos += STANDARD
                .decode_slice(&input[in_pos..in_pos + in_bytes], &mut output[out_pos..out_pos + out_bytes])
                .map_err(|_| Error::Corrupt)?;
            in_pos += in_bytes;
        }

        // A full group's worth of unconsumed input means the output
        // buffer ran out before a bulk group could be decoded.
        let remaining = input.len() - in_pos;
        if remaining >= ENCODED_GROUP {
            if out_pos == 0 {
                return Err(Error::OutputTooSmall);
            }
            return Ok((Progress { consumed: in_pos, written: out_pos }, Status::OutputFull));
        }

        // Buffer any leftover < ENCODED_GROUP characters for the next
        // call.
        if remaining > 0 {
            self.pending_group[..remaining].copy_from_slice(&input[in_pos..]);
            self.len = remaining;
            in_pos = input.len();
        }

        let status = if in_pos == input.len() { Status::InputEmpty } else { Status::OutputFull };
        Ok((Progress { consumed: in_pos, written: out_pos }, status))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        if self.len != 0 {
            // A trailing group of 1-3 base64 characters can never be
            // valid (a full group is always 4 chars, whitespace/padding
            // included) — the stream was truncated mid-symbol.
            return Err(Error::UnexpectedEnd);
        }
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
        // 4 bytes is base64's atomic output size (one encoded group);
        // a smaller buffer can never receive a full group.
        let mut reader = CodecReader::new(Cursor::new(INPUT), b64_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 4];
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
