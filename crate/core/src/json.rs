//! JSON string-content escaping, built on the [`json_escape`] crate.
//!
//! Escapes raw bytes into the form they'd take inside a JSON string
//! literal (the content between the quotes, quotes not included).
//! `json_escape::explicit::escape_bytes` does the actual escaping — it
//! scans for `"`, `\`, and control bytes and yields literal/escaped
//! chunks directly over `&[u8]`, so this codec never needs to know or
//! care about UTF-8 character boundaries, even when a `process` call's
//! input slice splits a multi-byte character. The `explicit` module
//! (rather than `token`) is used because it pairs each literal run with
//! its trailing escape in one chunk instead of two separate tokens,
//! which the crate's docs call out as measurably faster on inputs with a
//! high density of escape sequences.
//!
//! A literal chunk is already a slice of `input`, so it's copied
//! straight into `output` piece by piece — no buffering needed. The one
//! thing that can outlive a `process` call is a partially-written escape
//! sequence (`\uXXXX`, at most 6 bytes), but since those are `&'static
//! str` constants, holding one across calls costs nothing more than the
//! reference itself plus how much of it has been written so far.

use json_escape::explicit::escape_bytes;

use crate::{Codec, Error, Progress, Status};

/// Escapes raw bytes into JSON string content.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEnc {
    pending_escape: Option<&'static str>,
    pending_pos: usize,
}

impl JsonEnc {
    /// Copy as much of an unflushed pending escape sequence as fits into
    /// `output`. Returns how many bytes were written.
    fn flush_pending(&mut self, output: &mut [u8]) -> usize {
        let Some(s) = self.pending_escape else { return 0 };
        let left = &s.as_bytes()[self.pending_pos..];
        let n = left.len().min(output.len());
        output[..n].copy_from_slice(&left[..n]);
        self.pending_pos += n;
        if self.pending_pos == s.len() {
            self.pending_escape = None;
            self.pending_pos = 0;
        }
        n
    }
}

impl Codec for JsonEnc {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let mut written = self.flush_pending(output);
        if self.pending_escape.is_some() {
            return Ok((Progress { consumed: 0, written }, Status::OutputFull));
        }

        let mut consumed = 0;
        for chunk in escape_bytes(input) {
            let literal = chunk.literal();
            let n = literal.len().min(output.len() - written);
            output[written..written + n].copy_from_slice(&literal[..n]);
            written += n;
            consumed += n;
            if n < literal.len() {
                return Ok((Progress { consumed, written }, Status::OutputFull));
            }

            let Some(s) = chunk.escaped() else { continue };
            let bytes = s.as_bytes();
            let n = bytes.len().min(output.len() - written);
            output[written..written + n].copy_from_slice(&bytes[..n]);
            written += n;
            consumed += 1;
            if n < bytes.len() {
                self.pending_escape = Some(s);
                self.pending_pos = n;
                return Ok((Progress { consumed, written }, Status::OutputFull));
            }
        }
        // The loop above only returns early when output runs out
        // mid-chunk; reaching here means every chunk `escape_bytes`
        // yielded — i.e. all of `input` — was fully written.
        Ok((Progress { consumed, written }, Status::InputEmpty))
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let written = self.flush_pending(output);
        let status = if self.pending_escape.is_none() { Status::StreamEnd } else { Status::OutputFull };
        Ok((Progress { consumed: 0, written }, status))
    }
}

/// Build a [`JsonEnc`] codec.
pub fn json_enc() -> JsonEnc {
    JsonEnc::default()
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::json_enc;
    use crate::io::{to_vec, CodecReader, CodecWriter};

    const INPUT: &[u8] = b"He said \"hi\"\n\tBack\\slash\x01\x1f\x7f\xc3\xa9";
    const ESCAPED: &[u8] =
        b"He said \\\"hi\\\"\\n\\tBack\\\\slash\\u0001\\u001f\x7f\xc3\xa9";

    #[test]
    fn to_vec_round_trip() {
        assert_eq!(to_vec(json_enc(), INPUT).unwrap(), ESCAPED);
    }

    #[test]
    fn passes_through_plain_bytes_unchanged() {
        assert_eq!(to_vec(json_enc(), b"plain text 123").unwrap(), b"plain text 123");
    }

    #[test]
    fn passes_through_invalid_utf8_unchanged() {
        // Escaping never needs to decode characters, so bytes that aren't
        // valid UTF-8 at all are still handled correctly.
        assert_eq!(to_vec(json_enc(), b"\xff\xfe").unwrap(), b"\xff\xfe");
    }

    #[test]
    fn reader_with_small_output_buffer() {
        let mut reader = CodecReader::new(Cursor::new(INPUT), json_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 3];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, ESCAPED);
    }

    #[test]
    fn reader_with_one_byte_output_buffer_splits_escapes_across_reads() {
        let mut reader = CodecReader::new(Cursor::new(INPUT), json_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 1];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, ESCAPED);
    }

    #[test]
    fn writer_finish_reaches_stream_end() {
        let mut writer = CodecWriter::new(Vec::new(), json_enc());
        for chunk in INPUT.chunks(3) {
            writer.write_all(chunk).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, ESCAPED);
    }

    #[test]
    fn writer_splits_a_multibyte_character_one_byte_at_a_time() {
        // \xf0\x9f\x98\x80 (😀), fed one byte per write: no stitching is
        // needed, each byte is just an untouched literal.
        let mut writer = CodecWriter::new(Vec::new(), json_enc());
        for &b in b"\xf0\x9f\x98\x80" {
            writer.write_all(&[b]).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, b"\xf0\x9f\x98\x80");
    }
}
