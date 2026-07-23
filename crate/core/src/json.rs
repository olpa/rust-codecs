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
//! straight into `output` piece by piece — no buffering needed, at any
//! output size. An escape sequence (`\uXXXX`, at most 6 bytes), though,
//! is only ever written to `output` in one go, never split across
//! calls: as a `&'static str` constant, it costs nothing to just hold
//! the reference itself (`pending_escape`) until there's room to write
//! all of it, rather than track a partial-write position. The
//! consequence is a minimum-output-size contract, like the base64
//! codecs' atomic-group requirement: a call that both needs to write an
//! escape and makes no other progress, with an `output` too small to
//! ever fit that escape, returns `Error::OutputTooSmall` rather than
//! spinning forever.
//!
//! The tail of a literal run that's already been scanned but not yet
//! copied out also outlives a `process` call when `output` runs out
//! mid-chunk. The `Codec` contract guarantees a byte a call didn't
//! consume is presented again, unchanged, at the front of the next
//! call's `input` — so instead of re-running `escape_bytes` over it
//! (which would rescan the same already-known-literal bytes on every
//! call, turning one big literal run paired with a tiny output buffer
//! into O(n²) work), `pending_literal` just remembers how many leading
//! bytes of the next `input` are known-literal and copies them
//! directly.
//!
//! A chunk pairs a literal run with its one trailing escape, and both
//! can go unwritten in the same call if `output` runs out mid-literal —
//! `chunk.escaped()` is already known at that point, so it's cached in
//! `pending_escape` too rather than thrown away and rediscovered later.
//! `pending_literal` must drain first: its bytes precede the escape's
//! trigger byte in the stream, and `Progress::consumed` is a plain
//! prefix length, so the literal tail in front of it has to actually be
//! written out before the trigger byte can even be attempted. The
//! trigger byte itself is only ever marked consumed at the moment its
//! escape is actually copied into `output` — never earlier — so a
//! cached `pending_escape` that still doesn't fit leaves the trigger
//! byte uncounted, to be re-presented (and retried) on the next call.

use json_escape::explicit::escape_bytes;

use crate::{Codec, Error, Progress, Status};

/// Escapes raw bytes into JSON string content.
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonEnc {
    /// Leading bytes of the next `process` call's `input` already known
    /// (from a previous call's scan) to need no escaping.
    pending_literal: usize,
    /// The escape sequence for the byte right after `pending_literal`'s
    /// bytes, if that byte's chunk had one — known from the same scan
    /// that set `pending_literal`, so it doesn't need to be rediscovered
    /// once the literal tail finishes draining. Always written in one
    /// go, so unlike `pending_literal` there's no partial-write position
    /// to track.
    pending_escape: Option<&'static str>,
}

impl JsonEnc {
    /// Write `s` to `output[*written..]` atomically, bumping `*written`
    /// and `*consumed` past it and clearing `pending_escape` on success.
    /// On failure (`s` doesn't fit), caches `s` in `pending_escape` so a
    /// future call can retry without rediscovering it — unless this
    /// call made no progress on `output` at all, in which case no
    /// future call with the same output size could ever succeed either.
    fn write_escape(
        &mut self,
        s: &'static str,
        output: &mut [u8],
        written: &mut usize,
        consumed: &mut usize,
    ) -> Result<bool, Error> {
        let bytes = s.as_bytes();
        if output.len() - *written < bytes.len() {
            if *written == 0 {
                return Err(Error::OutputTooSmall);
            }
            self.pending_escape = Some(s);
            return Ok(false);
        }
        output[*written..*written + bytes.len()].copy_from_slice(bytes);
        *written += bytes.len();
        *consumed += 1;
        self.pending_escape = None;
        Ok(true)
    }

    /// Drain whatever `process`/`finish` left pending from a previous
    /// call: first the literal tail (copied straight from the front of
    /// `input`), then — only once that's fully drained, since its bytes
    /// precede the escape's trigger byte in the stream — the escape
    /// sequence. Returns `(consumed, written)`.
    fn flush_pending(&mut self, input: &[u8], output: &mut [u8]) -> Result<(usize, usize), Error> {
        let mut consumed = 0;
        let mut written = 0;

        if self.pending_literal > 0 {
            let n = self.pending_literal.min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            written += n;
            consumed += n;
            self.pending_literal -= n;
            if self.pending_literal > 0 {
                return Ok((consumed, written));
            }
        }

        if let Some(s) = self.pending_escape {
            if !self.write_escape(s, output, &mut written, &mut consumed)? {
                return Ok((consumed, written));
            }
        }

        Ok((consumed, written))
    }
}

impl Codec for JsonEnc {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let (mut consumed, mut written) = self.flush_pending(input, output)?;
        if self.pending_literal > 0 || self.pending_escape.is_some() {
            // Still pending: either flush_pending ran out of output
            // mid-literal (pending_escape may or may not also be set),
            // or an escape didn't fit. Either way, re-scanning
            // input[consumed..] now would rescan bytes already known to
            // be part of pending_literal's run.
            return Ok((Progress { consumed, written }, Status::OutputFull));
        }

        for chunk in escape_bytes(&input[consumed..]) {
            let literal = chunk.literal();
            let n = literal.len().min(output.len() - written);
            output[written..written + n].copy_from_slice(&literal[..n]);
            written += n;
            consumed += n;
            if n < literal.len() {
                // Output ran out mid-literal: remember both the
                // unwritten literal tail (`pending_literal`) and this
                // chunk's already-computed trailing escape
                // (`pending_escape`, if any), so the next call doesn't
                // have to re-run `escape_bytes` to rediscover it.
                self.pending_literal = literal.len() - n;
                self.pending_escape = chunk.escaped();
                return Ok((Progress { consumed, written }, Status::OutputFull));
            }

            let Some(s) = chunk.escaped() else { continue };
            if !self.write_escape(s, output, &mut written, &mut consumed)? {
                return Ok((Progress { consumed, written }, Status::OutputFull));
            }
        }
        // The loop above only returns early when output runs out
        // mid-chunk; reaching here means every chunk `escape_bytes`
        // yielded — i.e. all of `input` — was fully written.
        Ok((Progress { consumed, written }, Status::InputEmpty))
    }

    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        // A well-behaved driver never reaches this with pending_literal
        // nonzero: as long as it's nonzero, `process` reports consumed
        // < input.len(), which keeps the driver calling `process` again
        // instead of `finish`. But those leftover literal bytes only
        // ever existed in a previous `process` call's `input`, which
        // `finish` has no access to — so a caller that violates the
        // convention gets an error here instead of an out-of-bounds
        // panic from indexing `&[]` in the shared helper below.
        if self.pending_literal > 0 {
            return Err(Error::UnexpectedEnd);
        }
        let (_, written) = self.flush_pending(&[], output)?;
        let status = if self.pending_escape.is_none() { Status::StreamEnd } else { Status::OutputFull };
        Ok((Progress { consumed: 0, written }, status))
    }
}

/// Build a [`JsonEnc`] codec. `output` buffers passed to it should be at
/// least 6 bytes, enough to fit any single escape sequence.
pub fn json_enc() -> JsonEnc {
    JsonEnc::default()
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::{escape_bytes, json_enc, JsonEnc};
    use crate::io::{to_vec, CodecReader, CodecWriter};
    use crate::{Codec, Error};

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
    fn caches_the_escape_already_known_when_a_literal_only_partly_fits() {
        // "AAA\n" scanned as one chunk: literal="AAA", escaped=Some("\n"
        // -> "\\n"). With only 2 bytes of output room, the literal can't
        // fully fit, but the trailing escape was already determined by
        // that same scan — it must be cached (not discarded) so the
        // next call doesn't have to re-run escape_bytes to rediscover
        // it.
        let mut codec = JsonEnc::default();
        let mut output = [0u8; 2];
        let (progress, status) = codec.process(b"AAA\n", &mut output).unwrap();
        assert_eq!(progress, crate::Progress { consumed: 2, written: 2 });
        assert_eq!(status, crate::Status::OutputFull);
        assert_eq!(&output, b"AA");
        assert_eq!(codec.pending_literal, 1);
        assert_eq!(codec.pending_escape, Some("\\n"));
    }

    /// Input covering every kind of state transition: literal ->
    /// escape, escape -> escape, escape -> literal, and a literal run
    /// long enough to need `pending_literal` on its own. Paired with the
    /// expected output from a single, non-streaming call to the
    /// underlying `escape_bytes`, independent of this codec's
    /// pending-state bookkeeping.
    fn escape_transitions_fixture() -> (Vec<u8>, Vec<u8>) {
        let mut input = Vec::new();
        input.extend_from_slice(&[b'A'; 5]); // long literal run
        input.push(b'\n'); // 2-byte escape, right after a literal
        input.push(b'"'); // 2-byte escape, right after another escape
        input.extend_from_slice(b"BBB"); // literal run right after an escape
        input.push(0x01); // 6-byte \u escape
        input.push(b'C'); // literal right after a \u escape
        input.push(b'\\'); // 2-byte escape
        input.extend_from_slice(&[b'D'; 4000]); // literal run longer than any output buffer

        let expected = escape_bytes(&input)
            .flat_map(|chunk| {
                let mut v = chunk.literal().to_vec();
                if let Some(s) = chunk.escaped() {
                    v.extend_from_slice(s.as_bytes());
                }
                v
            })
            .collect();

        (input, expected)
    }

    #[test]
    fn reader_matches_one_shot_escape_through_the_minimum_output_buffer() {
        // 6 bytes: the smallest buffer that can always fit an escape
        // sequence atomically, so every pending-state transition in
        // `escape_transitions_fixture` gets forced to actually happen.
        let (input, expected) = escape_transitions_fixture();

        let mut reader = CodecReader::new(Cursor::new(input), json_enc());
        let mut via_reader = Vec::new();
        let mut buf = [0u8; 6];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            via_reader.extend_from_slice(&buf[..n]);
        }
        assert_eq!(via_reader, expected);
    }

    #[test]
    fn writer_matches_one_shot_escape_fed_one_byte_at_a_time() {
        // `CodecWriter`'s internal output buffer isn't caller-tunable
        // (see `SCRATCH` in `io::stream`), so this doesn't exercise a
        // small output buffer — only that feeding `write_all` a single
        // byte of input at a time still round-trips correctly.
        let (input, expected) = escape_transitions_fixture();

        let mut writer = CodecWriter::new(Vec::new(), json_enc());
        for chunk in input.chunks(1) {
            writer.write_all(chunk).unwrap();
        }
        let via_writer = writer.finish().unwrap();
        assert_eq!(via_writer, expected);
    }

    #[test]
    fn output_buffer_smaller_than_the_longest_escape_errors() {
        // An escape sequence is always written atomically, so an output
        // buffer that can never fit one (here, a call that writes
        // nothing else either) can never make progress.
        let mut codec = json_enc();
        let mut output = [0u8; 5];
        let result = codec.process(b"\x01", &mut output);
        assert!(matches!(result, Err(Error::OutputTooSmall)));
    }

    #[test]
    fn finish_before_pending_literal_drains_errors_instead_of_panicking() {
        // A caller that ignores `Status::OutputFull` and calls `finish`
        // anyway has no legitimate way to recover those leftover
        // literal bytes (they only ever existed in the prior `process`
        // call's `input`), so this must surface as an error rather than
        // panic on the empty-slice stand-in `finish` uses internally.
        let mut codec = json_enc();
        let mut output = [0u8; 2];
        let (_, status) = codec.process(b"AAA\n", &mut output).unwrap();
        assert_eq!(status, crate::Status::OutputFull);
        assert_eq!(codec.pending_literal, 1);

        let mut finish_output = [0u8; 16];
        let result = codec.finish(&mut finish_output);
        assert!(matches!(result, Err(Error::UnexpectedEnd)));
    }

    #[test]
    fn long_literal_run_through_a_one_byte_output_buffer() {
        // A literal run far longer than the output buffer, so it can
        // only be drained a byte at a time across many `process` calls
        // via `pending_literal`, never re-running `escape_bytes` over
        // already-scanned bytes.
        let input = vec![b'x'; 5000];
        let mut reader = CodecReader::new(Cursor::new(input.clone()), json_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 1];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                break;
            }
            out.extend_from_slice(&buf[..n]);
        }
        assert_eq!(out, input);
    }

    #[test]
    fn reader_with_small_output_buffer() {
        // 6 bytes: the smallest buffer that can always fit an escape
        // sequence atomically (see output_buffer_smaller_than_the_longest_escape_errors).
        let mut reader = CodecReader::new(Cursor::new(INPUT), json_enc());
        let mut out = Vec::new();
        let mut buf = [0u8; 6];
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
