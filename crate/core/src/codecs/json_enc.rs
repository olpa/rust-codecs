//! JSON string-content escaping codec, built on the [`json_escape`] crate.
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.

use core::mem::MaybeUninit;

use json_escape::explicit::escape_bytes;

use crate::{Codec, Drain, DrainCodec, Error, ErrorKind, Progress};

/// An escape sequence known for the byte right after `pending_literal_len`'s
/// bytes, tracked through its two possible states so the invalid
/// combination (known-but-not-started *and* mid-write at once) isn't
/// representable. Escape sequences are `&'static str`, so a partially
/// written one is just a re-slice of the original — no separate buffer
/// or position counter needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum PendingEscape {
    #[default]
    None,
    /// Known from a scan, but its trigger byte isn't marked consumed
    /// yet — that only happens once this moves to `Started`.
    NotStarted(&'static str),
    /// Trigger byte consumed; this is what's left to write.
    Started(&'static str),
}

/// Escapes raw bytes into JSON string content.
#[derive(Debug, Clone, Default)]
pub struct JsonEnc {
    /// Leading bytes of the next `process` call's `input` already known
    /// (from a previous call's scan) to need no escaping.
    pending_literal_len: usize,
    pending_escape: PendingEscape,
}

impl JsonEnc {
    fn flush_pending(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<(usize, usize), Error> {
        let mut consumed = 0;
        let mut written = 0;

        if let PendingEscape::Started(tail) = self.pending_escape {
            let bytes = tail.as_bytes();
            let n = bytes.len().min(output.len() - written);
            output[written..written + n].write_copy_of_slice(&bytes[..n]);
            written += n;
            if n < bytes.len() {
                self.pending_escape = PendingEscape::Started(&tail[n..]);
                return Ok((consumed, written));
            }
            self.pending_escape = PendingEscape::None;
        }

        if self.pending_literal_len > 0 {
            let n = self.pending_literal_len.min(output.len() - written);
            output[written..written + n].write_copy_of_slice(&input[..n]);
            written += n;
            consumed += n;
            self.pending_literal_len -= n;
            if self.pending_literal_len > 0 {
                return Ok((consumed, written));
            }
        }

        if let PendingEscape::NotStarted(s) = self.pending_escape {
            consumed += 1;
            let bytes = s.as_bytes();
            let n = bytes.len().min(output.len() - written);
            output[written..written + n].write_copy_of_slice(&bytes[..n]);
            written += n;
            self.pending_escape = if n < bytes.len() {
                PendingEscape::Started(&s[n..])
            } else {
                PendingEscape::None
            };
        }

        Ok((consumed, written))
    }
}

impl DrainCodec for JsonEnc {
    fn flush(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<Drain, Error> {
        // A well-behaved driver never reaches this with pending_literal_len
        // nonzero: as long as it's nonzero, `process` reports less than
        // full input consumed, which keeps the driver calling `process`
        // again instead of `finish`.
        if self.pending_literal_len > 0 {
            return Err(Error::new(ErrorKind::UnexpectedEnd, 0, 0));
        }
        let (_, written) = self.flush_pending(&[], output)?;
        if matches!(self.pending_escape, PendingEscape::Started(_)) {
            return Ok(Drain::OutputFilled);
        }
        Ok(Drain::Done { written })
    }
}

impl Codec for JsonEnc {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let (mut consumed, mut written) = self.flush_pending(input, output)?;
        if self.pending_literal_len > 0 || matches!(self.pending_escape, PendingEscape::Started(_))
        {
            // Still pending: either flush_pending ran out of output
            // mid-literal, or an escape's tail didn't fully drain.
            // Either way, re-scanning input[consumed..] now would
            // rescan bytes already known to be part of pending_literal_len's
            // run.
            return Ok(Progress::OutputFilled { consumed });
        }

        for chunk in escape_bytes(&input[consumed..]) {
            let literal = chunk.literal();
            let n = literal.len().min(output.len() - written);
            output[written..written + n].write_copy_of_slice(&literal[..n]);
            written += n;
            consumed += n;
            if n < literal.len() {
                // Output ran out mid-literal: remember both the
                // unwritten literal tail (`pending_literal_len`) and this
                // chunk's already-computed trailing escape
                // (`pending_escape`, if any), so the next call doesn't
                // have to re-run `escape_bytes` to rediscover it.
                self.pending_literal_len = literal.len() - n;
                self.pending_escape = match chunk.escaped() {
                    Some(s) => PendingEscape::NotStarted(s),
                    None => PendingEscape::None,
                };
                return Ok(Progress::OutputFilled { consumed });
            }

            let Some(s) = chunk.escaped() else { continue };
            consumed += 1;
            let bytes = s.as_bytes();
            let n = bytes.len().min(output.len() - written);
            output[written..written + n].write_copy_of_slice(&bytes[..n]);
            written += n;
            if n < bytes.len() {
                self.pending_escape = PendingEscape::Started(&s[n..]);
                return Ok(Progress::OutputFilled { consumed });
            }
        }
        // The loop above only returns early when output runs out
        // mid-chunk; reaching here means every chunk `escape_bytes`
        // yielded — i.e. all of `input` — was fully written.
        Ok(Progress::InputConsumed { written })
    }
}

/// Build a [`JsonEnc`] codec.
pub fn json_enc() -> JsonEnc {
    JsonEnc::default()
}

#[cfg(all(test, feature = "std"))]
mod tests {
    use std::io::{Cursor, Read, Write};

    use super::{escape_bytes, json_enc, JsonEnc, PendingEscape};
    use crate::sources_and_sinks::std_io::{CodecReader, CodecWriter};
    use crate::sources_and_sinks::vec::{encode_string, VecSink, VecSource};
    use crate::{Codec, Drain, DrainCodec, DriveError, ErrorKind, Progress};

    #[test]
    fn round_trip() {
        let input = "He said \"hi\"\n\tBack\\slash\x01\x1f\x7f\u{e9}";
        let escaped = "He said \\\"hi\\\"\\n\\tBack\\\\slash\\u0001\\u001f\x7f\u{e9}";
        assert_eq!(encode_string(json_enc(), input).unwrap(), escaped);
    }

    #[test]
    fn passes_through_plain_bytes_unchanged() {
        assert_eq!(
            encode_string(json_enc(), "plain text 123").unwrap(),
            "plain text 123"
        );
    }

    #[test]
    fn passes_through_invalid_utf8_unchanged() {
        // Escaping never needs to decode characters, so bytes that aren't
        // valid UTF-8 at all are still handled correctly. `encode_string`
        // requires valid UTF-8, so this drives the Vec adapters directly.
        let mut input = VecSource::new(b"\xff\xfe".to_vec());
        let mut output = VecSink::default();
        crate::stream_to_stream(&mut input, json_enc(), &mut output)
            .map_err(|error| match error {
                DriveError::Codec(error) => error,
                _ => unreachable!("infallible Vec adapter"),
            })
            .unwrap();
        assert_eq!(output.into_inner(), b"\xff\xfe");
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
        let progress = codec
            .process(b"AAA\n", crate::uninit::as_uninit_mut(&mut output))
            .unwrap();
        assert_eq!(progress, Progress::OutputFilled { consumed: 2 });
        assert_eq!(&output, b"AA");
        assert_eq!(codec.pending_literal_len, 1);
        assert_eq!(codec.pending_escape, PendingEscape::NotStarted("\\n"));
    }

    /// Input covering every kind of state transition: literal ->
    /// escape, escape -> escape, escape -> literal, and a literal run
    /// long enough to need `pending_literal_len` on its own. Paired with the
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

        let mut reader = CodecReader::new(Cursor::new(input), json_enc(), vec![0u8; 6]);
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
        let (input, expected) = escape_transitions_fixture();

        let mut writer = CodecWriter::new(Vec::new(), json_enc(), vec![0u8; 64]);
        for chunk in input.chunks(1) {
            writer.write_all(chunk).unwrap();
        }
        let via_writer = writer.finish().unwrap();
        assert_eq!(via_writer, expected);
    }

    #[test]
    fn escape_spans_a_below_minimum_output_buffer() {
        // A 1-byte output buffer can never fit a whole escape sequence
        // atomically — `PendingEscape::Started` spreads it across as
        // many calls as needed instead of erroring, the same way base64's carry lets
        // an encoded group span calls smaller than it.
        let mut writer = CodecWriter::new(Vec::new(), json_enc(), vec![0u8; 1]);
        writer.write_all(b"\x01").unwrap();
        let out = writer.finish().unwrap();
        assert_eq!(out, b"\\u0001");
    }

    #[test]
    fn finish_before_pending_literal_drains_errors_instead_of_panicking() {
        // A caller that ignores `Progress::OutputFilled` and calls
        // `finish` anyway has no legitimate way to recover those
        // leftover literal bytes (they only ever existed in the prior
        // `process` call's `input`), so this must surface as an error
        // rather than panic on the empty-slice stand-in `finish` uses
        // internally.
        let mut codec = json_enc();
        let mut output = [0u8; 2];
        let progress = codec
            .process(b"AAA\n", crate::uninit::as_uninit_mut(&mut output))
            .unwrap();
        assert_eq!(progress, Progress::OutputFilled { consumed: 2 });
        assert_eq!(codec.pending_literal_len, 1);

        let mut finish_output = [0u8; 16];
        let result = codec.finish(crate::uninit::as_uninit_mut(&mut finish_output));
        assert_eq!(result.unwrap_err().kind, ErrorKind::UnexpectedEnd);
    }

    #[test]
    fn writer_splits_a_multibyte_character_one_byte_at_a_time() {
        // \xf0\x9f\x98\x80 (😀), fed one byte per write: no stitching is
        // needed, each byte is just an untouched literal.
        let mut writer = CodecWriter::new(Vec::new(), json_enc(), vec![0u8; 64]);
        for &b in b"\xf0\x9f\x98\x80" {
            writer.write_all(&[b]).unwrap();
        }
        let out = writer.finish().unwrap();
        assert_eq!(out, b"\xf0\x9f\x98\x80");
    }

    #[test]
    fn finish_is_idempotent_once_done() {
        let mut codec = json_enc();
        let mut output = [0u8; 16];
        assert_eq!(
            codec
                .process(b"hi", crate::uninit::as_uninit_mut(&mut output))
                .unwrap(),
            Progress::InputConsumed { written: 2 }
        );
        assert_eq!(
            codec
                .finish(crate::uninit::as_uninit_mut(&mut output))
                .unwrap(),
            Drain::Done { written: 0 }
        );
        assert_eq!(
            codec
                .finish(crate::uninit::as_uninit_mut(&mut output))
                .unwrap(),
            Drain::Done { written: 0 }
        );
    }
}
