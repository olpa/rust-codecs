//! Example [`Codec`]s: base64 encode/decode, built on the `base64`
//! crate (<https://docs.rs/base64/>).
//!
//! Both codecs carry a `pending_group` of at most one incomplete group
//! between `process` calls: up to 2 leftover bytes for the encoder, up
//! to 3 leftover base64 characters for the decoder.
//!
//! [`b64_enc`]/[`b64_dec`] build the standard base64 alphabet with
//! padding. To use a different alphabet or padding behavior (e.g.
//! URL-safe, or no padding), construct [`B64Enc::with_engine`]/
//! [`B64Dec::with_engine`] with any other `base64` crate [`Engine`].

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use crate::{Codec, Error, Progress, Status};

// 3 bytes (24 bits) = four 6-bit groups, always — this ratio is part
// of the base64 algorithm itself, not a detail of any one alphabet or
// padding config. It's also a documented requirement of every `Engine`
// impl (see `Engine::internal_decode`'s doc: "each complete 4-byte
// chunk of encoded data decodes to 3 bytes"), so these constants hold
// no matter which `Engine` a caller plugs in via `with_engine`.
const GROUP: usize = 3;
const ENCODED_GROUP: usize = 4;

/// Base64 encoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it encodes with.
///
/// Generic over `E` rather than boxed as `dyn Engine`: `encode_slice`/
/// `decode_slice` (what this codec actually calls) are generic methods
/// on `Engine`, and generic methods can't go in a trait object's
/// vtable — there's no fixed function pointer for an unbounded set of
/// monomorphizations. `Engine` also has associated types (`Config`,
/// `DecodeEstimate`) that differ per implementation, so a single `dyn
/// Engine` couldn't represent more than one concrete engine type
/// anyway. Monomorphized generics are the only option here.
#[derive(Debug, Clone)]
pub struct B64Enc<E: Engine = GeneralPurpose> {
    engine: E,
    pending_group: [u8; GROUP],
    len: usize,
}

impl<E: Engine> B64Enc<E> {
    /// Build a [`B64Enc`] that encodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self { engine, pending_group: [0; GROUP], len: 0 }
    }
}

impl<E: Engine> Codec for B64Enc<E> {
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
            out_pos += self.engine
                .encode_slice(&self.pending_group[..], &mut output[..ENCODED_GROUP])
                .map_err(|_| Error::Corrupt)?;
            self.len = 0;
        }

        // Bulk-encode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        //
        // This sizing has to be done by hand: `encode_slice` is
        // all-or-nothing on whatever slice it's given — it computes
        // the padded encoded length of the *entire* input and either
        // encodes all of it or returns `Err` with nothing written if
        // `output` is too small, it never partially fills the buffer.
        // Handing it a slice whose length isn't a multiple of `GROUP`
        // would also make it treat that slice as the final chunk and
        // add padding, which must only ever appear once, in `finish`.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / GROUP).min(remaining_out / ENCODED_GROUP);
        if groups > 0 {
            let in_bytes = groups * GROUP;
            let out_bytes = groups * ENCODED_GROUP;
            out_pos += self.engine
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
        let written = self.engine
            .encode_slice(&self.pending_group[..self.len], &mut output[..ENCODED_GROUP])
            .map_err(|_| Error::Corrupt)?;
        self.len = 0;
        Ok((Progress { consumed: 0, written }, Status::StreamEnd))
    }
}

/// Build a [`B64Enc`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`B64Enc::with_engine`].
pub fn b64_enc() -> B64Enc {
    B64Enc::with_engine(STANDARD)
}

/// Base64 decoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it decodes with.
///
/// Generic over `E` rather than boxed as `dyn Engine` for the same
/// reason as [`B64Enc`]: `encode_slice`/`decode_slice` are generic
/// methods, which can't be called through a trait object.
#[derive(Debug, Clone)]
pub struct B64Dec<E: Engine = GeneralPurpose> {
    engine: E,
    pending_group: [u8; ENCODED_GROUP],
    len: usize,
}

impl<E: Engine> B64Dec<E> {
    /// Build a [`B64Dec`] that decodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self { engine, pending_group: [0; ENCODED_GROUP], len: 0 }
    }
}

impl<E: Engine> Codec for B64Dec<E> {
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
            out_pos += self.engine
                .decode_slice(&self.pending_group[..], &mut output[..GROUP])
                .map_err(|_| Error::Corrupt)?;
            self.len = 0;
        }

        // Bulk-decode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        //
        // This sizing has to be done by hand, same as the encoder:
        // `decode_slice` is all-or-nothing on whatever slice it's
        // given — it either decodes all of it or returns `Err` with
        // nothing written if `output` is too small. Handing it a slice
        // whose length isn't a multiple of `ENCODED_GROUP` would also
        // make it treat that slice as the final chunk, applying
        // end-of-stream padding validation to a group that isn't
        // actually the last one.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / ENCODED_GROUP).min(remaining_out / GROUP);
        if groups > 0 {
            let in_bytes = groups * ENCODED_GROUP;
            let out_bytes = groups * GROUP;
            out_pos += self.engine
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

    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error> {
        if self.len == 0 {
            return Ok((Progress::default(), Status::StreamEnd));
        }
        if output.len() < GROUP {
            return Ok((Progress::default(), Status::OutputFull));
        }
        // A short trailing group is only valid at true end-of-stream,
        // and only for engines that don't require padding (e.g.
        // URL_SAFE_NO_PAD); the engine itself enforces that — a padded
        // engine like STANDARD rejects an unpadded partial group here.
        let written = self
            .engine
            .decode_slice(&self.pending_group[..self.len], &mut output[..GROUP])
            .map_err(|_| Error::UnexpectedEnd)?;
        self.len = 0;
        Ok((Progress { consumed: 0, written }, Status::StreamEnd))
    }
}

/// Build a [`B64Dec`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`B64Dec::with_engine`].
pub fn b64_dec() -> B64Dec {
    B64Dec::with_engine(STANDARD)
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Read, Write};

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{b64_dec, b64_enc, B64Dec, B64Enc};
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

    #[test]
    fn decode_truncated_padded_stream_errors() {
        // finish() decodes a short trailing group instead of always
        // erroring (so no-pad engines' final 2-3 char group works),
        // but STANDARD requires padding and must still reject a
        // stream cut off mid-symbol.
        let truncated = &ENCODED[..ENCODED.len() - 2];
        assert!(to_vec(b64_dec(), truncated).is_err());
    }

    #[test]
    fn round_trip_with_custom_engine() {
        // URL_SAFE_NO_PAD drops the trailing '=' that STANDARD adds,
        // proving with_engine actually swaps the engine rather than
        // silently falling back to STANDARD.
        let encoded = to_vec(B64Enc::with_engine(URL_SAFE_NO_PAD), INPUT).unwrap();
        assert_eq!(encoded, ENCODED.strip_suffix(b"=").unwrap());
        let decoded = to_vec(B64Dec::with_engine(URL_SAFE_NO_PAD), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }
}
