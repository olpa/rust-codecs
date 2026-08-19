//! Example [`Codec`]s: base64 encode/decode, built on the `base64`
//! crate (<https://docs.rs/base64/>).
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.
//!
//! Both codecs buffer at most one incomplete group on each side of the
//! transform:
//!
//! - `pending_group` (input side): up to 2 leftover raw bytes for the
//!   encoder, up to 3 leftover base64 characters for the decoder,
//!   topped up from the next call's input.
//! - a [`Carry`] (output side): the tail of an emitted group that
//!   didn't fit the caller's output buffer, delivered first on the
//!   next call. This is what upholds the fully-consume-or-fully-fill
//!   contract even though base64 can only ever emit whole groups —
//!   any non-empty output buffer works, including a 1-byte one.
//!
//! [`base64_enc`]/[`base64_dec`] build the standard base64 alphabet with
//! padding. To use a different alphabet or padding behavior (e.g.
//! URL-safe, or no padding), construct [`Base64Enc::with_engine`]/
//! [`Base64Dec::with_engine`] with any other `base64` crate [`Engine`].

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use crate::{Carry, Codec, Drain, DrainCodec, Error, ErrorKind, Progress};

// 3 bytes (24 bits) = four 6-bit groups, always — this ratio is part
// of the base64 algorithm itself, not a detail of any one alphabet or
// padding config. It's also a documented requirement of every `Engine`
// impl (see `Engine::internal_decode`'s doc: "each complete 4-byte
// chunk of encoded data decodes to 3 bytes"), so these constants hold
// no matter which `Engine` a caller plugs in via `with_engine`.
const GROUP: usize = 3;
const ENCODED_GROUP: usize = 4;

/// Copy as much of `input` as fits into `pending[*len..]` (up to
/// `pending.len()`), advance `*len` by however much was taken, and
/// return that amount so the caller can advance its own `in_pos`.
fn append_to_pending(pending: &mut [u8], len: &mut usize, input: &[u8]) -> usize {
    let take = (pending.len() - *len).min(input.len());
    pending[*len..*len + take].copy_from_slice(&input[..take]);
    *len += take;
    take
}

/// Stash `tail` (shorter than `pending.len()`) in `pending` for the next
/// call, recording how many bytes are buffered in `*len`.
fn buffer_leftover(pending: &mut [u8], len: &mut usize, tail: &[u8]) {
    pending[..tail.len()].copy_from_slice(tail);
    *len = tail.len();
}

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
pub struct Base64Enc<E: Engine = GeneralPurpose> {
    engine: E,
    pending_group: [u8; GROUP],
    len: usize,
    carry: Carry<ENCODED_GROUP>,
}

impl<E: Engine> Base64Enc<E> {
    /// Build a [`Base64Enc`] that encodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending_group: [0; GROUP],
            len: 0,
            carry: Carry::new(),
        }
    }

    /// Encode one group directly into the carry. Partial input groups
    /// are reserved for `finish`; partial output is handled by draining
    /// the staged group afterward.
    fn stage_group(&mut self, group: &[u8], consumed: usize, written: usize) -> Result<(), Error> {
        let engine = &self.engine;
        let buffer = self
            .carry
            .buffer()
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))?;
        let n = engine
            .encode_slice(group, buffer)
            .map_err(|_| Error::new(ErrorKind::Corrupt, consumed, written))?;
        self.carry
            .set_len(n)
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))
    }
}

impl<E: Engine> DrainCodec for Base64Enc<E> {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Drain::OutputFilled);
        }
        if self.len > 0 {
            // The engine pads a final short group itself — that's why
            // partial groups are deferred to finish and never encoded
            // in process.
            let group = self.pending_group;
            self.stage_group(&group[..self.len], 0, out_pos)?;
            self.len = 0;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Drain::OutputFilled);
            }
        }
        Ok(Drain::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Enc<E> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let mut in_pos = 0;

        // Deliver the tail of a group from a previous call first.
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        // Top up a pending partial group with fresh input; emit it
        // through the carry once complete.
        if self.len > 0 {
            in_pos += append_to_pending(&mut self.pending_group, &mut self.len, input);
            if self.len < GROUP {
                // The top-up took everything `input` had.
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending_group;
            self.stage_group(&group, in_pos, out_pos)?;
            self.len = 0;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
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
            out_pos += self
                .engine
                .encode_slice(
                    &input[in_pos..in_pos + in_bytes],
                    &mut output[out_pos..out_pos + out_bytes],
                )
                .map_err(|_| Error::new(ErrorKind::Corrupt, in_pos, out_pos))?;
            in_pos += in_bytes;
        }

        // After bulk, at most one of these holds: a whole input group
        // remains (the output's remainder is under one encoded group —
        // emit through the carry to fill it completely), or the input
        // remainder is under one group (buffer it and report the input
        // consumed).
        if input.len() - in_pos >= GROUP && out_pos < output.len() {
            self.stage_group(&input[in_pos..in_pos + GROUP], in_pos, out_pos)?;
            in_pos += GROUP;
            out_pos += self.carry.drain(&mut output[out_pos..]);
        }
        if input.len() - in_pos >= GROUP {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Progress::OutputFilled { consumed: in_pos });
        }

        // Buffer any leftover < GROUP bytes for the next call.
        if in_pos < input.len() {
            buffer_leftover(&mut self.pending_group, &mut self.len, &input[in_pos..]);
        }
        Ok(Progress::InputConsumed { written: out_pos })
    }
}

/// Build a [`Base64Enc`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`Base64Enc::with_engine`].
pub fn base64_enc() -> Base64Enc {
    Base64Enc::with_engine(STANDARD)
}

/// Base64 decoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it decodes with.
///
/// Generic over `E` rather than boxed as `dyn Engine` for the same
/// reason as [`Base64Enc`]: `encode_slice`/`decode_slice` are generic
/// methods, which can't be called through a trait object.
#[derive(Debug, Clone)]
pub struct Base64Dec<E: Engine = GeneralPurpose> {
    engine: E,
    pending_group: [u8; ENCODED_GROUP],
    len: usize,
    carry: Carry<GROUP>,
}

impl<E: Engine> Base64Dec<E> {
    /// Build a [`Base64Dec`] that decodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending_group: [0; ENCODED_GROUP],
            len: 0,
            carry: Carry::new(),
        }
    }

    fn stage_group(&mut self, group: &[u8], consumed: usize, written: usize) -> Result<(), Error> {
        let engine = &self.engine;
        let buffer = self
            .carry
            .buffer()
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))?;
        let n = engine
            .decode_slice(group, buffer)
            .map_err(|_| Error::new(ErrorKind::Corrupt, consumed, written))?;
        self.carry
            .set_len(n)
            .map_err(|_| Error::new(ErrorKind::BufferOverrun, consumed, written))
    }
}

impl<E: Engine> DrainCodec for Base64Dec<E> {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Drain::OutputFilled);
        }
        if self.len > 0 {
            // A short trailing group is only valid at true
            // end-of-stream, and only for engines that don't require
            // padding (e.g. URL_SAFE_NO_PAD); the engine itself
            // enforces that — a padded engine like STANDARD rejects an
            // unpadded partial group here.
            let group = self.pending_group;
            self.stage_group(&group[..self.len], 0, out_pos)
                .map_err(|error| {
                    if error.kind == ErrorKind::Corrupt {
                        Error {
                            kind: ErrorKind::UnexpectedEnd,
                            ..error
                        }
                    } else {
                        error
                    }
                })?;
            self.len = 0;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Drain::OutputFilled);
            }
        }
        Ok(Drain::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Dec<E> {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
        let mut in_pos = 0;

        // Deliver the tail of a group from a previous call first.
        let mut out_pos = self.carry.drain(output);
        if !self.carry.is_empty() {
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        // Top up a pending partial group with fresh input.
        if self.len > 0 {
            in_pos += append_to_pending(&mut self.pending_group, &mut self.len, input);
            if self.len < ENCODED_GROUP {
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            if self.pending_group.contains(&b'=') {
                // Padding is only valid in the true last group of the
                // whole stream, and `process` can't tell it's at the
                // end — only `finish` can. Defer decoding this group
                // until then. If more input shows up first (right here,
                // or on a future call while self.len is still
                // ENCODED_GROUP), that proves this wasn't the last
                // group after all.
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::Corrupt, in_pos, out_pos));
                }
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending_group;
            self.stage_group(&group, in_pos, out_pos)?;
            self.len = 0;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
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
        //
        // A group containing padding is excluded from the bulk batch
        // even when it lands last in `input`: `decode_slice` validates
        // padding placement only within the slice it's given, so
        // padding at the end of *this* slice would be accepted even if
        // more (non-padding) input follows in a later `process` call.
        // Capping the batch before that group forces it through the
        // single-group deferred path below instead.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let mut groups = (remaining_in / ENCODED_GROUP).min(remaining_out / GROUP);
        if let Some(pad_pos) = input[in_pos..in_pos + groups * ENCODED_GROUP]
            .iter()
            .position(|&b| b == b'=')
        {
            groups = pad_pos / ENCODED_GROUP;
        }
        if groups > 0 {
            let in_bytes = groups * ENCODED_GROUP;
            let out_bytes = groups * GROUP;
            out_pos += self
                .engine
                .decode_slice(
                    &input[in_pos..in_pos + in_bytes],
                    &mut output[out_pos..out_pos + out_bytes],
                )
                .map_err(|_| Error::new(ErrorKind::Corrupt, in_pos, out_pos))?;
            in_pos += in_bytes;
        }

        // After bulk, whole input groups may remain because the next
        // one contains padding (defer it: buffer and stop, `finish`
        // will confirm it's truly last) or because the output's
        // remainder is under one decoded group (decode through the
        // carry to fill it completely).
        while input.len() - in_pos >= ENCODED_GROUP {
            let next_group: [u8; ENCODED_GROUP] =
                input[in_pos..in_pos + ENCODED_GROUP].try_into().unwrap();
            if next_group.contains(&b'=') {
                buffer_leftover(&mut self.pending_group, &mut self.len, &next_group);
                in_pos += ENCODED_GROUP;
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::Corrupt, in_pos, out_pos));
                }
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            if out_pos == output.len() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
            self.stage_group(&next_group, in_pos, out_pos)?;
            in_pos += ENCODED_GROUP;
            out_pos += self.carry.drain(&mut output[out_pos..]);
            if !self.carry.is_empty() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        // Buffer any leftover < ENCODED_GROUP characters for the next
        // call.
        if in_pos < input.len() {
            buffer_leftover(&mut self.pending_group, &mut self.len, &input[in_pos..]);
        }
        Ok(Progress::InputConsumed { written: out_pos })
    }
}

/// Build a [`Base64Dec`] codec using the standard base64 alphabet with
/// padding. For a different alphabet or padding behavior, use
/// [`Base64Dec::with_engine`].
pub fn base64_dec() -> Base64Dec {
    Base64Dec::with_engine(STANDARD)
}

#[cfg(all(test, feature = "alloc"))]
mod tests {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{base64_dec, base64_enc, Base64Dec, Base64Enc};
    use crate::sources_and_sinks::vec::encode_string;
    use crate::{Codec, Drain, DrainCodec, Progress};
    use alloc::vec::Vec;

    const INPUT: &str = "Hello, World! 123";
    const ENCODED: &str = "SGVsbG8sIFdvcmxkISAxMjM=";

    #[test]
    fn round_trip() {
        let encoded = encode_string(base64_enc(), INPUT).unwrap();
        assert_eq!(encoded, ENCODED);
        let decoded = encode_string(base64_dec(), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }

    #[test]
    fn decode_truncated_padded_stream_errors() {
        // finish() decodes a short trailing group instead of always
        // erroring (so no-pad engines' final 2-3 char group works),
        // but STANDARD requires padding and must still reject a
        // stream cut off mid-symbol.
        let truncated = &ENCODED[..ENCODED.len() - 2];
        assert!(encode_string(base64_dec(), truncated).is_err());
    }

    #[test]
    fn round_trip_with_custom_engine() {
        // URL_SAFE_NO_PAD drops the trailing '=' that STANDARD adds,
        // proving with_engine actually swaps the engine rather than
        // silently falling back to STANDARD.
        let encoded = encode_string(Base64Enc::with_engine(URL_SAFE_NO_PAD), INPUT).unwrap();
        assert_eq!(encoded, ENCODED.strip_suffix('=').unwrap());
        let decoded = encode_string(Base64Dec::with_engine(URL_SAFE_NO_PAD), &encoded).unwrap();
        assert_eq!(decoded, INPUT);
    }

    #[test]
    fn decode_rejects_padding_before_end_in_one_call() {
        // "QQ==" ("A") followed by more encoded data is corrupt: padding
        // is only valid in the true last group of the stream.
        assert!(encode_string(base64_dec(), "QQ==QQ==").is_err());
    }

    #[test]
    fn decode_rejects_padding_before_end_split_across_calls() {
        // Same corrupt input as above, but fed as two process() calls
        // that each happen to align exactly on the padded group's
        // boundary. Decoding a padded group must be deferred until
        // finish() confirms it's truly last, or this slips through as
        // "AA" instead of being rejected.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        dec.process(b"QQ==", &mut out).unwrap();
        assert!(dec.process(b"QQ==", &mut out).is_err());
    }

    #[test]
    fn decode_accepts_padded_final_group_as_sole_input() {
        // A legitimate padded final group handed to `process` on its
        // own (not preceded by any other group in the same call) must
        // still decode successfully once finish() confirms it's last.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        let outcome = dec.process(b"QQ==", &mut out).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let drain = dec.finish(&mut out).unwrap();
        assert_eq!(drain, Drain::Done { written: 1 });
        assert_eq!(&out[..1], b"A");
    }

    #[test]
    fn encode_into_one_byte_outputs() {
        // Drive process/finish by hand with a 1-byte output each call:
        // the carry must dribble every 4-byte group out one byte at a
        // time, upholding fully-consume-or-fully-fill throughout.
        let input = INPUT.as_bytes();
        let mut enc = base64_enc();
        let mut collected = Vec::new();
        let mut in_pos = 0;
        while in_pos < input.len() {
            let mut out = [0u8; 1];
            match enc.process(&input[in_pos..], &mut out).unwrap() {
                Progress::InputConsumed { written } => {
                    collected.extend_from_slice(&out[..written]);
                    in_pos = input.len();
                }
                Progress::OutputFilled { consumed } => {
                    collected.extend_from_slice(&out);
                    in_pos += consumed;
                }
            }
        }
        loop {
            let mut out = [0u8; 1];
            match enc.finish(&mut out).unwrap() {
                Drain::OutputFilled => collected.extend_from_slice(&out),
                Drain::Done { written } => {
                    collected.extend_from_slice(&out[..written]);
                    break;
                }
            }
        }
        assert_eq!(collected, ENCODED.as_bytes());
    }
}
