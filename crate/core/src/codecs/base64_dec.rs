//! Base64 decoding codec, built on the `base64` crate (<https://docs.rs/base64/>).
//!
//! This codec belongs in its own crate eventually. See the crate
//! docs' note on why it lives here for now.
//!
//! This file's code is mostly AI-generated.
//!
//! Behind the `base64` feature flag.
//!
//! The decoder ignores ASCII whitespace in the input (see
//! [`u8::is_ascii_whitespace`]). So it accepts the output of the
//! `base64` tool, and MIME and PEM data with line breaks.

use core::mem::MaybeUninit;

use base64::engine::general_purpose::{GeneralPurpose, STANDARD};
use base64::engine::Engine;

use super::base64_shared::{self, PendingInput, PendingOutput, ENCODED_GROUP, GROUP};
use crate::uninit::zero_init_mut;
use crate::{Codec, DrainCodec, DrainProgress, Error, ErrorKind, Progress};

/// Base64 decoder, parameterized over the [`Engine`] (alphabet and
/// padding behavior) it decodes with.
#[derive(Debug, Clone)]
pub struct Base64Dec<E: Engine = GeneralPurpose> {
    engine: E,
    pending_input: PendingInput<ENCODED_GROUP>,
    pending_output: PendingOutput<GROUP>,
    done: bool,
}

impl<E: Engine> Base64Dec<E> {
    /// Build a [`Base64Dec`] that decodes with a caller-supplied `Engine`
    /// (e.g. `base64::engine::general_purpose::URL_SAFE_NO_PAD`).
    pub fn with_engine(engine: E) -> Self {
        Self {
            engine,
            pending_input: PendingInput::new(),
            pending_output: PendingOutput::new(),
            done: false,
        }
    }

    fn stage_group(
        &mut self,
        group: &[u8],
        consumed: usize,
        written: usize,
    ) -> Result<usize, Error> {
        let engine = &self.engine;
        // Only `finish`'s deferred/partial call can ever hand this a
        // short group; every `process`-time call passes a full one. A
        // full group failing to decode is malformed data; a short one
        // failing just means the stream ended before completing a
        // unit — the base64 crate's own error variant for "too short"
        // isn't consistent enough across lengths to key off instead
        // (e.g. a 2-byte tail reports `InvalidPadding`, not
        // `InvalidLength`).
        let kind = if group.len() < ENCODED_GROUP {
            ErrorKind::UnexpectedEnd
        } else {
            ErrorKind::CorruptStream
        };
        base64_shared::stage_group(&mut self.pending_output, consumed, written, |buffer| {
            engine.decode_slice(group, buffer).map_err(|_| kind)
        })?;
        Ok(self.pending_output.len())
    }
}

impl<E: Engine> DrainCodec for Base64Dec<E> {
    /// Write the pending output. A partial group of input stays
    /// pending: only `finish` can convert it.
    fn flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let written = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            debug_assert_eq!(written, output.len());
            return Ok(DrainProgress::OutputFilled);
        }
        Ok(DrainProgress::Done { written })
    }

    fn finish(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            debug_assert_eq!(out_pos, output.len());
            return Ok(DrainProgress::OutputFilled);
        }
        if !self.pending_input.is_empty() {
            // A short trailing group is only valid at true
            // end-of-stream, and only for engines that don't require
            // padding (e.g. URL_SAFE_NO_PAD); the engine itself
            // enforces that — a padded engine like STANDARD rejects an
            // unpadded partial group here.
            let (group, len) = self.pending_input.take_partial();
            self.stage_group(&group[..len], 0, out_pos)?;
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                debug_assert_eq!(out_pos, output.len());
                return Ok(DrainProgress::OutputFilled);
            }
        }
        Ok(DrainProgress::Done { written: out_pos })
    }
}

impl<E: Engine> Codec for Base64Dec<E> {
    /// Split `input` into runs without whitespace, and decode each run
    /// with `process_run`. The whitespace bytes count as consumed.
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let mut in_pos = 0;
        let mut out_pos = 0;
        loop {
            while in_pos < input.len() && input[in_pos].is_ascii_whitespace() {
                in_pos += 1;
            }
            let run_end = input[in_pos..]
                .iter()
                .position(u8::is_ascii_whitespace)
                .map_or(input.len(), |len| in_pos + len);
            // Call `process_run` also for an empty run. It writes the
            // pending output.
            match self.process_run(&input[in_pos..run_end], &mut output[out_pos..]) {
                Ok(Progress::InputConsumed { written }) => {
                    out_pos += written;
                    in_pos = run_end;
                }
                Ok(Progress::OutputFilled { consumed }) => {
                    return Ok(Progress::OutputFilled {
                        consumed: in_pos + consumed,
                    });
                }
                Err(error) => {
                    return Err(Error::new(
                        error.kind,
                        in_pos + error.consumed,
                        out_pos + error.written,
                    ));
                }
            }
            if in_pos == input.len() {
                return Ok(Progress::InputConsumed { written: out_pos });
            }
        }
    }
}

impl<E: Engine> Base64Dec<E> {
    /// Decode `input` that contains no whitespace. Same contract as
    /// [`Codec::process`].
    fn process_run(
        &mut self,
        input: &[u8],
        output: &mut [MaybeUninit<u8>],
    ) -> Result<Progress, Error> {
        let mut in_pos = 0;

        //
        // ## Drain pending output
        //

        let mut out_pos = self.pending_output.drain(output);
        if !self.pending_output.is_empty() {
            debug_assert_eq!(out_pos, output.len());
            return Ok(Progress::OutputFilled { consumed: 0 });
        }

        if self.done {
            if !input.is_empty() {
                return Err(Error::new(ErrorKind::CorruptStream, 0, out_pos));
            }
            return Ok(Progress::InputConsumed { written: out_pos });
        }

        //
        // ## Collect and encode pending input
        //

        if !self.pending_input.is_empty() {
            in_pos += self.pending_input.fill(input);
            if !self.pending_input.is_full() {
                debug_assert_eq!(in_pos, input.len());
                return Ok(Progress::InputConsumed { written: out_pos });
            }
            let group = self.pending_input.take();
            let produced = self.stage_group(&group, in_pos, out_pos)?;
            if produced < GROUP {
                self.done = true;
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::CorruptStream, in_pos, out_pos));
                }
            }
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                debug_assert_eq!(out_pos, output.len());
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // ## Encode step: fill output as much as possible
        //

        // Bulk-decode as many whole groups as fit both remaining
        // input and output, straight from the caller's slices.
        let remaining_in = input.len() - in_pos;
        let remaining_out = output.len() - out_pos;
        let groups = (remaining_in / ENCODED_GROUP).min(remaining_out / GROUP);
        if groups > 0 {
            let in_bytes = groups * ENCODED_GROUP;
            let out_bytes = groups * GROUP;
            // `decode_slice` requires an already-initialized `&mut [u8]`
            // and fully overwrites the bytes it reports as written
            // before returning; block-init once to bridge to that
            // foreign API rather than reading through `output`'s
            // `MaybeUninit<u8>` elements one at a time.
            let dst = zero_init_mut(&mut output[out_pos..out_pos + out_bytes]);
            let written = self
                .engine
                .decode_slice(&input[in_pos..in_pos + in_bytes], dst)
                .map_err(|_| Error::new(ErrorKind::CorruptStream, in_pos, out_pos))?;
            out_pos += written;
            in_pos += in_bytes;
            if written < out_bytes {
                self.done = true;
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::CorruptStream, in_pos, out_pos));
                }
                return Ok(Progress::InputConsumed { written: out_pos });
            }
        }

        // After bulk, whole input groups may remain because the
        // output's remainder is under one decoded group (decode
        // through pending_output to fill it completely) -- or, once
        // one of them turns out to carry padding, because the rest
        // needs to be checked for trailing input instead of decoded.
        while input.len() - in_pos >= ENCODED_GROUP {
            let next_group: [u8; ENCODED_GROUP] =
                input[in_pos..in_pos + ENCODED_GROUP].try_into().unwrap();
            if out_pos == output.len() {
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
            let produced = self.stage_group(&next_group, in_pos, out_pos)?;
            in_pos += ENCODED_GROUP;
            if produced < GROUP {
                self.done = true;
                if in_pos < input.len() {
                    return Err(Error::new(ErrorKind::CorruptStream, in_pos, out_pos));
                }
            }
            out_pos += self.pending_output.drain(&mut output[out_pos..]);
            if !self.pending_output.is_empty() {
                debug_assert_eq!(out_pos, output.len());
                return Ok(Progress::OutputFilled { consumed: in_pos });
            }
        }

        //
        // ## Buffer leftover input
        //

        // Buffer any leftover < ENCODED_GROUP characters for the next
        // call.
        if in_pos < input.len() {
            self.pending_input.set(&input[in_pos..]);
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
    use crate::uninit::as_uninit_mut;

    use base64::engine::general_purpose::URL_SAFE_NO_PAD;

    use super::{base64_dec, Base64Dec};
    use crate::codecs::base64_enc::{base64_enc, Base64Enc};
    use crate::sources_and_sinks::vec::encode_string;
    use crate::{Codec, DrainCodec, DrainProgress, ErrorKind, Progress};

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
    fn decode_truncated_padded_stream_errors() {
        // finish() decodes a short trailing group instead of always
        // erroring (so no-pad engines' final 2-3 char group works),
        // but STANDARD requires padding and must still reject a
        // stream cut off mid-symbol. That's the stream ending too
        // early, not malformed data, so it must be UnexpectedEnd
        // rather than CorruptStream.
        let truncated = &ENCODED[..ENCODED.len() - 2];
        let mut dec = base64_dec();
        let mut out = [0u8; 32];
        dec.process(truncated.as_bytes(), as_uninit_mut(&mut out))
            .unwrap();
        let error = dec.finish(as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::UnexpectedEnd);
    }

    #[test]
    fn decode_misplaced_padding_is_corrupt_not_unexpected_end() {
        // "A=BC" is a full 4-byte group with padding in the wrong
        // position -- genuine corruption, not a stream cut short.
        let mut dec = base64_dec();
        let mut out = [0u8; 32];
        let error = dec.process(b"A=BC", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::CorruptStream);
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
        // boundary. The first call's padded group must not be trusted
        // as final until a later call proves no more input follows, or
        // this slips through as "AA" instead of being rejected.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        dec.process(b"QQ==", as_uninit_mut(&mut out)).unwrap();
        assert!(dec.process(b"QQ==", as_uninit_mut(&mut out)).is_err());
    }

    #[test]
    fn decode_accepts_padded_final_group_as_sole_input() {
        // A legitimate padded final group handed to `process` on its
        // own (not preceded by any other group in the same call)
        // decodes right away; finish() then has nothing left to do.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        let outcome = dec.process(b"QQ==", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 1 });
        assert_eq!(&out[..1], b"A");
        let drain = dec.finish(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(drain, DrainProgress::Done { written: 0 });
    }

    #[test]
    fn decode_latches_done_after_padding_seen_via_top_up() {
        // Exercise the top-up branch's padding check: "QQ==" split
        // across two process() calls that land mid-group.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        let outcome = dec.process(b"Q", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 0 });
        let outcome = dec.process(b"Q==", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 1 });
        assert_eq!(&out[..1], b"A");
        assert!(dec.process(b"more", as_uninit_mut(&mut out)).is_err());
    }

    #[test]
    fn decode_bulk_batch_ending_on_padded_group_latches_done_across_calls() {
        // "SGVsbG8=" is a whole call's worth of input: two groups, the
        // second one padded, nothing else in this call. The bulk path
        // must not trust that padding as final until a later call
        // proves no more input follows.
        let mut dec = base64_dec();
        let mut out = [0u8; 16];
        let outcome = dec.process(b"SGVsbG8=", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 5 });
        assert_eq!(&out[..5], b"Hello");
        assert!(dec
            .process(b"IFdvcmxkIQ==", as_uninit_mut(&mut out))
            .is_err());
    }

    #[test]
    fn decode_bulk_batch_ending_on_padded_group_rejects_trailing_input_same_call() {
        // Same padded-batch-boundary case, but the extra data arrives
        // in the same call right after the batch.
        assert!(encode_string(base64_dec(), "SGVsbG8=IFdvcmxkIQ==").is_err());
    }

    #[test]
    fn decode_tail_loop_padded_group_latches_done_across_calls() {
        // Output room for exactly one decoded group forces the bulk
        // step to handle only "SGVs", leaving the padded "bG8=" for
        // the tail loop to decode one group at a time.
        let mut dec = base64_dec();
        let mut out = [0u8; 4];
        let outcome = dec.process(b"SGVsbG8=", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::OutputFilled { consumed: 8 });
        assert_eq!(&out[..4], b"Hell");
        let mut out = [0u8; 16];
        let outcome = dec.process(&[], as_uninit_mut(&mut out)).unwrap();
        assert_eq!(outcome, Progress::InputConsumed { written: 1 });
        assert_eq!(&out[..1], b"o");
        assert!(dec.process(b"more", as_uninit_mut(&mut out)).is_err());
    }

    #[test]
    fn decode_tail_loop_padded_group_rejects_trailing_input_same_call() {
        // Same as above, but a further group follows the padded one
        // within the same call.
        let mut dec = base64_dec();
        let mut out = [0u8; 4];
        assert!(dec
            .process(b"SGVsbG8=SGVs", as_uninit_mut(&mut out))
            .is_err());
    }

    #[test]
    fn flush_writes_pending_output() {
        // "YWJj" decodes to "abc". A 1-byte output takes only "a". The
        // decoder holds "bc" as pending output.
        let mut dec = base64_dec();
        let mut out = [0u8; 8];
        let progress = dec.process(b"YWJj", as_uninit_mut(&mut out[..1])).unwrap();
        assert_eq!(progress, Progress::OutputFilled { consumed: 4 });
        assert_eq!(out[0], b'a');

        let drained = dec.flush(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(drained, DrainProgress::Done { written: 2 });
        assert_eq!(&out[..2], b"bc");
    }

    #[test]
    fn skips_final_newline() {
        // The output of `echo hello | base64`.
        assert_eq!(
            encode_string(base64_dec(), "aGVsbG8K\n").unwrap(),
            "hello\n"
        );
    }

    #[test]
    fn skips_line_breaks_and_spaces() {
        let expected = "Hello, World! 123";
        for encoded in [
            "SGVs\nbG8s\nIFdv\ncmxk\nISAx\nMjM=\n",
            "SGVsbG8sIFdv\r\ncmxkISAxMjM=\r\n",
            "SG Vs\tbG8sIF\ndvcmxkISAxMjM=",
            "\n\n  SGVsbG8sIFdvcmxkISAxMjM=  \n\n",
        ] {
            assert_eq!(encode_string(base64_dec(), encoded).unwrap(), expected);
        }
    }

    #[test]
    fn skips_whitespace_inside_a_group_split_across_calls() {
        // "QQ==" is "A". The group is split by whitespace and by the
        // call boundaries.
        let mut dec = base64_dec();
        let mut out = [0u8; 8];
        let progress = dec.process(b"Q\n", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 0 });
        let progress = dec.process(b" Q=", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 0 });
        let progress = dec.process(b"\r\n=\n", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 1 });
        assert_eq!(out[0], b'A');
        let drained = dec.finish(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(drained, DrainProgress::Done { written: 0 });
    }

    #[test]
    fn accepts_whitespace_after_padding_but_not_data() {
        let mut dec = base64_dec();
        let mut out = [0u8; 8];
        dec.process(b"QQ==", as_uninit_mut(&mut out)).unwrap();
        let progress = dec.process(b"\n \n", as_uninit_mut(&mut out)).unwrap();
        assert_eq!(progress, Progress::InputConsumed { written: 0 });
        let error = dec.process(b"\nQQ==", as_uninit_mut(&mut out)).unwrap_err();
        assert_eq!(error.kind, ErrorKind::CorruptStream);
        assert_eq!(error.consumed, 1);
    }

    #[test]
    fn counts_whitespace_in_consumed_when_output_fills() {
        // "YWJj ZGVm" is "abc" and "def". A 4-byte output takes "abc"
        // and "d". The decoder consumes the first group, the space, and
        // the second group, and holds "ef".
        let mut dec = base64_dec();
        let mut out = [0u8; 8];
        let progress = dec
            .process(b"YWJj ZGVm", as_uninit_mut(&mut out[..4]))
            .unwrap();
        assert_eq!(progress, Progress::OutputFilled { consumed: 9 });
        assert_eq!(&out[..4], b"abcd");
        let drained = dec.finish(as_uninit_mut(&mut out)).unwrap();
        assert_eq!(drained, DrainProgress::Done { written: 2 });
        assert_eq!(&out[..2], b"ef");
    }

    #[test]
    fn error_counts_include_earlier_runs() {
        // "YWJj" decodes to "abc". Then "A=BC" is corrupt.
        let mut dec = base64_dec();
        let mut out = [0u8; 8];
        let error = dec
            .process(b"YWJj\nA=BC", as_uninit_mut(&mut out))
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::CorruptStream);
        assert_eq!(error.consumed, 5);
        assert_eq!(error.written, 3);
    }
}
