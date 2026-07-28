//! Drive a [`Codec`] between two iterator-based streams of byte buffers,
//! for environments that hand out chunks rather than exposing a
//! `std::io::Read`/`Write` pair — e.g. a message queue delivering
//! already-received packets, or a pool of reusable output buffers.
//!
//! - The input stream is an iterator of ready-made input chunks, each
//!   fallible (`Result<B, E>`) so a fallible source (file reads,
//!   network) can report a failure per-chunk.
//! - The output stream is an iterator of caller-provided buffer *slots*
//!   (`S: AsMut<[u8]>`) to fill — the same convention the rc7-streaming
//!   plan uses for `Chain`'s staging buffer. [`stream_to_stream`] fills
//!   each slot as full as the codec allows before moving to the next
//!   one.
//!
//! This mirrors [`CodecReader`](super::CodecReader)/
//! [`CodecWriter`](super::CodecWriter), which drive the same `Codec`
//! trait over `std::io::Read`/`Write` instead — both are built on
//! [`Engine`](crate::Engine). Unlike those two, an output slot here can
//! legitimately be empty (a degenerate slot, or one just exhausted), so
//! `stream_to_stream` relies on
//! [`Step::NeedOutput`](crate::Step::NeedOutput) rather than treating
//! every empty-output turn as "needs input."

use crate::{Codec, Engine, Step};

/// Why [`stream_to_stream`] stopped before the codec reached
/// `StreamEnd`.
#[derive(Debug)]
pub enum CopyError<E> {
    /// The input iterator yielded an error.
    Input(E),
    /// The codec reported an error.
    Codec(crate::Error),
    /// The output iterator ran out of buffer slots before the codec
    /// finished producing output.
    OutputExhausted,
}

/// Run `codec` over `input` (an iterator of ready-made input chunks),
/// writing the transformed bytes into `output` (an iterator of
/// caller-provided buffer slots to fill).
///
/// Returns the total number of bytes written across all output slots on
/// success. Every slot `stream_to_stream` uses is filled completely
/// except possibly the last one — the caller derives how many of its
/// bytes are valid from the returned total and the sizes of the slots
/// it handed out.
pub fn stream_to_stream<I, B, E, O, S, C>(
    mut input: I,
    codec: C,
    mut output: O,
) -> Result<usize, CopyError<E>>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    O: Iterator<Item = S>,
    S: AsMut<[u8]>,
    C: Codec,
{
    let mut engine = Engine::new(codec);
    let mut cur_in: Option<(B, usize)> = None;
    let mut inner_eof = false;
    let mut cur_out: Option<(S, usize)> = None;
    let mut total = 0usize;

    loop {
        // Checked first and before pulling anything: a call can both
        // deliver final bytes and reach `StreamEnd` in the same turn,
        // so by the next turn `is_done` may already be true — return
        // right away rather than pulling an input chunk (or an output
        // slot) that will never be used. Unlike `CodecReader`, which
        // documents that trailing unread bytes are simply lost, an
        // unused pull here would silently drop a real item the caller
        // handed over (or claim a slot never actually needed).
        if engine.is_done() {
            return Ok(total);
        }

        let need_input = match &cur_in {
            Some((b, pos)) => *pos >= b.as_ref().len(),
            None => true,
        };
        if need_input && !inner_eof {
            match input.next() {
                Some(Ok(b)) => cur_in = Some((b, 0)),
                Some(Err(e)) => return Err(CopyError::Input(e)),
                None => inner_eof = true,
            }
        }

        // The slice fed to the codec this turn: empty once the current
        // chunk (or slot) is exhausted, or there is none yet — `Engine`
        // tells us which side is the bottleneck instead of us guessing.
        let in_buf: &[u8] = match &cur_in {
            Some((b, pos)) if *pos < b.as_ref().len() => &b.as_ref()[*pos..],
            _ => &[],
        };
        let out_buf: &mut [u8] = match &mut cur_out {
            Some((s, pos)) => &mut s.as_mut()[*pos..],
            None => &mut [],
        };

        let (consumed, step) = engine
            .step(in_buf, inner_eof, out_buf)
            .map_err(CopyError::Codec)?;
        if let Some((_, pos)) = cur_in.as_mut() {
            *pos += consumed;
        }

        match step {
            Step::Wrote(n) => {
                if let Some((_, pos)) = cur_out.as_mut() {
                    *pos += n;
                }
                total += n;
            }
            Step::Done => return Ok(total),
            Step::NeedInput => {
                // Loop around: `need_input` is recomputed from `cur_in`
                // at the top, so the next chunk gets pulled then.
            }
            Step::NeedOutput => match output.next() {
                Some(s) => cur_out = Some((s, 0)),
                None => return Err(CopyError::OutputExhausted),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{stream_to_stream, CopyError};
    use crate::base64::{base64_dec, base64_enc};
    use crate::identity::identity;
    use crate::io::to_vec;
    use crate::rot13::rot13;

    const INPUT: &[u8] = b"Hello, World! 123";

    /// `n`-byte buffer slots for the output iterator to hand out, each
    /// zero-initialized so leftover bytes beyond what's written are
    /// distinguishable from real output in assertions.
    fn slots(count: usize, size: usize) -> Vec<Vec<u8>> {
        vec![vec![0u8; size]; count]
    }

    fn slot_iter(slots: &mut [Vec<u8>]) -> impl Iterator<Item = &mut [u8]> {
        slots.iter_mut().map(|s| s.as_mut_slice())
    }

    fn ok_chunks<E>(data: &[u8], chunk_size: usize) -> impl Iterator<Item = Result<&[u8], E>> {
        data.chunks(chunk_size).map(Ok)
    }

    /// A test-only codec that copies bytes 1:1, like [`Identity`], but
    /// self-terminates: once `limit` bytes have been written, `process`
    /// reports `StreamEnd` even if more input is still sitting in the
    /// caller's slice (or in further chunks `stream_to_stream` hasn't pulled yet).
    /// Models a self-describing format (e.g. one with an in-band length
    /// or terminator) that ends before the input stream does.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl crate::Codec for EarlyEnd {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<(crate::Progress, crate::Status), crate::Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            let status = if self.done >= self.limit {
                crate::Status::StreamEnd
            } else if n == input.len() {
                crate::Status::InputEmpty
            } else {
                crate::Status::OutputFull
            };
            Ok((
                crate::Progress {
                    consumed: n,
                    written: n,
                },
                status,
            ))
        }

        fn finish(
            &mut self,
            _output: &mut [u8],
        ) -> Result<(crate::Progress, crate::Status), crate::Error> {
            Ok((crate::Progress::default(), crate::Status::StreamEnd))
        }
    }

    #[test]
    fn basic_round_trip() {
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(4, 8);
        let written = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn small_output_buffers() {
        // 1-byte slots force stream_to_stream to advance to the next
        // slot after every single byte written, mirroring
        // `reader_with_small_output_buffer` in rot13.rs.
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(expected.len(), 1);
        let written = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn many_small_input_chunks() {
        // 1-byte input chunks feeding into a single large output slot,
        // exercising repeated `process` calls accumulating into one
        // slot before it's exhausted.
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(1, 64);
        let written = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(INPUT, 1),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn empty_input() {
        // No chunks at all: `finish` still has to run to completion.
        let mut out = slots(1, 8);
        let empty: std::iter::Empty<Result<&[u8], ()>> = std::iter::empty();
        let written =
            stream_to_stream::<_, _, (), _, _, _>(empty, identity(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result =
            stream_to_stream::<_, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("source failed"))));
    }

    #[test]
    fn output_exhausted() {
        // Nowhere near enough slots to hold the transformed bytes.
        let mut out = slots(1, 1);
        let result = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::OutputExhausted)));
    }

    #[test]
    fn codec_error_propagates() {
        // Truncated padded base64 input: `Base64Dec::finish` errors
        // rather than reaching `StreamEnd` (see
        // `decode_truncated_padded_stream_errors` in base64.rs).
        let encoded = to_vec(base64_enc(), INPUT).unwrap();
        let truncated = &encoded[..encoded.len() - 2];
        let mut out = slots(4, 8);
        let result = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(truncated, 4),
            base64_dec(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::Codec(_))));
    }

    #[test]
    fn empty_input_chunk_does_not_consume_an_output_slot() {
        // A zero-length chunk from the input iterator makes `process`
        // report `InputEmpty` with zero bytes written — the same
        // "wrote nothing" signal `stream_to_stream` otherwise treats as "the
        // output slot is the bottleneck, fetch a new one". It must
        // not: there's plenty of room left in the single slot below,
        // and a second real chunk still to come. Fetching an unneeded
        // slot here starves `stream_to_stream` of the one it actually needs later
        // and errors as `OutputExhausted` even though nothing ever
        // ran out.
        let chunks: Vec<Result<&[u8], ()>> = vec![Ok(&INPUT[..4]), Ok(&[]), Ok(&INPUT[4..8])];
        let mut out = slots(1, 100);
        let written =
            stream_to_stream::<_, _, (), _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out))
                .unwrap();
        assert_eq!(written, 8);
    }

    #[test]
    fn early_stream_end_ignores_remaining_input() {
        // `EarlyEnd` reports `StreamEnd` from `process` itself after 3
        // bytes, with a whole second chunk left unpulled. `stream_to_stream` must
        // stop right there rather than erroring or trying to drain the
        // rest of the input.
        let chunks: Vec<Result<&[u8], ()>> = vec![Ok(b"Hello"), Ok(b"World")];
        let mut out = slots(4, 8);
        let written = stream_to_stream::<_, _, (), _, _, _>(
            chunks.into_iter(),
            EarlyEnd { limit: 3, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, 3);
    }

    #[test]
    fn done_does_not_pull_an_extra_input_chunk() {
        // `EarlyEnd`'s limit (5) lands exactly on the first chunk's
        // length, so `Wrote` and the engine's internal `done` both
        // happen in the same call that finishes consuming "Hello" —
        // exactly the case where a driver that refills its current
        // input *before* checking whether it's already done would pull
        // the never-needed second chunk. `stream_to_stream` must not: it checks
        // done-ness first, so the input iterator's second item stays
        // untouched.
        use std::cell::Cell;
        let pulls = Cell::new(0);
        let chunks: [&[u8]; 2] = [b"Hello", b"World"];
        let mut idx = 0;
        let input = std::iter::from_fn(|| {
            if idx >= chunks.len() {
                return None;
            }
            pulls.set(pulls.get() + 1);
            let chunk = chunks[idx];
            idx += 1;
            Some(Ok::<_, ()>(chunk))
        });
        let mut out = slots(4, 8);
        let written = stream_to_stream::<_, _, (), _, _, _>(
            input,
            EarlyEnd { limit: 5, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, 5);
        assert_eq!(pulls.get(), 1);
    }

    #[test]
    fn zero_length_output_slot_is_skipped() {
        // A degenerate slot (e.g. from a pool that handed back an
        // empty buffer) has no room at all; `stream_to_stream` must move past it
        // to the next slot rather than getting stuck reporting
        // `OutputExhausted` or corrupting later slots.
        let mut out = vec![vec![0u8; 10], vec![], vec![0u8; 10]];
        let expected = to_vec(rot13(), INPUT).unwrap();
        let written = stream_to_stream::<_, _, (), _, _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn input_error_as_first_item() {
        // No successful chunk ever arrives; `stream_to_stream` must report the
        // error without needing to touch the output iterator.
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> = vec![Err("boom")];
        let result =
            stream_to_stream::<_, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }
}
