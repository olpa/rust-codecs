//! Drive a [`Codec`] between two iterator-based streams of byte buffers,
//! for environments that hand out chunks rather than exposing a
//! `std::io::Read`/`Write` pair — e.g. a message queue delivering
//! already-received packets, or a pool of reusable output buffers.
//!
//! - The input stream is an iterator of ready-made input chunks, each
//!   fallible (`Result<B, E>`) so a fallible source (file reads,
//!   network) can report a failure per-chunk.
//! - The output stream is likewise an iterator of fallible
//!   caller-provided buffer *slots* (`Result<S, E2>`, `S: AsMut<[u8]>`)
//!   — the same convention the rc7-streaming plan uses for `Chain`'s
//!   staging buffer, made fallible because handing out a slot can fail
//!   too (a pool that's out of buffers, a channel that's closed).
//!   [`stream_to_stream`] fills each slot as full as the codec allows
//!   before moving to the next one. A slot handed back with zero
//!   capacity is a caller bug, not a case to tolerate — see
//!   [`CopyError::EmptySlot`].
//!
//! This mirrors [`CodecReader`](super::CodecReader)/
//! [`CodecWriter`](super::CodecWriter), which drive the same `Codec`
//! trait over `std::io::Read`/`Write` instead. Unlike those two,
//! `stream_to_stream` does *not* go through
//! [`Engine`](crate::Engine): its input can fail per-chunk and its
//! output slots can run out entirely, neither of which the std
//! adapters need to handle, so the process/finish/`StreamEnd`
//! bookkeeping is inlined here rather than shared. If a future driver
//! needs the same shape, lift it back out then — this is deliberately
//! the harder, more general case, kept concrete until a second
//! consumer shows what's actually common.

use crate::{Codec, Error, Status};

/// Why [`stream_to_stream`] stopped before the codec reached
/// `StreamEnd`.
#[derive(Debug)]
pub enum CopyError<E, E2> {
    /// The input iterator yielded an error.
    Input(E),
    /// The output iterator yielded an error instead of a slot.
    Output(E2),
    /// The codec reported an error.
    Codec(crate::Error),
    /// The output iterator ran out of buffer slots before the codec
    /// finished producing output.
    OutputExhausted,
    /// The output iterator handed back a slot with zero capacity. A
    /// pool that can't guarantee non-empty slots must filter them out
    /// before handing its iterator to `stream_to_stream`.
    EmptySlot,
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
pub fn stream_to_stream<I, B, E, O, S, E2, C>(
    mut input: I,
    mut codec: C,
    mut output: O,
) -> Result<usize, CopyError<E, E2>>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    O: Iterator<Item = Result<S, E2>>,
    S: AsMut<[u8]>,
    C: Codec,
{
    let mut cur_in: Option<(B, usize)> = None;
    let mut inner_eof = false;
    // (slot, position already written, slot length) — length is cached
    // at pull time since `S: AsMut` needs `&mut` to measure.
    let mut cur_out: Option<(S, usize, usize)> = None;
    let mut finishing = false;
    let mut done = false;
    let mut total = 0usize;

    loop {
        // Checked first and before pulling anything: a call can both
        // deliver final bytes and reach `StreamEnd` in the same turn,
        // so by the next turn `done` may already be true — return
        // right away rather than pulling an input chunk (or an output
        // slot) that will never be used. An unused pull here would
        // silently drop a real item the caller handed over (or claim a
        // slot never actually needed).
        if done {
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
        // chunk is exhausted, or there is none yet.
        let in_buf: &[u8] = match &cur_in {
            Some((b, pos)) if *pos < b.as_ref().len() => &b.as_ref()[*pos..],
            _ => &[],
        };

        if finishing || (in_buf.is_empty() && inner_eof) {
            finishing = true;

            let out_buf: &mut [u8] = match &mut cur_out {
                Some((s, pos, len)) => &mut s.as_mut()[*pos..*len],
                None => &mut [],
            };
            let output_was_empty = out_buf.is_empty();
            let (p, status) = codec.finish(out_buf).map_err(CopyError::Codec)?;
            if let Some((_, pos, _)) = cur_out.as_mut() {
                *pos += p.written;
            }
            total += p.written;
            if matches!(status, Status::StreamEnd) {
                done = true;
            }

            if p.written > 0 {
                // Loop around: re-check `done` before doing anything
                // else, matching the "can finish and end in the same
                // call" case above.
                continue;
            }
            if done {
                return Ok(total);
            }
            if output_was_empty {
                pull_next_slot(&mut output, &mut cur_out)?;
                continue;
            }
            // A real, non-empty buffer, and `finish` still made no
            // progress: retrying the same fixed-size slot can never
            // help — the codec's next atomic output unit doesn't fit.
            return Err(CopyError::Codec(Error::OutputTooSmall));
        }

        if in_buf.is_empty() {
            // Not at EOF: wait for more input. Loop around without
            // touching the output side — pulling a slot here would
            // claim one the codec doesn't need yet (e.g. a zero-length
            // chunk sitting between two real ones).
            continue;
        }

        let out_buf_empty = match &cur_out {
            Some((_, pos, len)) => *pos >= *len,
            None => true,
        };
        if out_buf_empty {
            pull_next_slot(&mut output, &mut cur_out)?;
            continue;
        }
        let out_buf = {
            let (s, pos, len) = cur_out.as_mut().unwrap();
            &mut s.as_mut()[*pos..*len]
        };

        let (p, status) = codec.process(in_buf, out_buf).map_err(CopyError::Codec)?;
        if let Some((_, pos)) = cur_in.as_mut() {
            *pos += p.consumed;
        }
        if let Some((_, pos, _)) = cur_out.as_mut() {
            *pos += p.written;
        }
        total += p.written;
        if matches!(status, Status::StreamEnd) {
            done = true;
        }
        // Zero-*written*, not-`StreamEnd` turns (ordinary internal
        // buffering, e.g. a partial base64 quad) aren't an error on
        // their own — `p.consumed` moving is real progress, so loop
        // around and let the top pull more input. But zero *and* zero
        // — neither side moved, against a slot that was genuinely
        // non-empty (checked above) — means the codec's next atomic
        // output unit doesn't fit this slot at all. Unlike `finish`,
        // `process` can't be retried mid-call with a fresh buffer, and
        // silently moving on to the next slot would leave this one
        // under-filled, breaking the "every slot but the last is
        // filled completely" guarantee — so this is a hard stall.
        if p.consumed == 0 && p.written == 0 && !done {
            return Err(CopyError::Codec(Error::OutputTooSmall));
        }
    }
}

/// Pull the next output slot, rejecting a degenerate (zero-capacity)
/// one outright rather than silently skipping it — a slot iterator that
/// can't guarantee non-empty slots is a caller bug.
fn pull_next_slot<O, S, E, E2>(
    output: &mut O,
    cur_out: &mut Option<(S, usize, usize)>,
) -> Result<(), CopyError<E, E2>>
where
    O: Iterator<Item = Result<S, E2>>,
    S: AsMut<[u8]>,
{
    match output.next() {
        Some(Ok(mut s)) => {
            let len = s.as_mut().len();
            if len == 0 {
                return Err(CopyError::EmptySlot);
            }
            *cur_out = Some((s, 0, len));
            Ok(())
        }
        Some(Err(e)) => Err(CopyError::Output(e)),
        None => Err(CopyError::OutputExhausted),
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

    fn slot_iter(slots: &mut [Vec<u8>]) -> impl Iterator<Item = Result<&mut [u8], ()>> {
        slots.iter_mut().map(|s| Ok(s.as_mut_slice()))
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

    /// A test-only codec with a minimum atomic output size: `process`
    /// makes no progress at all — zero consumed, zero written — until
    /// `output` has room for a whole 4-byte unit, mirroring how a real
    /// format like base64 can't emit a partial group. Unlike
    /// `base64.rs`, which self-guards this by returning
    /// `Err(OutputTooSmall)` directly, this codec reports the naive
    /// `Status::OutputFull` a less careful implementation might, so the
    /// test below exercises `stream_to_stream`'s own backstop.
    struct MinOutputUnit;

    impl crate::Codec for MinOutputUnit {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<(crate::Progress, crate::Status), crate::Error> {
            const UNIT: usize = 4;
            if output.len() < UNIT {
                return Ok((crate::Progress::default(), crate::Status::OutputFull));
            }
            let n = input.len().min(output.len());
            output[..n].copy_from_slice(&input[..n]);
            let status = if n == input.len() {
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
        let written = stream_to_stream::<_, _, (), _, _, _, _>(
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
        let written = stream_to_stream::<_, _, (), _, _, _, _>(
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
        let written = stream_to_stream::<_, _, (), _, _, _, _>(
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
            stream_to_stream::<_, _, (), _, _, _, _>(empty, identity(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result =
            stream_to_stream::<_, _, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("source failed"))));
    }

    #[test]
    fn output_exhausted() {
        // Nowhere near enough slots to hold the transformed bytes.
        let mut out = slots(1, 1);
        let result = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::OutputExhausted)));
    }

    #[test]
    fn output_error_propagates() {
        // The output iterator can fail to hand out a slot at all (a
        // pool that's out of buffers, a channel that's closed) —
        // distinct from `OutputExhausted`, which means the iterator
        // ran dry, not that it errored.
        let mut first = vec![0u8; 2];
        let mut second = vec![0u8; 8];
        let slots: Vec<Result<&mut [u8], &'static str>> = vec![
            Ok(first.as_mut_slice()),
            Err("pool exhausted"),
            Ok(second.as_mut_slice()),
        ];
        let result = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            slots.into_iter(),
        );
        assert!(matches!(result, Err(CopyError::Output("pool exhausted"))));
    }

    #[test]
    fn codec_error_propagates() {
        // Truncated padded base64 input: `Base64Dec::finish` errors
        // rather than reaching `StreamEnd` (see
        // `decode_truncated_padded_stream_errors` in base64.rs).
        let encoded = to_vec(base64_enc(), INPUT).unwrap();
        let truncated = &encoded[..encoded.len() - 2];
        let mut out = slots(4, 8);
        let result = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(truncated, 4),
            base64_dec(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::Codec(_))));
    }

    #[test]
    fn process_stall_on_undersized_slot_errors_instead_of_looping() {
        // `MinOutputUnit` needs 4 bytes of output room to write
        // anything; every slot here is 1 byte, so an unguarded driver
        // would call `process` forever, always getting zero consumed
        // and zero written back. `stream_to_stream` must recognize
        // that as a hard stall and error out rather than hang.
        let mut out = slots(4, 1);
        let result = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 4),
            MinOutputUnit,
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
            stream_to_stream::<_, _, (), _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out))
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
        let written = stream_to_stream::<_, _, (), _, _, _, _>(
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
        let written = stream_to_stream::<_, _, (), _, _, _, _>(
            input,
            EarlyEnd { limit: 5, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, 5);
        assert_eq!(pulls.get(), 1);
    }

    #[test]
    fn zero_length_output_slot_is_an_error() {
        // A degenerate slot (e.g. from a pool that handed back an
        // empty buffer) has no room at all. Unlike the earlier
        // tolerant behavior, `stream_to_stream` now treats this as a
        // caller bug rather than silently skipping past it — a pool
        // that can hand out empty slots must filter them out itself.
        let mut out = vec![vec![0u8; 10], vec![], vec![0u8; 10]];
        let result = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::EmptySlot)));
    }

    #[test]
    fn input_error_as_first_item() {
        // No successful chunk ever arrives; `stream_to_stream` must report the
        // error without needing to touch the output iterator.
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> = vec![Err("boom")];
        let result =
            stream_to_stream::<_, _, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }
}
