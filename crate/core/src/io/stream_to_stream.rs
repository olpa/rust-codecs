//! Drive a [`Codec`] between two iterator-based streams of byte
//! buffers, for environments that hand out chunks rather than exposing
//! a `std::io::Read`/`Write` pair — e.g. a message queue delivering
//! already-received packets, or a pool of reusable output buffers.
//!
//! - The input stream is an iterator of ready-made input chunks, each
//!   fallible (`Result<B, EI>`) so a fallible source (file reads,
//!   network) can report a failure per-chunk. Zero-length chunks are
//!   skipped (a keep-alive frame is not an error); it's only the
//!   iterator *ending* that means end of input.
//! - The output stream is an iterator of caller-provided buffer
//!   *slots*, likewise fallible (`Result<S, EO>`, `S: AsMut<[u8]>` —
//!   the same convention `Chain` uses for its staging buffer), since
//!   handing out a slot can fail too (a pool that's out of buffers, a
//!   channel that's closed). A slot must be non-empty — see
//!   [`CopyError::EmptySlot`].
//!
//! Every slot is filled completely except the last (the `Codec`
//! contract — fully consume or fully fill — guarantees a codec can
//! always fill a slot's remainder, however small), so the caller
//! derives per-slot byte counts from the returned total and the slot
//! sizes it handed out.
//!
//! This mirrors [`CodecReader`](super::CodecReader)/
//! [`CodecWriter`](super::CodecWriter), which drive the same `Codec`
//! trait over `std::io::Read`/`Write` instead. Unlike those two,
//! `stream_to_stream` does *not* go through
//! [`Engine`](crate::Engine): its input can fail per-chunk and its
//! output slots can run out entirely, neither of which the std
//! adapters need to handle, so the process/finish selection is inlined
//! here rather than shared.

use crate::{Codec, Drain, Outcome};

/// How much moved through [`stream_to_stream`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Totals {
    /// Bytes consumed from the input chunks. Less than the input
    /// stream offered when the codec ended its stream in-band — this
    /// is where in the input the stream ended, so the caller can
    /// resume reading the source past it.
    pub consumed: usize,
    /// Bytes written across all output slots. Every slot is filled
    /// completely except the last, so per-slot counts follow from
    /// this total and the slot sizes.
    pub written: usize,
}

/// Why [`stream_to_stream`] stopped before the codec finished its
/// stream.
#[derive(Debug)]
pub enum CopyError<EI, EO> {
    /// The input iterator yielded an error.
    Input(EI),
    /// The output iterator yielded an error instead of a slot.
    Output(EO),
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
/// Returns the [`Totals`] of bytes consumed and written on success.
/// Every slot is filled completely except the last — the `Codec`
/// contract guarantees any non-empty slot can be filled, so slot
/// sizes need no relation to the codec's internals.
pub fn stream_to_stream<I, B, EI, O, S, EO, C>(
    input: I,
    mut codec: C,
    mut output: O,
) -> Result<Totals, CopyError<EI, EO>>
where
    I: Iterator<Item = Result<B, EI>>,
    B: AsRef<[u8]>,
    O: Iterator<Item = Result<S, EO>>,
    S: AsMut<[u8]>,
    C: Codec,
{
    // (slot, bytes written so far, slot length) — length is cached at
    // pull time since `S: AsMut` needs `&mut` to measure. Outlives the
    // phases below: a slot's remainder carries from one chunk to the
    // next, and from the last chunk into `finish`. Invariant: `Some`
    // always has writable remainder — an exhausted slot is dropped on
    // the spot, never carried.
    let mut cur_out: Option<(S, usize, usize)> = None;
    let mut totals = Totals { consumed: 0, written: 0 };

    // Phase 1: pump input chunks through `process`. Each chunk is an
    // immutable local of the outer loop; only the position within it
    // advances. An empty chunk falls straight through the inner loop.
    for item in input {
        let chunk = item.map_err(CopyError::Input)?;
        let chunk: &[u8] = chunk.as_ref();
        let mut in_pos = 0;
        while in_pos < chunk.len() {
            // Take the carried slot, or pull a fresh one — inside this
            // iteration the slot is a plain (slot, position, length),
            // no Option. A slot with zero capacity is a caller bug — a
            // pool that can't guarantee non-empty slots must filter
            // them out itself.
            let (mut slot, pos, len) = match cur_out.take() {
                Some(t) => t,
                None => {
                    let mut slot = match output.next() {
                        Some(Ok(s)) => s,
                        Some(Err(e)) => return Err(CopyError::Output(e)),
                        None => return Err(CopyError::OutputExhausted),
                    };
                    let len = slot.as_mut().len();
                    if len == 0 {
                        return Err(CopyError::EmptySlot);
                    }
                    (slot, 0, len)
                }
            };
            let out_buf = &mut slot.as_mut()[pos..len];
            let out_len = out_buf.len();
            match codec.process(&chunk[in_pos..], out_buf).map_err(CopyError::Codec)? {
                Outcome::InputConsumed { written } => {
                    if pos + written < len {
                        cur_out = Some((slot, pos + written, len));
                    }
                    totals.consumed += chunk.len() - in_pos;
                    totals.written += written;
                    break;
                }
                Outcome::OutputFilled { consumed } => {
                    // The slot is spent; dropping it (rather than
                    // putting it back) is what upholds the `cur_out`
                    // invariant.
                    in_pos += consumed;
                    totals.consumed += consumed;
                    totals.written += out_len;
                }
                Outcome::StreamEnd { consumed, written } => {
                    // The stream ended in-band; input past its end (the
                    // rest of this chunk, and any unpulled chunks) is
                    // simply not this stream's to read — `totals.consumed`
                    // tells the caller where in the input that end is.
                    totals.consumed += consumed;
                    totals.written += written;
                    return Ok(totals);
                }
            }
        }
    }

    // Phase 2: no more input — drain `finish`. Called even with no
    // slot at all (the empty buffer): `Done` vs `OutputFilled` on an
    // empty buffer is exactly how a codec that owes nothing finishes
    // without claiming a slot it has no use for.
    loop {
        let out_buf: &mut [u8] = match &mut cur_out {
            Some((s, pos, len)) => &mut s.as_mut()[*pos..*len],
            None => &mut [],
        };
        let out_len = out_buf.len();
        match codec.finish(out_buf).map_err(CopyError::Codec)? {
            Drain::OutputFilled => {
                totals.written += out_len;
                // More bytes are owed and this slot (if any) is spent
                // — a fresh one is needed either way.
                let mut slot = match output.next() {
                    Some(Ok(s)) => s,
                    Some(Err(e)) => return Err(CopyError::Output(e)),
                    None => return Err(CopyError::OutputExhausted),
                };
                let len = slot.as_mut().len();
                if len == 0 {
                    return Err(CopyError::EmptySlot);
                }
                cur_out = Some((slot, 0, len));
            }
            Drain::Done { written } => {
                totals.written += written;
                return Ok(totals);
            }
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
    use crate::{Codec, Drain, Error, Outcome};

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

    impl Codec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Outcome::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Outcome::InputConsumed { written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn basic_round_trip() {
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(4, 8);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
        assert_eq!(totals.consumed, INPUT.len());
    }

    #[test]
    fn small_output_buffers() {
        // 1-byte slots force stream_to_stream to advance to the next
        // slot after every single byte written, mirroring
        // `reader_with_small_output_buffer` in rot13.rs.
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(expected.len(), 1);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
    }

    #[test]
    fn many_small_input_chunks() {
        // 1-byte input chunks feeding into a single large output slot,
        // exercising repeated `process` calls accumulating into one
        // slot before it's exhausted.
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(1, 64);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 1),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
    }

    #[test]
    fn empty_input() {
        // No chunks at all: `finish` still has to run to completion.
        let mut out = slots(1, 8);
        let empty: std::iter::Empty<Result<&[u8], ()>> = std::iter::empty();
        let totals =
            stream_to_stream::<_, _, (), _, _, _, _>(empty, identity(), slot_iter(&mut out))
                .unwrap();
        assert_eq!(totals.written, 0);
        assert_eq!(totals.consumed, 0);
    }

    #[test]
    fn zero_output_stream_needs_no_slots() {
        // Empty input through a codec that owes nothing on finish: the
        // driver completes without ever pulling a slot, so a caller
        // that knows the stream produces zero bytes may hand in zero
        // slots. This is what keeps `cur_out` an Option — "no slot
        // was ever needed" is a real, reachable final state, and an
        // eager first pull would turn this case into a spurious
        // `OutputExhausted`.
        let empty_in: std::iter::Empty<Result<&[u8], ()>> = std::iter::empty();
        let no_slots: std::iter::Empty<Result<&mut [u8], ()>> = std::iter::empty();
        let totals =
            stream_to_stream::<_, _, _, _, _, _, _>(empty_in, identity(), no_slots).unwrap();
        assert_eq!(totals.written, 0);
        assert_eq!(totals.consumed, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result = stream_to_stream::<_, _, _, _, _, _, _>(
            chunks.into_iter(),
            rot13(),
            slot_iter(&mut out),
        );
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
        // distinct from `OutputExhausted`, which means it ran dry, not
        // that it errored.
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
        // rather than completing (see
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
    fn base64_slots_not_multiple_of_encoded_group() {
        // 10-byte slots against base64's 4-byte encoded groups: the
        // codec's carry lets a group span slot boundaries, so every
        // slot is filled completely and the split is invisible.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        assert_eq!(expected.len(), 24);
        let mut out = slots(3, 10);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
        assert_eq!(&out[0][..], &expected[..10]);
        assert_eq!(&out[1][..], &expected[10..20]);
        assert_eq!(&out[2][..4], &expected[20..]);
        assert_eq!(&out[2][4..], &[0u8; 6]);
    }

    #[test]
    fn base64_slots_smaller_than_encoded_group() {
        // 1-byte slots are below the codec's atomic unit; the carry
        // dribbles each group out byte by byte. This exact case was a
        // hard error before the fully-consume-or-fully-fill contract.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        let mut out = slots(expected.len(), 1);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
        assert_eq!(totals.consumed, INPUT.len());
        let collected: Vec<u8> = out.iter().map(|s| s[0]).collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn finish_spans_slot_boundary() {
        // 17 input bytes = 5 whole base64 groups through `process` (20
        // encoded bytes) plus 2 leftover bytes that only `finish` can
        // emit, as one final 4-byte padded group. The first slot has
        // room for 22, so the final group is split 2/2 across the slot
        // boundary mid-finish.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        let mut out = slots(2, 22);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, expected.len());
        assert_eq!(&out[0][..], &expected[..22]);
        assert_eq!(&out[1][..2], &expected[22..]);
    }

    #[test]
    fn empty_input_chunk_does_not_consume_an_output_slot() {
        // A zero-length chunk from the input iterator is skipped:
        // there's plenty of room left in the single slot below, and a
        // second real chunk still to come — claiming a fresh slot for
        // the empty chunk would starve the driver of the one it
        // actually needs later.
        let chunks: Vec<Result<&[u8], ()>> = vec![Ok(&INPUT[..4]), Ok(&[]), Ok(&INPUT[4..8])];
        let mut out = slots(1, 100);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            chunks.into_iter(),
            rot13(),
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, 8);
    }

    #[test]
    fn early_stream_end_ignores_remaining_input() {
        // `EarlyEnd` reports `StreamEnd` from `process` itself after 3
        // bytes, with a whole second chunk left unpulled. `stream_to_stream` must
        // stop right there rather than erroring or trying to drain the
        // rest of the input.
        let chunks: Vec<Result<&[u8], ()>> = vec![Ok(b"Hello"), Ok(b"World")];
        let mut out = slots(4, 8);
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            chunks.into_iter(),
            EarlyEnd { limit: 3, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, 3);
        assert_eq!(totals.consumed, 3);
    }

    #[test]
    fn stream_end_does_not_pull_an_extra_input_chunk() {
        // `EarlyEnd`'s limit (5) lands exactly on the first chunk's
        // length, so `StreamEnd` arrives in the same call that
        // finishes consuming "Hello" — the driver returns right there,
        // so the input iterator's second item stays untouched.
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
        let totals = stream_to_stream::<_, _, (), _, _, _, _>(
            input,
            EarlyEnd { limit: 5, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(totals.written, 5);
        assert_eq!(totals.consumed, 5);
        assert_eq!(pulls.get(), 1);
    }

    #[test]
    fn zero_length_output_slot_is_an_error() {
        // A degenerate slot (e.g. from a pool that handed back an
        // empty buffer) has no room at all. `stream_to_stream` treats
        // this as a caller bug rather than silently skipping past it —
        // a pool that can hand out empty slots must filter them out
        // itself.
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
        let result = stream_to_stream::<_, _, _, _, _, _, _>(
            chunks.into_iter(),
            rot13(),
            slot_iter(&mut out),
        );
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }
}
