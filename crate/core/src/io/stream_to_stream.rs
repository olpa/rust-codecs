//! Drive a [`Codec`] between an iterator of input chunks and an
//! [`OutputSink`] of output slots, for environments that hand out
//! buffers rather than exposing a `std::io::Read`/`Write` pair — e.g.
//! a message queue delivering already-received packets, or a pool of
//! reusable output buffers.
//!
//! - The input stream is an iterator of ready-made input chunks, each
//!   fallible (`Result<B, E>`) so a fallible source (file reads,
//!   network) can report a failure per-chunk. Zero-length chunks are
//!   skipped (a keep-alive frame is not an error); it's only the
//!   iterator *ending* that means end of input.
//! - The output side is an [`OutputSink`]: a grant/commit supplier of
//!   buffer slots. The sink's [`commit`](OutputSink::commit) records
//!   tell the caller how many bytes landed in each slot; every slot is
//!   filled completely except the last (the `Codec` contract — fully
//!   consume or fully fill — guarantees a codec can always fill a
//!   slot's remainder, however small). [`IterSink`] adapts an iterator
//!   of buffers for callers whose slots come ready-made.
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

/// A grant/commit supplier of output buffer slots for
/// [`stream_to_stream`].
///
/// The driver's cycle: [`slot`](Self::slot) to see the writable
/// remainder of the current slot, [`commit`](Self::commit) after
/// writing into its prefix, [`next_slot`](Self::next_slot) once the
/// slot is full. The sink is the caller's record of how many bytes are
/// valid in each slot; there is no other channel for that information.
pub trait OutputSink {
    /// Why the sink failed to produce a slot (a pool that's out of
    /// buffers, a channel that's closed).
    type Error;

    /// Seal the current slot and make a fresh slot current.
    /// `Ok(false)` means no more slots will ever come.
    ///
    /// A fresh slot must be non-empty; handing back a zero-capacity
    /// slot is a sink bug the driver reports as
    /// [`CopyError::EmptySlot`].
    fn next_slot(&mut self) -> Result<bool, Self::Error>;

    /// The writable remainder of the current slot: empty before the
    /// first `next_slot` call, and shrinking with each `commit`.
    fn slot(&mut self) -> &mut [u8];

    /// Record that `n` more bytes were written into the front of
    /// [`slot`](Self::slot).
    fn commit(&mut self, n: usize);
}

/// Forwarding impl so a caller can pass `&mut sink` and keep the sink
/// — with its per-slot fill records — after the call returns.
impl<T: OutputSink + ?Sized> OutputSink for &mut T {
    type Error = T::Error;

    fn next_slot(&mut self) -> Result<bool, Self::Error> {
        (**self).next_slot()
    }

    fn slot(&mut self) -> &mut [u8] {
        (**self).slot()
    }

    fn commit(&mut self, n: usize) {
        (**self).commit(n)
    }
}

/// Adapts an iterator of fallible buffers (`Result<S, E2>`,
/// `S: AsMut<[u8]>` — the same convention `Chain` uses for its staging
/// buffer) into an [`OutputSink`], for callers whose slots come
/// ready-made and who don't need the sink's per-slot fill records —
/// with borrowed slices the bytes land in the caller's buffers either
/// way.
pub struct IterSink<O, S> {
    iter: O,
    // (slot, bytes committed, slot length) — length is cached at pull
    // time since `S: AsMut` needs `&mut` to measure.
    cur: Option<(S, usize, usize)>,
}

impl<O, S> IterSink<O, S> {
    pub fn new(iter: O) -> Self {
        Self { iter, cur: None }
    }
}

impl<O, S, E2> OutputSink for IterSink<O, S>
where
    O: Iterator<Item = Result<S, E2>>,
    S: AsMut<[u8]>,
{
    type Error = E2;

    fn next_slot(&mut self) -> Result<bool, E2> {
        match self.iter.next() {
            Some(Ok(mut s)) => {
                let len = s.as_mut().len();
                self.cur = Some((s, 0, len));
                Ok(true)
            }
            Some(Err(e)) => Err(e),
            None => {
                self.cur = None;
                Ok(false)
            }
        }
    }

    fn slot(&mut self) -> &mut [u8] {
        match &mut self.cur {
            Some((s, pos, len)) => &mut s.as_mut()[*pos..*len],
            None => &mut [],
        }
    }

    fn commit(&mut self, n: usize) {
        if let Some((_, pos, _)) = self.cur.as_mut() {
            *pos += n;
        }
    }
}

/// Why [`stream_to_stream`] stopped before the codec finished its
/// stream.
#[derive(Debug)]
pub enum CopyError<E, E2> {
    /// The input iterator yielded an error.
    Input(E),
    /// The output sink failed to produce a slot.
    Output(E2),
    /// The codec reported an error.
    Codec(crate::Error),
    /// The output sink ran out of slots before the codec finished
    /// producing output.
    OutputExhausted,
    /// The output sink handed back a slot with zero capacity. A pool
    /// that can't guarantee non-empty slots must filter them out
    /// before being used as a sink.
    EmptySlot,
}

/// Run `codec` over `input` (an iterator of ready-made input chunks),
/// writing the transformed bytes into `output` (an [`OutputSink`] of
/// buffer slots; pass `&mut sink` to keep the sink and its per-slot
/// fill records afterward).
///
/// Returns the total number of bytes written across all slots on
/// success. Every slot is filled completely except the last — the
/// `Codec` contract guarantees any non-empty slot can be filled, so
/// slot sizes need no relation to the codec's internals.
pub fn stream_to_stream<I, B, E, K, C>(
    mut input: I,
    mut codec: C,
    mut output: K,
) -> Result<usize, CopyError<E, K::Error>>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    K: OutputSink,
    C: Codec,
{
    let mut cur_in: Option<(B, usize)> = None;
    let mut inner_eof = false;
    let mut finishing = false;
    let mut total = 0usize;

    loop {
        let need_input = match &cur_in {
            Some((b, pos)) => *pos >= b.as_ref().len(),
            None => true,
        };
        if need_input && !inner_eof && !finishing {
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
            // Called even with a zero remainder (or no slot at all):
            // `Done` vs `OutputFilled` on an empty buffer is exactly
            // how a codec that owes nothing finishes without claiming
            // a slot it has no use for.
            let out_len = output.slot().len();
            match codec.finish(output.slot()).map_err(CopyError::Codec)? {
                Drain::OutputFilled => {
                    output.commit(out_len);
                    total += out_len;
                    if out_len == 0 {
                        // Owed bytes but had nowhere to put them.
                        advance(&mut output)?;
                    }
                }
                Drain::Done { written } => {
                    output.commit(written);
                    return Ok(total + written);
                }
            }
            continue;
        }

        if in_buf.is_empty() {
            // Not at EOF: an empty chunk (or none yet) — loop around
            // and pull the next one without touching the output side.
            continue;
        }

        if output.slot().is_empty() {
            advance(&mut output)?;
        }
        let out_len = output.slot().len();
        match codec.process(in_buf, output.slot()).map_err(CopyError::Codec)? {
            Outcome::InputConsumed { written } => {
                if let Some((b, pos)) = cur_in.as_mut() {
                    *pos = b.as_ref().len();
                }
                output.commit(written);
                total += written;
            }
            Outcome::OutputFilled { consumed } => {
                if let Some((_, pos)) = cur_in.as_mut() {
                    *pos += consumed;
                }
                output.commit(out_len);
                total += out_len;
            }
            Outcome::StreamEnd { consumed: _, written } => {
                // The stream ended in-band; input past its end (the
                // rest of this chunk, and any unpulled chunks) is
                // simply not this stream's to read.
                output.commit(written);
                return Ok(total + written);
            }
        }
    }
}

/// Seal the current slot and make a fresh, non-empty one current,
/// mapping the sink's three failure shapes onto `CopyError`.
fn advance<K, E>(output: &mut K) -> Result<(), CopyError<E, K::Error>>
where
    K: OutputSink,
{
    match output.next_slot() {
        Ok(true) => {
            if output.slot().is_empty() {
                Err(CopyError::EmptySlot)
            } else {
                Ok(())
            }
        }
        Ok(false) => Err(CopyError::OutputExhausted),
        Err(e) => Err(CopyError::Output(e)),
    }
}

#[cfg(test)]
mod tests {
    use super::{stream_to_stream, CopyError, IterSink};
    use crate::base64::{base64_dec, base64_enc};
    use crate::identity::identity;
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Outcome};

    const INPUT: &[u8] = b"Hello, World! 123";

    /// `n`-byte buffer slots for the sink to hand out, each
    /// zero-initialized so leftover bytes beyond what's written are
    /// distinguishable from real output in assertions.
    fn slots(count: usize, size: usize) -> Vec<Vec<u8>> {
        vec![vec![0u8; size]; count]
    }

    fn sink(
        slots: &mut [Vec<u8>],
    ) -> IterSink<impl Iterator<Item = Result<&mut [u8], ()>>, &mut [u8]> {
        IterSink::new(slots.iter_mut().map(|s| Ok(s.as_mut_slice())))
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), rot13(), sink(&mut out))
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 4), rot13(), sink(&mut out))
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 1), rot13(), sink(&mut out))
                .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn empty_input() {
        // No chunks at all: `finish` still has to run to completion.
        let mut out = slots(1, 8);
        let empty: std::iter::Empty<Result<&[u8], ()>> = std::iter::empty();
        let written = stream_to_stream::<_, _, (), _, _>(empty, identity(), sink(&mut out)).unwrap();
        assert_eq!(written, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result = stream_to_stream::<_, _, _, _, _>(chunks.into_iter(), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::Input("source failed"))));
    }

    #[test]
    fn output_exhausted() {
        // Nowhere near enough slots to hold the transformed bytes.
        let mut out = slots(1, 1);
        let result = stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 4), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::OutputExhausted)));
    }

    #[test]
    fn output_error_propagates() {
        // The sink can fail to hand out a slot at all (a pool that's
        // out of buffers, a channel that's closed) — distinct from
        // `OutputExhausted`, which means it ran dry, not that it
        // errored.
        let mut first = vec![0u8; 2];
        let mut second = vec![0u8; 8];
        let slots: Vec<Result<&mut [u8], &'static str>> = vec![
            Ok(first.as_mut_slice()),
            Err("pool exhausted"),
            Ok(second.as_mut_slice()),
        ];
        let result = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            IterSink::new(slots.into_iter()),
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
        let result =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(truncated, 4), base64_dec(), sink(&mut out));
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), base64_enc(), sink(&mut out))
                .unwrap();
        assert_eq!(written, expected.len());
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), base64_enc(), sink(&mut out))
                .unwrap();
        assert_eq!(written, expected.len());
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
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), base64_enc(), sink(&mut out))
                .unwrap();
        assert_eq!(written, expected.len());
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
        let written =
            stream_to_stream::<_, _, (), _, _>(chunks.into_iter(), rot13(), sink(&mut out)).unwrap();
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
        let written = stream_to_stream::<_, _, (), _, _>(
            chunks.into_iter(),
            EarlyEnd { limit: 3, done: 0 },
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, 3);
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
        let written = stream_to_stream::<_, _, (), _, _>(
            input,
            EarlyEnd { limit: 5, done: 0 },
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, 5);
        assert_eq!(pulls.get(), 1);
    }

    #[test]
    fn zero_length_output_slot_is_an_error() {
        // A degenerate slot (e.g. from a pool that handed back an
        // empty buffer) has no room at all. `stream_to_stream` treats
        // this as a sink bug rather than silently skipping past it —
        // a pool that can hand out empty slots must filter them out
        // itself.
        let mut out = vec![vec![0u8; 10], vec![], vec![0u8; 10]];
        let result = stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::EmptySlot)));
    }

    #[test]
    fn input_error_as_first_item() {
        // No successful chunk ever arrives; `stream_to_stream` must report the
        // error without needing to touch the sink.
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> = vec![Err("boom")];
        let result = stream_to_stream::<_, _, _, _, _>(chunks.into_iter(), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }

    #[test]
    fn sink_by_mutable_reference_survives_the_call() {
        // The `&mut T` forwarding impl: pass `&mut sink`, keep the
        // sink — and with it, its per-slot fill records — after the
        // call.
        let mut out = slots(3, 10);
        let mut s = sink(&mut out);
        let written =
            stream_to_stream::<_, _, (), _, _>(ok_chunks(INPUT, 5), base64_enc(), &mut s).unwrap();
        assert_eq!(written, 24);
    }
}
