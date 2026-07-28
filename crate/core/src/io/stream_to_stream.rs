//! Drive a [`Codec`] between an iterator of input chunks and an
//! [`OutputSink`] of output slots, for environments that hand out
//! buffers rather than exposing a `std::io::Read`/`Write` pair — e.g.
//! a message queue delivering already-received packets, or a pool of
//! reusable output buffers.
//!
//! - The input stream is an iterator of ready-made input chunks, each
//!   fallible (`Result<B, E>`) so a fallible source (file reads,
//!   network) can report a failure per-chunk.
//! - The output side is an [`OutputSink`]: a grant/commit supplier of
//!   buffer slots. Unlike a plain iterator of buffers, the sink learns
//!   via [`commit`](OutputSink::commit) how many bytes landed in each
//!   slot — which is what lets the driver *leave a slot's too-small
//!   remainder unfilled* and rotate to a fresh one when the codec's
//!   next atomic output unit (base64: a 4-byte encoded group) doesn't
//!   fit, instead of failing. [`IterSink`] adapts an iterator of
//!   buffers for callers who don't need per-slot accounting.
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
//!
//! # Known limitation: slots smaller than a codec's atomic output unit
//!
//! Slot rotation copes with a too-small *remainder*, but a whole slot
//! smaller than the codec's minimum atomic output can never work — the
//! driver fails with [`CopyError::SlotTooSmall`] rather than burning
//! through the sink's slots retrying. The `#[ignore]`d test below
//! reproduces it (base64 into 1-byte slots). The planned fix is an
//! output-carry inside the codec itself, letting it dribble a unit
//! across calls the way it already buffers partial input groups.

use crate::{Codec, Error, Status};

/// A grant/commit supplier of output buffer slots for
/// [`stream_to_stream`].
///
/// The driver's cycle: [`slot`](Self::slot) to see the writable
/// remainder of the current slot, [`commit`](Self::commit) after
/// writing into its prefix, [`next_slot`](Self::next_slot) when the
/// current slot is full — or when the codec declined its remainder, so
/// a slot's final committed length can be *less* than its capacity
/// mid-stream. The sink is the caller's record of how many bytes are
/// valid in each slot; there is no other channel for that information.
pub trait OutputSink {
    /// Why the sink failed to produce a slot (a pool that's out of
    /// buffers, a channel that's closed).
    type Error;

    /// Seal the current slot — its committed prefix is final, any
    /// unfilled remainder stays unused — and make a fresh slot
    /// current. `Ok(false)` means no more slots will ever come.
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

/// Why [`stream_to_stream`] stopped before the codec reached
/// `StreamEnd`.
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
    /// The codec made no progress against an entire fresh slot — the
    /// sink's slot size is below the codec's minimum atomic output
    /// size (base64 can't emit into fewer than 4 bytes). Rotating
    /// again would burn slots without ever helping, since the sink
    /// offered its full size and it wasn't enough.
    SlotTooSmall,
}

/// Run `codec` over `input` (an iterator of ready-made input chunks),
/// writing the transformed bytes into `output` (an [`OutputSink`] of
/// buffer slots; pass `&mut sink` to keep the sink and its per-slot
/// fill records afterward).
///
/// Returns the total number of bytes written across all slots on
/// success. Slots are filled front-to-back, but a slot may be sealed
/// short of its capacity when the codec's next atomic output unit
/// didn't fit the remainder — per-slot byte counts are the sink's
/// [`commit`](OutputSink::commit) records, not derivable from the
/// total alone.
///
/// A codec's `Error::OutputTooSmall` is handled here, not surfaced:
/// per its contract it means "this buffer can't fit my next atomic
/// unit, give me a bigger one and call again" with nothing consumed or
/// written, so the driver seals the current slot and retries with a
/// fresh one — only if an entire fresh slot still doesn't help does it
/// fail, as [`CopyError::SlotTooSmall`].
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
    let mut done = false;
    // Whether the current slot has taken no bytes since it was
    // granted: the difference between "seal it and retry with a fresh
    // slot" (its remainder was already partly used, a fresh slot has
    // more room) and "a whole fresh slot didn't help" (rotating again
    // can't either — fail).
    let mut fresh = false;
    let mut total = 0usize;

    loop {
        // Checked first and before pulling anything: a call can both
        // deliver final bytes and reach `StreamEnd` in the same turn,
        // so by the next turn `done` may already be true — return
        // right away rather than pulling an input chunk (or a slot)
        // that will never be used. An unused pull here would silently
        // drop a real item the caller handed over.
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

            let out_buf = output.slot();
            let had_room = !out_buf.is_empty();
            let (p, status) = match codec.finish(out_buf) {
                Ok(r) => r,
                Err(Error::OutputTooSmall) => {
                    if fresh {
                        return Err(CopyError::SlotTooSmall);
                    }
                    advance(&mut output)?;
                    fresh = true;
                    continue;
                }
                Err(e) => return Err(CopyError::Codec(e)),
            };
            output.commit(p.written);
            total += p.written;
            if matches!(status, Status::StreamEnd) {
                done = true;
            }

            if p.written > 0 {
                // Loop around: re-check `done` before doing anything
                // else, matching the "can finish and end in the same
                // call" case above.
                fresh = false;
                continue;
            }
            if done {
                return Ok(total);
            }
            // Wrote nothing and didn't end: `finish` is contractually
            // re-callable with a fresh output buffer, so seal this
            // slot (empty, or a remainder the codec declined) and
            // grant a new one — unless this slot already *was* fresh
            // and had room, in which case a bigger slot can't come.
            if fresh && had_room {
                return Err(CopyError::SlotTooSmall);
            }
            advance(&mut output)?;
            fresh = true;
            continue;
        }

        if in_buf.is_empty() {
            // Not at EOF: wait for more input. Loop around without
            // touching the output side — claiming a slot here would
            // take one the codec doesn't need yet (e.g. a zero-length
            // chunk sitting between two real ones).
            continue;
        }

        let out_buf = output.slot();
        if out_buf.is_empty() {
            advance(&mut output)?;
            fresh = true;
            continue;
        }
        let (p, status) = match codec.process(in_buf, out_buf) {
            Ok(r) => r,
            Err(Error::OutputTooSmall) => {
                if fresh {
                    return Err(CopyError::SlotTooSmall);
                }
                advance(&mut output)?;
                fresh = true;
                continue;
            }
            Err(e) => return Err(CopyError::Codec(e)),
        };
        if let Some((_, pos)) = cur_in.as_mut() {
            *pos += p.consumed;
        }
        output.commit(p.written);
        total += p.written;
        if p.written > 0 {
            fresh = false;
        }
        if matches!(status, Status::StreamEnd) {
            done = true;
        }
        // Zero-*written*, not-`StreamEnd` turns (ordinary internal
        // buffering, e.g. a partial base64 quad) aren't a stall on
        // their own — `p.consumed` moving is real progress, so loop
        // around and let the top pull more input. Zero on *both* sides
        // is the well-mannered form of `Err(OutputTooSmall)` (a codec
        // reporting `OutputFull` instead of erroring), and gets the
        // same treatment: rotate once, fail if the slot was fresh.
        if p.consumed == 0 && p.written == 0 && !done {
            if fresh {
                return Err(CopyError::SlotTooSmall);
            }
            advance(&mut output)?;
            fresh = true;
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

    const INPUT: &[u8] = b"Hello, World! 123";

    /// `n`-byte buffer slots for the sink to hand out, each
    /// zero-initialized so leftover bytes beyond what's written are
    /// distinguishable from real output in assertions.
    fn slots(count: usize, size: usize) -> Vec<Vec<u8>> {
        vec![vec![0u8; size]; count]
    }

    fn sink(slots: &mut [Vec<u8>]) -> IterSink<impl Iterator<Item = Result<&mut [u8], ()>>, &mut [u8]> {
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
    /// `Status::OutputFull` a less careful implementation might, so
    /// the tests below exercise `stream_to_stream`'s own handling of
    /// both forms.
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
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            sink(&mut out),
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
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            sink(&mut out),
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
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 1),
            rot13(),
            sink(&mut out),
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
            stream_to_stream::<_, _, (), _, _>(empty, identity(), sink(&mut out)).unwrap();
        assert_eq!(written, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result =
            stream_to_stream::<_, _, _, _, _>(chunks.into_iter(), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::Input("source failed"))));
    }

    #[test]
    fn output_exhausted() {
        // Nowhere near enough slots to hold the transformed bytes.
        let mut out = slots(1, 1);
        let result = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 4),
            rot13(),
            sink(&mut out),
        );
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
        // rather than reaching `StreamEnd` (see
        // `decode_truncated_padded_stream_errors` in base64.rs).
        let encoded = to_vec(base64_enc(), INPUT).unwrap();
        let truncated = &encoded[..encoded.len() - 2];
        let mut out = slots(4, 8);
        let result = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(truncated, 4),
            base64_dec(),
            sink(&mut out),
        );
        assert!(matches!(result, Err(CopyError::Codec(_))));
    }

    #[test]
    fn base64_slots_not_multiple_of_encoded_group() {
        // 10-byte slots against base64's 4-byte encoded groups: after
        // two groups (8 bytes) land in a slot, its 2-byte remainder
        // can't fit a third. The driver seals the slot short and
        // rotates to a fresh one instead of failing — the fix the
        // OutputSink trait exists for. The sealed slot keeps its
        // 8-byte prefix; the untouched remainder stays zeroed.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        assert_eq!(expected.len(), 24);
        let mut out = slots(6, 10);
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&out[0][..8], &expected[..8]);
        assert_eq!(&out[0][8..], &[0, 0]);
        assert_eq!(&out[1][..8], &expected[8..16]);
    }

    #[test]
    fn finish_rotates_to_a_fresh_slot() {
        // 17 input bytes = 5 whole base64 groups through `process` (20
        // encoded bytes) plus 2 leftover bytes that only `finish` can
        // emit, as one final 4-byte padded group. The first slot has
        // room for the 20 but only 2 bytes of remainder for the final
        // group, so `finish` reports no progress there and the driver
        // must rotate mid-finish — the previously untested
        // finish-writes-bytes and finish-rotation paths.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        let mut out = slots(2, 22);
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
        assert_eq!(&out[0][..20], &expected[..20]);
        assert_eq!(&out[0][20..], &[0, 0]);
        assert_eq!(&out[1][..4], &expected[20..]);
    }

    #[test]
    #[ignore = "known limitation: slots smaller than base64's 4-byte encoded group fail; needs an output-carry in the codec"]
    fn base64_slots_smaller_than_encoded_group() {
        // 1-byte slots are below the codec's atomic unit, so not even
        // slot rotation can help — the codec itself must learn to
        // dribble a unit across calls. See the module-doc "Known
        // limitation" section.
        let expected = to_vec(base64_enc(), INPUT).unwrap();
        let mut out = slots(64, 1);
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn whole_slot_below_atomic_unit_errors_instead_of_looping() {
        // `MinOutputUnit` needs 4 bytes of output room to write
        // anything; every slot here is 1 byte, so an unguarded driver
        // would either call `process` forever or burn all the sink's
        // slots rotating. `stream_to_stream` rotates at most once onto
        // a fresh slot, then reports the slot size as the problem.
        let mut out = slots(4, 1);
        let result = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 4),
            MinOutputUnit,
            sink(&mut out),
        );
        assert!(matches!(result, Err(CopyError::SlotTooSmall)));
    }

    #[test]
    fn ok_stall_rotates_like_output_too_small() {
        // The `Ok((0-consumed, 0-written), OutputFull)` form of "my
        // unit doesn't fit": 6-byte slots leave a 2-byte remainder
        // after `MinOutputUnit` fills 4, which it declines without
        // erroring. The driver must treat that exactly like
        // `Err(OutputTooSmall)`: seal the slot short, rotate, carry
        // on to a complete copy.
        let mut out = slots(6, 6);
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 4),
            MinOutputUnit,
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, INPUT.len());
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
            stream_to_stream::<_, _, (), _, _>(chunks.into_iter(), rot13(), sink(&mut out))
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
        let written = stream_to_stream::<_, _, (), _, _>(
            chunks.into_iter(),
            EarlyEnd { limit: 3, done: 0 },
            sink(&mut out),
        )
        .unwrap();
        assert_eq!(written, 3);
    }

    #[test]
    fn done_does_not_pull_an_extra_input_chunk() {
        // `EarlyEnd`'s limit (5) lands exactly on the first chunk's
        // length, so the write and the driver's `done` latch both
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
        let result = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            rot13(),
            sink(&mut out),
        );
        assert!(matches!(result, Err(CopyError::EmptySlot)));
    }

    #[test]
    fn input_error_as_first_item() {
        // No successful chunk ever arrives; `stream_to_stream` must report the
        // error without needing to touch the sink.
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> = vec![Err("boom")];
        let result =
            stream_to_stream::<_, _, _, _, _>(chunks.into_iter(), rot13(), sink(&mut out));
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }

    #[test]
    fn sink_by_mutable_reference_survives_the_call() {
        // The `&mut T` forwarding impl: pass `&mut sink`, keep the
        // sink — and with it, its per-slot fill records — after the
        // call. The 10-byte-slot rotation from
        // `base64_slots_not_multiple_of_encoded_group` makes the
        // records non-trivial: 8 bytes in each sealed slot, not 10.
        let mut out = slots(6, 10);
        let mut s = sink(&mut out);
        let written = stream_to_stream::<_, _, (), _, _>(
            ok_chunks(INPUT, 5),
            base64_enc(),
            &mut s,
        )
        .unwrap();
        assert_eq!(written, 24);
    }
}
