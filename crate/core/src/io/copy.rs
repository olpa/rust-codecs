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
//!   plan uses for `Chain`'s staging buffer. [`copy`] fills each slot as
//!   full as the codec allows before moving to the next one.
//!
//! This mirrors [`CodecReader`](super::CodecReader)/
//! [`CodecWriter`](super::CodecWriter), which drive the same `Codec`
//! trait over `std::io::Read`/`Write` instead.

use crate::{Codec, Status};

/// Why [`copy`] stopped before the codec reached `StreamEnd`.
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
/// success. Every slot `copy` uses is filled completely except possibly
/// the last one — the caller derives how many of its bytes are valid
/// from the returned total and the sizes of the slots it handed out.
pub fn copy<I, B, E, O, S, C>(
    mut input: I,
    mut codec: C,
    mut output: O,
) -> Result<usize, CopyError<E>>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    O: Iterator<Item = S>,
    S: AsMut<[u8]>,
    C: Codec,
{
    let mut cur_in: Option<(B, usize)> = None;
    let mut cur_out: Option<(S, usize)> = None;
    let mut finishing = false;
    let mut total = 0usize;

    loop {
        // The slice the codec can write into this turn: the unused tail
        // of the current output slot, or empty if there is no current
        // slot (or it's fully used) — a fresh slot is only pulled once
        // the codec proves it can't make progress without one, so a
        // codec that needs no more room (e.g. `finish` on a stateless
        // codec) never forces a slot it doesn't need.
        let out_buf: &mut [u8] = match &mut cur_out {
            Some((s, pos)) => &mut s.as_mut()[*pos..],
            None => &mut [],
        };

        let (progress, status) = if finishing {
            codec.finish(out_buf).map_err(CopyError::Codec)?
        } else {
            let need_input = match &cur_in {
                Some((b, pos)) => *pos >= b.as_ref().len(),
                None => true,
            };
            if need_input {
                match input.next() {
                    Some(Ok(b)) => cur_in = Some((b, 0)),
                    Some(Err(e)) => return Err(CopyError::Input(e)),
                    None => {
                        finishing = true;
                        continue;
                    }
                }
            }
            let in_buf: &[u8] = match &cur_in {
                Some((b, pos)) => &b.as_ref()[*pos..],
                None => unreachable!("just ensured cur_in is populated"),
            };
            let (progress, status) = codec.process(in_buf, out_buf).map_err(CopyError::Codec)?;
            if let Some((_, pos)) = cur_in.as_mut() {
                *pos += progress.consumed;
            }
            (progress, status)
        };

        if let Some((_, pos)) = cur_out.as_mut() {
            *pos += progress.written;
        }
        total += progress.written;

        if matches!(status, Status::StreamEnd) {
            return Ok(total);
        }
        if progress.written == 0 {
            // The current slot (if any) couldn't take another byte —
            // the codec made zero progress with the room it had, so
            // only a fresh slot can move things forward.
            match output.next() {
                Some(s) => cur_out = Some((s, 0)),
                None => return Err(CopyError::OutputExhausted),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{copy, CopyError};
    use crate::io::to_vec;
    use crate::rot13::rot13;
    use crate::identity::identity;
    use crate::base64::{base64_dec, base64_enc};

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
    /// caller's slice (or in further chunks `copy` hasn't pulled yet).
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
            Ok((crate::Progress { consumed: n, written: n }, status))
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
        let written =
            copy::<_, _, (), _, _, _>(ok_chunks(INPUT, 5), rot13(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn small_output_buffers() {
        // 1-byte slots force copy to advance to the next slot after
        // every single byte written, mirroring
        // `reader_with_small_output_buffer` in rot13.rs.
        let expected = to_vec(rot13(), INPUT).unwrap();
        let mut out = slots(expected.len(), 1);
        let written =
            copy::<_, _, (), _, _, _>(ok_chunks(INPUT, 4), rot13(), slot_iter(&mut out)).unwrap();
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
            copy::<_, _, (), _, _, _>(ok_chunks(INPUT, 1), rot13(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn empty_input() {
        // No chunks at all: `finish` still has to run to completion.
        let mut out = slots(1, 8);
        let empty: std::iter::Empty<Result<&[u8], ()>> = std::iter::empty();
        let written = copy::<_, _, (), _, _, _>(empty, identity(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, 0);
    }

    #[test]
    fn input_error_propagates() {
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> =
            vec![Ok(&INPUT[..4]), Err("source failed"), Ok(&INPUT[4..])];
        let result = copy::<_, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("source failed"))));
    }

    #[test]
    fn output_exhausted() {
        // Nowhere near enough slots to hold the transformed bytes.
        let mut out = slots(1, 1);
        let result =
            copy::<_, _, (), _, _, _>(ok_chunks(INPUT, 4), rot13(), slot_iter(&mut out));
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
        let result =
            copy::<_, _, (), _, _, _>(ok_chunks(truncated, 4), base64_dec(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Codec(_))));
    }

    #[test]
    fn empty_input_chunk_does_not_consume_an_output_slot() {
        // A zero-length chunk from the input iterator makes `process`
        // report `InputEmpty` with zero bytes written — the same
        // "wrote nothing" signal `copy` otherwise treats as "the
        // output slot is the bottleneck, fetch a new one". It must
        // not: there's plenty of room left in the single slot below,
        // and a second real chunk still to come. Fetching an unneeded
        // slot here starves `copy` of the one it actually needs later
        // and errors as `OutputExhausted` even though nothing ever
        // ran out.
        let chunks: Vec<Result<&[u8], ()>> =
            vec![Ok(&INPUT[..4]), Ok(&[]), Ok(&INPUT[4..8])];
        let mut out = slots(1, 100);
        let written = copy::<_, _, (), _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out))
            .unwrap();
        assert_eq!(written, 8);
    }

    #[test]
    fn early_stream_end_ignores_remaining_input() {
        // `EarlyEnd` reports `StreamEnd` from `process` itself after 3
        // bytes, with a whole second chunk left unpulled. `copy` must
        // stop right there rather than erroring or trying to drain the
        // rest of the input.
        let chunks: Vec<Result<&[u8], ()>> = vec![Ok(b"Hello"), Ok(b"World")];
        let mut out = slots(4, 8);
        let written = copy::<_, _, (), _, _, _>(
            chunks.into_iter(),
            EarlyEnd { limit: 3, done: 0 },
            slot_iter(&mut out),
        )
        .unwrap();
        assert_eq!(written, 3);
    }

    #[test]
    fn zero_length_output_slot_is_skipped() {
        // A degenerate slot (e.g. from a pool that handed back an
        // empty buffer) has no room at all; `copy` must move past it
        // to the next slot rather than getting stuck reporting
        // `OutputExhausted` or corrupting later slots.
        let mut out = vec![vec![0u8; 10], vec![], vec![0u8; 10]];
        let expected = to_vec(rot13(), INPUT).unwrap();
        let written =
            copy::<_, _, (), _, _, _>(ok_chunks(INPUT, 5), rot13(), slot_iter(&mut out)).unwrap();
        assert_eq!(written, expected.len());
    }

    #[test]
    fn input_error_as_first_item() {
        // No successful chunk ever arrives; `copy` must report the
        // error without needing to touch the output iterator.
        let mut out = slots(4, 8);
        let chunks: Vec<Result<&[u8], &'static str>> = vec![Err("boom")];
        let result = copy::<_, _, _, _, _, _>(chunks.into_iter(), rot13(), slot_iter(&mut out));
        assert!(matches!(result, Err(CopyError::Input("boom"))));
    }
}
