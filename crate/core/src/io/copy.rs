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

use crate::Codec;

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
pub fn copy<I, B, E, O, S, C>(input: I, codec: C, output: O) -> Result<usize, CopyError<E>>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
    O: Iterator<Item = S>,
    S: AsMut<[u8]>,
    C: Codec,
{
    let _ = (input, codec, output);
    // Stub: not yet implemented. Deliberately wrong (rather than
    // `todo!()`) so the tests below fail as real assertion mismatches,
    // exercising their own logic instead of short-circuiting on a panic.
    Ok(0)
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
}
