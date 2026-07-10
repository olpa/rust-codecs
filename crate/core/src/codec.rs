//! The [`Codec`] trait: a stateful byte-rewrite transform.

use compcol::{Error, Progress, Status};

/// A stateful byte-rewrite transform. See `CREATING-CODECS.md` for how to
/// write one.
pub trait Codec {
    /// Push input bytes and pull output bytes. `Status` reports which
    /// buffer ran out first (`InputEmpty`, `OutputFull`) or that the
    /// stream ended (`StreamEnd`).
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error>;

    /// Signal "no more input is coming": flush any buffered state and,
    /// for formats with one, write the trailer/checksum. Call repeatedly
    /// with a fresh output buffer until it reports `StreamEnd`.
    fn finish(&mut self, output: &mut [u8]) -> Result<(Progress, Status), Error>;

    /// Drain any bytes this codec is withholding to a sync boundary,
    /// *without* ending the stream — unlike `finish`, this never reports
    /// `StreamEnd`. Only meaningful for codecs that buffer output for a
    /// format-defined in-band sync marker (deflate/zlib/gzip do); the
    /// default is a no-op.
    fn flush(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::InputEmpty))
    }
}
