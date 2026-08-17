//! `std::io::Read`/`Write` wrappers over a [`Codec`](crate::Codec) or
//! [`EndCapableCodec`](crate::EndCapableCodec).
//!
//! - [`CodecReader`]: wraps a `Read`, runs the transform on the fly, and
//!   is itself a `Read` yielding the transformed bytes.
//! - [`BufReadCodecReader`]: the same, for an `R: std::io::BufRead` —
//!   no scratch buffer, since it lends straight out of `R`'s own buffer.
//! - [`CodecWriter`]: wraps a `Write`; bytes written to it are
//!   transformed on the fly before reaching the wrapped writer.
//!
//! Both use the same pump loops as `stream_to_stream`; they
//! retain only the lifecycle policy imposed by `Read` or `Write`. The reader owns input
//! scratch and writes directly into its caller's output; the writer
//! reads directly from its caller and owns output scratch. Both take a
//! caller-provided scratch buffer
//! (`S: AsMut<[u8]>` — same convention as [`Chain`](crate::Chain))
//! rather than allocating one internally: batching policy already has
//! a canonical, composable expression in `BufReader`/`BufWriter`
//! placement in the client's own stack, so a knob inside these
//! wrappers would just duplicate that.

use std::io::{self, BufRead, Read, Write};

use core::convert::Infallible;

use crate::pump::Pump;
use crate::sources_and_sinks::shared_io::{pump_finish, pump_flush, pump_read, pump_write};
use crate::{Codec, DriveError, Error, ErrorKind, EndCapableCodec};

use super::adapter::{BufReadSource, StdSource, StdSink};

fn to_io_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

/// `SinkExhausted`/`NoProgress` never carry endpoint data of their own
/// (`DriveError`'s two data-less variants) — they mean the pump/codec
/// pairing itself is broken, the same class of failure the crate's own
/// [`ErrorKind::ContractViolation`] already names, so both route
/// through `to_io_error` like a codec error would.
fn adapter_contract_violation() -> io::Error {
    to_io_error(Error::new(ErrorKind::ContractViolation, 0, 0))
}

fn reader_error(err: DriveError<io::Error, Infallible>) -> io::Error {
    match err {
        DriveError::Source(error) => error,
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

fn writer_error(err: DriveError<Infallible, io::Error>) -> io::Error {
    match err {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => error,
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

/// Wraps a `Read`, running `C` over the bytes as they're pulled through.
///
/// `C` may be an ordinary [`Codec`] or a [`EndCapableCodec`] — every
/// `Codec` is automatically a `EndCapableCodec` that never ends
/// in-band.
///
/// End-of-stream: when the wrapped reader hits EOF, the codec's
/// `finish` runs (trailer, padding) and its bytes are yielded before
/// this reader reports EOF itself — the caller never calls `finish`
/// explicitly. If the codec ends its stream in-band before the input
/// does, this reader yields exactly the bytes produced up to that
/// point and then reports EOF itself, without touching the codec
/// again; trailing input bytes already pulled from the wrapped reader
/// are lost.
pub struct CodecReader<R, C: EndCapableCodec, S> {
    input: StdSource<R, S>,
    pump: Pump<C>,
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `inbuf`: it could never hold a byte read
    /// from `inner`, so the codec could never see any input — a caller
    /// bug, not a runtime condition.
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self { input: StdSource::new(inner, inbuf), pump: Pump::new(codec) }
    }

    /// Unwrap this reader, discarding the codec, and return the wrapped
    /// reader. Any bytes already pulled from it but not yet yielded to
    /// the caller are lost.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Reclaim the reader, the codec, and the scratch buffer — e.g. to
    /// read state the codec holds (a checksum, a digest), or to reuse
    /// the buffer's allocation for another `CodecReader`. `into_inner`
    /// discards all three; this is the exhaustive teardown. Same
    /// caveat as `into_inner`: bytes already pulled from the reader
    /// but not yet yielded to the caller are lost.
    pub fn into_parts(self) -> (R, C, S) {
        let (inner, buffer) = self.input.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }

    /// Direct access to the wrapped reader, bypassing the codec.
    /// Bytes already pulled from it into this reader's scratch buffer,
    /// but not yet yielded to the caller, aren't visible here.
    pub fn get_ref(&self) -> &R {
        self.input.get_ref()
    }

    /// Mutable counterpart to [`CodecReader::get_ref`].
    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }

    /// Direct access to the codec — e.g. to read state a
    /// `EndCapableCodec` call doesn't expose (a checksum, a digest)
    /// once its stream has ended in-band.
    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    /// Mutable counterpart to [`CodecReader::codec_ref`].
    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        pump_read(&mut self.pump, &mut self.input, buf).map_err(reader_error)
    }
}

/// Like [`CodecReader`], but for an `R` that already implements
/// `std::io::BufRead` — no caller-provided scratch buffer, since
/// [`BufReadSource`] lends straight out of `R`'s own buffer instead of
/// copying into one of its own. Same end-of-stream behavior as
/// `CodecReader`.
pub struct BufReadCodecReader<R, C: EndCapableCodec> {
    input: BufReadSource<R>,
    pump: Pump<C>,
}

impl<R: BufRead, C: EndCapableCodec> BufReadCodecReader<R, C> {
    pub fn new(inner: R, codec: C) -> Self {
        Self { input: BufReadSource::new(inner), pump: Pump::new(codec) }
    }

    /// Unwrap this reader, discarding the codec, and return the wrapped
    /// reader. Any bytes already buffered by `inner` but not yet
    /// yielded to the caller are still there — unlike `CodecReader`,
    /// nothing was copied out of `inner`'s own buffer.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Reclaim the reader and the codec — e.g. to read state the codec
    /// holds (a checksum, a digest). Same caveat as `into_inner`.
    pub fn into_parts(self) -> (R, C) {
        (self.input.into_inner(), self.pump.into_inner())
    }

    /// Direct access to the wrapped reader, bypassing the codec.
    pub fn get_ref(&self) -> &R {
        self.input.get_ref()
    }

    /// Mutable counterpart to [`BufReadCodecReader::get_ref`].
    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }

    /// Direct access to the codec — e.g. to read state a
    /// `EndCapableCodec` call doesn't expose (a checksum, a digest)
    /// once its stream has ended in-band.
    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    /// Mutable counterpart to [`BufReadCodecReader::codec_ref`].
    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }
}

impl<R: BufRead, C: EndCapableCodec> Read for BufReadCodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        pump_read(&mut self.pump, &mut self.input, buf).map_err(reader_error)
    }
}

/// Wraps a `Write`; bytes written to this wrapper are run through `C`
/// before being written to the wrapped writer.
///
/// `C` is bound to [`Codec`], not [`EndCapableCodec`]: an in-band end
/// would otherwise become a permanent short write from `write`, which
/// `write_all`/`io::copy` would then turn into `ErrorKind::WriteZero`.
/// The caller must explicitly call [`CodecWriter::finish`] to finalize
/// the codec; `Write::flush` is a resumable synchronization point, not
/// a substitute for it.
///
/// # Don't forget to call `finish`
///
/// Dropping a `CodecWriter` without calling `finish` silently drops
/// any trailer/padding/checksum bytes the codec was still holding —
/// there is no compiler warning or runtime error, only truncated
/// output discovered later, on decode. This is the same footgun as
/// forgetting `std::io::BufWriter::flush`/`into_inner` or
/// `flate2::write::GzEncoder::finish`: make sure `finish` runs on
/// every path out of scope, including error paths.
pub struct CodecWriter<W, C: Codec, S> {
    output: StdSink<W, S>,
    pump: Pump<C>,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    /// Build a `CodecWriter`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `outbuf`, for the same reason
    /// [`CodecReader::new`] does: it could never hold a byte for
    /// `inner` to receive.
    pub fn new(inner: W, codec: C, outbuf: S) -> Self {
        Self { output: StdSink::new(inner, outbuf), pump: Pump::new(codec) }
    }

    /// Direct access to the wrapped writer, bypassing the codec — e.g.
    /// to interleave raw framing bytes with codec output. Safe to use
    /// any time the codec has no output still owed from a prior
    /// `write`/`flush` (fresh, or right after `flush`/`finish`);
    /// writing here while the codec is mid-unit reorders bytes ahead of
    /// whatever it's still holding.
    pub fn get_ref(&self) -> &W {
        self.output.get_ref()
    }

    /// Mutable counterpart to [`CodecWriter::get_ref`].
    pub fn get_mut(&mut self) -> &mut W {
        self.output.get_mut()
    }

    /// Direct access to the codec — e.g. to read state a `Codec` call
    /// doesn't expose (a checksum, a digest) before calling
    /// [`CodecWriter::finish`].
    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    /// Mutable counterpart to [`CodecWriter::codec_ref`].
    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }

    /// Flush any bytes the codec was still holding, finalize the stream
    /// (trailer, checksum, padding — for a stateful codec), and hand back
    /// ownership of the wrapped writer.
    pub fn finish(mut self) -> io::Result<W> {
        pump_finish(&mut self.pump, &mut self.output).map_err(writer_error)?;
        Ok(self.output.into_inner())
    }

    /// Reclaim the writer, the codec, and the scratch buffer without
    /// finishing the codec stream — e.g. to read state the codec holds
    /// (a checksum, a digest) after an error, or to reuse the buffer's
    /// allocation for another `CodecWriter`. Same caveat as dropping
    /// without calling `finish`: any trailer/padding/checksum bytes the
    /// codec was still holding are discarded, and any output already
    /// staged in the buffer via a prior `write`/`flush` but not yet
    /// written to the wrapped writer is discarded too.
    pub fn into_parts(self) -> (W, C, S) {
        let (inner, buffer) = self.output.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        pump_write(&mut self.pump, &mut self.output, buf).map_err(writer_error)
    }

    fn flush(&mut self) -> io::Result<()> {
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error)?;
        self.output.get_mut().flush()
    }
}

#[cfg(all(test, feature = "rot13"))]
mod tests {
    use std::io::{Cursor, Read};

    use super::{BufReadCodecReader, CodecReader, CodecWriter};
    use crate::rot13::rot13;
    use crate::{Drain, DrainCodec, Error, EndCapableCodec, EndCapableProgress};

    #[test]
    fn buf_read_codec_reader_needs_no_scratch_buffer() {
        let mut reader = BufReadCodecReader::new(Cursor::new(b"Uryyb, Jbeyq!".as_slice()), rot13());
        let mut out = String::new();
        reader.read_to_string(&mut out).unwrap();
        assert_eq!(out, "Hello, World!");
    }

    #[test]
    #[should_panic(expected = "buffer must be non-empty")]
    fn codec_reader_rejects_empty_buffer() {
        let _ = CodecReader::new(Cursor::new(b"hi".as_slice()), rot13(), Vec::<u8>::new());
    }

    #[test]
    #[should_panic(expected = "buffer must be non-empty")]
    fn codec_writer_rejects_empty_buffer() {
        let _ = CodecWriter::new(Vec::<u8>::new(), rot13(), Vec::<u8>::new());
    }

    /// Copies bytes 1:1 but ends its stream after `limit` bytes, like
    /// a self-describing format with an in-band terminator. A genuine
    /// `EndCapableCodec` (not `Codec` — a `Codec` can never report an
    /// in-band end), so only `CodecReader` (bound to `EndCapableCodec`)
    /// can drive it; `CodecWriter` is bound to `Codec` and rejects it
    /// at compile time.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl DrainCodec for EarlyEnd {
        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    impl EndCapableCodec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<EndCapableProgress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(EndCapableProgress::End { consumed: n, written: n })
            } else if n == input.len() {
                Ok(EndCapableProgress::InputConsumed { written: n })
            } else {
                Ok(EndCapableProgress::OutputFilled { consumed: n })
            }
        }
    }

    #[test]
    fn reader_stops_at_in_band_end() {
        // The codec ends after 3 bytes; the reader must yield exactly
        // those and then report EOF on every later call, without
        // touching the codec again.
        let mut reader =
            CodecReader::new(Cursor::new(b"Hello World".as_slice()), EarlyEnd { limit: 3, done: 0 }, vec![0u8; 8]);
        let mut out = Vec::new();
        reader.read_to_end(&mut out).unwrap();
        assert_eq!(out, b"Hel");
        let mut buf = [0u8; 4];
        assert_eq!(reader.read(&mut buf).unwrap(), 0);
    }
}
