//! `std::io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! - [`CodecReader`]: wraps a `Read`, runs the transform on the fly, and
//!   is itself a `Read` yielding the transformed bytes.
//! - [`CodecWriter`]: wraps a `Write`; bytes written to it are
//!   transformed on the fly before reaching the wrapped writer.
//!
//! Both use the same adapter-pumping loops as `stream_to_stream`; they
//! retain only the lifecycle policy imposed by `Read` or `Write`. The reader owns input
//! scratch and writes directly into its caller's output; the writer
//! reads directly from its caller and owns output scratch. Both take a
//! caller-provided scratch buffer
//! (`S: AsMut<[u8]>` — same convention as [`Chain`](crate::Chain))
//! rather than allocating one internally: batching policy already has
//! a canonical, composable expression in `BufReader`/`BufWriter`
//! placement in the client's own stack, so a knob inside these
//! adapters would just duplicate that.

use std::io::{self, Read, Write};

use core::convert::Infallible;

use crate::driver::{Driver, PumpEnd};
use crate::sources_and_sinks::slice::{SliceSource, SliceSink};
use crate::{Codec, DriveError, Error, Sink};

use super::bridge::{StdSource, StdSink};

fn to_io_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

fn reader_error(err: DriveError<io::Error, Infallible>) -> io::Error {
    match err {
        DriveError::Source(error) => error,
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => {
            io::Error::new(io::ErrorKind::InvalidData, "invalid slice output adapter")
        }
    }
}

fn writer_error(err: DriveError<Infallible, io::Error>) -> io::Error {
    match err {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => error,
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => {
            io::Error::new(io::ErrorKind::InvalidData, "invalid std output adapter")
        }
    }
}

fn slice_error(err: DriveError<Infallible, Infallible>) -> io::Error {
    match err {
        DriveError::Source(never) | DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => {
            io::Error::new(io::ErrorKind::InvalidData, "invalid slice output adapter")
        }
    }
}

/// Wraps a `Read`, running `C` over the bytes as they're pulled through.
///
/// End-of-stream: when the wrapped reader hits EOF, the codec's
/// `finish` runs (trailer, padding) and its bytes are yielded before
/// this reader reports EOF itself — the caller never calls `finish`
/// explicitly. If the codec ends its stream in-band before the input
/// does, trailing input bytes already pulled from the wrapped reader
/// are lost.
pub struct CodecReader<R, C: Codec, S> {
    input: StdSource<R, S>,
    driver: Driver<C>,
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `inbuf`: it could never hold a byte read
    /// from `inner`, so the codec could never see any input — a caller
    /// bug, not a runtime condition.
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self { input: StdSource::new(inner, inbuf), driver: Driver::new(codec) }
    }

    /// Unwrap this reader, discarding the codec, and return the wrapped
    /// reader. Any bytes already pulled from it but not yet yielded to
    /// the caller are lost.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.driver.is_done() {
            return Ok(0);
        }
        let mut output = SliceSink::new(buf);
        let moved = self.driver.transfer_from(&mut self.input, &mut output).map_err(reader_error)?;
        if moved.end == PumpEnd::SourceExhausted {
            self.driver.finish_to(&mut output).map_err(slice_error)?;
        }
        Ok(output.written())
    }
}

/// Wraps a `Write`; bytes written to this adapter are run through `C`
/// before being written to the wrapped writer.
pub struct CodecWriter<W, C: Codec, S> {
    output: StdSink<W, S>,
    driver: Driver<C>,
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
        Self { output: StdSink::new(inner, outbuf), driver: Driver::new(codec) }
    }

    /// Flush any bytes the codec was still holding, finalize the stream
    /// (trailer, checksum, padding — for a stateful codec), and hand back
    /// ownership of the wrapped writer.
    pub fn finish(mut self) -> io::Result<W> {
        self.driver.finish_to(&mut self.output).map_err(writer_error)?;
        self.output.finish()?;
        Ok(self.output.into_inner())
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut input = SliceSource::new(buf);
        self.driver.transfer_from(&mut input, &mut self.output).map_err(writer_error)?;
        Ok(input.consumed())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.driver.flush_to(&mut self.output).map_err(writer_error)?;
        self.output.finish()
    }
}

#[cfg(all(test, feature = "rot13"))]
mod tests {
    use std::io::{Cursor, Read};

    use super::{CodecReader, CodecWriter};
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Progress};

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
    /// a self-describing format with an in-band terminator.
    struct EarlyEnd {
        limit: usize,
        done: usize,
    }

    impl Codec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Progress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Progress::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Progress::InputConsumed { written: n })
            } else {
                Ok(Progress::OutputFilled { consumed: n })
            }
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn reader_stops_at_in_band_stream_end() {
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
