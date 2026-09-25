use std::io::{self, BufRead, Read, Write};

use core::convert::Infallible;

use crate::sources_and_sinks::shared_io::{
    boundary_aware_pump_read, pump_finish, pump_flush, pump_write,
};
use crate::stream::Pump;
use crate::{BoundaryAwareCodec, Codec, DriveError, EmptyBufferError, Error, ErrorKind};

use super::adapter::{BufReadSource, StdSink, StdSource};

fn to_io_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

fn adapter_contract_violation() -> io::Error {
    to_io_error(Error::new(ErrorKind::ByteCountClaim, 0, 0))
}

fn reader_error_to_io_error(err: DriveError<io::Error, Infallible>) -> io::Error {
    match err {
        DriveError::Source(error) => error,
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

fn writer_error_to_io_error(err: DriveError<Infallible, io::Error>) -> io::Error {
    match err {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => error,
        DriveError::Codec(error) => to_io_error(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

/// Wraps a `Read`, running `C` over the bytes as they're pulled through.
///
/// End-of-stream: when the wrapped reader hits EOF, the reader
/// runs codec's `finish` (trailer, padding) and its bytes are
/// yielded before this reader reports EOF itself.
///
/// End-of-codec: for an end-signalling codec that ends its stream before
/// the input does, the reader yields exactly the bytes produced up
/// to that point and then reports EOF itself:
///
/// - `finish` is not called: a codec that ends its own stream
///   is assumed to have already taken care of its own finalization
///   before reporting [`BoundaryAwareProgress::Boundary`](crate::BoundaryAwareProgress::Boundary).
/// - Trailing input bytes already pulled from the wrapped reader are
///   not yielded as output; retrieve them with [`CodecReader::pending`]
///   before dropping the reader.
pub struct CodecReader<R, C: BoundaryAwareCodec, S> {
    input: StdSource<R, S>,
    pump: Pump<C>,
}

impl<R: Read, C: BoundaryAwareCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `inbuf`.
    pub fn new(inner: R, codec: C, inbuf: S) -> Result<Self, EmptyBufferError> {
        Ok(Self {
            input: StdSource::new(inner, inbuf)?,
            pump: Pump::new(codec),
        })
    }

    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Reclaim the reader, the codec, and the scratch buffer. If the
    /// buffer holds unconsumed bytes, treat them as lost.
    pub fn into_parts(self) -> (R, C, S) {
        let (inner, buffer) = self.input.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }

    pub fn get_ref(&self) -> &R {
        self.input.get_ref()
    }

    /// The unconsumed bytes already pulled from the reader into the
    /// scratch buffer, but not yet yielded to the caller.
    pub fn pending(&mut self) -> &[u8] {
        self.input.pending()
    }

    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }

    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }
}

impl<R: Read, C: BoundaryAwareCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_io_error)
    }
}

/// Like [`CodecReader`], but for an `R: std::io::BufRead`, using the
/// `BufRead`'s buffer directly instead of a caller-provided scratch
/// buffer. Same end-of-stream and end-of-codec behavior as
/// `CodecReader`.
pub struct BufReadCodecReader<R, C: BoundaryAwareCodec> {
    input: BufReadSource<R>,
    pump: Pump<C>,
}

impl<R: BufRead, C: BoundaryAwareCodec> BufReadCodecReader<R, C> {
    /// Build a `BufReadCodecReader`.
    pub fn new(inner: R, codec: C) -> Self {
        Self {
            input: BufReadSource::new(inner),
            pump: Pump::new(codec),
        }
    }

    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Reclaim the reader and the codec.
    pub fn into_parts(self) -> (R, C) {
        (self.input.into_inner(), self.pump.into_inner())
    }

    pub fn get_ref(&self) -> &R {
        self.input.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }

    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }
}

impl<R: BufRead, C: BoundaryAwareCodec> Read for BufReadCodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_io_error)
    }
}

/// Wraps a `Write`; bytes written to this wrapper are run through `C`
/// before being written to the wrapped writer.
///
/// The caller must explicitly call [`finish`](CodecWriter::finish) to
/// finalize the codec.
pub struct CodecWriter<W, C: Codec, S> {
    output: StdSink<W, S>,
    pump: Pump<C>,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    /// Build a `CodecWriter`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `outbuf`.
    pub fn new(inner: W, codec: C, outbuf: S) -> Result<Self, EmptyBufferError> {
        Ok(Self {
            output: StdSink::new(inner, outbuf)?,
            pump: Pump::new(codec),
        })
    }

    pub fn get_ref(&self) -> &W {
        self.output.get_ref()
    }

    pub fn get_mut(&mut self) -> &mut W {
        self.output.get_mut()
    }

    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }

    /// Call the codec's `finish` repeatedly until it returns
    /// `DrainProgress::Done`. This writes any remaining trailer,
    /// checksum, or padding bytes.
    ///
    /// Then call [`Sink::finish`](crate::Sink::finish), which flushes
    /// the wrapped writer, and return ownership of the writer.
    ///
    /// You must call this method to complete the output. Dropping a
    /// `CodecWriter` without calling `finish` loses any remaining
    /// trailer, checksum, or padding bytes. This produces no compiler
    /// warning or runtime error.
    pub fn finish(mut self) -> io::Result<W> {
        pump_finish(&mut self.pump, &mut self.output).map_err(writer_error_to_io_error)?;
        Ok(self.output.into_inner())
    }

    /// Reclaim the writer, the codec, and the scratch buffer. If the
    /// buffer holds uncommitted bytes, treat them as lost.
    pub fn into_parts(self) -> (W, C, S) {
        let (inner, buffer) = self.output.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        pump_write(&mut self.pump, &mut self.output, buf).map_err(writer_error_to_io_error)
    }

    fn flush(&mut self) -> io::Result<()> {
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error_to_io_error)
    }
}
