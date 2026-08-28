use core::fmt;

use embedded_io::{BufRead, ErrorType, Read, Write};

use crate::sources_and_sinks::shared_io::{
    end_capable_pump_read, pump_finish, pump_flush, pump_write,
};
use crate::stream::Pump;
use crate::{Codec, DriveError, EndCapableCodec, Error, ErrorKind};

use super::adapter::{BufReadSource, EmbeddedSink, EmbeddedSource};

/// An endpoint error or a codec error from an embedded I/O wrapper.
#[derive(Debug)]
pub enum EmbeddedError<E> {
    Io(E),
    Codec(Error),
}

fn adapter_contract_violation<E>() -> EmbeddedError<E> {
    EmbeddedError::Codec(Error::new(ErrorKind::ContractViolation, 0, 0))
}

fn reader_error_to_embedded_error<E>(
    error: DriveError<E, core::convert::Infallible>,
) -> EmbeddedError<E> {
    match error {
        DriveError::Source(error) => EmbeddedError::Io(error),
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

fn writer_error_to_embedded_error<E>(
    error: DriveError<core::convert::Infallible, E>,
) -> EmbeddedError<E> {
    match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => EmbeddedError::Io(error),
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

impl<E: embedded_io::Error> fmt::Display for EmbeddedError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{self:?}")
    }
}

impl<E: embedded_io::Error> core::error::Error for EmbeddedError<E> {}

impl<E: embedded_io::Error> embedded_io::Error for EmbeddedError<E> {
    fn kind(&self) -> embedded_io::ErrorKind {
        match self {
            Self::Io(error) => error.kind(),
            Self::Codec(_) => embedded_io::ErrorKind::InvalidData,
        }
    }
}

/// Wraps an [`embedded_io::Read`], yielding bytes transformed by `C`.
///
/// Same end-of-stream and end-of-codec behavior as
/// [`std_io::CodecReader`](crate::sources_and_sinks::std_io::CodecReader).
pub struct CodecReader<R, C: EndCapableCodec, S> {
    input: EmbeddedSource<R, S>,
    pump: Pump<C>,
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `inbuf`.
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self {
            input: EmbeddedSource::new(inner, inbuf),
            pump: Pump::new(codec),
        }
    }

    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

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

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> ErrorType for CodecReader<R, C, S> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        end_capable_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_embedded_error)
    }
}

/// Like [`CodecReader`], but for an `R: embedded_io::BufRead`, using
/// the `BufRead`'s buffer directly instead of a caller-provided
/// scratch buffer. Same end-of-stream and end-of-codec behavior as
/// `CodecReader`.
pub struct BufReadCodecReader<R, C: EndCapableCodec> {
    input: BufReadSource<R>,
    pump: Pump<C>,
}

impl<R: BufRead, C: EndCapableCodec> BufReadCodecReader<R, C> {
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

impl<R: BufRead, C: EndCapableCodec> ErrorType for BufReadCodecReader<R, C> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: BufRead, C: EndCapableCodec> Read for BufReadCodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        end_capable_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_embedded_error)
    }
}

/// Wraps an [`embedded_io::Write`]; bytes written to this wrapper are
/// run through `C` before being written to the wrapped writer. Same
/// behavior as
/// [`std_io::CodecWriter`](crate::sources_and_sinks::std_io::CodecWriter);
/// don't forget to call [`finish`](CodecWriter::finish).
pub struct CodecWriter<W, C: Codec, S> {
    output: EmbeddedSink<W, S>,
    pump: Pump<C>,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    /// Build a `CodecWriter`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `outbuf`, same as [`CodecReader::new`].
    pub fn new(inner: W, codec: C, outbuf: S) -> Self {
        Self {
            output: EmbeddedSink::new(inner, outbuf),
            pump: Pump::new(codec),
        }
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

    /// Drain the codec, finalize the sink, and hand back ownership of
    /// the writer. Same behavior as
    /// [`std_io::CodecWriter::finish`](crate::sources_and_sinks::std_io::CodecWriter::finish).
    pub fn finish(mut self) -> Result<W, EmbeddedError<W::Error>> {
        pump_finish(&mut self.pump, &mut self.output).map_err(writer_error_to_embedded_error)?;
        Ok(self.output.into_inner())
    }

    pub fn into_parts(self) -> (W, C, S) {
        let (inner, buffer) = self.output.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for CodecWriter<W, C, S> {
    type Error = EmbeddedError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        pump_write(&mut self.pump, &mut self.output, buf).map_err(writer_error_to_embedded_error)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error_to_embedded_error)
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{Read, Write};

    use super::{BufReadCodecReader, CodecReader, CodecWriter, EmbeddedError};
    use crate::identity::identity;

    const INPUT: &[u8] = b"embedded io";

    #[test]
    fn reader_uses_caller_output_directly() {
        let mut reader = CodecReader::new(INPUT, identity(), [0u8; 3]);
        let mut output = [0u8; INPUT.len()];
        let mut pos = 0;
        while pos < output.len() {
            let read = reader.read(&mut output[pos..]).unwrap();
            if read == 0 {
                break;
            }
            pos += read;
        }
        assert_eq!(pos, INPUT.len());
        assert_eq!(&output, INPUT);
    }

    #[test]
    fn writer_uses_caller_input_directly_and_finishes() {
        let mut output = [0u8; 32];
        let remaining = {
            let mut writer = CodecWriter::new(&mut output[..], identity(), [0u8; 3]);
            writer.write_all(INPUT).unwrap();
            writer.finish().unwrap().len()
        };
        let written = output.len() - remaining;
        assert_eq!(&output[..written], INPUT);
    }

    #[test]
    fn endpoint_errors_remain_distinguishable() {
        let mut output = [0u8; 1];
        let mut writer = CodecWriter::new(&mut output[..], identity(), [0u8; 2]);
        assert!(matches!(writer.write(b"ab"), Err(EmbeddedError::Io(_))));
    }

    #[test]
    #[should_panic(expected = "buffer must be non-empty")]
    fn codec_reader_rejects_empty_buffer() {
        let _ = CodecReader::new(INPUT, identity(), [0u8; 0]);
    }

    #[test]
    #[should_panic(expected = "buffer must be non-empty")]
    fn codec_writer_rejects_empty_buffer() {
        let mut output = [0u8; 8];
        let _ = CodecWriter::new(&mut output[..], identity(), [0u8; 0]);
    }

    #[test]
    fn buf_read_codec_reader_needs_no_scratch_buffer() {
        let mut reader = BufReadCodecReader::new(INPUT, identity());
        let mut output = [0u8; INPUT.len()];
        let mut pos = 0;
        while pos < output.len() {
            let read = reader.read(&mut output[pos..]).unwrap();
            if read == 0 {
                break;
            }
            pos += read;
        }
        assert_eq!(pos, INPUT.len());
        assert_eq!(&output, INPUT);
    }
}
