//! `embedded_io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! The ownership is directional and zero-copy at the codec boundary:
//! [`CodecReader`] owns input scratch and writes directly into
//! its caller's output, while [`CodecWriter`] reads directly
//! from its caller and owns output scratch.

use core::fmt;

use embedded_io::{ErrorType, Read, Write};

use crate::driver::{Driver, PumpEnd};
use crate::sources_and_sinks::slice::{SliceSource, SliceSink};
use crate::{Codec, DriveError, Error, Sink};

use super::bridge::{EmbeddedSource, EmbeddedSink};

/// An endpoint error or a codec error from an embedded I/O adapter.
#[derive(Debug)]
pub enum EmbeddedError<E> {
    Io(E),
    Codec(Error),
    /// The codec ended without accepting any of a non-empty write.
    WriteZero,
}

fn reader_error<E>(error: DriveError<E, core::convert::Infallible>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(error) => EmbeddedError::Io(error),
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::EmptySlot => unreachable!("slice output adapter"),
    }
}

fn writer_error<E>(error: DriveError<core::convert::Infallible, E>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => EmbeddedError::Io(error),
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::EmptySlot => unreachable!("embedded output adapter"),
    }
}

fn slice_error<E>(error: DriveError<core::convert::Infallible, core::convert::Infallible>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(never) | DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::EmptySlot => unreachable!("slice output adapter"),
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
            Self::WriteZero => embedded_io::ErrorKind::WriteZero,
        }
    }
}

/// Wraps an [`embedded_io::Read`], yielding bytes transformed by `C`.
pub struct CodecReader<R, C: Codec, S> {
    input: EmbeddedSource<R, S>,
    driver: Driver<C>,
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self { input: EmbeddedSource::new(inner, inbuf), driver: Driver::new(codec) }
    }

    /// Return the wrapped reader. Any buffered, unconsumed input is
    /// discarded.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> ErrorType for CodecReader<R, C, S> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() || self.driver.is_done() {
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

/// Wraps an [`embedded_io::Write`], transforming bytes before writing
/// them to the wrapped endpoint.
pub struct CodecWriter<W, C: Codec, S> {
    output: EmbeddedSink<W, S>,
    driver: Driver<C>,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    pub fn new(inner: W, codec: C, outbuf: S) -> Self {
        Self { output: EmbeddedSink::new(inner, outbuf), driver: Driver::new(codec) }
    }

    /// Finish the codec stream, flush the endpoint, and return it.
    pub fn finish(mut self) -> Result<W, EmbeddedError<W::Error>> {
        self.driver.finish_to(&mut self.output).map_err(writer_error)?;
        self.output.finish().map_err(EmbeddedError::Io)?;
        Ok(self.output.into_inner())
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for CodecWriter<W, C, S> {
    type Error = EmbeddedError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if !buf.is_empty() && self.driver.is_done() {
            return Err(EmbeddedError::WriteZero);
        }
        let mut input = SliceSource::new(buf);
        self.driver.transfer_from(&mut input, &mut self.output).map_err(writer_error)?;
        if !buf.is_empty() && input.consumed() == 0 {
            Err(EmbeddedError::WriteZero)
        } else {
            Ok(input.consumed())
        }
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.driver.flush_to(&mut self.output).map_err(writer_error)?;
        self.output.finish().map_err(EmbeddedError::Io)
    }
}

#[cfg(all(test, feature = "identity"))]
mod tests {
    use embedded_io::{Error as _, ErrorKind, Read, Write};

    use super::{CodecReader, CodecWriter, EmbeddedError};
    use crate::identity::identity;
    use crate::{Codec, Drain, Error, Outcome};

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

    struct EndsImmediately;

    impl Codec for EndsImmediately {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(Outcome::StreamEnd {
                consumed: 0,
                written: 0,
            })
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            unreachable!()
        }
    }

    #[test]
    fn nonempty_write_never_returns_zero() {
        let mut output = [0u8; 1];
        let mut writer = CodecWriter::new(&mut output[..], EndsImmediately, [0u8; 1]);
        let error = writer.write(b"x").unwrap_err();
        assert!(matches!(error, EmbeddedError::WriteZero));
        assert_eq!(error.kind(), ErrorKind::WriteZero);
    }

    #[test]
    fn endpoint_errors_remain_distinguishable() {
        let mut output = [0u8; 1];
        let mut writer = CodecWriter::new(&mut output[..], identity(), [0u8; 2]);
        assert!(matches!(writer.write(b"ab"), Err(EmbeddedError::Io(_))));
    }
}
