use core::convert::Infallible;

use embedded_io::{BufRead, ErrorType, Read, Write};

use crate::sources_and_sinks::shared_io::{
    boundary_aware_pump_read, pump_finish, pump_flush, pump_write, CodecWriterFinishError,
};
use crate::stream::Pump;
use crate::{
    BoundaryAwareCodec, Codec, DriveError, DriveErrorKind, EmptyBufferError, ErrorKind,
    StreamError, StreamErrorKind, TransferCounts,
};

use super::adapter::{BufReadSource, EmbeddedSink, EmbeddedSource, WriteError};

fn reader_error_to_stream_error<E>(error: DriveError<E, Infallible>) -> StreamError<E> {
    let kind = match error.kind {
        DriveErrorKind::Source(error) => StreamErrorKind::Io(error),
        DriveErrorKind::Sink(never) => match never {},
        DriveErrorKind::Codec(error) => StreamErrorKind::Codec(error),
        DriveErrorKind::SinkExhausted => StreamErrorKind::SinkExhausted,
        DriveErrorKind::NoProgress => StreamErrorKind::NoProgress,
    };
    StreamError::new(kind, error.moved)
}

fn writer_error_to_stream_error<E>(error: DriveError<Infallible, WriteError<E>>) -> StreamError<E> {
    let kind = match error.kind {
        DriveErrorKind::Source(never) => match never {},
        DriveErrorKind::Sink(WriteError::Io(error)) => StreamErrorKind::Io(error),
        DriveErrorKind::Sink(WriteError::ZeroWrite) => StreamErrorKind::ZeroWrite,
        DriveErrorKind::Codec(error) => StreamErrorKind::Codec(error),
        DriveErrorKind::SinkExhausted => StreamErrorKind::SinkExhausted,
        DriveErrorKind::NoProgress => StreamErrorKind::NoProgress,
    };
    StreamError::new(kind, error.moved)
}

impl<E: embedded_io::Error> embedded_io::Error for StreamError<E> {
    fn kind(&self) -> embedded_io::ErrorKind {
        match &self.kind {
            StreamErrorKind::Io(error) => error.kind(),
            StreamErrorKind::Codec(error) => match error.kind {
                ErrorKind::CorruptStream | ErrorKind::UnexpectedEnd => {
                    embedded_io::ErrorKind::InvalidData
                }
                // Bugs in the codec or in the caller. The data is not at
                // fault.
                ErrorKind::CodecBufferTooSmall
                | ErrorKind::ByteCountClaim
                | ErrorKind::AfterFinish => embedded_io::ErrorKind::Other,
            },
            StreamErrorKind::ZeroWrite | StreamErrorKind::SinkExhausted => {
                embedded_io::ErrorKind::WriteZero
            }
            StreamErrorKind::NoProgress => embedded_io::ErrorKind::Other,
        }
    }
}

impl<E, T> From<CodecWriterFinishError<StreamError<E>, T>> for StreamError<E> {
    fn from(error: CodecWriterFinishError<StreamError<E>, T>) -> Self {
        error.into_error()
    }
}

/// Wraps an [`embedded_io::Read`], yielding bytes transformed by `C`.
///
/// Same end-of-stream and end-of-codec behavior as
/// [`std_io::CodecReader`](crate::sources_and_sinks::std_io::CodecReader).
pub struct CodecReader<R, C: BoundaryAwareCodec, S> {
    input: EmbeddedSource<R, S>,
    pump: Pump<C>,
}

impl<R, C: BoundaryAwareCodec, S> core::fmt::Debug for CodecReader<R, C, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CodecReader")
            .field("total", &self.pump.total())
            .finish_non_exhaustive()
    }
}

impl<R: Read, C: BoundaryAwareCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `inbuf`.
    pub fn new(inner: R, codec: C, inbuf: S) -> Result<Self, EmptyBufferError> {
        Ok(Self {
            input: EmbeddedSource::new(inner, inbuf)?,
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

    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }

    pub fn codec_ref(&self) -> &C {
        self.pump.get_ref()
    }

    pub fn codec_mut(&mut self) -> &mut C {
        self.pump.get_mut()
    }

    /// The input bytes that the codec consumed and the output bytes that
    /// it wrote, in all the calls so far.
    pub fn total(&self) -> TransferCounts {
        self.pump.total()
    }
}

impl<R, C: BoundaryAwareCodec, S: AsRef<[u8]>> CodecReader<R, C, S> {
    /// The unconsumed bytes already pulled from the reader into the
    /// scratch buffer, but not yet yielded to the caller.
    pub fn pending(&self) -> &[u8] {
        self.input.pending()
    }
}

impl<R: Read, C: BoundaryAwareCodec, S: AsMut<[u8]>> ErrorType for CodecReader<R, C, S> {
    type Error = StreamError<R::Error>;
}

impl<R: Read, C: BoundaryAwareCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_stream_error)
    }
}

/// Like [`CodecReader`], but for an `R: embedded_io::BufRead`, using
/// the `BufRead`'s buffer directly instead of a caller-provided
/// scratch buffer. Same end-of-stream and end-of-codec behavior as
/// `CodecReader`.
pub struct BufReadCodecReader<R, C: BoundaryAwareCodec> {
    input: BufReadSource<R>,
    pump: Pump<C>,
}

impl<R, C: BoundaryAwareCodec> core::fmt::Debug for BufReadCodecReader<R, C> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("BufReadCodecReader")
            .field("total", &self.pump.total())
            .finish_non_exhaustive()
    }
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

    /// The input bytes that the codec consumed and the output bytes that
    /// it wrote, in all the calls so far.
    pub fn total(&self) -> TransferCounts {
        self.pump.total()
    }
}

impl<R: BufRead, C: BoundaryAwareCodec> ErrorType for BufReadCodecReader<R, C> {
    type Error = StreamError<R::Error>;
}

impl<R: BufRead, C: BoundaryAwareCodec> Read for BufReadCodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
            .map_err(reader_error_to_stream_error)
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

impl<W, C: Codec, S> core::fmt::Debug for CodecWriter<W, C, S> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("CodecWriter")
            .field("total", &self.pump.total())
            .finish_non_exhaustive()
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    /// Build a `CodecWriter`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `outbuf`.
    pub fn new(inner: W, codec: C, outbuf: S) -> Result<Self, EmptyBufferError> {
        Ok(Self {
            output: EmbeddedSink::new(inner, outbuf)?,
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

    /// The input bytes that the codec consumed and the output bytes that
    /// it wrote, in all the calls so far.
    pub fn total(&self) -> TransferCounts {
        self.pump.total()
    }

    /// Drain the codec and hand back ownership of the writer, and the
    /// total bytes that the codec consumed and wrote. Same behavior as
    /// [`std_io::CodecWriter::finish`](crate::sources_and_sinks::std_io::CodecWriter::finish).
    // The type is the documentation: the writer and the totals on
    // success, the error and the `CodecWriter` on failure. An alias
    // would hide it.
    #[allow(clippy::type_complexity)]
    pub fn finish(
        mut self,
    ) -> Result<(W, TransferCounts), CodecWriterFinishError<StreamError<W::Error>, Self>> {
        match pump_finish(&mut self.pump, &mut self.output) {
            Ok(()) => Ok((self.output.into_inner(), self.pump.total())),
            Err(error) => Err(CodecWriterFinishError::new(
                writer_error_to_stream_error(error),
                self,
            )),
        }
    }

    /// Reclaim the writer, the codec, and the scratch buffer. If the
    /// buffer holds uncommitted bytes, treat them as lost.
    pub fn into_parts(self) -> (W, C, S) {
        let (inner, buffer) = self.output.into_parts();
        (inner, self.pump.into_inner(), buffer)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for CodecWriter<W, C, S> {
    type Error = StreamError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        pump_write(&mut self.pump, &mut self.output, buf).map_err(writer_error_to_stream_error)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error_to_stream_error)?;
        self.output
            .get_mut()
            .flush()
            .map_err(|error| StreamError::new(StreamErrorKind::Io(error), self.pump.total()))
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{ErrorType, Read};

    use super::{BufReadCodecReader, CodecReader, CodecWriter};
    use crate::codecs::test_support::{Collected, EndsAtBar, FailsAfterInner};
    use crate::identity::{identity, Identity};
    use crate::{ErrorKind, StreamError, StreamErrorKind};

    #[test]
    fn question_mark_turns_a_finish_error_into_an_embedded_error() {
        fn run(bytes: &mut [u8]) -> Result<(), StreamError<<&mut [u8] as ErrorType>::Error>> {
            let codec: FailsAfterInner<Identity> = FailsAfterInner {
                inner: identity(),
                ok_calls: 0,
                calls: 0,
            };
            let writer = CodecWriter::new(bytes, codec, [0u8; 8]).unwrap();

            // Act: the `?` operator calls the `From` impl.
            writer.finish()?;

            Ok(())
        }

        let mut bytes = [0u8; 8];
        assert!(matches!(
            run(&mut bytes),
            Err(StreamError { kind: StreamErrorKind::Codec(e), .. }) if e.kind == ErrorKind::CorruptStream
        ));
    }

    fn ends_at_bar() -> EndsAtBar<Identity> {
        EndsAtBar {
            inner: identity(),
            finish_calls: 0,
        }
    }

    fn read_to_end<R: Read>(reader: &mut R) -> Collected {
        let mut output = Collected::new();
        let mut buf = [0u8; 4];
        loop {
            let n = reader.read(&mut buf).unwrap();
            if n == 0 {
                return output;
            }
            output.extend_from_slice(&buf[..n]);
        }
    }

    #[test]
    fn codec_reader_returns_the_tail_after_an_in_band_end_as_pending() {
        let mut reader = CodecReader::new(&b"abc|tail"[..], ends_at_bar(), [0u8; 16]).unwrap();

        // Act
        let output = read_to_end(&mut reader);

        assert_eq!(output.as_slice(), b"abc");
        assert_eq!(reader.pending(), b"tail");
    }

    #[test]
    fn buf_read_codec_reader_leaves_the_tail_in_the_wrapped_reader() {
        let mut reader = BufReadCodecReader::new(&b"abc|tail"[..], ends_at_bar());

        // Act
        let output = read_to_end(&mut reader);

        assert_eq!(output.as_slice(), b"abc");
        assert_eq!(reader.into_inner(), b"tail");
    }
}
