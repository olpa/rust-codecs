//! `embedded_io::Read`/`Write` wrappers over a [`Codec`](crate::Codec)
//! or [`EndCapableCodec`](crate::EndCapableCodec).

use core::fmt;

use embedded_io::{ErrorType, Read, Write};

use crate::sources_and_sinks::shared_io::{
    pump_finish, pump_flush, pump_read, pump_write, ReadGranularity,
};
use crate::stream::Pump;
use crate::{Codec, DriveError, EndCapableCodec, Error, ErrorKind};

use super::adapter::{EmbeddedSink, EmbeddedSource};

/// An endpoint error or a codec error from an embedded I/O wrapper.
#[derive(Debug)]
pub enum EmbeddedError<E> {
    Io(E),
    Codec(Error),
}

fn adapter_contract_violation<E>() -> EmbeddedError<E> {
    EmbeddedError::Codec(Error::new(ErrorKind::ContractViolation, 0, 0))
}

fn reader_error<E>(error: DriveError<E, core::convert::Infallible>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(error) => EmbeddedError::Io(error),
        DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::NoProgress => adapter_contract_violation(),
    }
}

fn writer_error<E>(error: DriveError<core::convert::Infallible, E>) -> EmbeddedError<E> {
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
pub struct CodecReader<R, C: EndCapableCodec, S> {
    input: EmbeddedSource<R, S>,
    pump: Pump<C>,
    granularity: ReadGranularity,
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`. Reads at [`ReadGranularity::FillBuffer`]
    /// until changed via [`CodecReader::with_read_granularity`].
    ///
    /// # Panics
    ///
    /// Panics on an empty `inbuf`: it could never hold a byte read
    /// from `inner`, so the codec could never see any input — a caller
    /// bug, not a runtime condition.
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self {
            input: EmbeddedSource::new(inner, inbuf),
            pump: Pump::new(codec),
            granularity: ReadGranularity::default(),
        }
    }

    /// Set how much a single `read()` call pulls from the wrapped
    /// reader before returning — see [`ReadGranularity`]. Chainable,
    /// so it composes with `CodecReader::new`.
    pub fn with_read_granularity(mut self, granularity: ReadGranularity) -> Self {
        self.granularity = granularity;
        self
    }

    /// Unwrap this reader, discarding the codec, and return the
    /// wrapped reader. Any buffered, unconsumed input is discarded.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Reclaim the reader, the codec, and the scratch buffer — e.g. to
    /// read state the codec holds (a checksum, a digest), or to reuse
    /// the buffer's allocation for another `CodecReader`. `into_inner`
    /// discards the codec and the buffer; this is the exhaustive
    /// teardown, keeping all three. Same caveat as `into_inner`:
    /// buffered, unconsumed input is discarded.
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

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> ErrorType for CodecReader<R, C, S> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: Read, C: EndCapableCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        pump_read(&mut self.pump, &mut self.input, buf, self.granularity).map_err(reader_error)
    }
}

/// Wraps an [`embedded_io::Write`], transforming bytes before writing
/// them to the wrapped endpoint. Don't forget to call
/// [`finish`](CodecWriter::finish).
pub struct CodecWriter<W, C: Codec, S> {
    output: EmbeddedSink<W, S>,
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
        Self {
            output: EmbeddedSink::new(inner, outbuf),
            pump: Pump::new(codec),
        }
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

    /// Drain the codec by calling its `finish` repeatedly until
    /// `Drain::Done` (delivering any trailer/checksum/padding bytes it
    /// was still holding), finalize the sink itself ([`Sink::finish`](crate::Sink::finish),
    /// e.g. flushing the wrapped writer), and hand back ownership of it.
    ///
    /// Don't forget to call this: dropping a `CodecWriter` without
    /// calling `finish` silently drops any trailer/padding/checksum
    /// bytes the codec was still holding. There is no compiler
    /// warning or runtime error, only truncated output discovered
    /// later.
    pub fn finish(mut self) -> Result<W, EmbeddedError<W::Error>> {
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

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for CodecWriter<W, C, S> {
    type Error = EmbeddedError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        pump_write(&mut self.pump, &mut self.output, buf).map_err(writer_error)
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error)?;
        self.output.get_mut().flush().map_err(EmbeddedError::Io)
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{Read, Write};

    use super::{CodecReader, CodecWriter, EmbeddedError};
    use crate::identity::identity;
    use crate::{Codec, Drain, DrainCodec, EndCapableCodec, EndCapableProgress, Error, Progress};

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

    /// Buffers all input internally, emitting only on `flush`/`finish`
    /// — exercises the "flush is a resumable sync point, finish ends
    /// the stream" distinction `CodecWriter`'s docs draw. Uses a fixed
    /// backing array rather than `Vec` so this test doesn't need the
    /// `alloc` feature.
    #[derive(Default)]
    struct Hoarder {
        buf: [u8; 16],
        len: usize,
    }

    impl Hoarder {
        fn emit(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            let n = self.len.min(output.len());
            output[..n].copy_from_slice(&self.buf[..n]);
            self.buf.copy_within(n..self.len, 0);
            self.len -= n;
            if self.len == 0 {
                Ok(Drain::Done { written: n })
            } else {
                Ok(Drain::OutputFilled)
            }
        }
    }

    impl DrainCodec for Hoarder {
        fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            self.emit(output)
        }

        fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
            self.emit(output)
        }
    }

    impl Codec for Hoarder {
        fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
            self.buf[self.len..self.len + input.len()].copy_from_slice(input);
            self.len += input.len();
            Ok(Progress::InputConsumed { written: 0 })
        }
    }

    #[test]
    fn flush_does_not_end_the_stream() {
        let mut output = [0u8; 32];
        let remaining = {
            let mut writer = CodecWriter::new(&mut output[..], Hoarder::default(), [0u8; 64]);
            writer.write_all(b"first").unwrap();
            writer.flush().unwrap();
            writer.write_all(b"second").unwrap();
            writer.finish().unwrap().len()
        };
        let written = output.len() - remaining;
        assert_eq!(&output[..written], b"firstsecond");
    }

    /// Copies bytes 1:1 but ends its stream after `limit` bytes, like
    /// a self-describing format with an in-band terminator. A genuine
    /// `EndCapableCodec` (not `Codec`), so only `CodecReader` can
    /// drive it — `CodecWriter` is bound to `Codec` and rejects it at
    /// compile time.
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
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [u8],
        ) -> Result<EndCapableProgress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(EndCapableProgress::End {
                    consumed: n,
                    written: n,
                })
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
        // those and then report EOF (0) on every later call, without
        // touching the codec again.
        let mut reader = CodecReader::new(
            b"Hello World".as_slice(),
            EarlyEnd { limit: 3, done: 0 },
            [0u8; 8],
        );
        let mut out = [0u8; 8];
        let mut pos = 0;
        loop {
            let n = reader.read(&mut out[pos..]).unwrap();
            if n == 0 {
                break;
            }
            pos += n;
        }
        assert_eq!(&out[..pos], b"Hel");
    }
}
