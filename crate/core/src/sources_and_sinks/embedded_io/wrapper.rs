//! `embedded_io::Read`/`Write` wrappers over a [`Codec`](crate::Codec)
//! or [`TerminatingCodec`](crate::TerminatingCodec).
//!
//! The ownership is directional and zero-copy at the codec boundary:
//! [`CodecReader`] owns input scratch and writes directly into
//! its caller's output, while [`CodecWriter`] reads directly
//! from its caller and owns output scratch.

use core::fmt;

use embedded_io::{ErrorType, Read, Write};

use crate::pump::{Pump, PumpEnd};
use crate::sources_and_sinks::slice::{SliceSource, SliceSink};
use crate::{Codec, DriveError, Error, Sink, TerminatingCodec};

use super::adapter::{EmbeddedSource, EmbeddedSink};

/// An endpoint error or a codec error from an embedded I/O wrapper.
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
        DriveError::SinkExhausted | DriveError::NoProgress => unreachable!("slice output adapter"),
    }
}

fn writer_error<E>(error: DriveError<core::convert::Infallible, E>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(never) => match never {},
        DriveError::Sink(error) => EmbeddedError::Io(error),
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::NoProgress => unreachable!("embedded output adapter"),
    }
}

fn slice_error<E>(error: DriveError<core::convert::Infallible, core::convert::Infallible>) -> EmbeddedError<E> {
    match error {
        DriveError::Source(never) | DriveError::Sink(never) => match never {},
        DriveError::Codec(error) => EmbeddedError::Codec(error),
        DriveError::SinkExhausted | DriveError::NoProgress => unreachable!("slice output adapter"),
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
///
/// `C` may be an ordinary [`Codec`] or a [`TerminatingCodec`].
pub struct CodecReader<R, C: TerminatingCodec, S> {
    input: EmbeddedSource<R, S>,
    pump: Pump<C>,
}

impl<R: Read, C: TerminatingCodec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self { input: EmbeddedSource::new(inner, inbuf), pump: Pump::new(codec) }
    }

    /// Return the wrapped reader. Any buffered, unconsumed input is
    /// discarded.
    pub fn into_inner(self) -> R {
        self.input.into_inner()
    }

    /// Direct access to the wrapped reader, bypassing the codec.
    /// Bytes already pulled from it into this reader's scratch buffer,
    /// but not yet yielded to the caller, aren't visible here.
    pub fn get_mut(&mut self) -> &mut R {
        self.input.get_mut()
    }
}

impl<R: Read, C: TerminatingCodec, S: AsMut<[u8]>> ErrorType for CodecReader<R, C, S> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: Read, C: TerminatingCodec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() || self.pump.is_done() {
            return Ok(0);
        }

        let mut output = SliceSink::new(buf);
        let moved = self.pump.transfer_from(&mut self.input, &mut output).map_err(reader_error)?;
        if moved.end == PumpEnd::SourceExhausted {
            self.pump.finish_to(&mut output).map_err(slice_error)?;
        }
        Ok(output.written())
    }
}

/// Wraps an [`embedded_io::Write`], transforming bytes before writing
/// them to the wrapped endpoint.
///
/// `C` is bound to [`Codec`], not [`TerminatingCodec`]: an in-band end
/// would otherwise become a permanent `Err(EmbeddedError::WriteZero)`
/// from `write`. The caller must explicitly call
/// [`CodecWriter::finish`] to finalize the codec; `Write::flush` is a
/// resumable synchronization point, not a substitute for it.
pub struct CodecWriter<W, C: Codec, S> {
    output: EmbeddedSink<W, S>,
    pump: Pump<C>,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    pub fn new(inner: W, codec: C, outbuf: S) -> Self {
        Self { output: EmbeddedSink::new(inner, outbuf), pump: Pump::new(codec) }
    }

    /// Direct access to the wrapped writer, bypassing the codec — e.g.
    /// to interleave raw framing bytes with codec output. Safe to use
    /// any time the codec has no output still owed from a prior
    /// `write`/`flush` (fresh, or right after `flush`/`finish`);
    /// writing here while the codec is mid-unit reorders bytes ahead of
    /// whatever it's still holding.
    pub fn get_mut(&mut self) -> &mut W {
        self.output.get_mut()
    }

    /// Finish the codec stream, flush the endpoint, and return it.
    pub fn finish(mut self) -> Result<W, EmbeddedError<W::Error>> {
        self.pump.finish_to(&mut self.output).map_err(writer_error)?;
        self.output.finish().map_err(EmbeddedError::Io)?;
        Ok(self.output.into_inner())
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for CodecWriter<W, C, S> {
    type Error = EmbeddedError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if !buf.is_empty() && self.pump.is_done() {
            return Err(EmbeddedError::WriteZero);
        }
        let mut input = SliceSource::new(buf);
        self.pump.transfer_from(&mut input, &mut self.output).map_err(writer_error)?;
        if !buf.is_empty() && input.consumed() == 0 {
            Err(EmbeddedError::WriteZero)
        } else {
            Ok(input.consumed())
        }
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.pump.flush_to(&mut self.output).map_err(writer_error)?;
        self.output.get_mut().flush().map_err(EmbeddedError::Io)
    }
}

#[cfg(all(test, feature = "identity"))]
mod tests {
    use embedded_io::{Read, Write};

    use super::{CodecReader, CodecWriter, EmbeddedError};
    use crate::identity::identity;
    use crate::{Drain, DrainCodec, Error, TerminatingCodec, TerminatingProgress};

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

    /// Copies bytes 1:1 but ends its stream after `limit` bytes, like
    /// a self-describing format with an in-band terminator. A genuine
    /// `TerminatingCodec` (not `Codec`), so only `CodecReader` can
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

    impl TerminatingCodec for EarlyEnd {
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<TerminatingProgress, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(TerminatingProgress::End { consumed: n, written: n })
            } else if n == input.len() {
                Ok(TerminatingProgress::InputConsumed { written: n })
            } else {
                Ok(TerminatingProgress::OutputFilled { consumed: n })
            }
        }
    }

    #[test]
    fn reader_stops_at_in_band_end() {
        // The codec ends after 3 bytes; the reader must yield exactly
        // those and then report EOF (0) on every later call, without
        // touching the codec again.
        let mut reader = CodecReader::new(b"Hello World".as_slice(), EarlyEnd { limit: 3, done: 0 }, [0u8; 8]);
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
