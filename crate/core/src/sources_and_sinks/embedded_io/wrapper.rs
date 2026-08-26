//! `embedded_io::Read`/`Write` wrappers over a [`Codec`](crate::Codec)
//! or [`EndCapableCodec`](crate::EndCapableCodec).

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
        Self {
            input: EmbeddedSource::new(inner, inbuf),
            pump: Pump::new(codec),
        }
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
        end_capable_pump_read(&mut self.pump, &mut self.input, buf).map_err(reader_error)
    }
}

/// Like [`CodecReader`], but for an `R` that already implements
/// `embedded_io::BufRead` — no caller-provided scratch buffer, since
/// [`BufReadSource`] lends straight out of `R`'s own buffer instead of
/// copying into one of its own. Same end-of-stream behavior as
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

impl<R: BufRead, C: EndCapableCodec> ErrorType for BufReadCodecReader<R, C> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: BufRead, C: EndCapableCodec> Read for BufReadCodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        end_capable_pump_read(&mut self.pump, &mut self.input, buf).map_err(reader_error)
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
        pump_flush(&mut self.pump, &mut self.output).map_err(writer_error)
    }
}

#[cfg(test)]
mod tests {
    use embedded_io::{Read, Write};

    use super::{BufReadCodecReader, CodecReader, CodecWriter, EmbeddedError};
    use crate::identity::identity;
    use crate::sources_and_sinks::shared_io::test_support::EarlyEnd;
    #[cfg(feature = "alloc")]
    use crate::sources_and_sinks::shared_io::test_support::Hoarder;

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
    #[cfg(feature = "alloc")]
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

    #[test]
    fn buf_read_codec_reader_stops_at_in_band_end() {
        // Same latching behavior as `CodecReader`, but through the
        // no-scratch-buffer `BufReadCodecReader` path — its own doc
        // claims "same end-of-stream behavior as `CodecReader`".
        let mut reader =
            BufReadCodecReader::new(b"Hello World".as_slice(), EarlyEnd { limit: 3, done: 0 });
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
