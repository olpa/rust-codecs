use std::io::{self, BufRead, Read, Write};

use core::convert::Infallible;

use crate::sources_and_sinks::shared_io::{
    boundary_aware_pump_read, pump_finish, pump_flush, pump_sync_flush, pump_write,
};
use crate::stream::Pump;
use crate::{BoundaryAwareCodec, Codec, DriveError, Error, ErrorKind};

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
    /// # Panics
    ///
    /// Panics on an empty `inbuf`.
    pub fn new(inner: R, codec: C, inbuf: S) -> Self {
        Self {
            input: StdSource::new(inner, inbuf),
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
    /// # Panics
    ///
    /// Panics on an empty `outbuf`, same as [`CodecReader::new`].
    pub fn new(inner: W, codec: C, outbuf: S) -> Self {
        Self {
            output: StdSink::new(inner, outbuf),
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

    /// Drain the codec by calling its `finish` repeatedly until
    /// `DrainProgress::Done` (delivering any trailer/checksum/padding bytes it
    /// was still holding), finalize the sink itself ([`Sink::finish`](crate::Sink::finish),
    /// e.g. flushing the wrapped writer), and hand back ownership of it.
    ///
    /// Don't forget to call this: dropping a `CodecWriter` without
    /// calling `finish` silently drops any trailer/padding/checksum
    /// bytes the codec was still holding. There is no compiler
    /// warning or runtime error, only truncated output discovered
    /// later.
    pub fn finish(mut self) -> io::Result<W> {
        pump_finish(&mut self.pump, &mut self.output).map_err(writer_error_to_io_error)?;
        Ok(self.output.into_inner())
    }

    /// Ask the codec to emit buffered output and a sync marker without
    /// ending its stream, then flush the wrapped writer.
    ///
    /// Unlike [`Write::flush`], this can change the encoded byte stream.
    pub fn sync_flush(&mut self) -> io::Result<()> {
        pump_sync_flush(&mut self.pump, &mut self.output).map_err(writer_error_to_io_error)
    }

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
        pump_flush(&mut self.output).map_err(writer_error_to_io_error)
    }
}

#[cfg(test)]
mod tests {
    use core::mem::MaybeUninit;
    use std::io::Write;

    use crate::{Codec, DrainProgress, DrainCodec, Error, Progress};

    use super::CodecWriter;

    struct SyncMarker {
        synced: bool,
    }

    impl DrainCodec for SyncMarker {
        fn sync_flush(&mut self, output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            if self.synced {
                return Ok(DrainProgress::Done { written: 0 });
            }
            if output.is_empty() {
                return Ok(DrainProgress::OutputFilled);
            }
            output[0].write(b'!');
            self.synced = true;
            Ok(DrainProgress::Done { written: 1 })
        }

        fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
            Ok(DrainProgress::Done { written: 0 })
        }
    }

    impl Codec for SyncMarker {
        fn process(
            &mut self,
            input: &[u8],
            output: &mut [MaybeUninit<u8>],
        ) -> Result<Progress, Error> {
            self.synced = false;
            let written = input.len().min(output.len());
            for (slot, byte) in output.iter_mut().zip(input).take(written) {
                slot.write(*byte);
            }
            if written == input.len() {
                Ok(Progress::InputConsumed { written })
            } else {
                Ok(Progress::OutputFilled { consumed: written })
            }
        }
    }

    #[test]
    fn flush_does_not_emit_a_codec_sync_marker() {
        let mut writer = CodecWriter::new(Vec::new(), SyncMarker { synced: false }, [0; 8]);
        writer.write_all(b"a").unwrap();

        writer.flush().unwrap();
        assert_eq!(writer.get_ref(), b"a");

        writer.sync_flush().unwrap();
        assert_eq!(writer.get_ref(), b"a!");
    }
}
