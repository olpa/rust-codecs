//! `std::io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! - [`CodecReader`]: wraps a `Read`, runs the transform on the fly, and
//!   is itself a `Read` yielding the transformed bytes.
//! - [`CodecWriter`]: wraps a `Write`; bytes written to it are
//!   transformed on the fly before reaching the wrapped writer.
//!
//! Both share the bufferless codec lifecycle driver while retaining
//! only their direction-specific cursor loop. The reader owns input
//! scratch and writes directly into its caller's output; the writer
//! reads directly from its caller and owns output scratch. Both take a
//! caller-provided scratch buffer
//! (`S: AsMut<[u8]>` — same convention as [`Chain`](crate::Chain))
//! rather than allocating one internally: batching policy already has
//! a canonical, composable expression in `BufReader`/`BufWriter`
//! placement in the client's own stack, so a knob inside these
//! adapters would just duplicate that.

use std::io::{self, Read, Write};

use crate::driver::{DrainEnd, Driver};
use crate::transfer::TransferEnd;
use crate::{Codec, Error};

fn to_io_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
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
    inner: R,
    driver: Driver<C>,
    inbuf: S,
    inpos: usize,
    inlen: usize,
    inner_eof: bool,
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> CodecReader<R, C, S> {
    /// Build a `CodecReader`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `inbuf`: it could never hold a byte read
    /// from `inner`, so the codec could never see any input — a caller
    /// bug, not a runtime condition.
    pub fn new(inner: R, codec: C, mut inbuf: S) -> Self {
        assert!(!inbuf.as_mut().is_empty(), "CodecReader buffer must be non-empty");
        Self {
            inner,
            driver: Driver::new(codec),
            inbuf,
            inpos: 0,
            inlen: 0,
            inner_eof: false,
        }
    }

    /// Unwrap this reader, discarding the codec, and return the wrapped
    /// reader. Any bytes already pulled from it but not yet yielded to
    /// the caller are lost.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> Read for CodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            if self.driver.is_done() {
                return Ok(0);
            }

            if self.inpos == self.inlen && !self.inner_eof {
                self.inlen = self.inner.read(self.inbuf.as_mut())?;
                self.inpos = 0;
                if self.inlen == 0 {
                    self.inner_eof = true;
                }
            }

            let input = &self.inbuf.as_mut()[self.inpos..self.inlen];
            if input.is_empty() {
                let moved = self.driver.finish(buf).map_err(to_io_error)?;
                return Ok(moved.written);
            }
            let moved = self.driver.process(input, buf).map_err(to_io_error)?;
            self.inpos += moved.consumed;
            match moved.end {
                TransferEnd::InputExhausted => {
                    if moved.written > 0 {
                        return Ok(moved.written);
                    }
                    // Codec buffered internally — feed it more input.
                }
                TransferEnd::OutputExhausted | TransferEnd::StreamEnd => {
                    return Ok(moved.written);
                }
            }
        }
    }
}

/// Wraps a `Write`; bytes written to this adapter are run through `C`
/// before being written to the wrapped writer.
pub struct CodecWriter<W, C: Codec, S> {
    inner: W,
    driver: Driver<C>,
    outbuf: S,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> CodecWriter<W, C, S> {
    /// Build a `CodecWriter`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `outbuf`, for the same reason
    /// [`CodecReader::new`] does: it could never hold a byte for
    /// `inner` to receive.
    pub fn new(inner: W, codec: C, mut outbuf: S) -> Self {
        assert!(!outbuf.as_mut().is_empty(), "CodecWriter buffer must be non-empty");
        Self { inner, driver: Driver::new(codec), outbuf }
    }

    /// Flush any bytes the codec was still holding, finalize the stream
    /// (trailer, checksum, padding — for a stateful codec), and hand back
    /// ownership of the wrapped writer.
    pub fn finish(mut self) -> io::Result<W> {
        while !self.driver.is_done() {
            let outbuf = self.outbuf.as_mut();
            let moved = self.driver.finish(outbuf).map_err(to_io_error)?;
            self.inner.write_all(&outbuf[..moved.written])?;
        }
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut consumed = 0usize;
        // Once the codec has ended its stream in-band, no more bytes
        // are accepted; returning the short (possibly zero) count lets
        // a `write_all` caller surface it as `WriteZero`.
        while consumed < buf.len() && !self.driver.is_done() {
            let outbuf = self.outbuf.as_mut();
            let moved = self
                .driver
                .process(&buf[consumed..], outbuf)
                .map_err(to_io_error)?;
            self.inner.write_all(&outbuf[..moved.written])?;
            consumed += moved.consumed;
        }
        Ok(consumed)
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            let outbuf = self.outbuf.as_mut();
            let moved = self.driver.flush(outbuf).map_err(to_io_error)?;
            self.inner.write_all(&outbuf[..moved.written])?;
            if moved.end == DrainEnd::Done {
                break;
            }
        }
        self.inner.flush()
    }
}

#[cfg(all(test, feature = "rot13"))]
mod tests {
    use std::io::{Cursor, Read};

    use super::{CodecReader, CodecWriter};
    use crate::rot13::rot13;
    use crate::{Codec, Drain, Error, Outcome};

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
        fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
            let remaining = self.limit - self.done;
            let n = input.len().min(output.len()).min(remaining);
            output[..n].copy_from_slice(&input[..n]);
            self.done += n;
            if self.done >= self.limit {
                Ok(Outcome::StreamEnd { consumed: n, written: n })
            } else if n == input.len() {
                Ok(Outcome::InputConsumed { written: n })
            } else {
                Ok(Outcome::OutputFilled { consumed: n })
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
