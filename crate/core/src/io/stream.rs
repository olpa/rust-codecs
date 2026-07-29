//! `std::io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! - [`CodecReader`]: wraps a `Read`, runs the transform on the fly, and
//!   is itself a `Read` yielding the transformed bytes.
//! - [`CodecWriter`]: wraps a `Write`; bytes written to it are
//!   transformed on the fly before reaching the wrapped writer.
//!
//! Both drive their codec through an [`Engine`](crate::Engine) and take
//! a caller-provided scratch buffer (`S: AsMut<[u8]>` — same convention
//! as [`Chain`](crate::Chain)) rather than allocating one internally:
//! batching policy already has a canonical, composable expression in
//! `BufReader`/`BufWriter` placement in the client's own stack, so a
//! knob inside these adapters would just duplicate that.

use std::io::{self, Read, Write};

use crate::{Codec, Drain, Engine, Error, Step};

fn to_io_error(err: Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

/// Wraps a `Read`, running `C` over the bytes as they're pulled through.
pub struct CodecReader<R, C: Codec, S> {
    inner: R,
    engine: Engine<C>,
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
        Self { inner, engine: Engine::new(codec), inbuf, inpos: 0, inlen: 0, inner_eof: false }
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
            if self.inpos == self.inlen && !self.inner_eof {
                self.inlen = self.inner.read(self.inbuf.as_mut())?;
                self.inpos = 0;
                if self.inlen == 0 {
                    self.inner_eof = true;
                }
            }

            let input = &self.inbuf.as_mut()[self.inpos..self.inlen];
            let (consumed, step) =
                self.engine.step(input, self.inner_eof, buf).map_err(to_io_error)?;
            self.inpos += consumed;

            match step {
                Step::Wrote(n) => return Ok(n),
                Step::Done => return Ok(0),
                Step::NeedInput => {
                    // Nothing produced yet (e.g. codec is still
                    // buffering internally) — go around and feed it
                    // more input.
                }
                Step::NeedOutput => {
                    unreachable!("buf is checked non-empty above")
                }
            }
        }
    }
}

/// Wraps a `Write`; bytes written to this adapter are run through `C`
/// before being written to the wrapped writer.
pub struct CodecWriter<W, C: Codec, S> {
    inner: W,
    engine: Engine<C>,
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
        Self { inner, engine: Engine::new(codec), outbuf }
    }

    /// Flush any bytes the codec was still holding, finalize the stream
    /// (trailer, checksum, padding — for a stateful codec), and hand back
    /// ownership of the wrapped writer.
    pub fn finish(mut self) -> io::Result<W> {
        loop {
            let outbuf = self.outbuf.as_mut();
            let (_, step) = self.engine.step(&[], true, outbuf).map_err(to_io_error)?;
            match step {
                Step::Wrote(n) => self.inner.write_all(&outbuf[..n])?,
                Step::Done => break,
                Step::NeedInput => unreachable!("finishing never reports NeedInput"),
                Step::NeedOutput => unreachable!("outbuf is rejected empty by CodecWriter::new"),
            }
        }
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for CodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut consumed = 0usize;
        while consumed < buf.len() {
            let outbuf = self.outbuf.as_mut();
            let (n, step) =
                self.engine.step(&buf[consumed..], false, outbuf).map_err(to_io_error)?;
            consumed += n;
            match step {
                Step::Wrote(written) => self.inner.write_all(&outbuf[..written])?,
                Step::Done => break,
                Step::NeedInput => {}
                Step::NeedOutput => unreachable!("outbuf is rejected empty by CodecWriter::new"),
            }
        }
        Ok(consumed)
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            let outbuf = self.outbuf.as_mut();
            match self.engine.flush(outbuf).map_err(to_io_error)? {
                Drain::OutputFilled => self.inner.write_all(&outbuf[..])?,
                Drain::Done { written } => {
                    self.inner.write_all(&outbuf[..written])?;
                    break;
                }
            }
        }
        self.inner.flush()
    }
}

#[cfg(all(test, feature = "rot13"))]
mod tests {
    use std::io::Cursor;

    use super::{CodecReader, CodecWriter};
    use crate::rot13::rot13;

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
}
