//! `std::io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! - [`CodecReader`]: wraps a `Read`, runs the transform on the fly, and
//!   is itself a `Read` yielding the transformed bytes.
//! - [`CodecWriter`]: wraps a `Write`; bytes written to it are
//!   transformed on the fly before reaching the wrapped writer.

use std::io::{self, Read, Write};

use crate::{Codec, Status};

const SCRATCH: usize = 64 * 1024;

fn to_io_error(err: crate::Error) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("{err:?}"))
}

/// Wraps a `Read`, running `C` over the bytes as they're pulled through.
pub struct CodecReader<R, C> {
    inner: R,
    codec: C,
    inbuf: Vec<u8>,
    inpos: usize,
    inlen: usize,
    inner_eof: bool,
    stream_end: bool,
}

impl<R: Read, C: Codec> CodecReader<R, C> {
    pub fn new(inner: R, codec: C) -> Self {
        Self {
            inner,
            codec,
            inbuf: vec![0u8; SCRATCH],
            inpos: 0,
            inlen: 0,
            inner_eof: false,
            stream_end: false,
        }
    }

    /// Unwrap this reader, discarding the codec, and return the wrapped
    /// reader. Any bytes already pulled from it but not yet yielded to
    /// the caller are lost.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read, C: Codec> Read for CodecReader<R, C> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        loop {
            if self.stream_end {
                return Ok(0);
            }

            if self.inpos == self.inlen && !self.inner_eof {
                self.inlen = self.inner.read(&mut self.inbuf)?;
                self.inpos = 0;
                if self.inlen == 0 {
                    self.inner_eof = true;
                }
                continue;
            }

            let (progress, status) = if self.inpos == self.inlen {
                self.codec.finish(buf).map_err(to_io_error)?
            } else {
                self.codec
                    .process(&self.inbuf[self.inpos..self.inlen], buf)
                    .map_err(to_io_error)?
            };
            self.inpos += progress.consumed;

            if matches!(status, Status::StreamEnd) {
                self.stream_end = true;
            }
            if progress.written > 0 || self.stream_end {
                return Ok(progress.written);
            }
            // Nothing produced yet (e.g. codec is still buffering
            // internally) and the stream hasn't ended — go around and
            // feed it more input.
        }
    }
}

/// Wraps a `Write`; bytes written to this adapter are run through `C`
/// before being written to the wrapped writer.
pub struct CodecWriter<W, C> {
    inner: W,
    codec: C,
    outbuf: Vec<u8>,
}

impl<W: Write, C: Codec> CodecWriter<W, C> {
    pub fn new(inner: W, codec: C) -> Self {
        Self { inner, codec, outbuf: vec![0u8; SCRATCH] }
    }

    /// Flush any bytes the codec was still holding, finalize the stream
    /// (trailer, checksum, padding — for a stateful codec), and hand back
    /// ownership of the wrapped writer.
    pub fn finish(mut self) -> io::Result<W> {
        loop {
            let (progress, status) = self.codec.finish(&mut self.outbuf).map_err(to_io_error)?;
            if progress.written > 0 {
                self.inner.write_all(&self.outbuf[..progress.written])?;
            }
            if matches!(status, Status::StreamEnd) {
                break;
            }
            if progress.written == 0 {
                return Err(io::Error::other("codec made no progress finishing the stream"));
            }
        }
        self.inner.flush()?;
        Ok(self.inner)
    }
}

impl<W: Write, C: Codec> Write for CodecWriter<W, C> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut consumed = 0usize;
        while consumed < buf.len() {
            let (progress, status) = self
                .codec
                .process(&buf[consumed..], &mut self.outbuf)
                .map_err(to_io_error)?;
            if progress.written > 0 {
                self.inner.write_all(&self.outbuf[..progress.written])?;
            }
            consumed += progress.consumed;
            if matches!(status, Status::StreamEnd) {
                break;
            }
        }
        Ok(consumed)
    }

    fn flush(&mut self) -> io::Result<()> {
        loop {
            let (progress, status) = self.codec.flush(&mut self.outbuf).map_err(to_io_error)?;
            if progress.written > 0 {
                self.inner.write_all(&self.outbuf[..progress.written])?;
            }
            if progress.written == 0 || matches!(status, Status::InputEmpty) {
                break;
            }
        }
        self.inner.flush()
    }
}
