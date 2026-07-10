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

/// A [`Write`] that can still be finished after being boxed to an unknown
/// wrapping depth — e.g. a runtime-built chain of [`CodecWriter`]s over
/// `Box<dyn Codec>`, where the number of layers isn't known until the
/// program runs.
///
/// [`CodecWriter::finish`] consumes `self` by value and returns the
/// wrapped writer `W`, so it can't be reached through a plain `dyn
/// Write` — that trait has no such method, and once a value is behind a
/// trait object only that trait's methods are callable. `FinishWrite`
/// re-exposes a `finish`-like operation in a way that *is* dyn-callable
/// (`self: Box<Self>` instead of `self`), and the blanket impl below
/// lets each layer's `finish` feed into the next layer's, all the way
/// down — the same recursion `finish` already does, just expressed
/// through trait dispatch instead of a plain function, since a plain
/// function can't switch behavior on "is `W` a `CodecWriter` or is it
/// something else" once the depth is a runtime value.
///
/// The base of the chain — whatever concrete sink you're writing to
/// (`Stdout`, a `File`, ...) — needs its own one-line impl that just
/// flushes; there's no way to give a default for arbitrary `W: Write`
/// without conflicting with the blanket impl for `CodecWriter` below.
pub trait FinishWrite: Write {
    /// Finish this layer, and every layer it wraps, all the way down to
    /// the base of the chain.
    fn finish_boxed(self: Box<Self>) -> io::Result<()>;
}

impl<W: FinishWrite + ?Sized> FinishWrite for Box<W> {
    fn finish_boxed(self: Box<Self>) -> io::Result<()> {
        (*self).finish_boxed()
    }
}

impl<W: FinishWrite + 'static, C: Codec + 'static> FinishWrite for CodecWriter<W, C> {
    fn finish_boxed(self: Box<Self>) -> io::Result<()> {
        let inner = (*self).finish()?;
        Box::new(inner).finish_boxed()
    }
}

/// Base case for the chain: stdout has nothing of its own to finish
/// beyond a flush. Only this crate can give `Stdout` a `FinishWrite` impl
/// (orphan rules: a downstream crate owns neither the trait nor the
/// type), so it's provided here rather than left for every caller to
/// rediscover.
impl FinishWrite for io::Stdout {
    fn finish_boxed(mut self: Box<Self>) -> io::Result<()> {
        (*self).flush()
    }
}
