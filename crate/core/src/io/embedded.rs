//! `embedded_io::Read`/`Write` adapters over a [`Codec`](crate::Codec).
//!
//! The ownership is directional and zero-copy at the codec boundary:
//! [`EmbeddedCodecReader`] owns input scratch and writes directly into
//! its caller's output, while [`EmbeddedCodecWriter`] reads directly
//! from its caller and owns output scratch.

use core::fmt;

use embedded_io::{ErrorType, Read, Write};

use crate::driver::{DrainEnd, Driver};
use crate::transfer::TransferEnd;
use crate::{Codec, Error};

/// An endpoint error or a codec error from an embedded I/O adapter.
#[derive(Debug)]
pub enum EmbeddedError<E> {
    Io(E),
    Codec(Error),
    /// The codec ended without accepting any of a non-empty write.
    WriteZero,
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
pub struct EmbeddedCodecReader<R, C: Codec, S> {
    inner: R,
    driver: Driver<C>,
    inbuf: S,
    inpos: usize,
    inlen: usize,
    inner_eof: bool,
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> EmbeddedCodecReader<R, C, S> {
    pub fn new(inner: R, codec: C, mut inbuf: S) -> Self {
        assert!(
            !inbuf.as_mut().is_empty(),
            "EmbeddedCodecReader buffer must be non-empty"
        );
        Self {
            inner,
            driver: Driver::new(codec),
            inbuf,
            inpos: 0,
            inlen: 0,
            inner_eof: false,
        }
    }

    /// Return the wrapped reader. Any buffered, unconsumed input is
    /// discarded.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> ErrorType for EmbeddedCodecReader<R, C, S> {
    type Error = EmbeddedError<R::Error>;
}

impl<R: Read, C: Codec, S: AsMut<[u8]>> Read for EmbeddedCodecReader<R, C, S> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        if buf.is_empty() || self.driver.is_done() {
            return Ok(0);
        }

        loop {
            if self.inpos == self.inlen && !self.inner_eof {
                self.inlen = self
                    .inner
                    .read(self.inbuf.as_mut())
                    .map_err(EmbeddedError::Io)?;
                self.inpos = 0;
                if self.inlen == 0 {
                    self.inner_eof = true;
                }
            }

            let input = &self.inbuf.as_mut()[self.inpos..self.inlen];
            if input.is_empty() {
                return self
                    .driver
                    .finish(buf)
                    .map(|moved| moved.written)
                    .map_err(EmbeddedError::Codec);
            }

            let moved = self
                .driver
                .process(input, buf)
                .map_err(EmbeddedError::Codec)?;
            self.inpos += moved.consumed;
            match moved.end {
                TransferEnd::InputExhausted if moved.written == 0 => {}
                TransferEnd::InputExhausted
                | TransferEnd::OutputExhausted
                | TransferEnd::StreamEnd => return Ok(moved.written),
            }
        }
    }
}

/// Wraps an [`embedded_io::Write`], transforming bytes before writing
/// them to the wrapped endpoint.
pub struct EmbeddedCodecWriter<W, C: Codec, S> {
    inner: W,
    driver: Driver<C>,
    outbuf: S,
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> EmbeddedCodecWriter<W, C, S> {
    pub fn new(inner: W, codec: C, mut outbuf: S) -> Self {
        assert!(
            !outbuf.as_mut().is_empty(),
            "EmbeddedCodecWriter buffer must be non-empty"
        );
        Self {
            inner,
            driver: Driver::new(codec),
            outbuf,
        }
    }

    /// Finish the codec stream, flush the endpoint, and return it.
    pub fn finish(mut self) -> Result<W, EmbeddedError<W::Error>> {
        while !self.driver.is_done() {
            let outbuf = self.outbuf.as_mut();
            let moved = self.driver.finish(outbuf).map_err(EmbeddedError::Codec)?;
            self.inner
                .write_all(&outbuf[..moved.written])
                .map_err(EmbeddedError::Io)?;
        }
        self.inner.flush().map_err(EmbeddedError::Io)?;
        Ok(self.inner)
    }
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> ErrorType for EmbeddedCodecWriter<W, C, S> {
    type Error = EmbeddedError<W::Error>;
}

impl<W: Write, C: Codec, S: AsMut<[u8]>> Write for EmbeddedCodecWriter<W, C, S> {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Self::Error> {
        if !buf.is_empty() && self.driver.is_done() {
            return Err(EmbeddedError::WriteZero);
        }
        let mut consumed = 0;
        while consumed < buf.len() && !self.driver.is_done() {
            let outbuf = self.outbuf.as_mut();
            let moved = self
                .driver
                .process(&buf[consumed..], outbuf)
                .map_err(EmbeddedError::Codec)?;
            self.inner
                .write_all(&outbuf[..moved.written])
                .map_err(EmbeddedError::Io)?;
            consumed += moved.consumed;
        }
        if !buf.is_empty() && consumed == 0 {
            Err(EmbeddedError::WriteZero)
        } else {
            Ok(consumed)
        }
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        loop {
            let outbuf = self.outbuf.as_mut();
            let moved = self.driver.flush(outbuf).map_err(EmbeddedError::Codec)?;
            self.inner
                .write_all(&outbuf[..moved.written])
                .map_err(EmbeddedError::Io)?;
            if moved.end == DrainEnd::Done {
                break;
            }
        }
        self.inner.flush().map_err(EmbeddedError::Io)
    }
}

#[cfg(all(test, feature = "identity"))]
mod tests {
    use embedded_io::{Error as _, ErrorKind, Read, Write};

    use super::{EmbeddedCodecReader, EmbeddedCodecWriter, EmbeddedError};
    use crate::identity::identity;
    use crate::{Codec, Drain, Error, Outcome};

    const INPUT: &[u8] = b"embedded io";

    #[test]
    fn reader_uses_caller_output_directly() {
        let mut reader = EmbeddedCodecReader::new(INPUT, identity(), [0u8; 3]);
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
            let mut writer = EmbeddedCodecWriter::new(&mut output[..], identity(), [0u8; 3]);
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
        let mut writer = EmbeddedCodecWriter::new(&mut output[..], EndsImmediately, [0u8; 1]);
        let error = writer.write(b"x").unwrap_err();
        assert!(matches!(error, EmbeddedError::WriteZero));
        assert_eq!(error.kind(), ErrorKind::WriteZero);
    }

    #[test]
    fn endpoint_errors_remain_distinguishable() {
        let mut output = [0u8; 1];
        let mut writer = EmbeddedCodecWriter::new(&mut output[..], identity(), [0u8; 2]);
        assert!(matches!(writer.write(b"ab"), Err(EmbeddedError::Io(_))));
    }
}
