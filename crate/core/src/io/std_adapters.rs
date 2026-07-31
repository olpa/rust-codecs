use std::io::{Read, Write};

use super::stream_to_stream::{Input, Output};

pub struct StdInput<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> StdInput<R, S> {
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdInput buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            pos: 0,
            len: 0,
            eof: false,
        }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: Read, S: AsMut<[u8]>> Input for StdInput<R, S> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        if self.pos == self.len && !self.eof {
            self.len = self.inner.read(self.buffer.as_mut())?;
            self.pos = 0;
            self.eof = self.len == 0;
        }
        Ok((self.pos < self.len).then_some(&self.buffer.as_mut()[self.pos..self.len]))
    }

    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.len - self.pos);
        self.pos += amount;
    }
}

pub struct StdOutput<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> StdOutput<W, S> {
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdOutput buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            offered: 0,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }
}

impl<W: Write, S: AsMut<[u8]>> Output for StdOutput<W, S> {
    type Error = std::io::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        assert_eq!(self.offered, 0, "commit must follow spare");
        self.offered = self.buffer.as_mut().len();
        Ok(Some(self.buffer.as_mut()))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        self.inner.write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

#[cfg(all(test, feature = "identity"))]
mod tests {
    use std::io::Cursor;

    use super::{StdInput, StdOutput};
    use crate::identity::identity;
    use crate::io::{stream_to_stream, VecInput, VecOutput};

    #[test]
    fn std_input_can_feed_vec_output() {
        let mut input = StdInput::new(Cursor::new(b"std to vec"), [0u8; 3]);
        let mut output = VecOutput::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to vec");
    }

    #[test]
    fn vec_input_can_feed_std_output() {
        let mut input = VecInput::new(b"vec to std".to_vec());
        let mut output = StdOutput::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"vec to std");
    }
}
