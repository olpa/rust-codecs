use embedded_io::{Read, Write};

use super::stream_to_stream::{Input, Output};

pub struct EmbeddedInput<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> EmbeddedInput<R, S> {
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedInput buffer must be non-empty"
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

impl<R: Read, S: AsMut<[u8]>> Input for EmbeddedInput<R, S> {
    type Error = R::Error;

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

pub struct EmbeddedOutput<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> EmbeddedOutput<W, S> {
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "EmbeddedOutput buffer must be non-empty"
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

impl<W: Write, S: AsMut<[u8]>> Output for EmbeddedOutput<W, S> {
    type Error = W::Error;

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

#[cfg(all(test, feature = "identity", feature = "alloc"))]
mod tests {
    use super::{EmbeddedInput, EmbeddedOutput};
    use crate::identity::identity;
    use crate::io::{drive, VecInput, VecOutput};

    #[test]
    fn embedded_input_can_feed_vec_output() {
        let mut input = EmbeddedInput::new(&b"embedded to vec"[..], [0u8; 3]);
        let mut output = VecOutput::default();
        drive(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"embedded to vec");
    }

    #[test]
    fn vec_input_can_feed_embedded_output() {
        let mut input = VecInput::new(b"vec to embedded".to_vec());
        let mut bytes = [0u8; 32];
        let remaining = {
            let mut output = EmbeddedOutput::new(&mut bytes[..], [0u8; 3]);
            drive(&mut input, identity(), &mut output).unwrap();
            output.into_inner().len()
        };
        assert_eq!(&bytes[..bytes.len() - remaining], b"vec to embedded");
    }
}
