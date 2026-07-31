use core::convert::Infallible;

use super::stream_to_stream::{Input, Output};

pub(crate) struct SliceInput<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> SliceInput<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self { Self { bytes, pos: 0 } }
    pub(crate) fn consumed(&self) -> usize { self.pos }
}

impl Input for SliceInput<'_> {
    type Error = Infallible;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        Ok((self.pos < self.bytes.len()).then_some(&self.bytes[self.pos..]))
    }
    fn consume(&mut self, amount: usize) { self.pos += amount; }
}

pub(crate) struct SliceOutput<'a> {
    bytes: &'a mut [u8],
    pos: usize,
}

impl<'a> SliceOutput<'a> {
    pub(crate) fn new(bytes: &'a mut [u8]) -> Self { Self { bytes, pos: 0 } }
    pub(crate) fn written(&self) -> usize { self.pos }
}

impl Output for SliceOutput<'_> {
    type Error = Infallible;
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        Ok((self.pos < self.bytes.len()).then_some(&mut self.bytes[self.pos..]))
    }
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        self.pos += amount;
        Ok(())
    }
}
