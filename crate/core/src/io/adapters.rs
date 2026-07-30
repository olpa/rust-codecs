//! Allocation-backed and iterator adapters for the lending stream driver.

#[cfg(feature = "alloc")]
use core::convert::Infallible;

use super::stream_to_stream::{Input, Output};

/// An owned `Vec<u8>` used directly as an input stream.
#[cfg(feature = "alloc")]
pub struct VecInput {
    inner: alloc::vec::Vec<u8>,
    pos: usize,
}

#[cfg(feature = "alloc")]
impl VecInput {
    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self { inner, pos: 0 }
    }

    pub fn into_inner(self) -> alloc::vec::Vec<u8> {
        self.inner
    }
}

#[cfg(feature = "alloc")]
impl Input for VecInput {
    type Error = Infallible;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        Ok((self.pos < self.inner.len()).then_some(&self.inner[self.pos..]))
    }

    fn consume(&mut self, amount: usize) {
        assert!(amount <= self.inner.len() - self.pos);
        self.pos += amount;
    }
}

/// A `Vec<u8>` output stream. Codec output is written straight into
/// the vector's spare allocation without zero-initializing it first.
#[cfg(feature = "alloc")]
pub struct VecOutput {
    inner: alloc::vec::Vec<u8>,
    grow_by: usize,
    offered: usize,
}

#[cfg(feature = "alloc")]
impl VecOutput {
    pub const DEFAULT_GROWTH: usize = 64 * 1024;

    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self::with_growth(inner, Self::DEFAULT_GROWTH)
    }

    pub fn with_growth(inner: alloc::vec::Vec<u8>, grow_by: usize) -> Self {
        assert!(grow_by > 0, "VecOutput growth must be non-zero");
        Self {
            inner,
            grow_by,
            offered: 0,
        }
    }

    pub fn into_inner(self) -> alloc::vec::Vec<u8> {
        self.inner
    }
}

#[cfg(feature = "alloc")]
impl Default for VecOutput {
    fn default() -> Self {
        Self::new(alloc::vec::Vec::new())
    }
}

#[cfg(feature = "alloc")]
impl Output for VecOutput {
    type Error = Infallible;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        assert_eq!(self.offered, 0, "commit must follow spare");
        if self.inner.spare_capacity_mut().is_empty() {
            self.inner.reserve(self.grow_by);
        }
        let spare = self.inner.spare_capacity_mut();
        self.offered = spare.len();
        // SAFETY: this deliberately lends uninitialized `u8` storage to
        // `Codec::process`/`finish`. The codec contract permits claiming
        // only the prefix it initialized; `commit` is the sole operation
        // that adds that prefix to the Vec's length. Codecs must treat the
        // output slice as write-only before initialization.
        Ok(Some(unsafe {
            core::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), spare.len())
        }))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        // SAFETY: the codec reported that exactly this prefix was written.
        unsafe { self.inner.set_len(self.inner.len() + amount) };
        self.offered = 0;
        Ok(())
    }
}

/// Turns an iterator of fallible byte chunks into an [`Input`].
pub struct IteratorInput<I, B> {
    iter: I,
    current: Option<(B, usize)>,
}

impl<I, B> IteratorInput<I, B> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            current: None,
        }
    }
}

impl<I, B, E> Input for IteratorInput<I, B>
where
    I: Iterator<Item = Result<B, E>>,
    B: AsRef<[u8]>,
{
    type Error = E;

    fn chunk(&mut self) -> Result<Option<&[u8]>, E> {
        while self
            .current
            .as_ref()
            .is_none_or(|(chunk, pos)| *pos == chunk.as_ref().len())
        {
            self.current = match self.iter.next().transpose()? {
                Some(chunk) => Some((chunk, 0)),
                None => return Ok(None),
            };
        }
        let (chunk, pos) = self.current.as_ref().expect("current chunk");
        Ok(Some(&chunk.as_ref()[*pos..]))
    }

    fn consume(&mut self, amount: usize) {
        let (chunk, pos) = self.current.as_mut().expect("consume without chunk");
        assert!(amount <= chunk.as_ref().len() - *pos);
        *pos += amount;
    }
}

/// Turns an iterator of fallible writable slots into an [`Output`].
pub struct IteratorOutput<I, S> {
    iter: I,
    current: Option<(S, usize)>,
}

impl<I, S> IteratorOutput<I, S> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            current: None,
        }
    }
}

impl<I, S, E> Output for IteratorOutput<I, S>
where
    I: Iterator<Item = Result<S, E>>,
    S: AsMut<[u8]>,
{
    type Error = E;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, E> {
        let spent = self
            .current
            .as_mut()
            .is_some_and(|(slot, pos)| *pos == slot.as_mut().len());
        if spent {
            self.current = None;
        }
        if self.current.is_none() {
            self.current = self.iter.next().transpose()?.map(|slot| (slot, 0));
        }
        Ok(self
            .current
            .as_mut()
            .map(|(slot, pos)| &mut slot.as_mut()[*pos..]))
    }

    fn commit(&mut self, amount: usize) -> Result<(), E> {
        let (slot, pos) = self.current.as_mut().expect("commit without slot");
        assert!(amount <= slot.as_mut().len() - *pos);
        *pos += amount;
        Ok(())
    }
}

#[cfg(all(test, feature = "identity", feature = "alloc"))]
mod tests {
    use super::{VecInput, VecOutput};
    use crate::identity::identity;
    use crate::io::drive;

    #[test]
    fn vec_to_vec_uses_the_shared_driver() {
        let data = alloc::vec![1, 2, 3, 4, 5];
        let mut input = VecInput::new(data.clone());
        let mut output = VecOutput::with_growth(alloc::vec::Vec::new(), 2);

        let totals = drive(&mut input, identity(), &mut output).unwrap();

        assert_eq!(totals.consumed, data.len());
        assert_eq!(totals.written, data.len());
        assert_eq!(output.into_inner(), data);
    }
}
