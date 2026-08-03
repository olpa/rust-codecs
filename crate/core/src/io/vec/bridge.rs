//! Allocation-backed bridge for the lending stream driver.

#[cfg(feature = "alloc")]
use core::convert::Infallible;

#[cfg(feature = "alloc")]
use crate::io::stream_to_stream::{Source, Sink};

/// An owned `Vec<u8>` used directly as an input stream.
#[cfg(feature = "alloc")]
pub struct VecSource {
    inner: alloc::vec::Vec<u8>,
    pos: usize,
}

#[cfg(feature = "alloc")]
impl VecSource {
    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self { inner, pos: 0 }
    }

    pub fn into_inner(self) -> alloc::vec::Vec<u8> {
        self.inner
    }
}

#[cfg(feature = "alloc")]
impl Source for VecSource {
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
pub struct VecSink {
    inner: alloc::vec::Vec<u8>,
    grow_by: usize,
    offered: usize,
}

#[cfg(feature = "alloc")]
impl VecSink {
    pub const DEFAULT_GROWTH: usize = 64 * 1024;

    pub fn new(inner: alloc::vec::Vec<u8>) -> Self {
        Self::with_growth(inner, Self::DEFAULT_GROWTH)
    }

    pub fn with_growth(inner: alloc::vec::Vec<u8>, grow_by: usize) -> Self {
        assert!(grow_by > 0, "VecSink growth must be non-zero");
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
impl Default for VecSink {
    fn default() -> Self {
        Self::new(alloc::vec::Vec::new())
    }
}

#[cfg(feature = "alloc")]
impl Sink for VecSink {
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

#[cfg(all(test, feature = "identity", feature = "alloc"))]
mod tests {
    use super::{VecSource, VecSink};
    use crate::identity::identity;
    use crate::io::stream_to_stream;

    #[test]
    fn vec_to_vec_uses_the_shared_driver() {
        let data = alloc::vec![1, 2, 3, 4, 5];
        let mut input = VecSource::new(data.clone());
        let mut output = VecSink::with_growth(alloc::vec::Vec::new(), 2);

        let totals = stream_to_stream(&mut input, identity(), &mut output).unwrap();

        assert_eq!(totals.consumed, data.len());
        assert_eq!(totals.written, data.len());
        assert_eq!(output.into_inner(), data);
    }
}
