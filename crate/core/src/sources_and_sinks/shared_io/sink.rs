use core::mem::MaybeUninit;

use crate::uninit::as_uninit_mut;
use crate::{EmptyBufferError, Sink};

/// A backend's "write this whole buffer out", already retrying
/// internally on partial writes and on whatever that backend calls
/// "interrupted".
pub trait RetryingWrite {
    type Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error>;
}

/// A `Sink` over any [`RetryingWrite`], staging writes in an owned
/// scratch buffer.
pub struct ScratchSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: RetryingWrite, S: AsMut<[u8]>> ScratchSink<W, S> {
    /// Build a `ScratchSink`.
    ///
    /// # Errors
    ///
    /// Fails on an empty `buffer`.
    pub fn new(inner: W, mut buffer: S) -> Result<Self, EmptyBufferError> {
        if buffer.as_mut().is_empty() {
            return Err(EmptyBufferError);
        }
        Ok(Self {
            inner,
            buffer,
            offered: 0,
        })
    }

    pub fn get_ref(&self) -> &W {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Reclaim the writer, discarding the scratch buffer and any bytes
    /// staged in it via `spare` but not yet handed to `commit` — they
    /// are not written to `inner`.
    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Reclaim both the writer and the scratch buffer. If the buffer
    /// holds uncommitted bytes, treat them as lost.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: RetryingWrite, S: AsMut<[u8]>> Sink for ScratchSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error> {
        let buf = self.buffer.as_mut();
        self.offered = buf.len();
        Ok(Some(as_uninit_mut(buf)))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        debug_assert!(
            amount <= self.offered,
            "commit({amount}) exceeds the {} bytes offered by spare()",
            self.offered
        );
        let amount = amount.min(self.offered);
        self.inner
            .retrying_write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryingWrite, ScratchSink};
    use crate::uninit::copy_to_uninit;
    use crate::Sink;
    use core::convert::Infallible;

    /// A writer double that records the bytes it gets, to prove that
    /// `ScratchSink` actually reaches the wrapped writer. Panics if a
    /// test writes more than 32 bytes.
    #[derive(Default)]
    struct RecordingWriter {
        bytes: [u8; 32],
        written: usize,
    }

    impl RetryingWrite for RecordingWriter {
        type Error = Infallible;

        fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
            let end = self.written + buf.len();
            self.bytes[self.written..end].copy_from_slice(buf);
            self.written = end;
            Ok(())
        }
    }

    #[test]
    fn spare_offers_the_whole_buffer() {
        let mut output = ScratchSink::new(RecordingWriter::default(), [0u8; 6]).unwrap();
        assert_eq!(output.spare().unwrap().unwrap().len(), 6);
    }

    #[test]
    fn spare_without_commit_is_reissuable() {
        let mut output = ScratchSink::new(RecordingWriter::default(), [0u8; 6]).unwrap();
        let first_len = output.spare().unwrap().unwrap().len();
        let second_len = output.spare().unwrap().unwrap().len();
        assert_eq!(first_len, second_len);
    }

    #[test]
    fn commit_writes_only_the_committed_prefix_through() {
        let mut output = ScratchSink::new(RecordingWriter::default(), [0u8; 8]).unwrap();
        let spare = output.spare().unwrap().unwrap();
        copy_to_uninit(&mut spare[..5], b"abcde");
        output.commit(3).unwrap();
        let inner = output.get_ref();
        assert_eq!(&inner.bytes[..inner.written], b"abc");
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic]
    fn commit_more_than_offered_panics_in_debug() {
        let mut output = ScratchSink::new(RecordingWriter::default(), [0u8; 4]).unwrap();
        output.spare().unwrap();
        output.commit(5).unwrap();
    }

    #[test]
    #[cfg(not(debug_assertions))]
    fn commit_more_than_offered_clamps_in_release() {
        let mut output = ScratchSink::new(RecordingWriter::default(), [0u8; 4]).unwrap();
        output.spare().unwrap();
        output.commit(5).unwrap();
        let inner = output.get_ref();
        assert_eq!(inner.written, 4);
    }
}
