//! Generic `Sink` engine shared by every `std::io`/`embedded_io`-style
//! backend: the scratch-buffer/`spare`/`commit` bookkeeping
//! ([`ScratchSink`]) is identical across backends — only how a full
//! buffer actually gets written out, and what that backend's own
//! retry-on-interruption/partial-write looks like, differs. That's
//! the one thing a backend supplies, via [`RetryingWrite`].

use crate::Sink;

/// A backend's "write this whole buffer out", already retrying
/// internally on partial writes and on whatever that backend calls
/// "interrupted". The one piece of backend-specific knowledge
/// [`ScratchSink`] needs.
///
/// `std::io::Write::write_all` already retries on `Interrupted`
/// internally, so a `std::io` backend could delegate `write_all`
/// straight through; `embedded_io::Write::write_all` doesn't, so an
/// `embedded_io` backend's `retrying_write_all` must track its own
/// write position and retry the remainder itself. Both backends drive
/// the same shared `retry_write_all` helper regardless, so `commit`
/// below can trust this call already retries — hence the name.
pub trait RetryingWrite {
    type Error;

    fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error>;

    fn flush(&mut self) -> Result<(), Self::Error>;
}

/// A `Sink` over any [`RetryingWrite`], staging writes in an owned
/// scratch buffer. The transport-independent core of
/// `std_io::StdSink` / `embedded_io::EmbeddedSink`.
pub struct ScratchSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: RetryingWrite, S: AsMut<[u8]>> ScratchSink<W, S> {
    /// Build a `ScratchSink`.
    ///
    /// # Panics
    ///
    /// Panics on an empty `buffer`: it could never hold a byte for
    /// `commit` to write out.
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "ScratchSink buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            offered: 0,
        }
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

    /// Reclaim both the writer and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `ScratchSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: RetryingWrite, S: AsMut<[u8]>> Sink for ScratchSink<W, S> {
    type Error = W::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        let buf = self.buffer.as_mut();
        self.offered = buf.len();
        Ok(Some(buf))
    }

    fn commit(&mut self, amount: usize) -> Result<(), Self::Error> {
        assert!(amount <= self.offered);
        self.inner.retrying_write_all(&self.buffer.as_mut()[..amount])?;
        self.offered = 0;
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }

    fn flush(&mut self) -> Result<(), Self::Error> {
        self.inner.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::{RetryingWrite, ScratchSink};
    use crate::Sink;
    use core::convert::Infallible;

    /// A writer double that counts `flush` calls made on it, to prove
    /// `ScratchSink::finish` actually reaches the wrapped writer.
    #[derive(Default)]
    struct RecordingWriter {
        flushes: usize,
    }

    impl RetryingWrite for RecordingWriter {
        type Error = Infallible;

        fn retrying_write_all(&mut self, _buf: &[u8]) -> Result<(), Self::Error> {
            Ok(())
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            self.flushes += 1;
            Ok(())
        }
    }

    /// A minimal [`RetryingWrite`] over a borrowed byte slice, filling
    /// it left to right — stands in for a real `std::io`/`embedded_io`
    /// writer when testing `ScratchSink` itself. Panics (via the slice
    /// index) if written past capacity; tests using this should size
    /// the slice generously, the same way they'd size a real fixed
    /// buffer.
    struct SliceWriter<'a> {
        remaining: &'a mut [u8],
    }

    impl<'a> RetryingWrite for SliceWriter<'a> {
        type Error = Infallible;

        fn retrying_write_all(&mut self, buf: &[u8]) -> Result<(), Self::Error> {
            let n = buf.len();
            self.remaining[..n].copy_from_slice(buf);
            let remaining = core::mem::take(&mut self.remaining);
            self.remaining = &mut remaining[n..];
            Ok(())
        }

        fn flush(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[test]
    fn spare_offers_the_whole_buffer() {
        let mut bytes = [0u8; 32];
        let mut output = ScratchSink::new(
            SliceWriter {
                remaining: &mut bytes,
            },
            [0u8; 6],
        );
        assert_eq!(output.spare().unwrap().unwrap().len(), 6);
    }

    #[test]
    fn spare_without_commit_is_reissuable() {
        let mut bytes = [0u8; 32];
        let mut output = ScratchSink::new(
            SliceWriter {
                remaining: &mut bytes,
            },
            [0u8; 6],
        );
        let first_len = output.spare().unwrap().unwrap().len();
        let second_len = output.spare().unwrap().unwrap().len();
        assert_eq!(first_len, second_len);
    }

    #[test]
    fn commit_writes_only_the_committed_prefix_through() {
        let mut bytes = [0u8; 32];
        let written = {
            let mut output = ScratchSink::new(
                SliceWriter {
                    remaining: &mut bytes,
                },
                [0u8; 8],
            );
            let spare = output.spare().unwrap().unwrap();
            spare[..5].copy_from_slice(b"abcde");
            output.commit(3).unwrap();
            32 - output.into_inner().remaining.len()
        };
        assert_eq!(&bytes[..written], b"abc");
    }

    #[test]
    #[should_panic]
    fn commit_more_than_offered_panics() {
        let mut bytes = [0u8; 32];
        let mut output = ScratchSink::new(
            SliceWriter {
                remaining: &mut bytes,
            },
            [0u8; 4],
        );
        output.spare().unwrap();
        output.commit(5).unwrap();
    }

    #[test]
    fn finish_flushes_the_inner_writer() {
        let mut output = ScratchSink::new(RecordingWriter { flushes: 0 }, [0u8; 4]);
        output.finish().unwrap();
        assert_eq!(output.get_ref().flushes, 1);
    }

    #[test]
    fn flush_flushes_the_inner_writer() {
        let mut output = ScratchSink::new(RecordingWriter { flushes: 0 }, [0u8; 4]);
        output.flush().unwrap();
        assert_eq!(output.get_ref().flushes, 1);
    }
}
