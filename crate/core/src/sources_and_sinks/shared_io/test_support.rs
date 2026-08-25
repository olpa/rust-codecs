//! Test doubles for exercising a `Source`/`Sink` backend and the
//! `Read`/`Write`-style wrapper built on top of it with `shared_io`'s
//! `pump_*` functions — the same codecs and endpoints `std_io`'s and
//! `embedded_io`'s own test suites use. Enabled for this crate's own
//! tests unconditionally; a third-party backend crate gets it behind
//! the `test-support` feature, so it can reuse these doubles in its
//! own tests instead of reimplementing them.
//!
//! None of these types depend on `std` or `embedded_io` themselves —
//! only [`Hoarder`] needs an allocator (behind the `alloc` feature),
//! for its backing `Vec`. [`RecordingWriter`] and [`CountingReader`]
//! are generic over any backend, native-trait or
//! [`RetryingRead`]/[`RetryingWrite`]-based: each backend supplies its
//! own `Read`/`Write` trait impl over them for testing its own native
//! wiring, while [`SliceReader`], [`SliceWriter`] and
//! [`GrowsAfterAnEmptyFill`] — plus the `RetryingRead`/
//! `RetryingFillBuf`/`RetryingWrite` impls on `CountingReader`/
//! `RecordingWriter` themselves — exercise `ScratchSource`/
//! `LendingSource`/`ScratchSink` directly, so a backend built on those
//! shared engines (this crate's own `std_io`/`embedded_io`, or a
//! third party's) doesn't need to reprove their bookkeeping itself.

use core::convert::Infallible;

#[cfg(feature = "alloc")]
use crate::{Codec, Progress};
use crate::{Drain, DrainCodec, EndCapableCodec, EndCapableProgress, Error};

use super::{RetryingFillBuf, RetryingRead, RetryingWrite};

/// An [`EndCapableCodec`] that copies bytes 1:1 but ends its stream
/// after `limit` bytes, like a self-describing format with an in-band
/// terminator.
///
/// A genuine `EndCapableCodec` (not [`Codec`](crate::Codec) — a `Codec`
/// can never report an in-band end), so only a reader bound to
/// `EndCapableCodec` can drive it; a writer bound to `Codec` rejects it
/// at compile time. Use it to prove a reader wrapper stops yielding
/// bytes and reports EOF right after the codec's in-band end, without
/// touching the codec again.
pub struct EarlyEnd {
    /// How many bytes to copy before reporting
    /// [`EndCapableProgress::End`].
    pub limit: usize,
    /// How many bytes have been copied so far. Start this at `0`.
    pub done: usize,
}

impl DrainCodec for EarlyEnd {
    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}

impl EndCapableCodec for EarlyEnd {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<EndCapableProgress, Error> {
        let remaining = self.limit - self.done;
        let n = input.len().min(output.len()).min(remaining);
        output[..n].copy_from_slice(&input[..n]);
        self.done += n;
        if self.done >= self.limit {
            Ok(EndCapableProgress::End {
                consumed: n,
                written: n,
            })
        } else if n == input.len() {
            Ok(EndCapableProgress::InputConsumed { written: n })
        } else {
            Ok(EndCapableProgress::OutputFilled { consumed: n })
        }
    }
}

/// A writer double that counts `flush` calls made on it, to prove a
/// `Sink::finish` implementation actually reaches the wrapped writer.
/// Start `flushes` at `0` (or use `Default`); each backend supplies its
/// own `Write`-trait impl over this struct, since the trait itself
/// (`std::io::Write`, `embedded_io::Write`, ...) is what's
/// backend-specific. It's also directly [`RetryingWrite`], for testing
/// [`ScratchSink`](super::ScratchSink) itself.
#[derive(Default)]
pub struct RecordingWriter {
    /// How many times `flush` has been called.
    pub flushes: usize,
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

/// Wraps a reader, counting how many times `read` was actually called
/// on it — lets a test prove a `Source::chunk` implementation didn't
/// refill ahead of its consumed position (the `Source` contract point
/// that new bytes must not be handed out until the old ones are
/// released via `consume`). Each backend supplies its own
/// `Read`-trait impl over this struct, forwarding to `inner` and
/// incrementing `reads`; it's also generically [`RetryingRead`] over
/// any `R: RetryingRead`, for testing
/// [`ScratchSource`](super::ScratchSource) itself.
pub struct CountingReader<R> {
    /// The wrapped reader.
    pub inner: R,
    /// How many times `read` has been called on `inner`. Start this
    /// at `0`.
    pub reads: usize,
}

impl<R: RetryingRead> RetryingRead for CountingReader<R> {
    type Error = R::Error;

    fn retrying_read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        self.reads += 1;
        self.inner.retrying_read(buf)
    }
}

/// A minimal [`RetryingRead`]/[`RetryingFillBuf`] over a borrowed byte
/// slice — stands in for a real `std::io`/`embedded_io` reader when
/// testing [`ScratchSource`](super::ScratchSource)/
/// [`LendingSource`](super::LendingSource) themselves, which don't
/// care which backend supplies bytes.
pub struct SliceReader<'a> {
    pub bytes: &'a [u8],
}

impl<'a> RetryingRead for SliceReader<'a> {
    type Error = Infallible;

    fn retrying_read(&mut self, buf: &mut [u8]) -> Result<usize, Self::Error> {
        let n = buf.len().min(self.bytes.len());
        buf[..n].copy_from_slice(&self.bytes[..n]);
        self.bytes = &self.bytes[n..];
        Ok(n)
    }
}

impl<'a> RetryingFillBuf for SliceReader<'a> {
    type Error = Infallible;

    fn retrying_fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        Ok(self.bytes)
    }

    fn consume(&mut self, amount: usize) {
        self.bytes = &self.bytes[amount..];
    }
}

/// A minimal [`RetryingWrite`] over a borrowed byte slice, filling it
/// left to right — stands in for a real `std::io`/`embedded_io` writer
/// when testing [`ScratchSink`](super::ScratchSink) itself. Panics
/// (via the slice index) if written past capacity; tests using this
/// should size the slice generously, the same way they'd size a real
/// fixed buffer.
pub struct SliceWriter<'a> {
    pub remaining: &'a mut [u8],
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

/// A [`RetryingFillBuf`] that yields `b"hi"`, then an empty fill, then
/// `b"more"` — stands in for a transport whose "nothing right now"
/// isn't forever (a growing file, a pipe), proving a `LendingSource`
/// doesn't latch itself shut after a single empty fill. Implements
/// `RetryingFillBuf` directly (rather than relying on a `BufReader`-
/// style cache the way a plain `Read`-based version would) so it works
/// the same for every backend, including ones with no such adapter.
#[derive(Default)]
pub struct GrowsAfterAnEmptyFill {
    stage: usize,
    buf: &'static [u8],
}

impl RetryingFillBuf for GrowsAfterAnEmptyFill {
    type Error = Infallible;

    fn retrying_fill_buf(&mut self) -> Result<&[u8], Self::Error> {
        if self.buf.is_empty() {
            self.buf = match self.stage {
                0 => b"hi",
                1 => b"",
                2 => b"more",
                _ => b"",
            };
            self.stage += 1;
        }
        Ok(self.buf)
    }

    fn consume(&mut self, amount: usize) {
        self.buf = &self.buf[amount..];
    }
}

/// A [`Codec`] that buffers all input internally, emitting only on
/// `flush`/`finish` — exercises the "flush is a resumable sync point,
/// finish ends the stream" distinction a `CodecWriter`-style wrapper's
/// docs typically draw.
///
/// Needs the `alloc` feature for its backing `Vec`; a backend built
/// without it (a bare `embedded-io`, no `std`/`alloc`) can't use this
/// one.
#[cfg(feature = "alloc")]
#[derive(Default)]
pub struct Hoarder {
    buf: alloc::vec::Vec<u8>,
}

#[cfg(feature = "alloc")]
impl Hoarder {
    fn emit(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        let n = self.buf.len().min(output.len());
        output[..n].copy_from_slice(&self.buf[..n]);
        self.buf.drain(..n);
        if self.buf.is_empty() {
            Ok(Drain::Done { written: n })
        } else {
            Ok(Drain::OutputFilled)
        }
    }
}

#[cfg(feature = "alloc")]
impl DrainCodec for Hoarder {
    fn finish(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        self.emit(output)
    }

    fn flush(&mut self, output: &mut [u8]) -> Result<Drain, Error> {
        self.emit(output)
    }
}

#[cfg(feature = "alloc")]
impl Codec for Hoarder {
    fn process(&mut self, input: &[u8], _output: &mut [u8]) -> Result<Progress, Error> {
        self.buf.extend_from_slice(input);
        Ok(Progress::InputConsumed { written: 0 })
    }
}
