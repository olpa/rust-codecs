//! Test doubles for exercising the `Read`/`Write`-style wrapper built
//! on top of a `Source`/`Sink` backend with `shared_io`'s `pump_*`
//! functions — the same codecs `std_io`'s and `embedded_io`'s own test
//! suites use. Enabled for this crate's own tests unconditionally; a
//! third-party backend crate gets it behind the `test-support` feature,
//! so it can reuse these doubles in its own tests instead of
//! reimplementing them.
//!
//! Both types are plain [`Codec`]/[`EndCapableCodec`] implementations —
//! backend-agnostic, since a `CodecReader`/`CodecWriter`-style wrapper
//! only ever drives a codec through that trait, never through anything
//! transport-specific. [`Hoarder`] needs an allocator (behind the
//! `alloc` feature) for its backing `Vec`.

#[cfg(feature = "alloc")]
use crate::{Codec, Progress};
use crate::{Drain, DrainCodec, EndCapableCodec, EndCapableProgress, Error};

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
