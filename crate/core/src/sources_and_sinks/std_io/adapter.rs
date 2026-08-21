use std::io::{BufRead, Read, Write};

use crate::{Sink, Source};

pub struct StdSource<R, S> {
    inner: R,
    buffer: S,
    pos: usize,
    len: usize,
    eof: bool,
}

impl<R: Read, S: AsMut<[u8]>> StdSource<R, S> {
    pub fn new(inner: R, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSource buffer must be non-empty"
        );
        Self {
            inner,
            buffer,
            pos: 0,
            len: 0,
            eof: false,
        }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Reclaim the reader, discarding the scratch buffer and any
    /// buffered, unconsumed bytes (already read from `inner` into the
    /// buffer via `chunk`, but not yet passed to `consume`).
    pub fn into_inner(self) -> R {
        self.inner
    }

    /// Reclaim both the reader and the scratch buffer, e.g. to reuse
    /// the buffer's allocation for another `StdSource`. Any buffered,
    /// unconsumed bytes (already read from `inner` into the buffer via
    /// `chunk`, but not yet passed to `consume`) are discarded along
    /// with it.
    pub fn into_parts(self) -> (R, S) {
        (self.inner, self.buffer)
    }
}

impl<R: Read, S: AsMut<[u8]>> Source for StdSource<R, S> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Only refills once the current window is fully consumed; an
        // unconsumed remainder overlaps the previous call's chunk.
        if self.pos == self.len && !self.eof {
            // `Read::read` doesn't retry `Interrupted` itself (unlike
            // `write_all`'s default impl), so a signal landing mid-read
            // must be retried here — matching `std::io::copy`.
            self.len = loop {
                match self.inner.read(self.buffer.as_mut()) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e),
                }
            };
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

/// A `std::io::BufRead` used directly as an input stream, with no
/// scratch buffer of its own.
///
/// Unlike [`StdSource`], which owns a buffer and calls `Read::read`
/// into it, this forwards straight to `fill_buf`/`consume` — `BufRead`
/// is already a lending API with the same shape as [`Source`], so a
/// reader that already implements it (`BufReader`, `&[u8]`,
/// `Cursor<Vec<u8>>`, `VecDeque<u8>`, ...) can be adapted with no extra
/// copy. Wrapping an `R` that doesn't already buffer (a raw `File` or
/// `TcpStream`) in a `BufReader` first, then in this, is the equivalent
/// of `StdSource` minus the double allocation.
pub struct BufReadSource<R> {
    inner: R,
    eof: bool,
}

impl<R: BufRead> BufReadSource<R> {
    pub fn new(inner: R) -> Self {
        Self { inner, eof: false }
    }

    pub fn get_ref(&self) -> &R {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: BufRead> Source for BufReadSource<R> {
    type Error = std::io::Error;

    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
        // Once a fill has come back empty, latch it: `Pump` calls
        // `chunk()` again on every subsequent driver call for as long as
        // the caller keeps asking (a plain `Codec` never reports an
        // in-band `End`, so nothing else stops that), and re-running
        // `fill_buf` on an already-exhausted reader would re-enter its
        // real-I/O path on every one of those calls — an unbounded
        // stream of pointless syscalls on a `BufReader<File>` or
        // `BufReader<TcpStream>`. `StdSource` avoids this the same way.
        if self.eof {
            return Ok(None);
        }

        // `fill_buf` already returns the current unconsumed window
        // without over-reading, and an empty result means EOF — the
        // same contract `Source::chunk` asks for, so no bookkeeping of
        // our own is needed beyond the latch above. It also doesn't
        // retry `Interrupted` itself, so a signal landing mid-fill must
        // be retried here — matching `std::io::copy`.
        //
        // The retry loop can't return the borrowed slice directly from
        // within its own `match` (returning a borrow across a `continue`
        // edge doesn't pass borrowck), so it retries against an owned
        // length instead, then a final, non-looping call reads out the
        // now-settled buffer. That second call is only made when `len`
        // is non-zero: a non-zero length proves the buffer still has
        // that many unconsumed bytes (nothing else touched `self.inner`
        // in between), so `fill_buf` takes its no-I/O fast path and
        // can't itself need retrying. Skipping it on `len == 0` avoids
        // triggering a second, unguarded real read on the EOF/boundary
        // case — one that could hit its own `Interrupted` and propagate
        // unretried, undoing the fix above.
        let len = loop {
            match self.inner.fill_buf() {
                Ok(buf) => break buf.len(),
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e),
            }
        };
        if len == 0 {
            self.eof = true;
            return Ok(None);
        }
        let buf = self.inner.fill_buf()?;
        Ok(Some(buf))
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}

pub struct StdSink<W, S> {
    inner: W,
    buffer: S,
    offered: usize,
}

impl<W: Write, S: AsMut<[u8]>> StdSink<W, S> {
    pub fn new(inner: W, mut buffer: S) -> Self {
        assert!(
            !buffer.as_mut().is_empty(),
            "StdSink buffer must be non-empty"
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
    /// the buffer's allocation for another `StdSink`. Any bytes
    /// staged in the buffer via `spare` but not yet handed to `commit`
    /// are discarded along with it, and are not written to `inner`.
    pub fn into_parts(self) -> (W, S) {
        (self.inner, self.buffer)
    }
}

impl<W: Write, S: AsMut<[u8]>> Sink for StdSink<W, S> {
    type Error = std::io::Error;

    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error> {
        let buf = self.buffer.as_mut();
        self.offered = buf.len();
        Ok(Some(buf))
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

#[cfg(test)]
mod tests {
    use std::io::{BufReader, Cursor, Read};

    use super::{BufReadSource, StdSink, StdSource};
    use crate::identity::identity;
    use crate::sources_and_sinks::vec::{VecSink, VecSource};
    use crate::stream_to_stream;
    use crate::Source;

    /// Fails its first `read` with `Interrupted`, then delegates.
    struct FlakyOnce<R> {
        inner: R,
        failed: bool,
    }

    impl<R: Read> Read for FlakyOnce<R> {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if !self.failed {
                self.failed = true;
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "eintr",
                ));
            }
            self.inner.read(buf)
        }
    }

    #[test]
    fn std_source_retries_an_interrupted_read() {
        let flaky = FlakyOnce {
            inner: Cursor::new(b"retry me".as_slice()),
            failed: false,
        };
        let mut input = StdSource::new(flaky, [0u8; 8]);
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"retry me");
    }

    #[test]
    fn buf_read_source_retries_an_interrupted_fill() {
        // `BufReader::fill_buf` calls the wrapped `Read::read` directly
        // when its own buffer is empty, so `FlakyOnce`'s interruption
        // surfaces through `fill_buf` too.
        let flaky = FlakyOnce {
            inner: Cursor::new(b"retry me too".as_slice()),
            failed: false,
        };
        let mut input = BufReadSource::new(BufReader::new(flaky));
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"retry me too");
    }

    /// Yields `b"hi"`, then a genuine EOF (`Ok(0)`), then errors with
    /// `Interrupted` on any further call — standing in for the second,
    /// unguarded `fill_buf` call `chunk` must never make once it has
    /// already seen a zero-length fill.
    struct EofThenPoisoned {
        calls: usize,
    }

    impl Read for EofThenPoisoned {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            self.calls += 1;
            match self.calls {
                1 => {
                    buf[..2].copy_from_slice(b"hi");
                    Ok(2)
                }
                2 => Ok(0),
                _ => Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "chunk() must not fill_buf again after a zero-length fill",
                )),
            }
        }
    }

    #[test]
    fn buf_read_source_does_not_refill_after_a_zero_length_fill() {
        let mut input = BufReadSource::new(BufReader::new(EofThenPoisoned { calls: 0 }));
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"hi");
    }

    #[test]
    fn buf_read_source_latches_eof_across_repeated_chunk_calls() {
        // `stream_to_stream` stops calling `chunk()` once it sees `None`,
        // so this drives it directly — standing in for a plain `Codec`
        // driven through `BufReadCodecReader`, where `Pump::is_done()`
        // never latches and every `Read::read()` re-enters `chunk()`
        // regardless of the source having ended already.
        let mut input = BufReadSource::new(BufReader::new(EofThenPoisoned { calls: 0 }));
        assert_eq!(input.chunk().unwrap(), Some(b"hi".as_slice()));
        input.consume(2);
        assert_eq!(input.chunk().unwrap(), None);
        // A third call to the wrapped reader would return `Interrupted`;
        // the `eof` latch must keep it from ever being made.
        assert_eq!(input.chunk().unwrap(), None);
        assert_eq!(input.chunk().unwrap(), None);
    }

    #[test]
    fn std_input_can_feed_vec_output() {
        let mut input = StdSource::new(Cursor::new(b"std to vec"), [0u8; 3]);
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"std to vec");
    }

    #[test]
    fn buf_read_input_can_feed_vec_output() {
        // `Cursor<&[u8]>` implements `BufRead`, so no scratch buffer is
        // supplied here, unlike `StdSource` above.
        let mut input = BufReadSource::new(Cursor::new(b"bufread to vec".as_slice()));
        let mut output = VecSink::default();
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"bufread to vec");
    }

    #[test]
    fn vec_input_can_feed_std_output() {
        let mut input = VecSource::new(b"vec to std".to_vec());
        let mut output = StdSink::new(Vec::new(), [0u8; 3]);
        stream_to_stream(&mut input, identity(), &mut output).unwrap();
        assert_eq!(output.into_inner(), b"vec to std");
    }
}
