// Backend-independent retry helpers.
//
// These helpers take an `is_interrupted` predicate rather than using an
// `IsInterrupted`-style trait. Supporting every `embedded_io::Error`
// generically with such a trait would require a blanket implementation, while
// `std::io::Error` would require a concrete implementation. Those
// implementations cannot coexist: `embedded_io` could implement its `Error`
// trait for `std::io::Error` in a future release, making them overlap.
//
// Requiring individual implementations instead would burden every backend
// error type and prevent generic support for arbitrary `embedded_io` errors.
// Passing a backend-specific predicate avoids this coherence problem while
// keeping the retry loops shared.

/// Retry `attempt` for as long as `is_interrupted` says its error is
/// interrupted, returning the first result that either succeeds or
/// fails for a real reason.
pub fn retry_on_interrupted<T, E>(
    mut attempt: impl FnMut() -> Result<T, E>,
    is_interrupted: impl Fn(&E) -> bool,
) -> Result<T, E> {
    loop {
        match attempt() {
            Ok(value) => return Ok(value),
            Err(e) if is_interrupted(&e) => continue,
            Err(e) => return Err(e),
        }
    }
}

/// Wrapper over `fill_buf`, with implementation complications forced
/// by borrow-checker limitations, resolved by calling `fill_buf`
/// twice. That's fine: the underlying buffer stays available,
/// unconsumed, until `consume` is called, so the second call is a
/// free re-borrow, not a new read.
pub fn retry_fill_buf<T, E>(
    target: &mut T,
    mut fill_buf: impl FnMut(&mut T) -> Result<&[u8], E>,
    is_interrupted: impl Fn(&E) -> bool,
) -> Result<&[u8], E> {
    loop {
        match fill_buf(target) {
            Ok([]) => {
                // Returned immediately, without the second call below: a
                // `fill_buf`-style reader doesn't latch EOF, so retrying
                // past a transient empty result would silently skip it
                // (a growing file/pipe: "nothing right now" isn't
                // necessarily EOF).
                return Ok(&[]);
            }
            Ok(_) => {
                // A borrow-checker limitation: NLL "loop + early-return", RFC 2094's problem case #3.
                break;
            }
            Err(e) if is_interrupted(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    fill_buf(target)
}

/// `write_all` reimplementation, calling `write` retryable.
pub fn retry_write_all<T, E>(
    target: &mut T,
    mut write: impl FnMut(&mut T, &[u8]) -> Result<usize, E>,
    mut buf: &[u8],
    is_interrupted: impl Fn(&E) -> bool,
    zero_write_error: impl FnOnce() -> E,
) -> Result<(), E> {
    while !buf.is_empty() {
        match retry_on_interrupted(|| write(target, buf), &is_interrupted)? {
            0 => return Err(zero_write_error()),
            n => buf = &buf[n..],
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{retry_fill_buf, retry_on_interrupted, retry_write_all};

    fn is_interrupted(e: &&str) -> bool {
        *e == "eintr"
    }

    /// Yields `data` one byte per real call, alternating an
    /// `Interrupted` error in between every two real calls (real,
    /// eintr, real, eintr, ...).
    struct FlakyBytes<'a> {
        remaining: &'a [u8],
        attempts: usize,
    }

    impl<'a> FlakyBytes<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self {
                remaining: data,
                attempts: 0,
            }
        }

        fn read_one(&mut self) -> Result<Option<u8>, &'static str> {
            self.attempts += 1;
            if self.attempts.is_multiple_of(2) {
                return Err("eintr");
            }
            let Some((&byte, rest)) = self.remaining.split_first() else {
                return Ok(None);
            };
            self.remaining = rest;
            Ok(Some(byte))
        }
    }

    #[test]
    fn retry_on_interrupted_reads_hello_through_eintr() {
        let mut source = FlakyBytes::new(b"hello");
        let mut got = Vec::new();
        while let Some(byte) = retry_on_interrupted(|| source.read_one(), is_interrupted).unwrap() {
            got.push(byte);
        }
        assert_eq!(got, b"hello");
    }

    /// The `fill_buf`-shaped counterpart to [`FlakyBytes`]: a fetched
    /// byte is held as "pending" and handed back on every call until
    /// `consume` clears it, since a real `fill_buf` doesn't forget an
    /// unconsumed byte just because it's asked for again — only
    /// fetching a *new* byte can hit an `Interrupted`.
    struct FlakyFillBuf<'a> {
        remaining: &'a [u8],
        pending: Option<u8>,
        attempts: usize,
        one: [u8; 1],
    }

    impl<'a> FlakyFillBuf<'a> {
        fn new(data: &'a [u8]) -> Self {
            Self {
                remaining: data,
                pending: None,
                attempts: 0,
                one: [0],
            }
        }

        fn fill_buf(&mut self) -> Result<&[u8], &'static str> {
            let byte = match self.pending {
                Some(byte) => byte,
                None => {
                    self.attempts += 1;
                    if self.attempts.is_multiple_of(2) {
                        return Err("eintr");
                    }
                    let Some((&byte, rest)) = self.remaining.split_first() else {
                        return Ok(&[]);
                    };
                    self.remaining = rest;
                    self.pending = Some(byte);
                    byte
                }
            };
            self.one[0] = byte;
            Ok(&self.one[..])
        }

        fn consume(&mut self, amount: usize) {
            if amount > 0 {
                self.pending = None;
            }
        }
    }

    #[test]
    fn retry_fill_buf_reads_hello_through_eintr() {
        let mut source = FlakyFillBuf::new(b"hello");
        let mut got = Vec::new();
        loop {
            let len = {
                let buf =
                    retry_fill_buf(&mut source, FlakyFillBuf::fill_buf, is_interrupted).unwrap();
                if buf.is_empty() {
                    break;
                }
                got.extend_from_slice(buf);
                buf.len()
            };
            source.consume(len);
        }
        assert_eq!(got, b"hello");
    }

    #[test]
    fn retry_fill_buf_returns_empty_immediately_at_eof() {
        // An empty `Ok` is EOF, not "try again": returned straight
        // from the loop, without the extra re-fetch call a non-empty
        // result needs.
        let mut source = FlakyFillBuf::new(b"");
        let buf = retry_fill_buf(&mut source, FlakyFillBuf::fill_buf, is_interrupted).unwrap();
        assert!(buf.is_empty());
        assert_eq!(source.attempts, 1);
    }

    /// Accepts one byte of whatever `buf` offers per real call,
    /// alternating an `Interrupted` error in between every two real
    /// calls, the same pattern as [`FlakyBytes`].
    struct FlakyWriter {
        written: Vec<u8>,
        attempts: usize,
    }

    impl FlakyWriter {
        fn new() -> Self {
            Self {
                written: Vec::new(),
                attempts: 0,
            }
        }

        fn write_one(&mut self, buf: &[u8]) -> Result<usize, &'static str> {
            self.attempts += 1;
            if self.attempts.is_multiple_of(2) {
                return Err("eintr");
            }
            self.written.push(buf[0]);
            Ok(1)
        }
    }

    #[test]
    fn retry_write_all_writes_hello_through_eintr() {
        let mut sink = FlakyWriter::new();
        retry_write_all(
            &mut sink,
            FlakyWriter::write_one,
            b"hello",
            is_interrupted,
            || "unreachable",
        )
        .unwrap();
        assert_eq!(sink.written, b"hello");
    }
}
