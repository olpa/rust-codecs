//! A single "retry this fallible operation until it succeeds or fails
//! for a real reason" loop, shared by every backend's `RetryingRead`/
//! `RetryingWrite` impl. The loop body is identical across backends —
//! only what counts as "interrupted" for a given error type differs
//! (`std::io::ErrorKind::Interrupted`, `embedded_io::ErrorKind::Interrupted`,
//! ...), so that's passed in as `is_interrupted` rather than baked in
//! here: a blanket "does this error type count as interrupted" trait
//! would need one impl per backend error type, and — since a future
//! `embedded_io` release could in principle implement its `Error`
//! trait for `std::io::Error` — the compiler won't let a blanket impl
//! over `E: embedded_io::Error` coexist with a concrete impl for
//! `std::io::Error` (E0119), even though that overlap will never
//! actually happen. A plain predicate sidesteps that entirely.

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

/// The `fill_buf`-shaped counterpart to [`retry_on_interrupted`] — a
/// `fill_buf` call returns a slice borrowed from `target`, so it can't
/// be handed in as a plain `FnMut() -> Result<&[u8], E>` closure
/// capturing `target` the way `retry_on_interrupted`'s `attempt` can:
/// a closure can't express "call this again, yielding a fresh borrow
/// tied to the new call" over a captured `&mut`. Taking `target` and
/// `fill_buf` as separate arguments sidesteps that: each call
/// reborrows `target` fresh, so `fill_buf` only ever needs to be a
/// plain `fn(&mut T) -> Result<&[u8], E>`-shaped closure with no
/// captures of its own (e.g. `|r| r.fill_buf()`).
///
/// An `Ok` empty slice is returned immediately, without a second call:
/// a `fill_buf`-style reader doesn't latch EOF, so retrying past a
/// transient empty result here would silently skip it (a growing
/// file/pipe: "nothing right now" isn't necessarily EOF). A non-empty
/// `Ok` result is safe — and, separately, necessary — to re-fetch: the
/// underlying buffer stays available, unconsumed, until `consume` is
/// called, so the second call is a free re-borrow, not a new read; and
/// that second call is the only way to return the buffer at all, since
/// a borrow-checker limitation doesn't allow returning it directly out
/// of the loop below.
pub fn retry_fill_buf<T, E>(
    target: &mut T,
    mut fill_buf: impl FnMut(&mut T) -> Result<&[u8], E>,
    is_interrupted: impl Fn(&E) -> bool,
) -> Result<&[u8], E> {
    loop {
        match fill_buf(target) {
            Ok([]) => return Ok(&[]),
            Ok(_) => break,
            Err(e) if is_interrupted(&e) => continue,
            Err(e) => return Err(e),
        }
    }
    fill_buf(target)
}

/// The `write`-shaped counterpart to [`retry_on_interrupted`]: writes
/// the whole of `buf`, retrying both on an interrupted call and on a
/// partial write (a single `write` call is free to write fewer bytes
/// than offered, same as `std::io::Write::write`/
/// `embedded_io::Write::write`).
///
/// `std::io::Write::write_all` already does this internally, so a
/// `std::io` backend *could* just delegate straight to it — but doing
/// that here too, rather than only where a backend's own `write_all`
/// doesn't retry (`embedded_io`'s doesn't), keeps both backends
/// exercising the same, one, position-tracking retry loop instead of
/// splitting into "trust the backend's write_all" and "hand-roll it"
/// cases.
///
/// `zero_write_error` builds the error to fail with if `write` returns
/// `Ok(0)` for a still-nonempty remainder: unlike a `read`/`fill_buf`
/// returning empty (a legitimate EOF signal), `std::io::Write::write`
/// returning `Ok(0)` for non-empty input means the writer can't accept
/// more right now but isn't reporting an error either (e.g. a
/// `&mut [u8]` at capacity) — looping on that would spin forever, so
/// `std::io::Write::write_all` itself escalates it to `WriteZero`, and
/// this does the same rather than trusting every `write` impl not to
/// do that. (`embedded_io::Write::write` is documented to never return
/// `Ok(0)` for non-empty input, so an `embedded_io` backend can supply
/// an unreachable `zero_write_error`.)
pub fn retry_write_all<T, E>(
    target: &mut T,
    mut write: impl FnMut(&mut T, &[u8]) -> Result<usize, E>,
    buf: &[u8],
    is_interrupted: impl Fn(&E) -> bool,
    zero_write_error: impl FnOnce() -> E,
) -> Result<(), E> {
    let mut pos = 0;
    while pos < buf.len() {
        match retry_on_interrupted(|| write(target, &buf[pos..]), &is_interrupted)? {
            0 => return Err(zero_write_error()),
            n => pos += n,
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

    #[test]
    fn retry_on_interrupted_retries_until_success() {
        let mut calls = 0;
        let result = retry_on_interrupted(
            || {
                calls += 1;
                if calls < 3 { Err("eintr") } else { Ok(calls) }
            },
            is_interrupted,
        );
        assert_eq!(result, Ok(3));
        assert_eq!(calls, 3);
    }

    #[test]
    fn retry_on_interrupted_propagates_a_real_error_without_retrying() {
        let mut calls = 0;
        let result: Result<(), &str> = retry_on_interrupted(
            || {
                calls += 1;
                Err("boom")
            },
            is_interrupted,
        );
        assert_eq!(result, Err("boom"));
        assert_eq!(calls, 1);
    }

    #[test]
    fn retry_fill_buf_retries_until_a_real_result() {
        // Two `Interrupted`s, then a real (non-empty) result — which
        // itself costs one extra re-fetch (see the refetch test below),
        // so the real data comes back on the 4th call, not the 3rd.
        let mut calls = 0usize;
        let got = retry_fill_buf(
            &mut calls,
            |c| {
                *c += 1;
                match *c {
                    1 | 2 => Err("eintr"),
                    _ => Ok(b"data".as_slice()),
                }
            },
            is_interrupted,
        );
        assert_eq!(got, Ok(b"data".as_slice()));
        assert_eq!(calls, 4);
    }

    #[test]
    fn retry_fill_buf_returns_an_empty_result_after_exactly_one_call() {
        let mut calls = 0;
        let got: Result<&[u8], &str> = retry_fill_buf(
            &mut calls,
            |c| {
                *c += 1;
                Ok(&[][..])
            },
            is_interrupted,
        );
        assert_eq!(got, Ok(&[][..]));
        assert_eq!(calls, 1);
    }

    #[test]
    fn retry_fill_buf_refetches_once_for_a_non_empty_result() {
        // The second call is the only way to hand the buffer back out
        // (a borrow-checker limitation, documented on the function
        // itself) — this pins down that it's exactly one extra call,
        // not zero or several.
        let mut calls = 0;
        let got = retry_fill_buf(
            &mut calls,
            |c| {
                *c += 1;
                Ok::<_, &str>(b"x".as_slice())
            },
            is_interrupted,
        );
        assert_eq!(got, Ok(b"x".as_slice()));
        assert_eq!(calls, 2);
    }

    #[test]
    fn retry_write_all_with_empty_buf_never_calls_write() {
        let mut calls = 0;
        let result = retry_write_all(
            &mut calls,
            |c, _buf| {
                *c += 1;
                Ok::<_, &str>(0)
            },
            b"",
            is_interrupted,
            || "unreachable",
        );
        assert_eq!(result, Ok(()));
        assert_eq!(calls, 0);
    }

    #[test]
    fn retry_write_all_retries_interruptions_and_partial_writes() {
        struct Flaky<'a> {
            out: &'a mut [u8],
            attempt: usize,
        }
        fn write(target: &mut Flaky<'_>, buf: &[u8]) -> Result<usize, &'static str> {
            target.attempt += 1;
            match target.attempt {
                1 => Err("eintr"),
                2 => {
                    let n = 2.min(buf.len());
                    target.out[..n].copy_from_slice(&buf[..n]);
                    let out = core::mem::take(&mut target.out);
                    target.out = &mut out[n..];
                    Ok(n)
                }
                3 => Err("eintr"),
                _ => {
                    let n = buf.len();
                    target.out[..n].copy_from_slice(buf);
                    Ok(n)
                }
            }
        }

        let mut out = [0u8; 6];
        let mut flaky = Flaky {
            out: &mut out,
            attempt: 0,
        };
        retry_write_all(&mut flaky, write, b"abcdef", is_interrupted, || "unreachable").unwrap();
        assert_eq!(&out, b"abcdef");
    }

    #[test]
    fn retry_write_all_escalates_a_zero_length_write_to_the_given_error() {
        fn always_zero(_target: &mut (), buf: &[u8]) -> Result<usize, &'static str> {
            assert!(!buf.is_empty());
            Ok(0)
        }
        let result = retry_write_all(&mut (), always_zero, b"x", is_interrupted, || "zero-write");
        assert_eq!(result, Err("zero-write"));
    }
}
