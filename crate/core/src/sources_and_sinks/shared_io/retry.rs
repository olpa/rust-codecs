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
