//! Benchmark: escape the string value of every "text" attribute found in
//! twitter.json, comparing several JSON-string-escaping implementations.
//!
//! Each contender writes a full JSON string token (surrounding quotes
//! included) as raw bytes into a caller-supplied `&mut Vec<u8>`, so all six
//! produce the same shape of output and are directly comparable.
//!
//! Buffers are pre-allocated once per item (capacity = 8x the input byte
//! length) and reused across iterations, `clear()`-ed but not
//! reallocated between calls, so the timed loop measures escaping work
//! rather than allocator churn. The `clear()` itself happens in an
//! untimed `iter_batched_ref` setup phase, so its cost isn't folded into
//! the measured escape time.

use std::cell::RefCell;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};

#[path = "common.rs"]
mod common;
use common::load_corpus;

/// One pre-sized, reusable output buffer per corpus item.
fn make_buffers(texts: &[String]) -> Vec<Vec<u8>> {
    texts.iter().map(|s| Vec::with_capacity(s.len() * 8)).collect()
}

// ---------------------------------------------------------------------
// Contenders: each appends a JSON string token (with quotes) as bytes
// into `buf`, which the caller has already `clear()`-ed.
// ---------------------------------------------------------------------

fn escape_serde_json(s: &str, buf: &mut Vec<u8>) {
    serde_json::to_writer(&mut *buf, s).unwrap();
}

fn escape_naive(s: &str, buf: &mut Vec<u8>) {
    buf.push(b'"');
    let mut char_buf = [0u8; 4];
    for c in s.chars() {
        match c {
            '"' => buf.extend_from_slice(b"\\\""),
            '\\' => buf.extend_from_slice(b"\\\\"),
            '\n' => buf.extend_from_slice(b"\\n"),
            '\r' => buf.extend_from_slice(b"\\r"),
            '\t' => buf.extend_from_slice(b"\\t"),
            '\u{08}' => buf.extend_from_slice(b"\\b"),
            '\u{0C}' => buf.extend_from_slice(b"\\f"),
            c if (c as u32) < 0x20 => {
                buf.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes());
            }
            c => buf.extend_from_slice(c.encode_utf8(&mut char_buf).as_bytes()),
        }
    }
    buf.push(b'"');
}

fn escape_json_escape(s: &str, buf: &mut Vec<u8>) {
    buf.push(b'"');
    for chunk in json_escape::escape_str(s) {
        buf.extend_from_slice(chunk.as_bytes());
    }
    buf.push(b'"');
}

fn escape_v_jsonescape(s: &str, buf: &mut Vec<u8>) {
    buf.push(b'"');
    v_jsonescape::escape_bytes(s, buf);
    buf.push(b'"');
}

fn escape_simd_json(s: &str, buf: &mut Vec<u8>) {
    simd_json::to_writer(&mut *buf, s).unwrap();
}

fn escape_sonic_rs(s: &str, buf: &mut Vec<u8>) {
    sonic_rs::to_writer(&mut *buf, s).unwrap();
}

// ---------------------------------------------------------------------
// Correctness guard: an escaper that's "fast" because it's wrong is
// useless. Every contender must round-trip back to the original string.
// ---------------------------------------------------------------------

fn assert_round_trips(name: &str, escape: impl Fn(&str, &mut Vec<u8>), texts: &[String]) {
    let mut buf = Vec::new();
    for original in texts {
        buf.clear();
        escape(original, &mut buf);
        let escaped = std::str::from_utf8(&buf)
            .unwrap_or_else(|e| panic!("{name}: output is not valid UTF-8: {e}"));
        let decoded: String = serde_json::from_str(escaped)
            .unwrap_or_else(|e| panic!("{name}: output {escaped:?} is not valid JSON: {e}"));
        assert_eq!(&decoded, original, "{name}: round-trip mismatch");
    }
}

fn bench_escape(c: &mut Criterion) {
    let texts = load_corpus();
    let total_bytes: u64 = texts.iter().map(|s| s.len() as u64).sum();

    let contenders: Vec<(&str, fn(&str, &mut Vec<u8>))> = vec![
        ("serde_json", escape_serde_json),
        ("naive", escape_naive),
        ("json_escape", escape_json_escape),
        ("v_jsonescape", escape_v_jsonescape),
        ("simd_json", escape_simd_json),
        ("sonic_rs", escape_sonic_rs),
    ];

    for (name, escape) in &contenders {
        assert_round_trips(name, escape, &texts);
    }

    let mut group = c.benchmark_group("escape_text");
    group.throughput(Throughput::Bytes(total_bytes));
    for (name, escape) in &contenders {
        // `buffers` is reused across every timed call below (that's the
        // whole point -- no per-call allocation). Both the `setup` and
        // `routine` closures passed to `iter_batched_ref` need mutable
        // access to it, and since they're two separate closures alive at
        // the same time, the borrow checker can't verify their borrows
        // never overlap even though the runtime never actually overlaps
        // them. `RefCell` moves that check to runtime (a cheap flag
        // check) so both closures can just capture `&buffers` and call
        // `.borrow_mut()` when they run.
        let buffers = RefCell::new(make_buffers(&texts));
        group.bench_function(*name, |b| {
            // `iter_batched_ref(setup, routine, batch_size)` runs `setup`
            // *before* starting the clock, then times only `routine`.
            // We use this instead of a plain `b.iter(...)` closure so
            // that `buf.clear()` -- which must happen before every call,
            // see below -- doesn't get counted as part of the measured
            // escape time.
            b.iter_batched_ref(
                || {
                    // Reset every buffer's `len` to 0 before the timed
                    // call. This does NOT touch `capacity` or free the
                    // allocation (`Vec::clear` is just `truncate(0)`) --
                    // it's still the same pre-sized buffer. It's needed
                    // because `escape()` *appends* starting at the
                    // buffer's current length rather than overwriting
                    // from the start; without clearing, each of the many
                    // timed calls below would pile another copy of the
                    // escaped string on top of the last one, corrupting
                    // the content and eventually forcing a reallocation
                    // anyway once capacity is exceeded.
                    //
                    // This closure's return value becomes `I` in
                    // `iter_batched_ref`'s `setup: FnMut() -> I`. Here
                    // the for-loop is the whole body, so it evaluates to
                    // `()` -- we're not threading any real data through
                    // the setup/routine hand-off; the routine closure
                    // below reaches the actual buffers by capturing
                    // `&buffers` directly, same as this closure does.
                    for buf in buffers.borrow_mut().iter_mut() {
                        buf.clear();
                    }
                },
                |()| {
                    // Criterion calls this once per sample and only
                    // times this closure. Its parameter type is
                    // `&mut I` = `&mut ()`; the `()` pattern matches
                    // that via match ergonomics (destructuring through
                    // the reference), even though there's nothing to
                    // bind -- we ignore the value and reach `buffers`
                    // via the closure's own capture instead.
                    for (s, buf) in texts.iter().zip(buffers.borrow_mut().iter_mut()) {
                        // `black_box` is a compiler-optimization barrier
                        // (`std::hint::black_box`, stable since Rust
                        // 1.66): it returns its argument unchanged at
                        // runtime but is opaque to the optimizer, which
                        // otherwise could cheat in two ways here:
                        //  - on the input: `s` is the exact same string
                        //    every timed call, so without `black_box`
                        //    the compiler could constant-fold or hoist
                        //    the escape work since nothing about the
                        //    input appears to change between calls.
                        //  - on the output: `buf`'s contents are never
                        //    read anywhere the compiler can see (it just
                        //    gets `clear()`-ed again next time), so
                        //    without `black_box` the compiler could
                        //    prove the whole `escape()` call has no
                        //    observable effect and delete it as dead
                        //    code.
                        escape(black_box(s.as_str()), buf);
                        black_box(&*buf);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_escape);
criterion_main!(benches);
