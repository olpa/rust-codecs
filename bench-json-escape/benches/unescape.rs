//! Benchmark: unescape a JSON string token (surrounding quotes included)
//! back into its original text, comparing several Rust unescaping
//! implementations.
//!
//! The escaped-token corpus is derived once, up front, from the same
//! "text" values used by `escape.rs`, encoded with `serde_json::to_string`
//! (already proven correct by that benchmark). Each contender writes the
//! decoded bytes into a caller-supplied `&mut Vec<u8>`.
//!
//! Buffers are pre-allocated once per item (capacity = the escaped
//! token's byte length -- unescaping only ever shrinks or preserves
//! length, never grows) and reused across iterations, `clear()`-ed but
//! not reallocated between calls, so the timed loop measures unescaping
//! work rather than allocator churn. The `clear()` itself happens in an
//! untimed `iter_batched_ref` setup phase, so its cost isn't folded into
//! the measured unescape time. (`simd_json`'s scratch-copy refresh is
//! different: that `clear()` + `extend_from_slice` is inherent to how
//! simd_json's in-place parser must be fed, so it stays inside the timed
//! routine.)
//!
//! `serde_json`, `simd_json`, and `sonic_rs` all implement
//! `deserialize_str` by handing a `Visitor` an already-unescaped
//! `&str`/`String`; `BufVisitor` below just copies those bytes into our
//! reused buffer instead of letting them allocate their own `String`.
//! `simd_json` additionally parses in place, so it needs a reused
//! *mutable scratch copy* of the escaped bytes, refreshed each call.
//!
//! `v_jsonescape` is not included: it is an escape-only generated crate
//! with no unescape counterpart.

use std::cell::RefCell;
use std::fmt;
use std::hint::black_box;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde::de::{Deserializer as _, Visitor};

#[path = "common.rs"]
mod common;
use common::load_corpus;

/// Escaped JSON string tokens (with quotes) derived from `texts`, plus
/// their originals, used as the input corpus for every contender.
fn make_escaped_corpus(texts: &[String]) -> Vec<String> {
    texts.iter().map(|s| serde_json::to_string(s).unwrap()).collect()
}

/// One pre-sized, reusable output buffer per corpus item.
fn make_buffers(tokens: &[String]) -> Vec<Vec<u8>> {
    tokens.iter().map(|t| Vec::with_capacity(t.len())).collect()
}

// ---------------------------------------------------------------------
// A serde Visitor that copies an already-unescaped string straight into
// a caller-owned buffer, instead of allocating its own String.
// ---------------------------------------------------------------------

struct BufVisitor<'a>(&'a mut Vec<u8>);

impl<'de> Visitor<'de> for BufVisitor<'_> {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a JSON string")
    }

    fn visit_str<E>(self, v: &str) -> Result<(), E> {
        self.0.extend_from_slice(v.as_bytes());
        Ok(())
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<(), E> {
        self.0.extend_from_slice(v.as_bytes());
        Ok(())
    }

    fn visit_string<E>(self, v: String) -> Result<(), E> {
        self.0.extend_from_slice(v.as_bytes());
        Ok(())
    }
}

// ---------------------------------------------------------------------
// Contenders: each decodes `token` (a full JSON string literal, quotes
// included) and appends the decoded bytes into `buf`, which the caller
// has already `clear()`-ed.
// ---------------------------------------------------------------------

fn unescape_serde_json(token: &str, buf: &mut Vec<u8>) {
    let mut de = serde_json::Deserializer::from_str(token);
    de.deserialize_str(BufVisitor(buf)).unwrap();
}

fn unescape_naive(token: &str, buf: &mut Vec<u8>) {
    let inner = &token[1..token.len() - 1];
    let mut chars = inner.chars();
    let mut char_buf = [0u8; 4];
    while let Some(c) = chars.next() {
        if c != '\\' {
            buf.extend_from_slice(c.encode_utf8(&mut char_buf).as_bytes());
            continue;
        }
        match chars.next().expect("dangling escape") {
            '"' => buf.push(b'"'),
            '\\' => buf.push(b'\\'),
            '/' => buf.push(b'/'),
            'b' => buf.push(0x08),
            'f' => buf.push(0x0C),
            'n' => buf.push(b'\n'),
            'r' => buf.push(b'\r'),
            't' => buf.push(b'\t'),
            'u' => {
                let high = read_hex4(&mut chars);
                let code = if (0xD800..=0xDBFF).contains(&high) {
                    let backslash = chars.next();
                    let u = chars.next();
                    debug_assert_eq!((backslash, u), (Some('\\'), Some('u')));
                    let low = read_hex4(&mut chars);
                    0x10000 + (high - 0xD800) * 0x400 + (low - 0xDC00)
                } else {
                    high
                };
                let ch = char::from_u32(code).expect("invalid unicode escape");
                buf.extend_from_slice(ch.encode_utf8(&mut char_buf).as_bytes());
            }
            other => panic!("unknown escape \\{other}"),
        }
    }
}

fn read_hex4(chars: &mut std::str::Chars) -> u32 {
    let mut code = 0u32;
    for _ in 0..4 {
        code = code * 16 + chars.next().expect("truncated \\u escape").to_digit(16).expect("invalid hex digit");
    }
    code
}

fn unescape_json_escape(token: &str, buf: &mut Vec<u8>) {
    for chunk in json_escape::unescape_quoted(token) {
        buf.extend_from_slice(chunk.expect("invalid escape sequence"));
    }
}

fn unescape_simd_json(token: &str, scratch: &mut Vec<u8>, buf: &mut Vec<u8>) {
    scratch.clear();
    scratch.extend_from_slice(token.as_bytes());
    let mut de = simd_json::Deserializer::from_slice(scratch).unwrap();
    serde::de::Deserializer::deserialize_str(&mut de, BufVisitor(buf)).unwrap();
}

fn unescape_sonic_rs(token: &str, buf: &mut Vec<u8>) {
    let mut de = sonic_rs::Deserializer::from_str(token);
    de.deserialize_str(BufVisitor(buf)).unwrap();
}

// ---------------------------------------------------------------------
// Correctness guard: an unescaper that's "fast" because it's wrong is
// useless. Every contender must decode back to the original string.
// ---------------------------------------------------------------------

fn assert_round_trips(name: &str, unescape: impl Fn(&str, &mut Vec<u8>), tokens: &[String], originals: &[String]) {
    let mut buf = Vec::new();
    for (token, original) in tokens.iter().zip(originals) {
        buf.clear();
        unescape(token, &mut buf);
        let decoded = std::str::from_utf8(&buf)
            .unwrap_or_else(|e| panic!("{name}: output is not valid UTF-8: {e}"));
        assert_eq!(decoded, original, "{name}: round-trip mismatch");
    }
}

fn bench_unescape(c: &mut Criterion) {
    let texts = load_corpus();
    let tokens = make_escaped_corpus(&texts);
    let total_bytes: u64 = tokens.iter().map(|t| t.len() as u64).sum();

    let contenders: Vec<(&str, fn(&str, &mut Vec<u8>))> = vec![
        ("serde_json", unescape_serde_json),
        ("naive", unescape_naive),
        ("json_escape", unescape_json_escape),
        ("sonic_rs", unescape_sonic_rs),
    ];

    for (name, unescape) in &contenders {
        assert_round_trips(name, unescape, &tokens, &texts);
    }
    {
        let mut scratch = Vec::new();
        let mut buf = Vec::new();
        for (token, original) in tokens.iter().zip(&texts) {
            buf.clear();
            unescape_simd_json(token, &mut scratch, &mut buf);
            let decoded = std::str::from_utf8(&buf).unwrap();
            assert_eq!(decoded, original, "simd_json: round-trip mismatch");
        }
    }

    let mut group = c.benchmark_group("unescape_text");
    group.throughput(Throughput::Bytes(total_bytes));
    // `RefCell` + `iter_batched_ref` + `black_box` here follow the same
    // pattern as `escape.rs`'s equivalent loop -- see the comments there
    // for why each piece is needed.
    for (name, unescape) in &contenders {
        let buffers = RefCell::new(make_buffers(&tokens));
        group.bench_function(*name, |b| {
            b.iter_batched_ref(
                || {
                    for buf in buffers.borrow_mut().iter_mut() {
                        buf.clear();
                    }
                },
                |()| {
                    for (t, buf) in tokens.iter().zip(buffers.borrow_mut().iter_mut()) {
                        unescape(black_box(t.as_str()), buf);
                        black_box(&*buf);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    // simd_json can't share the loop above because `unescape_simd_json`
    // doesn't fit the `fn(&str, &mut Vec<u8>)` shape the `contenders`
    // vec relies on: simd_json parses in place, so it needs its own
    // mutable scratch copy of the token's bytes (`scratch`) in addition
    // to the output buffer, refreshed before every call since parsing
    // consumes/mutates it. Hence a separate block with its own buffer
    // setup instead of another entry in `contenders`.
    {
        let scratch_buffers = RefCell::new(make_buffers(&tokens));
        let buffers = RefCell::new(make_buffers(&tokens));
        group.bench_function("simd_json", |b| {
            b.iter_batched_ref(
                || {
                    for buf in buffers.borrow_mut().iter_mut() {
                        buf.clear();
                    }
                },
                |()| {
                    for ((t, scratch), buf) in tokens
                        .iter()
                        .zip(scratch_buffers.borrow_mut().iter_mut())
                        .zip(buffers.borrow_mut().iter_mut())
                    {
                        unescape_simd_json(black_box(t.as_str()), scratch, buf);
                        black_box(&*buf);
                    }
                },
                BatchSize::PerIteration,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_unescape);
criterion_main!(benches);
