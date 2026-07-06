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
//! rather than allocator churn.

use std::hint::black_box;
use std::path::Path;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use serde_json::Value;

/// Walk a serde_json::Value tree and collect every string found under a key
/// literally named "text", at any depth (tweets, retweets, quoted tweets...).
fn collect_text_values(value: &Value, out: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, v) in map {
                if key == "text" {
                    if let Value::String(s) = v {
                        out.push(s.clone());
                    }
                }
                collect_text_values(v, out);
            }
        }
        Value::Array(items) => {
            for v in items {
                collect_text_values(v, out);
            }
        }
        _ => {}
    }
}

fn load_corpus() -> Vec<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("twitter.json");
    let data = std::fs::read_to_string(path).expect("read twitter.json");
    let root: Value = serde_json::from_str(&data).expect("parse twitter.json");

    let mut out = Vec::new();
    collect_text_values(&root, &mut out);
    assert!(!out.is_empty(), "no \"text\" attributes found in fixture");
    out
}

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
        let mut buffers = make_buffers(&texts);
        group.bench_function(*name, |b| {
            b.iter(|| {
                for (s, buf) in texts.iter().zip(buffers.iter_mut()) {
                    buf.clear();
                    escape(black_box(s.as_str()), buf);
                    black_box(&*buf);
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_escape);
criterion_main!(benches);
