//! Benchmark: escape the string value of every "text" attribute found in
//! twitter.json, comparing several JSON-string-escaping implementations.
//!
//! Each contender turns a raw `&str` into a full JSON string token
//! (surrounding quotes included), so all six are producing the same shape
//! of output and are directly comparable.

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

// ---------------------------------------------------------------------
// Contenders: each turns a raw &str into a JSON string token (with quotes).
// ---------------------------------------------------------------------

fn escape_serde_json(s: &str) -> String {
    serde_json::to_string(s).unwrap()
}

fn escape_naive(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn escape_json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    out.extend(json_escape::escape_str(s));
    out.push('"');
    out
}

fn escape_v_jsonescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    v_jsonescape::escape_string(s, &mut out);
    out.push('"');
    out
}

fn escape_simd_json(s: &str) -> String {
    simd_json::to_string(s).unwrap()
}

fn escape_sonic_rs(s: &str) -> String {
    sonic_rs::to_string(s).unwrap()
}

// ---------------------------------------------------------------------
// Correctness guard: an escaper that's "fast" because it's wrong is
// useless. Every contender must round-trip back to the original string.
// ---------------------------------------------------------------------

fn assert_round_trips(name: &str, escape: impl Fn(&str) -> String, texts: &[String]) {
    for original in texts {
        let escaped = escape(original);
        let decoded: String =
            serde_json::from_str(&escaped).unwrap_or_else(|e| {
                panic!("{name}: output {escaped:?} is not valid JSON: {e}")
            });
        assert_eq!(&decoded, original, "{name}: round-trip mismatch");
    }
}

fn bench_escape(c: &mut Criterion) {
    let texts = load_corpus();
    let total_bytes: u64 = texts.iter().map(|s| s.len() as u64).sum();

    let contenders: Vec<(&str, fn(&str) -> String)> = vec![
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
        group.bench_function(*name, |b| {
            b.iter(|| {
                for s in &texts {
                    black_box(escape(black_box(s.as_str())));
                }
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_escape);
criterion_main!(benches);
