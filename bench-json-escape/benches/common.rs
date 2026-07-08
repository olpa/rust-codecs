//! Shared corpus-loading helpers used by both the escape and unescape
//! benchmarks.

use std::path::Path;

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

/// Every string found under a "text" key in twitter.json.
pub fn load_corpus() -> Vec<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("twitter.json");
    let data = std::fs::read_to_string(path).expect("read twitter.json");
    let root: Value = serde_json::from_str(&data).expect("parse twitter.json");

    let mut out = Vec::new();
    collect_text_values(&root, &mut out);
    assert!(!out.is_empty(), "no \"text\" attributes found in fixture");
    out
}
