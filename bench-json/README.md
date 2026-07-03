# bench-json fixtures

## twitter.json

A page of tweets from the Twitter API, used widely as a standard fixture for
JSON parsing/serialization speed benchmarks. Rich in UTF-8/multibyte strings
(non-English text, emoji), nested objects, and numeric IDs.

- Source: [simdjson/simdjson](https://github.com/simdjson/simdjson) — `jsonexamples/twitter.json`
- Direct link: https://raw.githubusercontent.com/simdjson/simdjson/master/jsonexamples/twitter.json
- Also bundled as one of the standard fixtures (alongside `canada.json` and
  `citm_catalog.json`) in
  [miloyip/nativejson-benchmark](https://github.com/miloyip/nativejson-benchmark),
  the RapidJSON author's JSON library benchmark suite that many other
  benchmarks (simd-json, sonic-rs, json-rust, serde_json) derive their test
  data from.
