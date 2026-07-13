# rc4-base64: add b64-enc / b64-dec codecs

Add `b64-enc` and `b64-dec` codecs based on the `base64` crate
(https://docs.rs/base64/), following the existing `identity`/`rot13`
pattern in `crate/core`, and wire them into the CLI.

## Streaming design

Both encoder and decoder buffer up to one incomplete group between
`process()` calls (encoder: <=2 leftover input bytes; decoder: <=3
leftover base64 chars). Each `process()` call tops up a pending group
if any, then bulk-encodes/decodes as many whole groups as fit both
remaining input and output via `Engine::encode_slice`/`decode_slice`
(no unbounded copying). `finish()` flushes the final partial group with
padding (encoder) or errors with `Error::UnexpectedEnd` if a partial
group is still pending (decoder — malformed/truncated base64).

Dependency: `base64 = "0.22"`, engine =
`base64::engine::general_purpose::STANDARD` (with padding).

## Commits (red = compiles, tests fail; green = tests pass)

1. **Red — scaffold**: `crate/core/src/base64.rs` with `B64Enc`/`B64Dec`
   structs, `Codec` impls stubbed via `todo!()`, `b64_enc()`/`b64_dec()`
   constructors, `base64` feature flag + dep in `crate/core/Cargo.toml`,
   module wired into `lib.rs`, and a test module (round-trip,
   small-output-buffer, small-input-chunk cases) copied in the style of
   `rot13.rs`'s tests.
2. **Green — encoder**: implement `B64Enc::process`/`finish`; encoder
   tests pass (decoder tests still panic on `todo!()`).
3. **Green — decoder**: implement `B64Dec::process`/`finish`; all tests
   pass.
4. **CLI wiring**: add `"b64-enc"`/`"b64-dec"` to `make_codec` in
   `crate/cli/src/main.rs`, add the `base64` feature to the `cli`
   crate's dependency on `rust-codecs-core`, update the error message
   listing known codec names.

Each commit is `cargo build`-clean; commits 2-3 are `cargo test`-clean.
