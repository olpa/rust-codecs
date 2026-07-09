# Creating a codec

This document covers how to **create** a codec crate on top of
`rust-codecs-core`. See [`README.md`](./README.md) for how to **use**
one.

A codec crate depends on `rust-codecs-core`, never on `compcol`
directly — the whole point of this crate is to be the only place that
name appears.

## 1. Implement `Encoder` and/or `Decoder`

```rust
use rust_codecs_core::{Decoder, Encoder, Error, Progress, Status};

#[derive(Debug, Clone, Copy, Default)]
pub struct Rot13;

fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

fn transcribe(input: &[u8], output: &mut [u8]) -> (Progress, Status) {
    let n = input.len().min(output.len());
    for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
        *out = rot13_byte(inp);
    }
    let status = if n == input.len() { Status::InputEmpty } else { Status::OutputFull };
    (Progress { consumed: n, written: n }, status)
}

impl Encoder for Rot13 {
    fn encode(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok(transcribe(input, output))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }

    fn reset(&mut self) {}
}

impl Decoder for Rot13 {
    fn decode(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok(transcribe(input, output))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }

    fn reset(&mut self) {}
}
```

Notes on the trait contract:

- `encode`/`decode` push input bytes and pull output bytes; `Status`
  tells the caller which buffer ran out first (`InputEmpty`,
  `OutputFull`) or that the stream ended (`StreamEnd`).
- `finish` signals "no more input is coming" — flush any buffered state
  and, for formats with one, write the trailer/checksum. Call it
  repeatedly with a fresh output buffer until it reports `StreamEnd`.
  A stateless, self-inverse codec like ROT13 has nothing to flush, so
  `finish` returns `StreamEnd` immediately.
- `reset` returns the codec to its just-constructed state, preserving
  any configuration passed at construction time.
- `Encoder` also has `flush` (drain pending state to a sync boundary
  *without* ending the stream — unlike `finish`, it never reports
  `StreamEnd`). It has a no-op default; only override it if your format
  defines an in-band sync marker (deflate/zlib/gzip do, ROT13 doesn't).
- `Decoder` also has `discard_output` (advance the decoded stream by `n`
  bytes without emitting them, for `tar`-style archive skimming) — the
  default implementation decodes-and-discards through a scratch buffer;
  override it if your format can skip faster (ROT13 can: skipping `n`
  decoded bytes is just consuming `n` input bytes, no transform needed).

One type implementing both traits is fine when the transform is
genuinely the same both ways (ROT13 is self-inverse); most real codecs
will have two distinct types instead, an `Encoder` and a `Decoder`.

## 2. Expose `<name>_encoder()` / `<name>_decoder()` constructors

```rust
pub fn rot13_encoder() -> Rot13 {
    Rot13
}

pub fn rot13_decoder() -> Rot13 {
    Rot13
}
```

This is the one place we deviate from compcol's own idiom
(`Algorithm::encoder()`/`decoder()`, which requires the `Algorithm`
trait in scope at every call site). Plain functions need no trait
import and no `Algorithm` impl — callers just call
`rot13_decoder()`/`rot13_encoder()` and get a value ready to hand to
`DecoderReader`, `EncoderWriter`, `decode_to_vec`, etc.

If your codec takes configuration (compression level, dictionary, …),
give the constructor a parameter or add a `_with` variant — there's no
`EncoderConfig`/`DecoderConfig` associated-type machinery to satisfy.

## 3. Test it

At minimum, exercise:

- One-shot round-trip via `rust_codecs_core::io::{encode_to_vec, decode_to_vec}`.
- The streaming adapters (`DecoderReader`/`EncoderWriter`) over a
  `Cursor`/`Vec<u8>`, including a case where the output buffer is
  smaller than the input, to confirm `Status::OutputFull` is handled and
  the call resumes correctly.
- `finish()` reaching `Status::StreamEnd`.
- `discard_output` if you overrode the default implementation.

That's the whole surface: implement the trait(s), expose two
constructor functions, and the rest of RustCodecs (stream adapters,
`Vec<u8>` helpers) works with your codec for free.
