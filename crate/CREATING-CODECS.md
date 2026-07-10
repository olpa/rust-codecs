# Creating a codec

This document covers how to **create** a codec crate on top of
`rust-codecs-core`. See [`README.md`](./README.md) for how to **use**
one.

A codec crate depends on `rust-codecs-core`, never on `compcol`
directly — the whole point of this crate is to be the only place that
name appears.

## 1. Implement `Codec`

```rust
use rust_codecs_core::{Codec, Error, Progress, Status};

#[derive(Debug, Clone, Copy, Default)]
pub struct Rot13;

fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<(Progress, Status), Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            *out = rot13_byte(inp);
        }
        let status = if n == input.len() { Status::InputEmpty } else { Status::OutputFull };
        Ok((Progress { consumed: n, written: n }, status))
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<(Progress, Status), Error> {
        Ok((Progress::default(), Status::StreamEnd))
    }

    fn reset(&mut self) {}
}
```

Notes on the trait contract:

- `process` pushes input bytes and pulls output bytes; `Status` tells the
  caller which buffer ran out first (`InputEmpty`, `OutputFull`) or that
  the stream ended (`StreamEnd`).
- `finish` signals "no more input is coming" — flush any buffered state
  and, for formats with one, write the trailer/checksum. Call it
  repeatedly with a fresh output buffer until it reports `StreamEnd`.
  A stateless, self-inverse codec like ROT13 has nothing to flush, so
  `finish` returns `StreamEnd` immediately.
- `reset` returns the codec to its just-constructed state, preserving
  any configuration passed at construction time.
- `Codec` also has `flush` (drain pending state to a sync boundary
  *without* ending the stream — unlike `finish`, it never reports
  `StreamEnd`). It has a no-op default; only override it if your format
  defines an in-band sync marker (deflate/zlib/gzip do, ROT13 doesn't).
- `Codec` also has `discard_output` (advance the transformed stream by
  `n` bytes without emitting them, for `tar`-style archive skimming) —
  the default implementation runs `process` and discards the result
  through a scratch buffer; override it if your format can skip faster
  (ROT13 can: skipping `n` output bytes is just consuming `n` input
  bytes, no transform needed).

A codec that reverses another one (e.g. a compressor and its matching
decompressor) is a separate, independent value with its own `Codec`
impl — there's no shared type or trait connecting the two. ROT13 doesn't
need a second type: the transform is genuinely the same both ways, so
one `Rot13` value serves both constructors below.

## 2. Expose `<name>_enc()` / `<name>_dec()` constructors

```rust
pub fn rot13_enc() -> Rot13 {
    Rot13
}

pub fn rot13_dec() -> Rot13 {
    Rot13
}
```

Plain functions need no trait import and no pairing machinery — callers
just call `rot13_dec()`/`rot13_enc()` and get a value ready to hand to
`CodecReader`, `CodecWriter`, `to_vec`, etc.

If your codec takes configuration (compression level, dictionary, …),
give the constructor a parameter or add a `_with` variant — there's no
associated-type machinery to satisfy.

## 3. Test it

At minimum, exercise:

- One-shot round-trip via `rust_codecs_core::io::to_vec`.
- The streaming adapters (`CodecReader`/`CodecWriter`) over a
  `Cursor`/`Vec<u8>`, including a case where the output buffer is
  smaller than the input, to confirm `Status::OutputFull` is handled and
  the call resumes correctly.
- `finish()` reaching `Status::StreamEnd`.
- `discard_output` if you overrode the default implementation.

That's the whole surface: implement `Codec`, expose constructor
function(s), and the rest of RustCodecs (stream adapters, `Vec<u8>`
helper) works with your codec for free.
