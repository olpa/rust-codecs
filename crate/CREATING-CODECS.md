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
}
```

Notes on the trait contract:

- `process` pushes input bytes and pulls output bytes; `Status` tells the
  caller which buffer ran out first (`InputEmpty`, `OutputFull`) or that
  the stream ended (`StreamEnd`).
- **`process` must consume or make progress.** Given non-empty input and
  non-empty output, a call must consume at least one input byte or write
  at least one output byte — "I need more input before I can produce
  anything" is *not* an excuse to return zero progress. Buffer the
  available bytes internally and report `InputEmpty` (that status means
  "all of `input` was consumed", so returning it with unconsumed input
  is a contract violation); the drivers will feed the next chunk and,
  at end of stream, call `finish` to drain what you buffered. Drivers
  never coalesce input into a larger contiguous slice for you, and they
  treat a zero-progress call as a stall — misdiagnosed as an
  output-size problem (`CopyError::SlotTooSmall`), never as "waiting
  for input". See `Base64Enc::pending_group` for the pattern: a partial
  group is stashed across calls and topped up first thing on the next
  one. The one legitimate zero-progress return is `Err(OutputTooSmall)`
  when the output buffer can't fit your minimum atomic output unit —
  and that must be a pure precondition check, returned before any state
  change, because drivers retry the call with a fresh buffer.
- `finish` signals "no more input is coming" — flush any buffered state
  and, for formats with one, write the trailer/checksum. Call it
  repeatedly with a fresh output buffer until it reports `StreamEnd`.
  A stateless, self-inverse codec like ROT13 has nothing to flush, so
  `finish` returns `StreamEnd` immediately.
- `Codec` also has `flush` (drain pending state to a sync boundary
  *without* ending the stream — unlike `finish`, it never reports
  `StreamEnd`). It has a no-op default; only override it if your format
  defines an in-band sync marker (deflate/zlib/gzip do, ROT13 doesn't).

A codec that reverses another one (e.g. a compressor and its matching
decompressor) is a separate, independent value with its own `Codec`
impl — there's no shared type or trait connecting the two.

## 2. Expose a constructor

```rust
pub fn rot13() -> Rot13 {
    Rot13
}
```

Plain functions need no trait import and no pairing machinery — callers
just call `rot13()` and get a value ready to hand to `CodecReader`,
`CodecWriter`, `to_vec`, etc.

ROT13 is stateless and self-inverse — the same value handles both
directions — so one `<name>()` constructor is enough. If encoding and
decoding genuinely need different values (different initial state,
different configuration), expose the pair as `<name>_enc()` /
`<name>_dec()` instead, one returning each.

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

That's the whole surface: implement `Codec`, expose constructor
function(s), and the rest of RustCodecs (stream adapters, `Vec<u8>`
helper) works with your codec for free.
