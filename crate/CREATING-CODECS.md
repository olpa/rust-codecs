# Creating a codec

This document covers how to **create** a codec crate on top of
`rust-codecs-core`. See [`README.md`](./README.md) for how to **use**
one.

## 1. Implement `Codec`

```rust
use rust_codecs_core::{Codec, Drain, Error, Outcome};

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
    fn process(&mut self, input: &[u8], output: &mut [u8]) -> Result<Outcome, Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            *out = rot13_byte(inp);
        }
        if n == input.len() {
            Ok(Outcome::InputConsumed { written: n })
        } else {
            Ok(Outcome::OutputFilled { consumed: n })
        }
    }

    fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
        Ok(Drain::Done { written: 0 })
    }
}
```

## The contract

**Every call fully consumes its input, fully fills its output, or ends
the stream.** That's the whole contract, and the return types make any
other outcome unrepresentable:

- `process` returns an [`Outcome`]:
  - `InputConsumed { written }` — all of `input` was taken (some
    possibly into internal buffering, so `written` may be less than
    what will eventually come out, even zero).
  - `OutputFilled { consumed }` — every byte of `output` was written;
    `consumed` says how much of `input` that took (possibly zero, when
    output held over from an earlier call filled the buffer by
    itself).
  - `StreamEnd { consumed, written }` — the format is self-terminating
    and just ended; input past the stream's end stays unconsumed, and
    neither side needs to be "full".
- `finish` (and `flush`) return a [`Drain`]: `OutputFilled` (all of
  `output` written, more to come — the driver will call again) or
  `Done { written }` (everything owed was delivered).
- Errors carry `kind` plus the `consumed`/`written` progress the call
  made before failing, so no bytes become unaccounted for.

The drivers do not take your word for it: every reported count is
checked against the buffer sizes the call was given
(`Outcome::validated`/`Drain::validated`), and an overclaimed count
surfaces as an `ErrorKind::ContractViolation` error rather than
corrupting driver state. If you build your own driver, apply the same
check at your codec boundary.

Two consequences worth spelling out:

- **"I need more input before I can produce anything" is expressed by
  consuming.** Buffer the partial unit internally (see
  `pending_group` in `core/src/base64.rs`) and return
  `InputConsumed { written: 0 }`; the driver feeds the next chunk,
  and at end of stream `finish` drains what you buffered. Drivers
  never coalesce input into a larger contiguous slice for you.
- **"This output buffer is too small for my atomic unit" does not
  exist.** A codec that can only emit whole units (base64: 4-byte
  encoded groups) uses a [`Carry`]: render the unit into a scratch
  array, `carry.emit(&scratch, output)` writes what fits and holds the
  tail, and `carry.drain(output)` delivers the tail first thing on the
  next call. Size the carry to your largest atomic unit — it's a
  compile-time constant of the format. This is what makes every buffer
  size legal everywhere: 1-byte staging in a `Chain`, 1-byte slots in
  `stream_to_stream`, 1-byte reads from a `CodecReader`.

Degenerate buffers: with empty `input`, `process` drains pending
output (if any) and reports `InputConsumed`; drivers avoid calling
with empty `output`, where `OutputFilled` would be trivially true.
`finish` with an empty buffer is meaningful and expected: `Done` says
the codec owes nothing, `OutputFilled` says it owes bytes and needs
room.

`Codec` also has `flush` (drain pending state to a sync boundary
*without* ending the stream — the stream continues afterward). It has
a default that owes nothing; only override it if your format defines
an in-band sync marker (deflate/zlib/gzip do, ROT13 doesn't).

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
`CodecWriter`, `drive` with `VecOutput`, etc.

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

- In-memory round-trip via `drive`, `VecInput`, and `VecOutput`.
- The streaming adapters (`CodecReader`/`CodecWriter`) over a
  `Cursor`/`Vec<u8>`, including a case where the output buffer is
  smaller than the input, to confirm `OutputFilled` is handled and the
  call resumes correctly.
- If the codec has an atomic output unit: buffers *smaller than the
  unit* on both sides (a 1-byte output is the strongest version), to
  prove the carry spans buffers correctly.
- `finish()` reaching `Drain::Done`.

That's the whole surface: implement `Codec`, expose constructor
function(s), and the rest of RustCodecs (stream adapters, `Vec<u8>`
helper) works with your codec for free.
