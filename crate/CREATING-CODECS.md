# Creating a codec

This document covers how to **create** a codec crate on top of
`rust-codecs-core`. See [`README.md`](./README.md) for how to **use**
one.

## The simplest codec

A codec is a `struct` plus two trait impls: [`Codec`] for `process`,
and [`DrainCodec`] for `finish`. In the simplest case — a transform
with no internal state and no trailer to write — `process` does the
work and `finish` is a no-op. `core/src/codecs/rot13.rs` is exactly
that:

```rust
use core::mem::MaybeUninit;
use crate::{Codec, DrainCodec, DrainProgress, Error, Progress};

fn rot13_byte(b: u8) -> u8 {
    match b {
        b'A'..=b'M' | b'a'..=b'm' => b + 13,
        b'N'..=b'Z' | b'n'..=b'z' => b - 13,
        _ => b,
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Rot13;

impl DrainCodec for Rot13 {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}

impl Codec for Rot13 {
    fn process(&mut self, input: &[u8], output: &mut [MaybeUninit<u8>]) -> Result<Progress, Error> {
        let n = input.len().min(output.len());
        for (out, &inp) in output[..n].iter_mut().zip(&input[..n]) {
            out.write(rot13_byte(inp));
        }
        if n == input.len() {
            Ok(Progress::InputConsumed { written: n })
        } else {
            Ok(Progress::OutputFilled { consumed: n })
        }
    }
}

pub fn rot13() -> Rot13 {
    Rot13
}
```

Then expose a plain constructor (`rot13()` above) so callers get a
value ready to hand to `CodecReader`, `CodecWriter`, `stream_to_stream`
with `VecSink`, and so on — no associated-type machinery to satisfy.

## The contract: fully consume, or fully fill

The one rule every `process` call must obey:

**Each call either consumes all of `input`, or fills all of `output`.**

`Progress` makes any other outcome unrepresentable — it's an enum with
exactly those two variants, `InputConsumed { written }` and
`OutputFilled { consumed }`. `Rot13::process` above shows the pattern:
compute `n = input.len().min(output.len())`, then report whichever
side ran out.

Why this matters: a driver (`CodecReader`, `CodecWriter`,
`stream_to_stream`, `Chain`) calls `process` in a loop, feeding it
whatever buffers it currently has. If a codec could stop partway
through both `input` and `output` — some input left, some output space
left, no clear reason to stop — the driver would have no way to tell
"call me again with the same buffers" from "you're stuck, give me
different buffers." Every driver would need bespoke logic to guess
which case it's in. Holding codecs to one of the two outcomes means
one driver loop works for every codec, unconditionally.

## A codec that can't finish an atomic unit mid-buffer: the carry buffer

Base64 shows why the contract above isn't always free. Base64 turns
3-byte groups of input into 4-byte groups of output; it can only
produce output in whole 4-byte groups, and it can only consume input
in whole 3-byte groups (the last, short group at the very end of the
stream aside — see below). But the contract says every call must fully
consume `input` or fully fill `output`, and neither the caller's
`input` nor its `output` is guaranteed to be a multiple of the group
size.

Two mismatches follow:

- If `input` ends mid-group, `process` cannot consume the trailing 1
  or 2 bytes yet — there's nothing valid to produce from a partial
  group. It has to hold onto those bytes and wait for more input on
  the next call.
- If `output` doesn't have room for a whole encoded group, `process`
  cannot write a partial group either. It has to render the group
  somewhere else, hand over as much as fits, and keep the remainder
  for the next call.

Base64 solves both with a small internal buffer sized to one atomic
unit: `PendingInput<3>` on the input side, `PendingOutput<4>` on the
output side (`core/src/codecs/base64_shared.rs`). `Base64Enc::process`
(`core/src/codecs/base64_enc.rs`) threads through them in order: drain
whatever `PendingOutput` already holds into `output` first; top up
`PendingInput` and encode it if a full group just completed; then
transform as many whole groups as possible directly between `input`
and `output` (no buffering — this is the hot path); and finally stage
one more group into `PendingOutput` if a whole input group remains but
less than one encoded group of output space is left, or buffer a
leftover partial input group into `PendingInput` for next time.

The general shape: **a codec with an atomic transform unit needs a
carry buffer sized to that unit**, holding a read and a write position,
so the unit can be delivered a few bytes at a time across as many
calls as it takes. This is what makes every buffer size legal
everywhere — a 1-byte output slice must still work, just slowly.

## `finish` is not always a no-op

`Rot13::finish` above does nothing because ROT13 has no trailing
state. Base64 is the counter-example: `finish` is where the format's
padding gets written.

Only whole 3-byte groups pass through `process`, so a stream whose
length isn't a multiple of 3 always ends with 1 or 2 bytes still
sitting in `PendingInput` when the caller signals end-of-input. There
is no more input coming to complete that group, and the base64 format
defines what to do about it: pad the short group out with `=` bytes so
it still decodes to the right length. `Base64Enc::finish`
(`core/src/codecs/base64_enc.rs`) is where that padding gets emitted —
first draining anything still sitting in `PendingOutput`, then, if
`PendingInput` holds a partial group, encoding it (the underlying
`Engine` pads it) and draining that too. Only once both are empty does
`finish` report `DrainProgress::Done`.

The general rule: whatever a codec deferred while waiting for more
input that will never arrive, `finish` is where it gets settled. If
your format has a trailer, a checksum, or padding rules, `finish` is
not optional.

## Boundary-aware codecs

Base64 and ROT13 cover the two methods every codec needs. One more
exists in the trait vocabulary — [`BoundaryAwareCodec`] — for a case
neither example above runs into: a self-terminating format embedded in
a larger stream.

A [`BoundaryAwareCodec`] is a `Codec` whose `process` can also report
[`BoundaryAwareProgress::Boundary`] — "the logical stream ended right
here, inside this `input` slice, with bytes past that point belonging
to whatever comes next." From `core/src/protocol.rs`:

> A stateful transform that can recognize the logical end of its input
> inside a byte stream. It leaves the rest of the source available to
> whatever comes next.

Every `Codec` already gets a `BoundaryAwareCodec` impl for free (it
just never returns `Boundary`), so drivers on the input side
(`CodecReader`, `stream_to_stream`) accept either kind interchangeably.
`core/tests/tokenizer.rs` is the worked example: a small hand-written
parser drives a `BoundaryAwareCodec` one step at a time instead of
running it through `stream_to_stream` end to end.

There is deliberately no mid-stream "sync flush" method here (write
buffered state to a sync point without ending the stream, the way
deflate/zlib/gzip support). No codec in this crate needs one yet — the
shape such a method should take (see
[`compcol::Encoder::flush`](https://docs.rs/compcol/latest/compcol/trait.Encoder.html#tymethod.flush),
which takes a `Sync`/`Full` mode) is better derived from a real
compressor's requirements than guessed at ahead of one. When gzip or
deflate get ported into this crate (`compcol` is the intended source),
add it then, sized to what that port actually needs.

## Expose a constructor

```rust
pub fn rot13() -> Rot13 {
    Rot13
}
```

ROT13 is stateless and self-inverse, so one `<name>()` constructor
covers both directions. If encoding and decoding need different
values (different initial state, different configuration), expose the
pair as `<name>_enc()` / `<name>_dec()` instead. If your codec takes
configuration (compression level, dictionary, …), give the constructor
a parameter or add a `_with` variant.

## Test it

At minimum, exercise:

- In-memory round-trip via `stream_to_stream`, `VecSource`, and `VecSink`.
- The streaming adapters (`CodecReader`/`CodecWriter`) over a
  `Cursor`/`Vec<u8>`, including a case where the output buffer is
  smaller than the input, to confirm `OutputFilled` is handled and the
  call resumes correctly.
- If the codec has an atomic output unit: buffers *smaller than the
  unit* on both sides (a 1-byte output is the strongest version), to
  prove the carry spans buffers correctly.
- `finish()` reaching `DrainProgress::Done`.

If you implemented `BoundaryAwareCodec`, additionally exercise:

- `Boundary` reporting exact consumed/written counts, with the delimiter
  handled the way you documented (consumed or left for the caller).
- Input after `Boundary` staying unconsumed when driven through a `Source`.
- Driver calls after `Boundary` returning permanent zero-progress terminal
  results without re-entering the codec.
- EOF arriving before the in-band boundary, per whatever policy you
  documented for that case.
