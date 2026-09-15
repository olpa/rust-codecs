# Creating a codec

## The simplest codec

A codec is a `struct` plus two trait impls: `Codec` for `process`,
and `DrainCodec` for `finish`. In the simplest case, a transform has
no internal state and no trailer to write. Then `process` does the
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

impl DrainCodec for Rot13 {
    fn finish(&mut self, _output: &mut [MaybeUninit<u8>]) -> Result<DrainProgress, Error> {
        Ok(DrainProgress::Done { written: 0 })
    }
}
```

## The contract: fully consume, or fully fill

There is one rule every `process` call must obey. Each call either consumes
all of `input`, or fills all of `output`.

`Progress` is an enum with exactly those two variants, so no other
outcome can be reported. `Rot13::process` above shows the pattern:
compute `n = input.len().min(output.len())`, then report whichever
side ran out.

Why this matters: it avoids a class of bugs.
Earlier, `Progress` was more relaxed. It had two fields,
`consumed` and `written`. The driver code (`CodecReader`, `CodecWriter`,
`stream_to_stream`, `Chain`) grew too complicated to reason about, with many
corner cases. Moving some of that complexity to the codec side keeps the
driver simple. This helps the crate, and it helps anyone who writes their
own driver.


## A codec that can't finish an atomic unit mid-buffer: the carry buffer

Base64 shows why the contract above is not always easy to meet. Base64
turns 3-byte groups of input into 4-byte groups of output. It can only
produce output in whole 4-byte groups. It can only consume input in
whole 3-byte groups.

Two mismatches follow:

- If `input` ends mid-group, `process` cannot consume the trailing 1
  or 2 bytes yet. A partial group has nothing valid to produce. So
  `process` must hold onto those bytes and wait for more input on the
  next call.
- If `output` has no room for a whole encoded group, `process` cannot
  write a partial group either. It must render the group somewhere
  else, hand over as much as fits, and keep the rest for the next
  call.

Base64 solves both with a small internal buffer sized to one atomic
unit: `PendingInput<3>` on the input side, `PendingOutput<4>` on the
output side (`core/src/codecs/base64_shared.rs`).
`Base64Enc::process`
(`core/src/codecs/base64_enc.rs`) threads through them in order: drain
whatever `PendingOutput` already holds into `output` first; top up
`PendingInput` and encode it if a full group just completed; then
transform the input.

The general rule: a codec with an atomic transform unit needs a carry
buffer sized to that unit. This makes every input or output buffer
size legal, even a 1-byte output slice. It will just work slowly.

## `finish` is not always a no-op

`Rot13::finish` above does nothing because ROT13 has no trailing
state.

Base64 is the counter-example: `finish` is where the format's
padding gets written.

Only whole 3-byte groups pass through `base64::process`. So a stream whose
length is not a multiple of 3 always ends with 1 or 2 bytes still
sitting in `PendingInput` when the caller signals end-of-input. No more
input is coming to complete that group. The base64 format defines what
to do: pad the short group with `=` bytes so it still decodes to the
right length.

As a general rule: if a format has a trailer, a checksum, or padding rules,
`finish` is not optional.

## Boundary-aware codecs

A `BoundaryAwareCodec` is a `Codec` whose `process` can also report
`BoundaryAwareProgress::Boundary`: The logical stream ended right
here, inside this `input` slice. Bytes past that point belong to
whatever comes next.

One example is parsing a token embedded in a larger stream.
`core/tests/tokenizer.rs` is the worked example: a small hand-written
parser drives a `BoundaryAwareCodec` one step at a time instead of
running it through `stream_to_stream` end to end.

Every `Codec` already gets a `BoundaryAwareCodec` impl for free (it
just never returns `Boundary`), so drivers on the input side
(`CodecReader`, `stream_to_stream`) accept either kind interchangeably.
