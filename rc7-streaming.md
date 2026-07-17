# rc7-streaming: one streaming mechanism for all environments

Goal: stack and connect codecs in any streaming environment — std::io,
embedded-io, vectors, async later — without re-inventing the plumbing per
environment.

Underlying mechanism: the existing sans-io `Codec` trait
(`process(&[u8], &mut [u8]) -> (Progress, Status)`) stays the single core.
It owns no I/O, allocates nothing, and speaks only in slices — the same
pattern as flate2's `Compress`/`Decompress`, rustls, quinn-proto.
Everything else is either a *combinator* (a `Codec` built from `Codec`s)
or a *driver* (a thin per-environment adapter that owns buffers and the
loop). Composition happens at the `Codec` level so every driver gets
chaining for free.

## Recorded decisions (revisit if they stop paying off)

1. **Buffer genericity via `S: AsMut<[u8]>`**, not a hard `&'a mut [u8]`.
   The caller always provides the staging buffer, but the type is generic:
   `&mut [u8]` (borrowed, embedded), `[u8; N]` (owned inline, no_std),
   `Vec<u8>` (std) all satisfy one signature. A hard `&'a mut [u8]` would
   also honor "user provides the buffer" but forces a lifetime parameter
   on every `Chain` and makes boxed-dyn chains (CLI) awkward.
   *Change of opinion looks like:* the generic parameter causes more type
   noise than the lifetime would, or a `dyn`-friendly erased form is
   needed anyway.

2. **`Chain` lands before the engine extraction** (Step 1 before Step 3).
   `Chain` is a `Codec`, not a driver, so it doesn't depend on the shared
   drive loop, and it immediately pays off by letting the CLI drop
   `FinishWrite`. *Change of opinion looks like:* Step 1's finish/flush
   sequencing turns out to duplicate engine logic that Step 3 would have
   provided — then swap the order.

3. **Staging is "return-clean": drained lazily inside a call, clean at
   the boundary.** Within one `process` call, fill staging from `first`
   as full as the input allows *before* draining to `second` (biggest
   possible chunks, invisible from outside). But when `process` returns,
   staging holds only bytes `second` declined (internal buffering, or the
   caller's output filled) — never bytes `Chain` chose to withhold.
   Rationale: interactive applications wrapping stdin/stdout must see a
   typed line traverse the whole chain in the same call; hoarding across
   calls also makes `Status` lie (`InputEmpty` while holding deliverable
   bytes), which every driver would have to defend against. The residual
   holdback is format-mandated only (e.g. a base64 decoder holding <4
   chars) — that belongs to the codec and is what `Codec::flush` drains.
   *Change of opinion looks like:* nothing foreseeable; this is a
   client-facing guarantee, not a tunable.

4. **No buffering-policy knob for clients.** Return-clean is
   unconditional, not a default. Every real client lands on the same
   setting: bulk callers already fill staging within a call (their knob
   is buffer size, which they hold), interactive callers need
   return-clean, and "trickling but throughput-sensitive" is a
   contradiction — trickle *is* the bottleneck. A hoarding mode would
   break the `Status` invariant drivers rely on, double the test matrix
   of the most semantics-dense component, and be a semver ratchet
   (adding `Chain::with_policy(...)` later is compatible; removing a
   policy isn't). Codecs that genuinely benefit from larger inputs
   should buffer internally — the trait contract (`flush`) already
   provides for that. *Change of opinion looks like:* a benchmark
   showing per-call overhead dominating for a real codec — then add an
   opt-in policy parameter, never a new default.

5. **The io adapters are policy-free, and their buffers are
   caller-provided too.** `CodecReader`/`CodecWriter` never withhold:
   the writer `write_all`s everything the codec emits before returning
   (its buffer is workspace, not a queue), and the reader's `inner.read`
   returns as soon as any bytes arrive. Batching policy already has a
   canonical, composable expression in each ecosystem — `BufReader`/
   `BufWriter` placement in the client's stack (`BufWriter<CodecWriter>`
   batches codec calls; `CodecWriter<BufWriter>` batches sink syscalls;
   tokio and embedded have their own equivalents) — so a knob inside our
   adapters would duplicate that non-composably. The hardcoded
   `SCRATCH = 64 * 1024` goes away: adapter constructors take a
   caller-provided buffer, same `S: AsMut<[u8]>` convention as `Chain`
   (decision 1). Exception: `to_vec` keeps allocating internally — it is
   the alloc-gated convenience API and already allocates its output.
   *Change of opinion looks like:* the buffer argument proves too noisy
   for std users — then add back a `Vec`-allocating convenience
   constructor behind `alloc`, keeping the buffer-taking one primary.

## Step 1 — `Chain` combinator: compose two codecs into one `Codec`

New `core/src/chain.rs`, exported from `lib.rs`.

```rust
pub struct Chain<A, B, S> {
    first: A,
    second: B,
    staging: S,          // caller-provided, S: AsMut<[u8]>
    filled: usize,       // bytes in staging awaiting the second codec
    drained: usize,      // bytes of `filled` already consumed by it
    first_ended: bool,   // first codec reported StreamEnd / was finished
}

impl<A: Codec, B: Codec, S: AsMut<[u8]>> Codec for Chain<A, B, S> { ... }
```

`Chain::new(a, b, buf)` rejects an empty buffer (it could never make
progress).

Semantics — the whole substance of the step:

- `process`: loop — fill staging from `first` as full as the input
  allows, then drain staging through `second` into the caller's output —
  until input is exhausted or output is full. Uphold the return-clean
  invariant (decision 3): on return, staging holds only bytes `second`
  declined. Report combined `Progress` (consumed = what `first` took,
  written = what `second` wrote). If `first` reports `StreamEnd`
  mid-stream (self-terminating format), latch `first_ended` and stop
  feeding it.
- `finish`: drive `first.finish` into staging until `StreamEnd`, pushing
  staging through `second.process` whenever it fills; only then loop
  `second.finish`. Re-callable with fresh output buffers until
  `StreamEnd`, like every codec.
- `flush`: drain `first.flush` output through `second.process`, then
  `second.flush`.
- Overall `Status`: `OutputFull` if the caller's output is the
  bottleneck, `InputEmpty` when input is exhausted and staging drained,
  `StreamEnd` only from `second`'s end.

Tests: rot13∘rot13 is identity; base64-enc∘base64-dec round-trip; the
same assertions with 1-byte staging and 1-byte output buffers to force
every `OutputFull`/partial-progress path; a
`Chain<Box<dyn Codec>, Box<dyn Codec>, Vec<u8>>` to confirm dyn
composition; a nested `Chain<Chain<…>, C, _>` (three-codec stack)
compiles and runs.

## Step 2 — CLI uses `Chain`; delete `FinishWrite`

Rework `cli/src/main.rs`: fold each side's codec list into one
`Box<dyn Codec>` by repeated chaining (each link gets a `vec![0u8; N]`
staging buffer). The reader side becomes a single
`CodecReader<Stdin, Box<dyn Codec>>`, the writer side a single
`CodecWriter<Stdout, Box<dyn Codec>>` — so `CodecWriter::finish(self)`
is directly callable and the entire `FinishWrite` apparatus (the trait,
its `Box` impl, the `CodecWriter` blanket, the `Stdout` base case, ~50
lines of `io/stream.rs`) is deleted.

Needs one helper for the "zero or one codec" case — special-case it or
fold starting from `identity()`. Note in the commit message that per-link
scratch drops from 64 KiB per `CodecWriter` layer to one adapter buffer
plus small staging buffers.

Tests: the end-to-end invocation from the module doc
(`--readers identity identity rot13 --writers rot13 rot13 identity`)
still round-trips; add it as a real test if it isn't one. Plus an
interactivity test enforcing the return-clean guarantee (decision 3):
write one short line into the writer stack, call `flush`, and assert
the transformed bytes reached the sink *before* `finish` — so the
latency guarantee is held by CI, not just prose.

## Step 3 — Extract the drive loop into a shared engine

New `core/src/engine.rs` (name bikesheddable): a no_std-compatible state
struct owning the finish/EOF/no-progress bookkeeping currently
re-implemented three times in `CodecReader::read`,
`CodecWriter::{write,finish}`, and `to_vec`:

```rust
pub enum Step { NeedInput, Wrote(usize), Done }

pub struct Engine<C: Codec> { codec: C, finishing: bool, done: bool }
impl<C: Codec> Engine<C> {
    /// One turn of the crank: `at_eof` tells it to switch to finish().
    pub fn step(&mut self, input: &[u8], at_eof: bool, out: &mut [u8])
        -> Result<(usize /* consumed */, Step), Error>;
}
```

`step` encapsulates: process-vs-finish selection, `StreamEnd` latching,
and the "finish wrote nothing but didn't end" guard — currently
inconsistent (`to_vec` returns `Error::Corrupt`, `CodecWriter::finish`
invents an `io::Error`, `CodecReader` loops); pick one behavior and
document it.

Rewrite `CodecReader`, `CodecWriter`, `to_vec` as thin loops over
`Engine`. While the constructors are open anyway, apply decision 5:
drop the hardcoded `SCRATCH` const — `CodecReader::new` /
`CodecWriter::new` take a caller-provided buffer (`S: AsMut<[u8]>`,
same convention as `Chain`); `to_vec` keeps allocating internally.
Existing tests pass with mechanical constructor updates; the only
intended behavior change is the unified no-progress error.

## Step 4 — Make the core no_std

`compcol` is explicitly a no_std crate, so this is feature plumbing in
`core`:

- `#![cfg_attr(not(feature = "std"), no_std)]` in `lib.rs`.
- Features: `std = ["alloc", "compcol/std"]` (default), `alloc`; drop the
  hardcoded `features = ["std"]` on the compcol dependency in favor of
  forwarding.
- Gate `io::stream` (and the `io::Error` conversion) behind `std`; gate
  `to_vec` behind `alloc` (switch to `alloc::vec::Vec`); the `Box<C>`
  `Codec` impl moves behind `alloc`.
- `base64 = { version = "0.22", default-features = false }` — the
  `GeneralPurpose` engine used at `base64.rs:13` is core-only; confirm,
  and forward `base64/alloc` only if the compiler demands it.
- Tests using `std::io` rely on default features (or
  `#[cfg(all(test, feature = "std"))]`).
- Prove it in the commit:
  `cargo check -p rust-codecs-core --no-default-features --features identity,rot13,base64`
  must pass; ideally also `--target thumbv7em-none-eabi` if the toolchain
  is available. Record the command in `CREATING-CODECS.md` or CI so it
  can't silently rot.

`Codec`, `Chain`, `Engine` need no changes — already slice-only.

## Step 5 — `embedded-io` adapters

New `core/src/io/embedded.rs` behind an `embedded-io` feature:
`CodecReader`/`CodecWriter` equivalents implementing
`embedded_io::Read`/`Write`, written over `Engine` so they're mechanical.
The buffer is caller-provided — after Step 3 that's the same convention
the std adapters use (decision 5), so the only real difference from std
is error mapping: errors map into an `embedded_io::Error`-implementing
wrapper enum (`Codec(crate::Error)` / `Io(E)`).

Test host-side: `embedded_io` traits are testable on std targets (or via
`embedded_io_adapters` over `std::io` types); round-trip the same
rot13/base64 fixtures.

## Step 6 — async adapters (follow-up, not blocking)

`embedded-io-async` and/or `tokio::io::AsyncRead/AsyncWrite` adapters,
each a feature-gated module over the same `Engine`. Same shape as
Step 5; deliberately left unplanned in detail until there's a consumer.

## Explicitly rejected

Defining our own general-purpose `Source`/`Sink` I/O traits and asking
every environment to implement them (the `genio`/`core2`/`acid_io`
route) — that fragments rather than unifies. Each ecosystem keeps its
native traits; the codec is the portable part.
