# Rust twin v2: codecs on compcol's `Encoder` / `Decoder` traits

Second Rust equivalent of `../python-ref` (see `../rust-twin-v1` for the
first). The difference from v1: the codec implements the
[`compcol`](https://github.com/KarpelesLab/compcol) crate's `Encoder` and
`Decoder` traits instead of our own `Codec` trait — and as payoff, the
stream wrappers are **not written here at all**. Implementing compcol's
traits buys its `compcol::io` adapters and `compcol::vec` one-shot helpers
for free:

| Python | v1 (own trait) | v2 (compcol traits) |
|---|---|---|
| registered `"my-rot13"` codec | explicit `Rot13` value | explicit `Rot13::decoder()` / `Rot13::encoder()` |
| `codecs.getreader(enc)(stream)` | own `StreamReader` | `compcol::io::DecoderReader` |
| `codecs.getwriter(enc)(stream)` | own `StreamWriter` | `compcol::io::EncoderWriter` |
| `getreader` + `shutil.copyfileobj` | reader + `std::io::copy` | reader + `std::io::copy` |

Still no codec registry: compcol does have one (`factory::encoder_by_name`,
feature `factory`), and we deliberately don't enable it. `Rot13` is
constructed explicitly. We do implement compcol's `Algorithm` trait — that
is not a registry, just the compile-time convention for "a codec with
encoder/decoder constructors", and it is what makes `Rot13::encoder()` and
the `vec` helpers work.

The trait shape also differs from v1. Our v1 `Codec::transform(&[u8], last)
-> Vec<u8>` allocates its output; compcol is `no_std`-first, so the caller
owns both buffers and each call reports `(Progress {consumed, written},
Status)`, where `Status ∈ {InputEmpty, OutputFull, StreamEnd}` tells the
caller what to do next. That makes implementations and call sites loop-ier,
but composes without allocation and handles the "output buffer smaller than
what this input expands to" case that v1 simply side-stepped by allocating.

## Layout

- `src/rot13.rs` — the ROT13 codec. One zero-sized `Rot13` type implements
  `Encoder`, `Decoder` (same transform — ROT13 is self-inverse and
  stateless) and `Algorithm` (with itself as both associated codec types).
  Includes a genuinely accelerated `discard_output` (skip): 1 encoded byte
  = 1 decoded byte, so skipping is just counting.
- `src/bin/{wrap_input,wrap_output,connect,chain}.rs` — the four
  python-ref scenarios, one-to-one with v1's binaries but using compcol's
  adapters. `cargo run --bin wrap-input` etc.; `cargo test` for the unit
  tests (trait-level contract tests plus the same wrapper tests as v1,
  including the 4-reader chain).

## Do we really need the Encoder/Decoder distinction?

Short answer: **not as two method lists — the split is two *contracts*
that happen to share a shape.** The method sets are nearly identical and
the differences (`skip`, `flush`) could live on one trait; what actually
differs is who controls end-of-stream, what can fail, and the type safety
you want at codec-picking boundaries. Evidence, from compcol's source and
from writing `Rot13`:

**The overlap is real.** Both traits are `{encode|decode}(input, output)
-> (Progress, Status)`, `finish(output)`, `reset()`. `Rot13` implements
both with the same 10-line transform. compcol itself hints at the
redundancy: internally both public traits are generated from `RawEncoder`/
`RawDecoder` via blanket impls, and those raw traits share their three
required methods verbatim, differing only in direction-specific *provided*
methods (`raw_flush` with a no-op default on the encoder, `raw_skip` with
a scratch-buffer default on the decoder). And the small annoyance of the
overlap shows up immediately: with both impls on one type,
`codec.finish(&mut buf)` no longer compiles (ambiguous), you need
`Encoder::finish(&mut codec, ..)`.

**The `skip` difference is incidental, not fundamental.** `Decoder::
discard_output(input, n)` ("advance by n decoded bytes without writing
them") exists for use cases like listing a `.tar.gz` without materialising
file contents, and its default implementation needs nothing but `decode`
into a scratch buffer — so on a unified trait it would be a provided
method. It's decoder-only simply because there is no use case on the other
side: an encoder's caller controls the input (to "skip input" you just
don't pass it), and an encoder's *output* can never be discarded because
the receiving decoder needs every byte. So `skip` doesn't justify the
split; it's a convenience that only ever gets called in one direction.

**The real asymmetries are elsewhere:**

1. **Who knows where the stream ends.** An encoder is *told* the end
   (caller invokes `finish`, encoder emits the trailer). A decoder
   *discovers* the end in-band (`decode` returns `StreamEnd` when it
   consumed the trailer). Same `finish` signature, opposite direction of
   information flow.
2. **`flush` only means something for encoders.** An encoder deliberately
   withholds output to compress better; `flush(mode)` forces it to a sync
   boundary (zlib's `Z_SYNC_FLUSH`/`Z_FULL_FLUSH`) for packetized
   protocols. A decoder never withholds output by choice — it emits as
   soon as it can — so a decoder-side `flush` has nothing to do.
3. **Errors are decode-side.** Nearly every variant of compcol's `Error`
   (`Corrupt`, `BadHeader`, `ChecksumMismatch`, `InvalidHuffmanTree`, …)
   can only come from a decoder, because encoders accept arbitrary bytes
   while decoders parse a format.
4. **Type safety at selection boundaries.** For every real compressor the
   encoder and decoder are different state machines with different
   configs. With separate traits, `EncoderWriter::new(file,
   Gzip::decoder())` is a compile error. With one `Codec` trait it would
   compile and produce garbage at runtime — and this matters most
   precisely where compcol needs it, at the runtime factory
   (`encoder_by_name`), where a `Box<dyn Codec>` couldn't say which
   direction it runs in.

**Verdict.** For a general compression library the two-trait split earns
its keep through (1)–(4), even though the visible method-list difference —
`skip` — is the one part of it that *doesn't* matter. For symmetric,
stateless transforms like ROT13 the split is pure ceremony (one type
implements both, plus an ambiguity annoyance). A unified design loses
nothing mechanically but must recover the direction distinction some other
way — see below.

## If compcol migrates to our `Codec` trait as the base

Notes for that future, based on this exercise:

- **Adopt compcol's calling convention, not v1's.** Caller-owned buffers +
  `(Progress, Status)` is the more primitive shape: v1's allocating
  `transform(input, last) -> Vec<u8>` can be built on top of it as a
  provided convenience method (loop until `InputEmpty`/`StreamEnd`, grow a
  Vec), but not the reverse without imposing `alloc` on `no_std` users.
- **One base trait can carry the shared mechanics** — `process(input,
  output) -> (Progress, Status)`, `finish`, `reset`, with `skip` and
  `flush` as provided methods (the defaults already exist in compcol:
  scratch-buffer skip, no-op flush).
- **Keep direction in the type system.** The cheapest way that preserves
  compcol's guarantees: keep `Encoder`/`Decoder` as thin *marker* traits
  (or newtype wrappers `Encoding<C>`/`Decoding<C>`) over the base `Codec`,
  containing only what is genuinely directional. Adapters like
  `DecoderReader` and the factory keep their direction-safe signatures;
  symmetric codecs implement the base trait once and opt into both
  markers.
- **Migration hazard:** compcol already has blanket impls `impl<T:
  RawEncoder> Encoder for T`. A new blanket `impl<T: Codec> Encoder for T`
  would collide with them, so the raw traits have to be replaced in the
  same move, not bridged incrementally.
