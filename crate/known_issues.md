# Known issues and design decisions

This file is for reviewers, human or automatic. Read it before you
review. Do not report an item here as a new finding. Report it only if
you have new facts: a new reproduction, a new impact, or a new fix.

## Open problems

### Uninitialized bytes through the safe output API (issue #19)

Safe code can make uninitialized bytes part of an output buffer. There
are three paths:

- `VecSink::commit` is a safe function that calls `Vec::set_len`.
- `as_uninit_mut` lends initialized `u8` storage as
  `&mut [MaybeUninit<u8>]`. Safe code can store `MaybeUninit::uninit()`
  into it.
- `Codec` is a safe trait. The drivers trust a codec that reports
  `OutputFilled` or `InputConsumed { written }`. They check only the
  bounds of the counts.

Making only `Sink::commit` unsafe does not close the second and third
path. The fix will change the public `Codec` and `Sink` signatures.
The plan is to do it before the first release.

The clamp in `VecSink::commit` keeps a count inside the capacity. It
does not make the bytes initialized.

### A failed `Write::write` has already consumed input (issue #20)

`transfer_step` consumes input before it commits output. If the commit
fails, `write` returns `Err`, but the codec has already processed the
bytes. A normal retry then duplicates output (`abc` becomes `aabc`).
`ScratchSink::commit` also forgets how much of a partial `write_all`
succeeded.

The fix direction is to keep the uncommitted output in the sink, return
`Ok(consumed)`, and report the error on the next call. `Pump` already
has a latch for codec errors that the fix can use.

### The minimum Rust version is 1.87, the goal is 1.81 (issue #23)

`json-escape` blocks the goal. It uses edition 2024, and it comes from
a git commit. A git dependency also blocks `cargo publish`. The plan is
to copy the needed part of `json-escape` into the crate.

The same work fixes a second problem: `--no-default-features
--features json` still enables `std`. `default-features = false` on
`json-escape` does not help, because `json-escape` does not compile
without its default features. `memchr` also keeps `std`.

The repository has no CI yet. No job tests the feature combinations or
the old compiler.

### A short base64 group with a byte outside the alphabet

A short final group with a byte outside the alphabet, for example
`QQ!` at the end of the stream, reports `UnexpectedEnd`. It should
report `CorruptStream`. The case is rare, because the decoder now
ignores ASCII whitespace.


## Decisions

### `JsonEnc` keeps state about bytes that it did not consume (decided 2026-10-01)

`JsonEnc` stores `pending_literal_len` and `PendingEscape::NotStarted`.
These describe input bytes that the codec has not consumed. The cache
is safe only when the caller passes the same unconsumed bytes again.
The `Codec` contract requires this.

Two cases break the rule. They are the open problem #20, and a `write`
that returns `Ok(n)` with `n < buf.len()` when the sink is full. In
these cases the codec can emit an unescaped quote, or it can panic.

We keep the code as it is. The codec will move to its own crate and get
a rewrite there. Check both cases again when you rewrite it.

`JsonEnc` also passes invalid UTF-8 through unchanged. The output can
be invalid JSON. The module docs say so.

### The drivers trust byte counts

The drivers trust the counts that a codec, a source, or a sink reports.
Nobody can check most wrong counts. The one visible error is an
`amount` larger than the buffer. In this crate, every source and sink
checks it with `debug_assert!` and clamps the amount.

We do not use a panic in release builds. A panic on the one visible
error would suggest that the contract is enforced, but it is not. The
trait docs of `Source::consume` and `Sink::commit` say this.

The `std_io` and `embedded_io` source adapters pass `consume` to the
inner reader. They follow the rules of that reader. This is on purpose.

### No limit on codecs that never finish

If `finish` or `process` returns `OutputFilled` on every call, the
drain loop runs forever. With `VecSink`, it allocates until memory runs
out. We do not add a limit. A limit on the number of passes would be
arbitrary, and it would break valid codecs with very large output. The
docs of `flush` and `finish` say "call again until `Done`", which
assumes that the codec ends.

### The reader asks for input before it drains held codec output

`Pump::transfer_step` reads input before it drains the codec. A
blocking source can stall here. This happens for example when a codec
holds an incomplete base64 group. The delay is one read at each
boundary of an atomic unit. We accept it and do not plan to fix it.
A design note in `stream.rs` explains why.

To see the delay, run this in a terminal and type single-letter lines:
`cargo run -q -p cli -- --readers base64-enc --writers base64-dec`.
The `--help` text of the CLI describes the limit.

Issue #24 describes an effect that we can see in the CLI. It might be
caused by this limit. We have not confirmed it.

### `CodecWriter` has no `Drop` impl and no `#[must_use]` (decided 2026-10-07)

If the caller drops a `CodecWriter` without `finish`, the trailer is
lost. The docs say so. We add nothing, for these reasons:

- `#[must_use]` does not warn for a bound variable that goes out of
  scope. On `new`, it adds nothing, because `Result` is already
  `#[must_use]`.
- A `debug_assert!` in `Drop` gives false alarms. It can also abort the
  process by a double panic. `std::thread::panicking()` needs `std`,
  but the `embedded_io` wrapper is `no_std`.
- A `finish` in `Drop` hides I/O and loses the error.

We can add a `Drop` impl later without a breaking change, because the
fields are private. We will do it if users report lost trailers.

### The error enums are not `#[non_exhaustive]` (decided 2026-10-03)

A third-party codec cannot add a variant to `ErrorKind`. The enum grows
only when this crate adds a category. That is rare, and a new variant
before 1.0 needs only a 0.x version bump. `#[non_exhaustive]` would
also stop users from matching all variants, so the compiler would not
tell them about a new kind.

`EmbeddedError` and `WriteError` have no `source()`. Their `Display`
prints the inner error. A `source()` impl would need a `'static` bound
on the error type of the wrapped reader or writer.

### Comment style in private comments

Some private comments in `base64_shared.rs`, `base64_dec.rs`,
`json_enc.rs`, and the `shared_io/source.rs` tests use long sentences,
semicolons, and dashes. We do not rewrite them. Only readers of the
source see them. Do not report them. Public docs follow a plain style.

### `Base64Dec` latches `done`, `Base64Enc` does not

`process` after `finish` on `Base64Enc` emits padding in the middle of
the output. The `Codec` contract leaves `process` after `finish` to the
codec. A codec can continue, or it can return `Err`. Nobody can rely on
either behavior.


## Known limits that we do not plan to fix

- A third-party codec cannot report its own error detail. It must
  choose one of the four kinds of `ErrorKind`. "Checksum mismatch" and
  "invalid Huffman table" both become `CorruptStream`.
- `Pump` and `Chain` cannot check that a codec reports its counts
  honestly. They check only that the counts fit the buffers.
- The `Sink` contract does not say what a failed `commit` did. The
  counts that `stream_to_stream` returns with an error assume that it
  committed nothing. See issue #20.
