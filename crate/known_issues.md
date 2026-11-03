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

`pump_write` already does this for codec errors. In these cases
`write` can still return `Err` after it took bytes from `buf`:

- A sink error (`spare` or `commit`) after an earlier step committed
  output.
- A failed commit. The codec has consumed the input of that step.
- `NoProgress` after an earlier step moved bytes.

The issue stays open until all three are fixed. Then remove the
`FIXME` in `pump_write`.

The same problem affects `CodecWriter::finish`. Its error gives back
the `CodecWriter`, but a second `finish` after a sink error loses the
trailer bytes of the failed commit. The doc of
`std_io::CodecWriter::finish` warns about this. Remove the warning
after the fix.

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

### `Base64Dec` ignores whitespace with every engine

`Base64Dec` ignores whitespace for every engine. An alphabet with a
space does not work with it. No common alphabet uses a space, so we
document the limit and do not add an option.

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

### The step asks for input before it looks at the output side

`Pump::transfer_step` calls `input.chunk()` first. A blocking source
can stall there in two cases.

**Case 1: the codec holds output.** For example, the codec holds an
incomplete base64 group. The output waits for the next read. The
delay is one read at each boundary of an atomic unit. We accept it
and do not plan to fix it. A design note in `stream.rs` explains why.

To see the delay, run this in a terminal and type single-letter lines:
`cargo run -q -p cli -- --readers base64-enc --writers base64-dec`.
The `--help` text of the CLI describes the limit.

Issue #24 describes an effect that we can see in the CLI. It might be
caused by this limit. We have not confirmed it.

**Case 2: the sink is full.** In `stream_to_stream`, the sink can be
full while the source has no buffered bytes. The drive waits for one
read, and then it reports `SinkExhausted`. No data is lost.

We accept it and do not plan to fix it. If the step calls
`output.spare()` first, it cannot tell end of input from more input.
Then a sink that the output fills exactly can report `SinkExhausted`
instead of success, or it can allocate memory that nobody uses.

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

`StreamError` and `WriteError` have no `source()` for the error of the
wrapped reader or writer. Their `Display` prints the inner error. A
`source()` impl would need a `'static` bound on the error type of the
wrapped reader or writer. `StreamError::source` returns the codec
`Error` only.

### Error chains repeat the codec error message (decided 2026-10-23)

`StreamError` prints the codec error in its `Display` and returns it
from `source()`. `EncodeError::Stream` prints the `StreamError` and
returns it from `source()`. A printer that walks `Error::source()`
(`anyhow`, `eyre`, the CLI's `error_chain`) shows the text two or three
times, for example "corrupt stream (after 0 input bytes, 0 output
bytes): corrupt stream".

We accept this. Users who print with plain `{}` (embedded, `no_std`,
`unwrap` panics, test output) must get the whole message from
`Display`, and they are the users that we care about. `source()` stays,
so generic code can still downcast to `StreamError` and read the byte
counts. Removing the `StreamError` level from `EncodeError::source`
would hide those counts from code that holds only a `dyn Error`.

### The CLI engines print the same error in different words (decided 2026-10-23)

For the same failure, the `copy` engine prints "corrupt stream (after 0
input bytes, 0 output bytes)", and the `stream` engine prints "codec
error (after 0 input bytes, 0 output bytes): corrupt stream".

The `copy` engine gets an `io::Error` with a `StreamError` inside, and
its `Display` has the codec reason. The `stream` engine gets a
`DriveError`, and its `Display` has no reason, so the CLI appends the
`source()` chain. Both messages have the same facts, and the CLI is a
small demo tool. We do not change the CLI or the `Display` of
`DriveError` for this.

### Comment style in private comments

Some private comments in `base64_shared.rs`, `base64_dec.rs`,
`json_enc.rs`, and the `shared_io/source.rs` tests use long sentences,
semicolons, and dashes. We do not rewrite them. Only readers of the
source see them. Do not report them. Public docs follow a plain style.

### The "Why this matters" paragraph in `CREATING-CODECS.md` mentions an earlier `Progress`

The section "The contract: fully consume, or fully fill" says that
`Progress` once had two fields, `consumed` and `written`, and that the
drivers became too complicated. This is design rationale, not history
for its own sake. A reader who asks "why not report both counts?" learns
that the crate tried it and why it failed. We keep the paragraph. Do not
report it as text that only makes sense to someone who knew the old
behavior.

### `Base64Dec` latches `done`, `Base64Enc` does not

`process` after `finish` on `Base64Enc` emits padding in the middle of
the output. The `Codec` contract leaves `process` after `finish` to the
codec. A codec can continue, or it can return `Err`. Nobody can rely on
either behavior.

### No randomized test compares the drivers and the buffer sizes (decided 2026-10-16)

A review proposed a randomized test. It would run random chains of the
bundled codecs through all drivers with small random buffers. It would
compare each result with a one-shot run. We do not add this test.

The test is not worth the effort. A generic test does not trigger the
errors on the error paths. Only a concrete test triggers them, so we
write a concrete test for each error path. On the happy paths, a driver
and the one-shot run would give different results only after a critical
defect in the core. The core tests catch such a defect.

### The accessors of the wrapped reader or writer have no warning (decided 2026-10-19)

`get_mut` on `CodecReader` and `BufReadCodecReader` (std and embedded
variants) gives the caller the wrapped reader. A direct read skips the
bytes that the wrapper has already pulled into its buffer. The doc
comments do not say this. The same holds for `into_inner`, `into_parts`
and the accessors of the writer wrappers. We add no warning.

A user who wraps a stream expects that the wrapper reads ahead. The
standard library uses the same accessors on `BufReader` and
`BufWriter`. A direct read is a misuse that the caller must do on
purpose. Do not report it.

### `encode_str` and `encode_string` accept only `str` input (decided 2026-10-19)

Both functions take `impl AsRef<str>`. They are for strings, and they
are small wrappers over `VecSource`, `VecSink` and `stream_to_stream`.
We add no bytes variant. A client that needs bytes builds the pipeline
with its own wrapper. The name "encode" for a decoder call such as
`encode_string(base64_dec(), ...)` stays. Do not report it.

### Local `cargo doc` warns about four links (decided in 592acf2)

With the default features, `cargo doc` reports unresolved links to
`rot13`, `base64_enc`, `base64_dec` and `json_enc`. docs.rs builds
with all features, so the published docs are correct. Use
`cargo doc --all-features` locally. Do not report it.

### The buffer types need `AsMut<[u8]>` to be a stable view (decided 2026-10-24)

`Chain`, `ScratchSource` and `ScratchSink` take `S: AsMut<[u8]>`. They
call `as_mut()` many times and keep indexes between the calls:
`stage_start` and `stage_end` in `Chain`, `pos` and `len` in
`ScratchSource`, `offered` in `ScratchSink`. The trait does not say
that `as_mut()` returns the same slice each time. The code needs this
implicit contract:

> As long as no other `&mut self` method of the object is called,
> `as_mut()` is stable: it always returns the same slice.

The crate writes only through the slice from `as_mut()`. Only
`ScratchSource::pending` calls `as_ref()`, and it only reads. It needs
one more rule:

> `as_ref()` returns the same slice as `as_mut()`.

So the same slice means the same length, the same memory and the
bytes that were written earlier.

`Vec<u8>`, `[u8; N]`, `&mut [u8]` and `Box<[u8]>` keep the contract.
A type that switches between buffers, returns a shorter slice, or
drops writes does not. With such a type the code panics on an index
out of range, or it sends wrong bytes. There is no undefined behavior,
because all indexing is checked and all bytes are initialized.

We cannot call `as_mut()` once and keep the slice. The struct owns the
buffer, and a slice into it would be a self-referential borrow, which
the compiler rejects. A struct that only borrows the buffer could keep
the slice, but then the caller must keep the buffer alive, and there is
no `into_parts`. A concrete `Box<[u8]>` or `&mut [u8]` instead of the
generic bound would remove the contract, but we lose the other buffer
types. We keep the generic bound, add no runtime check and no
documentation on the constructors. Do not report it.

## Known limits that we do not plan to fix

- A third-party codec cannot report its own error detail. It must
  choose one of the existing kinds of `ErrorKind`. "Checksum mismatch"
  and "invalid Huffman table" both become `CorruptStream`.
- `Pump` and `Chain` cannot check that a codec reports its counts
  honestly. They check only that the counts fit the buffers.
- The `Sink` contract does not say what a failed `commit` did. The
  counts that `stream_to_stream` returns with an error assume that it
  committed nothing. See issue #20.
