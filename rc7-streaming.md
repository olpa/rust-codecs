# rc7-streaming: one streaming mechanism across I/O domains

## Goal

Support the main data path

```text
input stream -> one or more codecs -> output stream
```

when the endpoints may come from different domains:

- `std::io`;
- `embedded_io`;
- asynchronous I/O traits;
- iterators of chunks or reusable buffer slots;
- vectors;
- network or message-buffer pools.

Also support two important partial forms:

- wrap an input stream or an output stream with a codec;
- combine several codecs into one codec.

The implementation must not reproduce byte-transfer, validation,
cursor, EOF, and finishing logic in every environment. The portable
core must not choose an executor, perform blocking I/O, allocate
implicitly, or depend on one I/O trait family.

## Current state

`Codec` is already the correct portable primitive:

```rust
fn process(
    &mut self,
    input: &[u8],
    output: &mut [u8],
) -> Result<Outcome, Error>;
```

It is Sans-I/O, allocation-free, and slice-based. Its contract says
that a successful call consumes all input, fills all output, or ends
the codec stream. `Outcome::validated` checks reported counts.

`Chain<A, B, S>` is also present. It composes two codecs with
caller-provided staging (`S: AsMut<[u8]>`) and is itself a `Codec`. It
implements early-end, finish, flush, return-clean, and validation
semantics.

Checkpoint A is implemented as the private validated window-transfer
primitive used by `stream_to_stream` and every `Chain` process transfer.

Checkpoint B is implemented as a private, bufferless `Driver<C>`. It
owns codec lifecycle and normalization while each directional frontend
retains only the buffers and cursors its endpoint contract requires.

The core is now feature-layered for portability: `std` (default)
implies `alloc`; `alloc` enables vector conveniences and boxed codecs;
without either, codecs, `Chain`, A, B, and `stream_to_stream` remain
available.

With the optional `embedded-io` feature, allocation-free native
`EmbeddedCodecReader` and `EmbeddedCodecWriter` adapters are also
available without `std`.

The endpoint loops remain necessarily directional:

- `stream_to_stream` drives iterators of chunks and slots;
- `CodecReader` and `CodecWriter` drive `std::io`;
- `to_vec` is an allocating convenience driver;
- `Chain` transfers both into and out of staging.

The same non-trivial operation appears repeatedly:

1. select remaining input and output windows;
2. call `Codec::process`;
3. validate the outcome;
4. derive exact consumed and written counts from the asymmetric
   `Outcome`;
5. advance cursors;
6. distinguish input exhaustion, output exhaustion, and in-band stream
   termination.

They no longer repeat codec trust-boundary or lifecycle interpretation.
They still acquire and drain native endpoints themselves, because those
operations have different ownership, readiness, and error contracts.

## Architecture

Use three layers:

```text
I/O-domain adapters
        |
bufferless Sans-I/O lifecycle driver
        |
Codec (possibly a Chain)
        |
validated window-transfer primitive
```

### A — validated window transfer

Extract one internal operation over the current input and output
windows:

```rust
struct Transfer {
    consumed: usize,
    written: usize,
    end: TransferEnd,
}

enum TransferEnd {
    InputExhausted,
    OutputExhausted,
    StreamEnd,
}
```

The exact API is still open, but it must:

- call `Codec::process` and validate its outcome;
- return exact progress on both sides;
- use neutral boundary events rather than adapter-specific errors;
- know nothing about iterators, readers, writers, staging, EOF, or
  endpoint error conversion.

This operation is independently useful and should be shared by:

- the Sans-I/O stream driver;
- caller input -> `Chain` staging;
- `Chain` staging -> caller output;
- direct slice-based convenience code.

Errors remain codec-level here. A caller such as `Chain` rewrites
counts into its own boundary-level accounting.

Because the `Codec` contract guarantees that one successful call
reaches one of the three boundaries, there is no meaningful multi-call
loop over one fixed pair of windows. Reuse above A requires a mechanism
for replacing input or output windows.

### B — bufferless Sans-I/O lifecycle driver

`Driver<C>` owns the codec and its ended state. It accepts input and
output windows lent by a frontend and provides three normalized
operations:

- `process` uses A and latches in-band `StreamEnd`;
- `finish` validates exact drain progress and latches completion;
- `flush` validates exact drain progress without ending the stream.

The driver owns no byte storage and therefore cannot introduce a copy.
Directional frontends retain cursor state according to their natural
ownership model:

- `CodecReader` owns input scratch and lends caller output directly;
- `CodecWriter` lends caller input and owns output scratch;
- `stream_to_stream` lends its current iterator chunk and slot;
- `to_vec` lends its original input and existing output scratch.

The driver does not:

- call `std::io`, `embedded_io`, or async traits;
- wait, poll, spawn, or select an executor;
- choose whether blocking an async executor is acceptable;
- convert endpoint errors;
- allocate or copy bytes;
- compose multiple codecs internally.

The rejected prototype owned both input and output buffers. Applying it
to the existing frontends would have required a second scratch buffer,
copied caller input, copied generated output, and public constructor
changes. Those costs were not incidental implementation details; they
conflicted with the endpoint ownership models. Directional cursor loops
are therefore intentional rather than failed deduplication.

An async adapter will own whichever stable scratch its native trait
requires across `Pending`, just as the synchronous reader and writer do.
The lifecycle core itself does not need to retain endpoint borrows.

### I/O-domain adapters

Each ecosystem retains its native traits. Do not define a new public
universal `Read`/`Write`, `Source`/`Sink`, or async trait family.

Adapters lend their current windows to B, acquire or drain their native
endpoint, map errors, and suspend or return according to the native
trait contract.

This shares scheduling without pretending all domains have identical
readiness or error semantics.

#### Mixed domains

Input and output adapters are independent, so a frontend may connect
different domains. Its execution policy must nevertheless be explicit.

For example, `std::io::Read -> async output` has no universally correct
policy. Calling the reader may block the executor; an executor-specific
blocking pool or thread/channel bridge may be needed; or the caller may
accept blocking. Likewise, synchronous code cannot drive a genuinely
asynchronous endpoint without an executor.

B supports the data flow, but the chosen frontend or application owns
the executor and blocking policy.

### Codec composition

Keep composition orthogonal to endpoint driving:

```text
Chain<A, B, S>: Codec
        |
ordinary Sans-I/O driver
```

Every frontend therefore sees one `Codec`, elementary or composed.
Static nested chains and folded `Box<dyn Codec>` chains remain possible.

Do not initially make B understand an N-stage codec graph. Such a
scheduler might later improve dynamic pipelines or buffer management,
but it would have to define N-stage flush, finish, early termination,
error accounting, and a way for the pipeline to remain a `Codec`.
Start with composition at the `Codec` level and share A with `Chain`.

## Recorded decisions

### Caller-provided generic storage

Continue using `S: AsMut<[u8]>`:

- `&mut [u8]` for borrowed or embedded storage;
- `[u8; N]` for owned inline, `no_std` storage;
- `Vec<u8>` for allocating environments.

Allocating convenience constructors may exist behind `alloc`, but the
portable mechanism must not require allocation.

### Return-clean composition

`Chain::process` remains return-clean: when it returns, staging contains
only bytes the second codec could not accept because caller output was
full or because the codec buffered a partial unit. It must not withhold
deliverable bytes across calls.

This is an interactive semantic guarantee, not a buffering-policy knob.

### Buffering policy remains external

Scratch buffers are workspaces, not queues. Batching belongs in the
native ecosystem (`BufReader`, `BufWriter`, async or embedded buffering)
or in an explicit higher-level frontend.

### Native I/O traits remain native

Reject a library-specific universal endpoint trait as the public
integration mechanism. It would fragment interoperability and would
not solve sync/async execution policy. Shared logic belongs in B; thin
adapters implement native traits.

## Evaluation before implementation

Design B on paper and trace it through:

1. `std::io::Read -> codec chain -> std::io::Write`;
2. async input -> codec chain -> `Vec`;
3. `std::io::Read -> codec -> async output`, with blocking policy
   outside the core;
4. iterator chunks -> codec -> iterator/buffer-pool slots;
5. wrapped `CodecReader`;
6. wrapped `CodecWriter`;
7. `embedded_io::Read` and `embedded_io::Write` wrappers;
8. one-byte input, output, scratch, and chain staging;
9. a codec which buffers input and produces no output for a turn;
10. early in-band `StreamEnd`;
11. endpoint EOF followed by multi-buffer `finish`;
12. partial sink writes and async `Pending`;
13. endpoint failure with pending driver input or output;
14. codec error after earlier progress in one public adapter call.

For each trace, record:

- state owned by the driver and by the adapter;
- when input bytes are considered accepted;
- when output bytes may be overwritten;
- how EOF is declared;
- how errors preserve progress;
- whether copying is required;
- where suspension may occur.

Compare two concrete implementations:

- **A-only:** adapters retain their loops and share validated window
  transfer only.
- **A+B:** adapters use the resumable driver while `Chain` uses A.

B earns its complexity only if reader, writer, iterator, embedded, and
async traces become materially simpler without callbacks, lending
traits, or lifetime machinery which merely hides the same loops.

## Implementation plan

### Step 1 — specify and test A — complete

- Choose the internal `Transfer` API.
- Centralize validation and exact count derivation.
- Test every outcome, zero-sized windows, overclaims, in-band end, and
  errors.
- Replace the three transfer sites in `stream_to_stream` and `Chain`.
- Later migrate direct transfer logic in existing std/vector adapters.

Implemented in `core/src/transfer.rs` and reused by
`stream_to_stream`/`Chain`.

### Step 2 — prototype B privately — complete, then revised

- Implement B over caller-provided buffers.
- Define input submission, EOF declaration, output exposure and
  consumption, and advancement.
- Test suspension at every state boundary.
- Make finish and no-progress behavior explicit and uniform.

The first prototype owned stable buffers and proved that lifecycle state
could survive suspension. Step 3 showed that using it universally would
add copies and buffers, so it was replaced by the bufferless directional
core described above.

### Step 3 — rewrite current frontends over B — complete

- Make `stream_to_stream` an iterator frontend over B.
- Rewrite `CodecReader`, `CodecWriter`, and `to_vec`.
- Preserve native short-read/write, EOF, `WriteZero`, partial sink,
  flush, and error behavior.
- Compare code size and clarity with A-only before committing to B.

All four frontends now use `Driver<C>` without changing public APIs or
buffer ownership. The migration removes more code than it adds and the
approved `stream_to_stream` behavior remains covered by its existing
tests.

### Step 4 — establish portability — complete

- Make core `no_std`, with `alloc` and `std` feature layers.
- Gate `std::io` behind `std`; gate vectors and boxed codecs behind
  `alloc`.
- Keep `Codec`, `Chain`, A, and B available without either.
- Defer CI and target-specific setup until pre-production work begins.

Implemented with `std` and `alloc` features and a no-default-features
core. `base64` has default features disabled. The current verification
matrix is:

```text
cargo test -p rust-codecs-core --no-default-features
cargo test -p rust-codecs-core --no-default-features \
  --features alloc,identity,rot13,base64
cargo test --workspace --all-features
```

CI, GitHub Actions, and target installation are intentionally outside
the current development phase.

### Step 5 — embedded adapters — complete

- Add native `embedded_io` reader/writer wrappers over B.
- Use caller-provided buffers.
- Preserve both endpoint and codec errors.
- Test on host and at least one `no_std` target where practical.

Implemented against `embedded-io` 0.7 as an optional dependency with
default features disabled. The reader owns only input scratch and lends
caller output directly; the writer lends caller input and owns only
output scratch. `EmbeddedError<E>` preserves endpoint errors, codec
errors, and the embedded trait's required `WriteZero` condition.

Allocation-free host tests cover reading, writing/finalization,
endpoint errors, and early codec termination:

```text
cargo test -p rust-codecs-core --no-default-features \
  --features embedded-io,identity
```

### Step 6 — asynchronous adapters

- Add the async trait family required by a real consumer first.
- Keep readiness and executor behavior in the adapter/frontend.
- Verify B survives `Pending` without retaining temporary endpoint
  borrows or losing cursor state.
- Add other async ecosystems only when demanded.

### Step 7 — reconsider graph-aware pipelines with evidence

Measure folded/nested `Chain` behavior. Consider an N-codec scheduler
only if dynamic pipelines, buffer count, nesting, or performance give a
concrete reason. It is not required for cross-domain streaming.

## Success criteria

- Exact `Codec::process` outcome interpretation exists once.
- EOF, finish, and cursor scheduling exist once for all frontends.
- `Chain` remains a `Codec` and shares A.
- Core transfer and driving require neither `std` nor allocation.
- Sync, embedded, iterator, vector, and async adapters preserve native
  contracts.
- Input and output endpoints are selected independently, with execution
  policy explicit when domains differ.
- One-byte buffers work everywhere; size affects performance, not
  correctness.
- No abstraction merely hides loops while making ownership, errors, or
  suspension harder to understand.
