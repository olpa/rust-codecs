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

The drivers remain fragmented:

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

Stream adapters additionally repeat scheduling policy: acquiring new
input, draining output, detecting endpoint EOF, entering `finish`, and
retaining state across partial operations.

## Architecture

Use three layers:

```text
I/O-domain adapters
        |
Sans-I/O resumable stream driver
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

### B — Sans-I/O resumable stream driver

Build a domain-neutral driver which owns stream scheduling state but
does not perform I/O. Advancing it conceptually produces an externally
actionable state:

```rust
enum DriverState {
    NeedInput,
    HaveOutput,
    Finished,
}
```

The final API may need explicit `Runnable`, `Finishing`, or
`CodecEnded` states; these names are illustrative.

The driver owns:

- input and output cursor state;
- repeated use of A across changing windows;
- partial input and pending output;
- explicit input EOF;
- transition from `process` to `finish`;
- in-band `StreamEnd` latching;
- state across suspension;
- consistent no-progress handling.

The driver does not:

- call `std::io`, `embedded_io`, or async traits;
- wait, poll, spawn, or select an executor;
- choose whether blocking an async executor is acceptable;
- convert endpoint errors;
- allocate without an explicit convenience API;
- compose multiple codecs internally.

The preferred interaction is Sans-I/O buffer exchange: an adapter
supplies input bytes and consumes pending output while the driver keeps
cursors and lifecycle state. Storage remains caller-provided where
practical, using `S: AsMut<[u8]>`.

Before fixing the API, compare two storage models:

1. **Stable scratch buffers.** The driver owns caller-provided generic
   buffer values which adapters fill and drain. Suspension across async
   `Pending` and partial writes is straightforward.
2. **Lent endpoint windows.** Adapters lend slices directly. This can
   avoid copies, but lifetimes become harder, especially across async
   suspension and wrapper calls.

The expected design is hybrid: correctness uses stable caller-provided
storage; direct-window paths are optional optimizations where clearly
safe and useful.

### I/O-domain adapters

Each ecosystem retains its native traits. Do not define a new public
universal `Read`/`Write`, `Source`/`Sink`, or async trait family.

Adapters translate between a native endpoint and B:

- satisfy `NeedInput`;
- drain `HaveOutput`;
- map endpoint and codec errors;
- suspend or return according to the native trait contract.

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

### Step 1 — specify and test A

- Choose the internal `Transfer` API.
- Centralize validation and exact count derivation.
- Test every outcome, zero-sized windows, overclaims, in-band end, and
  errors.
- Replace the three transfer sites in `stream_to_stream` and `Chain`.
- Later migrate direct transfer logic in existing std/vector adapters.

### Step 2 — prototype B privately

- Implement B over caller-provided buffers.
- Define input submission, EOF declaration, output exposure and
  consumption, and advancement.
- Test suspension at every state boundary.
- Make finish and no-progress behavior explicit and uniform.

### Step 3 — rewrite current frontends over B

- Make `stream_to_stream` an iterator frontend over B.
- Rewrite `CodecReader`, `CodecWriter`, and `to_vec`.
- Preserve native short-read/write, EOF, `WriteZero`, partial sink,
  flush, and error behavior.
- Compare code size and clarity with A-only before committing to B.

### Step 4 — establish portability

- Make core `no_std`, with `alloc` and `std` feature layers.
- Gate `std::io` behind `std`; gate vectors and boxed codecs behind
  `alloc`.
- Keep `Codec`, `Chain`, A, and B available without either.
- Add host and embedded-target checks to CI.

### Step 5 — embedded adapters

- Add native `embedded_io` reader/writer wrappers over B.
- Use caller-provided buffers.
- Preserve both endpoint and codec errors.
- Test on host and at least one `no_std` target where practical.

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
