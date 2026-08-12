# Creating an I/O backend

This document covers how to **add** a new byte-transport backend under
`core/src/sources_and_sinks/` — an adapter from some transport
(`std::io`, `embedded_io`, a hypothetical async runtime, a ring
buffer, …) to the crate's own `Source`/`Sink` traits, optionally with
a `Read`/`Write`-style wrapper on top. See [`CREATING-CODECS.md`](./CREATING-CODECS.md)
for how to **create** a codec, and the `sources_and_sinks` module docs
for how to **use** an existing backend.

The existing backends are the reference: `std_io` and `embedded_io`
(adapter + wrapper, for incremental stream transports), `vec` and
`slice` (adapter only, for fully in-memory transports).

## 1. Implement `Source`/`Sink` for your transport

```rust
pub trait Source {
    type Error;
    fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error>;
    fn consume(&mut self, amount: usize);
}

pub trait Sink {
    type Error;
    fn spare(&mut self) -> Result<Option<&mut [u8]>, Self::Error>;
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;
    fn finish(&mut self) -> Result<(), Self::Error> { Ok(()) }
}
```

Both are lending: `chunk`/`spare` hand back a borrowed window into
storage the adapter itself owns (a scratch buffer, in `StdSource`'s
case), not the caller's. Load-bearing details, easiest to get wrong:

- **"Current" is not "fresh."** `chunk`/`spare` return whatever hasn't
  been released by `consume`/`commit` yet. A caller is never required
  to consume/commit a whole window in one call — the unconsumed
  remainder is exactly what the next call returns, so consecutive
  windows can overlap. Don't hand out new bytes ahead of the
  unconsumed position.
- **`None` means exhausted**, not "call again later" — end of input
  for `Source`, no room for `Sink`.
- **`spare` must be followed by `commit`** before the next `spare`
  call; both existing `Sink` impls (`StdSink`, `EmbeddedSink`,
  `VecSink`) enforce this with `assert_eq!(self.offered, 0, "commit
  must follow spare")`, so a driver that violates it panics loudly
  rather than corrupting state silently.
- **`Sink::finish` defaults to a no-op** — override it only if your
  transport needs a final flush once the codec's stream has ended
  (`StdSink`/`EmbeddedSink` forward to the wrapped writer's own
  `flush`).

`StdSource`/`StdSink` (`core/src/sources_and_sinks/std_io/adapter.rs`)
are the template: a caller-provided scratch buffer
(`S: AsMut<[u8]>`), asserted non-empty at construction (a caller bug,
not a runtime condition — panic, don't return a `Result`), plus
`into_inner`/`get_mut` for reclaiming/bypassing the wrapped transport.
`Self::Error` is whatever error type the transport itself reports —
`std::io::Error` for `std_io`, `R::Error`/`W::Error` for `embedded_io`.

## 2. Decide whether you need a `Read`/`Write`-style wrapper

If your transport is fully in-memory, stop here —
[`stream_to_stream`](core/src/lib.rs) is the whole story, the same way
`vec`/`slice` have no wrapper (see `sources_and_sinks/vec/mod.rs`'s
module doc).

If it's an incremental stream transport, add `wrapper.rs` with
`CodecReader`/`CodecWriter`, built on the crate-internal `Pump` plus
`sources_and_sinks::shared_io`'s `pump_read`/`pump_write`/
`pump_finish`/`pump_flush` — these already implement the chunk/commit
loop, so a `Read`/`Write` impl is a couple of lines each. Follow
`std_io::wrapper`'s ownership split: the reader owns input scratch and
writes straight into the caller's output buffer; the writer reads
straight from the caller's input buffer and owns output scratch — see
that module's doc comment for why (`BufReader`/`BufWriter` placement
in the client's own stack is the right place for batching policy, not
a knob inside these wrappers).

`Pump` and `shared_io` are `pub(crate)` — this only works for a
backend added inside this crate. A `Source`/`Sink` impl from outside
the crate (both traits are public) still works with `stream_to_stream`
for free, but can't build its own incremental wrapper without
reimplementing the pump loop by hand.

Map `DriveError` into your own error type at the wrapper boundary
(`reader_error`/`writer_error` in `std_io::wrapper`/`embedded_io::wrapper`
are the templates). `DriveError::SinkExhausted`/`NoProgress` carry no
endpoint data of their own — they mean the pump/codec pairing itself
broke, not the transport, so route both through the crate's own
`ErrorKind::ContractViolation` rather than a bespoke message (see
`adapter_contract_violation` in either existing wrapper).

`CodecWriter` must be bound to `Codec`, never `TerminatingCodec`: an
in-band end has no representation as a permanent short write from
`Write::write`. It also has no `Drop`-based safety net — forgetting to
call `finish` silently truncates output, and that's accepted,
documented behavior (see `CodecWriter`'s own doc comment), not
something a new backend needs to solve differently.

## 3. Wire it into `sources_and_sinks`

- Add `mod your_backend;` in `sources_and_sinks/mod.rs`, `pub` if it
  should be part of the public API (every current backend is). Gate
  it behind a feature if it pulls in a new dependency, mirroring
  `embedded-io`.
- Add one bullet to `sources_and_sinks/mod.rs`'s own module doc,
  matching the existing list.
- If it uses `shared_io`, make sure your feature is included in that
  module's `#[cfg(any(feature = "embedded-io", feature = "std", ...))]`
  gate.

## 4. Test it

- Round-trip via `stream_to_stream` against `VecSource`/`VecSink` in
  both directions (see `std_io::adapter::tests::std_input_can_feed_vec_output`/
  `vec_input_can_feed_std_output` for the pattern).
- Empty scratch buffer panics on construction
  (`#[should_panic(expected = "buffer must be non-empty")]`).
- If you added a wrapper: buffer-size edge cases through
  `CodecReader`/`CodecWriter`, including a buffer smaller than the
  codec's atomic output unit if it has one.
- If bound to `TerminatingCodec`: in-band end handling — the reader
  stops exactly at the codec's reported end and reports EOF/`0`
  forever after, without touching the codec again (see
  `reader_stops_at_in_band_end` in either existing wrapper).

That's the whole surface: adapt the transport to `Source`/`Sink`,
optionally wrap it as `Read`/`Write` on top of `Pump` + `shared_io`,
wire it into `sources_and_sinks`, and the rest of RustCodecs
(`stream_to_stream`, `Chain`, every codec) works with it for free.
