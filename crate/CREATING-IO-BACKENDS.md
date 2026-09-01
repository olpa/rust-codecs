# Creating an I/O backend

How to add a `Source`/`Sink` adapter for a new byte transport, in your
own crate, against `rust-codecs-core`'s public API — the same shape as
this crate's own `std_io`/`embedded_io` backends.

Note what this crate's own backends deliberately don't do: `StdSource`/
`EmbeddedSource`/`BufReadSource` never cache "the wrapped reader once
returned nothing" — they just re-attempt the read on the very next
`chunk()` call, with no memory of the last one. That's what lets one of
these be handed a transport whose "nothing right now" isn't forever (a
growing file, a pipe) and have it pick up later bytes on its own,
instead of latching itself shut the first time it sees an empty read —
at the cost of one real I/O attempt per `chunk()` call for as long as
the transport stays empty. A backend for a transport with a genuine,
final EOF (and no reason to expect more bytes ever) is free to cache
that instead and skip the repeated attempts — the trait doesn't require
either choice.

## Implement `Source`/`Sink` for your transport

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
- **`spare` never needs a matching `commit`.** A caller is free to
  call `spare` again without having committed the previous one — the
  same span (or an equivalent one) is simply re-offered. This mirrors
  `chunk`/`consume`: nothing is lost by not committing, since whatever
  was (or wasn't) written into the returned window is still there, or
  is irrelevant, either way.
- **`Sink::finish` defaults to a no-op** — override it only if your
  transport needs a final flush once the codec's stream has ended
  (`StdSink`/`EmbeddedSink` forward to the wrapped writer's own
  `flush`).

`StdSource`/`StdSink` (in `rust-codecs-core`'s own
`sources_and_sinks::std_io` module — both public, so you can read them
directly) are the template: a caller-provided scratch buffer
(`S: AsMut<[u8]>`), asserted non-empty at construction (a caller bug,
not a runtime condition — panic, don't return a `Result`), plus
`into_inner`/`get_mut` for reclaiming/bypassing the wrapped transport.
`Self::Error` is whatever error type your transport itself reports.

## Skipping the scratch-buffer bookkeeping

You don't have to implement `Source`/`Sink` by hand. `std_io` and
`embedded_io` share their scratch-buffer/`spare`/`commit` bookkeeping
and interrupt-retry logic through `sources_and_sinks::shared_io`'s
`ScratchSource`/`LendingSource`/`ScratchSink` — all public, along with
the `EintrRead`/`EintrFillBuf`/`RetryingWrite` traits they're
generic over. If your transport looks like a single retrying
`read`/`write`/`fill_buf` call, you can build on these instead of
reimplementing that bookkeeping yourself, the same way `std_io`'s and
`embedded_io`'s own `Source`/`Sink` adapters do. See
`sources_and_sinks::std_io::adapter`/`sources_and_sinks::embedded_io::adapter`
(`StdSource`/`StdSink`, `EmbeddedSource`/`EmbeddedSink`) for the
pattern to copy.

## Wrapping it as `Read`/`Write`

Reuse `Pump` and `sources_and_sinks::shared_io` — don't hand-roll the
chunk/commit drive loop; `shared_io` already is that loop, public for
exactly this purpose. Hold a `Pump<C>` next to your adapter, the same
way `std_io::wrapper::CodecReader`/`CodecWriter` hold one next to
`StdSource`/`StdSink`, and let one `shared_io` call implement each
`Read`/`Write` method:

```rust
use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::shared_io::end_signalling_pump_read;
use rust_codecs_core::{DriveError, Pump, Source, EndSignallingCodec};

struct YourReader<I: Source, C: EndSignallingCodec> {
    input: I,
    pump: Pump<C>,
}

impl<I: Source, C: EndSignallingCodec> YourReader<I, C> {
    fn new(input: I, codec: C) -> Self {
        Self { input, pump: Pump::new(codec) }
    }

    // Wire this into `std::io::Read`/`embedded_io::Read`/whatever
    // your transport's own read trait is, mapping `DriveError` into
    // your error type at the boundary (see `reader_error` in
    // `std_io::wrapper`/`embedded_io::wrapper` for the pattern).
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, DriveError<I::Error, Infallible>> {
        end_signalling_pump_read(&mut self.pump, &mut self.input, buf)
    }
}
```

`shared_io` has one function per operation — `end_signalling_pump_read`, `pump_write`,
`pump_finish`, `pump_flush` — each the whole body of the matching
`Read`/`Write` method. Map their `DriveError` result into your own
error type at the call site (`reader_error`/`writer_error` in
`std_io::wrapper`/`embedded_io::wrapper` are the templates).

`end_signalling_pump_read` returns as soon as one pull from `input` actually yields
output, instead of chasing a full `buf` — the interactive-application
behavior: a handler downstream of your reader sees each unit `input`
produces (a terminal line, a network datagram) as soon as it arrives,
not only once enough of them have piled up to fill whatever buffer a
caller driving your reader through something like `std::io::copy`
happens to be using. A pull that consumes input but produces no output
yet (a codec buffering several input bytes before it can emit
anything) doesn't count as a stopping point: `end_signalling_pump_read` loops past
those, since returning `0` there would be indistinguishable from EOF
to the caller.

If your transport already exposes a lending, buffered read (an
`fill_buf`/`consume` shape, like `std::io::BufRead`/
`embedded_io::BufRead`), skip the scratch buffer entirely — see
`BufReadSource`/`BufReadCodecReader` in `std_io`/`embedded_io` for the
pattern: adapt straight from `fill_buf`/`consume` instead of copying
into a buffer of your own.

## Testing your `Read`/`Write` wrapper

Write your own test doubles against your own transport trait, the way
this crate's `std_io`/`embedded_io` adapters each keep their own
`FlakyOnce` for retry tests, and its `shared_io::read` module keeps its
own minimal `EndSignallingCodec` double (a codec that ends its stream
in-band after a fixed number of bytes) — use it the same way to prove
your reader stops yielding bytes and reports EOF right at a codec's
in-band end.
