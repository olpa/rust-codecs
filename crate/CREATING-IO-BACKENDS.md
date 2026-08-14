# Creating an I/O backend

How to add a `Source`/`Sink` adapter for a new byte transport, in your
own crate, against `rust-codecs-core`'s public API — the same shape as
this crate's own `std_io`/`embedded_io` backends.

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

## Wrapping it as `Read`/`Write`

Reuse `Pump` and `sources_and_sinks::shared_io` — don't hand-roll the
chunk/commit drive loop; `shared_io` already is that loop, public for
exactly this purpose. Hold a `Pump<C>` next to your adapter, the same
way `std_io::wrapper::CodecReader`/`CodecWriter` hold one next to
`StdSource`/`StdSink`, and let one `shared_io` call implement each
`Read`/`Write` method:

```rust
use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::shared_io::pump_read;
use rust_codecs_core::{DriveError, Pump, Source, TerminatingCodec};

struct YourReader<I: Source, C: TerminatingCodec> {
    input: I,
    pump: Pump<C>,
}

impl<I: Source, C: TerminatingCodec> YourReader<I, C> {
    fn new(input: I, codec: C) -> Self {
        Self { input, pump: Pump::new(codec) }
    }

    // Wire this into `std::io::Read`/`embedded_io::Read`/whatever
    // your transport's own read trait is, mapping `DriveError` into
    // your error type at the boundary (see `reader_error` in
    // `std_io::wrapper`/`embedded_io::wrapper` for the pattern).
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, DriveError<I::Error, Infallible>> {
        pump_read(&mut self.pump, &mut self.input, buf)
    }
}
```

`shared_io` has one function per operation — `pump_read`, `pump_write`,
`pump_finish`, `pump_flush` — each the whole body of the matching
`Read`/`Write` method. Map their `DriveError` result into your own
error type at the call site (`reader_error`/`writer_error` in
`std_io::wrapper`/`embedded_io::wrapper` are the templates).
