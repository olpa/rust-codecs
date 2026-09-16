# Creating an I/O backend

The traits `Source` and `Sink` abstract a custom byte transport. This is the
only part you must implement.

Optionally, if your transport provides counterparts of `std::io::Read`/`Write`
or `std::io::BufRead`, add a way to wrap them with a `Codec`. This produces
a new `Read`/`Write`.

Use the same approach as this crate's own `std_io`/`embedded_io` backends.
These backends are thin wrappers around `shared_io`.

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

Both traits are lending. `chunk` and `spare` return a borrowed window
into storage that the adapter itself owns, for example a scratch
buffer in `StdSource`. The window is not the caller's own storage. The
following details matter most, and are the easiest to get wrong:

- **"Current" does not mean "fresh."** `chunk` and `spare` return
  whatever `consume`/`commit` has not released yet. A caller does not
  have to consume or commit a whole window in one call. The next call
  returns exactly the unconsumed remainder, so consecutive windows can
  overlap. Do not hand out new bytes ahead of the unconsumed position.
- **`None` means exhausted, not "call again later."** For `Source`, it
  means end of input. For `Sink`, it means no room is left.
- **`spare` never needs a matching `commit`.** A caller can call
  `spare` again without committing the previous one. The adapter
  simply re-offers the same span, or an equivalent one. This mirrors
  `chunk`/`consume`: nothing is lost by skipping a commit. Whatever
  was, or was not, written into the returned window is still there, or
  does not matter either way.
- **`Sink::finish` defaults to a no-op.** Override it only if your
  transport needs a final flush once the codec's stream has ended.
  `StdSink` and `EmbeddedSink` forward this call to the wrapped
  writer's own `flush`.

`StdSource` and `StdSink` are the template to follow. You find them in
`rust-codecs-core`'s own `sources_and_sinks::std_io` module, and both
are public, so you can read them directly. Each takes a caller-
provided scratch buffer (`S: AsMut<[u8]>`). The constructor asserts
the buffer is non-empty: this is a caller bug, not a runtime
condition, so it panics instead of returning a `Result`. Each also
provides `into_inner` and `get_mut`, to reclaim or bypass the wrapped
transport. `Self::Error` is whatever error type your own transport
reports.

Note what this crate's own backends deliberately do not do.
`StdSource`, `EmbeddedSource`, and `BufReadSource` never remember that
the wrapped reader once returned nothing. Each `chunk()` call just
tries the read again, with no memory of the last attempt.

This choice matters for a transport whose "nothing right now" is not
permanent, such as a growing file or a pipe. The backend can pick up
later bytes on its own. It does not latch shut the first time it sees
an empty read. The cost is one real I/O attempt per `chunk()` call, for
as long as the transport stays empty.

A transport with a genuine, final EOF is different. If you know no
more bytes will ever come, your backend can cache that fact and skip
the repeated attempts. The trait does not require either choice.

## Skipping the scratch-buffer bookkeeping

You do not have to implement `Source`/`Sink` by hand. `std_io` and
`embedded_io` share their scratch-buffer, `spare`/`commit` bookkeeping,
and interrupt-retry logic through `sources_and_sinks::shared_io`. This
module exposes `ScratchSource`, `LendingSource`, and `ScratchSink`, all
public, along with the `EintrRead`, `EintrFillBuf`, and `RetryingWrite`
traits they are generic over.

If your transport looks like a single retrying `read`, `write`, or
`fill_buf` call, build on these types instead of reimplementing the
bookkeeping yourself. This is exactly what `std_io`'s and
`embedded_io`'s own `Source`/`Sink` adapters do. See
`sources_and_sinks::std_io::adapter` and
`sources_and_sinks::embedded_io::adapter` (`StdSource`/`StdSink`,
`EmbeddedSource`/`EmbeddedSink`) for the pattern to copy.

## Wrapping it as `Read`/`Write`

Reuse `Pump` and `sources_and_sinks::shared_io`. Do not hand-roll the
chunk/commit drive loop yourself: `shared_io` already is that loop,
and it is public for exactly this purpose. Hold a `Pump<C>` next to
your adapter, the same way `std_io::wrapper::CodecReader`/
`CodecWriter` hold one next to `StdSource`/`StdSink`. Then let one
`shared_io` call implement each `Read`/`Write` method:

```rust
use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::shared_io::boundary_aware_pump_read;
use rust_codecs_core::{BoundaryAwareCodec, DriveError, Pump, Source};

struct YourReader<I: Source, C: BoundaryAwareCodec> {
    input: I,
    pump: Pump<C>,
}

impl<I: Source, C: BoundaryAwareCodec> YourReader<I, C> {
    fn new(input: I, codec: C) -> Self {
        Self { input, pump: Pump::new(codec) }
    }

    // Wire this into `std::io::Read`, `embedded_io::Read`, or whatever
    // your transport's own read trait is. Map `DriveError` into your
    // error type at the boundary. See `reader_error` in
    // `std_io::wrapper`/`embedded_io::wrapper` for the pattern.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize, DriveError<I::Error, Infallible>> {
        boundary_aware_pump_read(&mut self.pump, &mut self.input, buf)
    }
}
```

`shared_io` has one function per operation: `boundary_aware_pump_read`,
`pump_write`, `pump_finish`, and `pump_flush`. Each one is the whole
body of the matching `Read`/`Write` method. Map their `DriveError`
result into your own error type at the call site. `reader_error` and
`writer_error`, in `std_io::wrapper`/`embedded_io::wrapper`, are the
templates for this.

`boundary_aware_pump_read` returns as soon as one pull from `input`
actually yields output. It does not wait to fill the whole `buf`. This
gives interactive-application behavior: a handler downstream of your
reader sees each unit that `input` produces, such as a terminal line
or a network datagram, as soon as it arrives. It does not wait until
enough units pile up to fill whatever buffer a caller happens to use,
for example one driving your reader through `std::io::copy`.

A pull that consumes input but produces no output yet does not count
as a stopping point. This can happen when a codec buffers several
input bytes before it can emit anything. `boundary_aware_pump_read`
loops past these pulls, because returning `0` there would look like
EOF to the caller.

## Supporting a buffered reader

If your transport already exposes a lending, buffered read, in the
`fill_buf`/`consume` shape of `std::io::BufRead` or
`embedded_io::BufRead`, add a `BufReadSource`/`BufReadCodecReader`
pair. `BufReadSource` adapts straight from `fill_buf`/`consume`,
instead of copying into a scratch buffer of its own. It plays the same
role as `StdSource`/`EmbeddedSource`, but for a buffered reader.
`BufReadCodecReader` then wraps `BufReadSource` the same way
`CodecReader` wraps `StdSource`/`EmbeddedSource`. See
`BufReadSource`/`BufReadCodecReader` in `std_io::adapter`/
`std_io::wrapper` and `embedded_io::adapter`/`embedded_io::wrapper`
for the pattern to copy.

## Testing your `Read`/`Write` wrapper

Write your own test doubles against your own transport trait. This
crate's `std_io`/`embedded_io` adapters each keep their own `FlakyOnce`
double for retry tests. Its `shared_io::read` module keeps its own
minimal `BoundaryAwareCodec` double: a codec that ends its stream
in-band after a fixed number of bytes. Use a double like this the same
way, to prove that your reader stops yielding bytes and reports EOF
right at a codec's in-band end.
