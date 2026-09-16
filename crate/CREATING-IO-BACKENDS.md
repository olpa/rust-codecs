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
    fn spare(&mut self) -> Result<Option<&mut [MaybeUninit<u8>]>, Self::Error>;
    fn commit(&mut self, amount: usize) -> Result<(), Self::Error>;
    fn finish(&mut self) -> Result<(), Self::Error> { Ok(()) }
}
```

`chunk` and `spare` return a borrowed window into storage that the
adapter itself owns. A caller should call `consume`/`commit` to say
how much of the window it used.

The next call returns at least the
  unconsumed remainder, and may append more data after it. 

If a caller calls
  `spare` again without committing the previous one, the adapter may
  return the same span back, and bytes already written into it but not committed may
  be overwritten.

Return `None` for exhausted: end of
  input for `chunk`, no room left for `spare`. Never return `Some` of
  an empty slice; return `None` instead.

`Sink::finish` defaults to a no-op. Override it only if your
  transport needs a final flush once the codec's stream has ended.

A custom `Source`/`Sink` can be a thin wrapper over `shared_io`'s
template implementation, which provides:

- scratch buffer management
- retry on interrupted reads and writes

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
