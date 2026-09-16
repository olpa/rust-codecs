# Creating an I/O backend

The traits `Source` and `Sink` abstract a custom byte transport. This is the
only part you must implement.

Optionally, if your transport provides counterparts of `std::io::Read`/`Write`
or `std::io::BufRead`, add a way to wrap them with a `Codec`. This produces
a new `Read`/`Write`.

## `Source`/`Sink`

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
adapter itself owns. A caller calls `consume`/`commit` to say how much
of the window it used. The next call then returns at least the
unconsumed remainder, possibly with more data appended.

A caller can call `spare` again without committing first. The adapter
may then return the same span again. Bytes already written into it but
not committed may be overwritten.

`chunk` returns `None` at the end of input. `spare` returns `None` when
there is no room left. Neither returns `Some` of an empty slice. Return
`None` instead.

`Sink::finish` defaults to a no-op. Override it if your transport
needs a final flush once the codec's stream has ended.

A custom `Source`/`Sink` can be a thin wrapper over `shared_io`'s
template implementation, which provides:

- scratch buffer management
- retry on interrupted reads and writes

## `Read`/`Write`

This part is more boilerplate code, but straightforward.

The shared work lives in `Pump` and the `pump_*` functions.
Your wrapper is again a thin shell around them.

For a `BoundaryAwareCodec`, the reader yields whatever bytes the codec
produced up to its boundary. It then reports EOF on the next call. The
shared code already gives you this behavior. You do not need to
implement anything extra. It is mentioned here because this behavior
is not obvious in advance.

Nothing else about this is a surprise. Follow `std_io`/`embedded_io`
as a template. Do not forget the buffered version.
