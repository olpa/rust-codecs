# rust-codecs-core

The foundation crate of RustCodecs. It re-exports the trait surface and
stream adapters that every codec crate and its clients build on, so
neither ever has to `use compcol` directly — `compcol` is an
implementation detail behind this crate's boundary.

This document covers **using** an existing codec. See
[`CREATING-CODECS.md`](./CREATING-CODECS.md) for how to **create** one.

## What's exposed

```rust
pub use compcol::{Error, Progress, Status};

pub trait Codec { /* ... */ } // implement this to add a codec

pub mod io; // CodecReader, CodecWriter, to_vec
```

Deliberately **not** exposed: any `Algorithm`-style pairing trait. A codec
crate exposes each codec through a plain constructor function —
conventionally `<name>_enc()` / `<name>_dec()` for a pair that reverse
each other — so building a codec never needs a trait in scope, just a
function call. See `CREATING-CODECS.md` for why.

## Streaming through `std::io`

Wrap a `Read` to run a codec on the fly as bytes are pulled through, or a
`Write` to run one as bytes are pushed through. These examples use a
ROT13 codec (in a real codec crate, its constructor would come from that
crate, e.g. `rust_codecs_rot13::rot13_dec()`).

**Transforming a file as you read it**:

```rust
use std::fs::File;
use std::io;

use rust_codecs_core::io::CodecReader;
use rust_codecs_rot13::rot13_dec;

fn main() -> std::io::Result<()> {
    let raw = File::open("encoded-hello.txt")?;
    let mut reader = CodecReader::new(raw, rot13_dec());
    io::copy(&mut reader, &mut io::stdout())?;
    Ok(())
}
```

`CodecReader` reads raw bytes from the wrapped source, runs the codec on
them on the fly, and yields the transformed bytes to its own caller. It
detects end-of-stream from the inner reader's EOF and drains the codec
internally — the caller never needs to call `finish()` explicitly.

**Transforming as you write**:

```rust
use std::io::Write;

use rust_codecs_core::io::CodecWriter;
use rust_codecs_rot13::rot13_enc;

fn main() -> std::io::Result<()> {
    let plain = std::fs::read("input-hello.txt")?;
    let mut writer = CodecWriter::new(std::io::stdout().lock(), rot13_enc());
    writer.write_all(&plain)?;
    let _stdout = writer.finish()?;
    Ok(())
}
```

`CodecWriter` runs the codec on bytes on the fly as they're written to
it. Unlike reading, writing has no built-in "no more input" signal —
`write_all` just accepts bytes — so the caller must call `.finish()`
explicitly once all input has been written. `finish()` flushes any bytes
the codec was still holding, finalizes the stream (trailer, checksum,
padding — for a stateful codec), and hands back ownership of the wrapped
writer.

There's one `CodecReader` and one `CodecWriter` — not four — since a
codec no longer comes in a direction-typed pair; which one you reach for
depends only on which side you control and which way the bytes need to
flow.

Because `Read` pulls and `Write` pushes, a wrapper of one direction can't
be nested directly inside a wrapper of the other — bridging a
read-then-write (or write-then-read) boundary needs an explicit
`std::io::copy` through an intermediate buffer.

## One-shot `Vec<u8>` helper

For a payload you already have fully in memory:

```rust
use rust_codecs_core::io::to_vec;
// rot13_enc()/rot13_dec() come from a codec crate, as above.

let encoded = to_vec(rot13_enc(), b"Hello, world!\n")?;
let decoded = to_vec(rot13_dec(), &encoded)?;
assert_eq!(decoded, b"Hello, world!\n");
```

Unlike `compcol::vec::compress_to_vec`/`decompress_to_vec`, this takes an
already-constructed codec value (built via the codec crate's own
constructor function) rather than being generic over `Algorithm`.

## Trying it from the command line

The [`cli`](../cli/README.md) crate wires named codecs into a
`CodecReader`/`CodecWriter` chain over stdin/stdout, for exercising a
chain without writing Rust:

```
echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
```

See `cli/README.md` for the full flag reference and more examples.
