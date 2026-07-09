# rust-codecs-core

The foundation crate of RustCodecs. It re-exports the trait surface and
stream adapters that every codec crate and its clients build on, so
neither ever has to `use compcol` directly — `compcol` is an
implementation detail behind this crate's boundary.

This document covers **using** an existing codec. See
[`CREATING-CODECS.md`](./CREATING-CODECS.md) for how to **create** one.

## What's exposed

```rust
pub use compcol::{Decoder, Encoder, Error, Progress, Status};

pub mod io; // DecoderReader, DecoderWriter, EncoderReader, EncoderWriter,
            // encode_to_vec, decode_to_vec
```

Deliberately **not** re-exported: `compcol::Algorithm`. A codec crate
exposes its codec through a pair of plain constructor functions instead
— conventionally `<name>_encoder()` / `<name>_decoder()` — so building a
codec never needs a trait in scope, just two function calls. See
`CREATING-CODECS.md` for why.

## Streaming through `std::io`

Wrap a `Read` to decode on the fly, or a `Write` to encode on the fly.
These two examples are taken directly from the `rust-twin-v2` experiment
that this crate grew out of (there `rot13_decoder()`/`rot13_encoder()`
come from the experiment's own `rot13` module; in a real codec crate
they'd come from that crate, e.g. `rust_codecs_rot13::rot13_decoder()`).

**Decoding a file as you read it** (`design-interface/rust-twin-v2/src/bin/wrap_input.rs`):

```rust
use std::fs::File;
use std::io;

use compcol::io::DecoderReader;
use rust_twin_v2::rot13_decoder;

fn main() -> std::io::Result<()> {
    let raw = File::open(concat!(env!("CARGO_MANIFEST_DIR"), "/encoded-hello.txt"))?;
    let mut reader = DecoderReader::new(raw, rot13_decoder());
    io::copy(&mut reader, &mut io::stdout())?;
    Ok(())
}
```

`DecoderReader` reads raw bytes from the wrapped source, decodes them
on the fly, and yields the *decoded* bytes to its own caller. It detects
end-of-stream from the inner reader's EOF and drains the codec
internally — the caller never needs to call `finish()` explicitly.

**Encoding as you write** (`design-interface/rust-twin-v2/src/bin/wrap_output.rs`):

```rust
use std::io::Write;

use compcol::io::EncoderWriter;
use rust_twin_v2::rot13_encoder;

fn main() -> std::io::Result<()> {
    let plain = std::fs::read(concat!(env!("CARGO_MANIFEST_DIR"), "/input-hello.txt"))?;
    let mut writer = EncoderWriter::new(std::io::stdout().lock(), rot13_encoder());
    writer.write_all(&plain)?;
    let _stdout = writer.finish()?;
    Ok(())
}
```

`EncoderWriter` encodes bytes on the fly as they're written to it. Unlike
reading, writing has no built-in "no more input" signal — `write_all`
just accepts bytes — so the caller must call `.finish()` explicitly once
all input has been written. `finish()` flushes any bytes the encoder was
still holding, finalizes the stream (trailer, checksum, padding — for a
stateful codec), and hands back ownership of the wrapped writer.

`EncoderReader` (a `Read` that encodes what it pulls from its source) and
`DecoderWriter` (a `Write` that decodes what's pushed to it) round out
the four combinations; pick by which side you control and which
direction the bytes need to flow.

Because `Read` pulls and `Write` pushes, a wrapper of one direction can't
be nested directly inside a wrapper of the other — bridging a
read-then-write (or write-then-read) boundary needs an explicit
`std::io::copy` through an intermediate buffer.

## One-shot `Vec<u8>` helpers

For a payload you already have fully in memory:

```rust
use rust_codecs_core::io::{decode_to_vec, encode_to_vec};
// rot13_encoder()/rot13_decoder() come from a codec crate, as above.

let encoded = encode_to_vec(rot13_encoder(), b"Hello, world!\n")?;
let decoded = decode_to_vec(rot13_decoder(), &encoded)?;
assert_eq!(decoded, b"Hello, world!\n");
```

Unlike `compcol::vec::compress_to_vec`/`decompress_to_vec`, these take an
already-constructed codec value (built via the codec crate's
`<name>_encoder()`/`<name>_decoder()`) rather than being generic over
`Algorithm`.
