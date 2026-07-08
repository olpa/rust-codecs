# Rust twin of the Python `codecs` stream-wrapping reference

Rust equivalent of `../python-ref`: wrap an existing byte stream to get a new
one that encodes or decodes on the fly. One deliberate difference from
Python: **there is no codec registry**. Instead of registering `"my-rot13"`
and looking it up by name, the `Rot13` codec is constructed explicitly and
handed to the stream wrapper.

| I have... | I want... | Python | Rust twin |
|---|---|---|---|
| a byte input stream | a decoded input stream | `codecs.getreader(enc)(stream)` | `StreamReader::new(stream, codec)` |
| a byte output stream | an encoded output stream | `codecs.getwriter(enc)(stream)` | `StreamWriter::new(stream, codec)` |
| an input and an output stream | data flowing between them through a codec | `getreader` + `shutil.copyfileobj` | `StreamReader::new` + `std::io::copy` |

Everything streams incrementally in chunks; nothing loads the whole stream
into memory.

## Layout

- `src/codec.rs` — the `Codec` trait: an incremental bytes-in/bytes-out
  transform, the twin of `codecs.IncrementalEncoder`/`IncrementalDecoder`.
  A single trait covers both directions: `StreamReader` drives its codec as
  a decoder, `StreamWriter` as an encoder.
- `src/rot13.rs` — the explicit `Rot13` codec (twin of `rot13_codec.py`,
  minus the registration).
- `src/stream.rs` — `StreamReader` (implements `std::io::Read`) and
  `StreamWriter` (implements `std::io::Write`), the twins of
  `codecs.StreamReader`/`codecs.StreamWriter`. Because they implement the
  standard traits, they compose with the whole `std::io` ecosystem and with
  each other (readers can be stacked).

## Runnable examples

Each binary mirrors the Python script of the same name and uses the same
`input-hello.txt` / `encoded-hello.txt` data files (copied here):

```sh
cargo run --bin wrap-input    # wrap-input.py — decode encoded-hello.txt through a reader
cargo run --bin wrap-output   # wrap-output.py — encode input-hello.txt into stdout through a writer
cargo run --bin connect       # connect.py — reader + io::copy into stdout
cargo run --bin chain         # chain.py — four stacked ROT13 readers cancel out
cargo test                    # unit tests for the codec and both wrappers
```

## Design notes / differences from Python

- **No registry.** `codecs.register` + `codecs.lookup` are replaced by
  constructing the codec value at the call site. The wrappers are generic
  over `C: Codec`, so any codec plugs in the same way.
- **One trait instead of six classes.** Python's codec kit has stateless
  `Codec.encode`/`.decode`, incremental encoder/decoder classes, and stream
  reader/writer classes. Here the incremental transform
  (`Codec::transform(&mut self, input, last)`) is the single primitive and
  the stream wrappers are built on it; the stateless whole-buffer form is
  just `transform(data, true)` on a fresh codec.
- **Explicit end-of-stream for writers.** `StreamWriter::finish()` signals
  `last = true` to the codec, writes the tail, flushes, and returns the
  underlying stream. ROT13 has no tail, but a buffering codec (e.g. base64)
  needs this hook; Python's `StreamWriter` has no equivalent.
- **`last` flag for readers.** When the underlying stream hits EOF, the
  reader calls `transform(&[], true)` once so a buffering codec can flush —
  the twin of Python's `decode(input, final=True)`.
- **Chaining is free.** As in Python, a wrapped reader is itself a readable
  stream, so wraps stack arbitrarily (see `chain.rs`).
