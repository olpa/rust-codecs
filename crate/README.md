# rust-codecs-core

This crate gives you a stream interface for rewriting input and
output byte streams. The core is `no_std`-compatible. It runs
incrementally and does not allocate.

- It introduces the `Source` and `Sink` abstractions for I/O
  backends, and the `Codec` trait for byte rewriting.
- Its entry points are `stream_to_stream` and
  `encode_str`/`encode_string`.
- It bundles the `identity` and `rot13` codecs. It also bundles
  `base64_enc`/`base64_dec` and `json_enc` escaping, until these get
  their own crates.
- It bundles I/O backends for `std::io` and `embedded_io`. These
  provide the `CodecReader`, `BufReadCodecReader`, and `CodecWriter`
  wrappers around a `Read`, `BufRead`, or `Write`.
- It invites third-party codecs and I/O backends.

See the crate documentation for the full write-up, including
runnable examples:

- Wrapping an output
- Wrapping an input
- Any stream to any stream
- Chain of codecs
- Parsing using early-stop codecs


## Trying it from the command line

The [`cli`](./cli/README.md) crate wires named codecs into a
`CodecReader`/`CodecWriter` chain over stdin/stdout. Use it to try
a chain without writing Rust code:

```
echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
```

See `cli/README.md` for the full flag reference and more examples.


## Colophon

License: MIT

Author: Oleg Parashchenko, olpa@ <https://uucode.com/>
