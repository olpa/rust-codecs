# cli

A small command-line tool for exercising codec chains by hand, without
writing any Rust. It wires a `--readers` list into a chain of
[`CodecReader`](../core/README.md)s around stdin, a `--writers` list into
a chain of `CodecWriter`s around stdout, and copies bytes from one to the
other.

## Usage

```
cargo run -p cli -- [--readers <name>...] [--writers <name>...]
```

Both lists are optional and each codec name may repeat. Reader codecs
apply in the order listed — the first name runs on the raw stdin bytes
first, the next runs on its output, and so on. Writer codecs also apply
in the order listed — the first name runs first, closest to the incoming
bytes, before the result reaches stdout.

Currently known codec names: `identity`, `rot13`, `base64-enc`,
`base64-dec`, `json-enc`.

## Example

```
echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
```

Reading side: `identity` → `identity` → `rot13` turns `hello` into
`uryyb` (identity is a no-op, so only the `rot13` has any effect).
Writing side then re-applies `rot13` → `rot13` → `identity` to that
`uryyb`; the two `rot13`s cancel out and `identity` leaves the result
unchanged, so `uryyb` passes straight through to stdout:

```
$ echo hello | cargo run -q -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
uryyb
```

Passing neither flag makes the tool a passthrough (equivalent to `cat`).
An unrecognized codec name prints an error to stderr and exits non-zero.
