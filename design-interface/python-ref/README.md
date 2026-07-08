# Python `codecs`: wrapping streams

## TL;DR

Python's `codecs` module lets you wrap an existing stream to get a new one,
in all three directions you'd want:

| I have... | I want... | Use |
|---|---|---|
| a byte input stream | a decoded input stream | `codecs.getreader(enc)(stream)` |
| a byte output stream | an encoded output stream | `codecs.getwriter(enc)(stream)` |
| an input stream and an output stream | data flowing between them through a codec | wrap the input with `getreader`, then `shutil.copyfileobj` into the output (see `connect.py`) |

A codec doesn't have to be a text encoding — it can be any bytes-in/bytes-out
(or str-in/str-out) transform, as shown here with a custom ROT13 codec
(`rot13_codec.py`). Once registered via `codecs.register`, it works with
`getreader`/`getwriter` exactly like `"utf-8"` or any built-in encoding.

Everything below is a thin, allocation-light wrapper: none of these copy the
whole stream into memory, they decode/encode incrementally as data is pulled
or pushed through.

Runnable examples in this directory, using the hand-written ROT13 codec
(`rot13_codec.py`) and reading `input-hello.txt` / `encoded-hello.txt`:

- `wrap-input.py` — scenario 1
- `wrap-output.py` — scenario 2
- `connect.py` — scenario 3
- `chain.py` — stacking `getreader` wraps: since a `StreamReader` is itself
  a readable stream, you can wrap a wrapped stream again, chaining any number
  of codecs. Here, four `"my-rot13"` readers are stacked on top of each
  other; since ROT13 is self-inverse, four rounds cancel out and the output
  matches the plain-text input.

All verified against Python 3.12.3.

## 1. Input stream → wrap with codec → new input stream

Use `codecs.getreader(encoding)` (or `codecs.lookup(encoding).streamreader`)
to wrap a byte-oriented readable stream and get back a decoding readable
stream (`codecs.StreamReader`).

```python
import io, codecs

raw = io.BytesIO("héllo wörld\n".encode("utf-8"))
Reader = codecs.getreader("utf-8")
wrapped = Reader(raw)          # encodings.utf_8.StreamReader
wrapped.read()                 # 'héllo wörld\n'
wrapped.readline()             # also supported
```

Equivalent via `codecs.lookup`:

```python
info = codecs.lookup("utf-8")
wrapped2 = info.streamreader(raw2)
```

`StreamReader` exposes the usual read API (`read`, `readline`, `readlines`,
iteration) and decodes lazily as data is pulled from the underlying stream.

See `wrap-input.py`, which does this with the custom `"my-rot13"` codec:
reads `encoded-hello.txt` (ROT13-encoded bytes) through a wrapped reader and
prints the decoded bytes.

## 2. Output stream → wrap with codec → new output stream

Symmetric to (1): `codecs.getwriter(encoding)` (or
`codecs.lookup(encoding).streamwriter`) wraps a byte-oriented writable stream
and gives back an encoding writable stream (`codecs.StreamWriter`).

```python
raw = io.BytesIO()
Writer = codecs.getwriter("utf-8")
wrapped = Writer(raw)
wrapped.write("héllo wörld\n")
raw.getvalue()                 # b'h\xc3\xa9llo w\xc3\xb6rld\n'
wrapped.writelines([...])      # also supported
```

See `wrap-output.py`, which wraps `sys.stdout.buffer` with the `"my-rot13"`
writer and writes the plain bytes of `input-hello.txt` into it, so the
terminal receives the ROT13-encoded bytes.

## 3. Input + output stream connected through a codec

The straightforward way: wrap the input stream with `getreader` and copy it
into the output stream (e.g. with `shutil.copyfileobj`). This streams data
through the codec in chunks rather than loading everything into memory.

See `connect.py`, which reads `encoded-hello.txt` through a `"my-rot13"`
reader and streams the decoded bytes straight into `sys.stdout.buffer`.

Two more specialized tools exist for single-stream recoding scenarios:

**a) `codecs.EncodedFile(file, data_encoding, file_encoding=None)`** — wraps
a *single* file-like object and transcodes on the fly between the file's
on-disk encoding and the encoding the caller wants to read/write in.

```python
raw = io.BytesIO("héllo\n".encode("latin-1"))
ef = codecs.EncodedFile(raw, data_encoding="utf-8", file_encoding="latin-1")
ef.read()   # b'h\xc3\xa9llo\n'  -- utf-8 bytes, even though raw is latin-1
```

**b) `codecs.StreamRecoder(stream, encode, decode, Reader, Writer, errors="strict")`**
— more general: wraps one stream and recodes between two arbitrary codecs.
`encode`/`decode` are the codec functions for the *outer* (caller-facing)
representation; `Reader`/`Writer` are the `StreamReader`/`StreamWriter`
classes for the *underlying* stream's encoding.

```python
info_utf8 = codecs.lookup("utf-8")
info_latin1 = codecs.lookup("latin-1")
raw2 = io.BytesIO("héllo\n".encode("utf-8"))
recoder = codecs.StreamRecoder(
    raw2,
    encode=info_latin1.encode, decode=info_latin1.decode,
    Reader=info_utf8.streamreader, Writer=info_utf8.streamwriter,
)
recoder.read()   # b'h\xe9llo\n'  -- transcoded utf-8 -> latin-1 bytes
```

Bonus, not strictly "connecting two separate streams" but relevant: a single
stream can be wrapped to be simultaneously readable and writable via
**`codecs.StreamReaderWriter(stream, Reader, Writer, errors)`**, which is
exactly what `codecs.open(path, mode, encoding)` returns under the hood for
files.

```python
rw = codecs.StreamReaderWriter(raw, info.streamreader, info.streamwriter, "strict")
rw.read(); rw.write("nice\n")

with codecs.open(path, "w", encoding="utf-8") as f:
    f.write("héllo\n")
```

## Writing your own codec

`rot13_codec.py` implements a byte-in/byte-out ROT13 codec from scratch
(its own translation table, not delegating to the stdlib `rot_13` codec) and
registers it under the name `"my-rot13"`. Notes for writing your own:

- Register with `codecs.register(search_function)`, where `search_function`
  maps a name to a `codecs.CodecInfo`.
- The name passed to your `search_function` is normalized by
  `encodings.normalize_encoding` (lowercased, `-`/space → `_`), so match
  against the normalized form (e.g. `"my_rot13"`), even though callers can
  spell it `"my-rot13"`.
- Don't reuse a name the stdlib already claims (`"rot13"` is aliased to the
  built-in `encodings.rot_13` module and will shadow a custom codec of the
  same name) — that's why this codec is named `"my-rot13"` instead.
- If your `Codec.decode`/`encode` produce `bytes` rather than `str` (as here,
  since this is a bytes<->bytes transform, not a text encoding), override
  `charbuffertype = bytes` on your `StreamReader` subclass. The stdlib
  `codecs.StreamReader` base class defaults its internal buffer to `str` and
  will raise `TypeError: can only concatenate str (not "bytes") to str`
  otherwise.

## Summary table

| Scenario | Tool |
|---|---|
| byte input stream → decoded input stream | `codecs.getreader(enc)(stream)` |
| byte output stream → encoded output stream | `codecs.getwriter(enc)(stream)` |
| input stream → output stream, transformed | `getreader` + `shutil.copyfileobj` |
| single stream, transcode file-encoding ↔ data-encoding | `codecs.EncodedFile(stream, data_enc, file_enc)` |
| single stream, arbitrary recode between two codecs | `codecs.StreamRecoder(stream, encode, decode, Reader, Writer)` |
| single stream, both readable and writable | `codecs.StreamReaderWriter(stream, Reader, Writer, errors)` (what `codecs.open` returns) |
