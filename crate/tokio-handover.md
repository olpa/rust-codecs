# Tokio support: handover

Status when this note was written: branch `rc21-tokio-adapters`, everything below is committed.
The note is not committed.

## 1. Where the discussion ended

- Tokio was a guess because it is popular. The real goal is not tokio.
  It is probably "async wire wrappers for the codecs", like the sync backends `std_io` and `embedded_io`.
- The candidate plan (not decided): add async backends as thin shells around the shared `Pump` logic.
  - `futures-io` first. It reaches async-std, smol, libp2p and similar crates directly. Tokio users reach it with `tokio_util::compat`.
  - `embedded-io-async` if `no_std` async matters.
  - tokio as an optional extra.
- `futures-io` needs `std`. The crate is `#![no_std]`, but all its traits sit behind the default `std` feature (checked in 0.3.32).
  For `no_std` async the only family is `embedded-io-async`.
- Open questions for the break:
  1. Is the goal "async wire wrappers for codecs, read and write"?
  2. Does `no_std` async matter?
  3. Keep the tokio-util framing adapters (section 2), or drop them?
  4. Delete the two tokio-to-codec bridges (section 2)? They were proposed, not deleted.
  5. Chain: allow a `BoundaryAwareCodec` as first stage. You planned this "a few tasks later".

## 2. What exists in `core/src/tokio/` (feature `tokio`)

Direction: tokio codec to our codec ("bridges").
- `TokioDecoderAsBoundaryAwareCodec` (`decoder_to_codec.rs`): runs a tokio `Decoder` as a `BoundaryAwareCodec`.
  A logical stream ends after the first decoded object. The object comes out of `take_item`. The codec writes no bytes.
- `TokioEncoderAsBoundaryAwareCodec` (`encoder_to_codec.rs`): runs a tokio `Encoder`.
  It ignores its input bytes. It encodes the items from `with_item` or `with_items` (an iterator).
  It reports `Boundary { consumed: 0, .. }` after the last item. The output also comes from `finish`, so an empty source works.
  The encoder error stays inside; get it with `take_error`.

Direction: our codec to tokio codec (the useful half).
- `CodecAsTokioDecoder<C: Codec>`: works like `BytesCodec`. An item is the output made from the bytes in the buffer now.
  `decode_eof` runs `finish` once.
- `BoundaryAwareCodecAsTokioDecoder<C>`: works like `LinesCodec`. An item is all output of one logical stream.
- `CodecAsTokioEncoder<C: Codec>` and the marker item `Finish`.
- All use `std::io::Error`. The crate `Error` is its source. `CorruptStream` maps to `InvalidData`, `UnexpectedEnd` to `UnexpectedEof`, the rest to `Other`.
- `codec_support.rs` (private) holds the shared loops. It has one `unsafe` block (`BytesMut::set_len` after the codec wrote into spare capacity).

Tests and samples: `samples.rs` has four sections (decoder to codec, encoder to codec, our codec to tokio decoder with `FramedRead`, our codec to tokio encoder with `FramedWrite`).
`test_support.rs` has `bar_delimited_codec()` (tokio-util `AnyDelimiterCodec` with `|`), `three_strings_input()`, `NullSource`, `NullSink`.
Dev-dependencies: `tokio`, `tokio-stream`, `futures-util`.

Removed on the way: `TokioReader`/`ReadError` (pull reader over a `Source`), `CodecTokioPair`, a hand-written length-prefixed sample codec.

Key commits: `633f2d5` our codecs as tokio decoders/encoders, `9cb6f08` renames and merged samples, `77e3fa3` encoder bridge, `7bb4b94` reader removed, `eb4c42b` AnyDelimiterCodec as sample, `aba0523` Cargo.lock.

## 3. Findings and decisions

1. **Typed items do not fit our traits.** `Codec`, `Source` and `Sink` carry bytes only.
   Every item bridge needed a side channel (`take_item`, a handler, items fixed at construction). Our codec to tokio fits without tricks. Tokio codec to our codec does not.
2. **Two decoders, because the item rule differs.** Plain codec: chunk per output, no wait for EOF. Boundary-aware codec: one item per stream, at the boundary.
3. **The encoder accepts only a plain `Codec`.** A boundary is a marker in the input of a self-delimiting format. That fits a decoder. The input of an encoder is a payload.
4. **`Finish` marker.** Tokio gives an `Encoder` no end-of-stream call, and `FramedWrite` never calls one.
   Calling the encoder directly fails: `encoder_mut()` and `write_buffer_mut()` both borrow the writer mutably.
   A decoder has no such gap: `FramedRead` calls `decode_eof` at EOF.
5. **Error type `io::Error`**, like tokio-util `BytesCodec` and `LengthDelimitedCodec`.
   (`LinesCodec` uses its own enum; we avoided a new public type.)
6. **`Chain` needs `Codec` for both stages.** A `BoundaryAwareCodec` cannot be in a `Chain` today (see `chain.rs` header).
7. **Only `stream_to_stream` is a public driver.** `Pump` has public `new`, `get_ref`, `get_mut`, `into_inner`, `total`; its step methods (`transfer_from`, `transfer_step`, `flush_to`, `finish_to`) are `pub(crate)`.
   Each `stream_to_stream` call is a whole stream and ends with `finish`.
8. **Tokio has no codec layering.** `tokio_util::codec` has only `Framed*` and the built-in codecs.
   The `io` feature has `StreamReader` (stream of chunks to `AsyncRead`) and `SinkWriter` (`Sink<&[u8]>` to `AsyncWrite`).
   Layering is possible by composing them, but `SinkWriter`/`FramedWrite` close without calling the encoder's `finish`. Stateful codecs (base64 trailer) would lose data.
   A purpose-built `AsyncWrite` wrapper does not have this problem: `poll_shutdown` is a real end signal.

## 4. Research on "the reader wants to stop"

No thread asks for a "stop" signal directly. The ecosystem has three patterns:
1. **Hand the leftover back.** `Framed::into_parts` (also `FramedRead::into_parts`, `FramedWrite::into_parts`, added in tokio-util 0.7.17) returns the stream, the unprocessed read buffer and the codec.
   hyper's `upgrade::Parts` has `read_buf` (bytes read but not processed as HTTP); its docs say to check it before reading from the I/O again.
   Note: `FramedRead::into_inner` drops the buffer.
2. **A buffered reader contract.** `tokio::io::BufReader::into_inner` loses leftover data. Advice: keep the `BufReader`, `consume` only what you used.
   `async-compression` follows this (below).
3. **Pushback / rewind.** `hyper_util` has a `Rewind` wrapper that replays buffered bytes first.

A security note (my inference, unchecked against RFC 3207): after a protocol switch such as STARTTLS, bytes left in the old buffer came in plaintext. Discard or reject them.

### `async-compression` (0.4.52), the closest precedent

- Modules by runtime (`tokio`, `futures`) and algorithm. Each runtime has `bufread` (wraps `AsyncBufRead`, gives `AsyncRead`) and `write` (wraps `AsyncWrite`).
  The logic is runtime-neutral in `generic/`; the runtime files are thin macros.
- Codec trait `DecodeV2`: `reinit`, `decode(input, output) -> bool` ("end of stream read", like our `Boundary`), `flush`, `finish`. `EncodeV2` has `encode`, `flush`, `finish -> bool`.
- Reader: loop of `poll_fill_buf`, decode, `consume(only what the decoder took)`. State machine `Decoding`, `Flushing`, `Done`, with `Next` for `multiple_members`.
  After the end of the stream, trailing bytes stay in the inner reader (read from the code, not tested).
  With `multiple_members(true)`: after `finish` it calls `reinit()` and, if the buffer is empty, polls for more data to see whether another member follows.
  Not found: what happens to non-gzip bytes after a member in this mode.
- Writer: `poll_shutdown` (futures: `poll_close`) loops `finish`, then shuts down the inner writer.
  The write-side decoder cannot stop; it returns the error "Write after end of stream".

## 5. Use cases for stopping on a boundary

- Real in principle: HTTP chunked body before the next response, SMTP `DATA`, gzip with several members, armored blocks (PEM), a token inside a larger document (our own `core/tests/tokenizer.rs`).
- But none of the shipped codecs (`rot13`, `identity`, `base64`, `json_enc`) reports a boundary. Only the test codecs do (`EndsAtBar`, `QuoteEnd`).
- `std_io` readers already stop at a boundary: the reader stops pulling input at the codec's in-band end, drains with `finish`, and reports EOF (`CREATING-IO-BACKENDS.md`).
- An async reader over `AsyncBufRead` would give the same, with no read-ahead loss, because `AsyncBufRead` has the same contract as our `Source` (`poll_fill_buf` lends, `consume` takes).

## 6. Tokio versus `futures-io`

- Separate trait families, not wrappers of each other. `tokio_util::compat` converts both ways.
- Tokio `poll_read` takes `ReadBuf` (supports uninitialized memory, fits our `MaybeUninit` output). `futures-io` takes `&mut [u8]` (must be initialized).
- Tokio: `poll_shutdown`. `futures-io`: `poll_close`. `AsyncBufRead` has the same shape in both.
- The framing traits (`Decoder`, `Encoder`, `Framed`) are tokio-util only. The `futures-io` counterpart is the `asynchronous-codec` crate with its own traits.
- hyper 1.x has its own `Read`/`Write` traits. `embedded-io-async` is a third family (`no_std`).
- Libraries that serve both runtimes keep a neutral core and make the trait impls thin.

## 7. Suggested next steps (after the break)

1. Answer the open questions in section 1.
2. Read how `std_io` splits work between `Pump` and the thin shell. Decide how an async reader can reuse the step logic (the step methods are `pub(crate)`, so an in-crate backend can use them).
3. Sketch the async reader (`AsyncBufRead`, stops at a boundary, finish at EOF) and writer (finish at shutdown) as red tests, as was done for the tokio adapters.
4. Decide the fate of the tokio-to-codec bridges and the framing adapters.
5. Chain: allow a boundary-aware first stage.
