//! Test harness for chaining codecs: folds `--readers` into one `Chain`
//! and `--writers` into another, then copies stdin through the readers,
//! through the writers, to stdout.
//!
//! ```text
//! echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
//! ```
//!
//! Reader codecs apply in the order listed (first listed runs on the raw
//! bytes first). Writer codecs also apply in the order listed (first
//! listed runs first, closest to the incoming bytes, before reaching
//! stdout).

use std::io::{self, Read, Write};

use rust_codecs_core::sources_and_sinks::std_io::{
    CodecReader, CodecWriter, ReadGranularity, StdSink, StdSource,
};
use rust_codecs_core::{stream_to_stream, Chain, Codec};

/// Staging buffer size for each link in a `--readers`/`--writers` chain.
const STAGING: usize = 4 * 1024;

/// A codec name paired with its constructor.
type CodecEntry = (&'static str, fn() -> Box<dyn Codec>);

/// Single source of truth for known codec names: `make_codec`, the
/// "unknown codec" error, and `--help`'s codec list all read from
/// this instead of repeating the name list.
const CODECS: &[CodecEntry] = &[
    ("identity", || {
        Box::new(rust_codecs_core::identity::identity())
    }),
    ("rot13", || Box::new(rust_codecs_core::rot13::rot13())),
    ("base64-enc", || {
        Box::new(rust_codecs_core::base64_enc::base64_enc())
    }),
    ("base64-dec", || {
        Box::new(rust_codecs_core::base64_dec::base64_dec())
    }),
    ("json-enc", || {
        Box::new(rust_codecs_core::json_enc::json_enc())
    }),
];

fn usage() -> String {
    let names = CODECS
        .iter()
        .map(|(name, _)| *name)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "\
Test harness for chaining codecs: folds --readers into one Chain and
--writers into another, then copies stdin through the readers, through
the writers, to stdout.

Usage:
  cargo run -p cli -- [--engine copy|stream] [--granularity fill-buffer|single-read] [--readers CODEC...] [--writers CODEC...]

Codecs: {names}

--engine selects which copy path drives the chain: `copy` (default)
wraps the chain in CodecReader/CodecWriter and drives it with
std::io::copy; `stream` drives the same chain directly via
stream_to_stream over StdSource/StdSink, with no Read/Write adapter
in between.

--granularity only affects --engine copy (CodecReader always reads at
ReadGranularity::FillBuffer under --engine stream's stream_to_stream
path, which reacts immediately regardless). `fill-buffer` (default)
keeps std::io::copy's read() calls filling its whole internal buffer
before returning, which can stall an interactive pipe (e.g. a
terminal) waiting for enough input to arrive. `single-read` is the
interactive-application setting: read() returns as soon as one pull
from the wrapped reader made progress, so a handler downstream (here,
stdout) sees each unit of input — e.g. a typed terminal line — as
soon as possible, instead of only once enough of them have piled up
to fill std::io::copy's buffer.

Reader codecs apply in the order listed (first listed runs on the raw
bytes first). Writer codecs also apply in the order listed (first
listed runs first, closest to the incoming bytes, before reaching
stdout).

Example:
  echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
"
    )
}

fn make_codec(name: &str) -> Result<Box<dyn Codec>, String> {
    CODECS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, ctor)| ctor())
        .ok_or_else(|| {
            let names = CODECS
                .iter()
                .map(|(n, _)| *n)
                .collect::<Vec<_>>()
                .join(", ");
            format!("unknown codec {name:?} (expected one of: {names})")
        })
}

enum Mode {
    None,
    Readers,
    Writers,
}

/// Which copy path drives the composed reader/writer chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    /// `CodecReader`/`CodecWriter` (a `Read`/`Write` adapter pair)
    /// driven by `std::io::copy`.
    Copy,
    /// `StdSource`/`StdSink` driven directly by `stream_to_stream`,
    /// with no `Read`/`Write` adapter in between.
    Stream,
}

fn parse_args(
    args: impl Iterator<Item = String>,
) -> Result<(Engine, ReadGranularity, Vec<String>, Vec<String>), String> {
    let mut mode = Mode::None;
    let mut engine = Engine::Copy;
    let mut granularity = ReadGranularity::FillBuffer;
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    let mut args = args.peekable();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--engine" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--engine requires a value (copy or stream)".to_string())?;
                engine = match value.as_str() {
                    "copy" => Engine::Copy,
                    "stream" => Engine::Stream,
                    other => {
                        return Err(format!(
                            "unknown engine {other:?} (expected copy or stream)"
                        ))
                    }
                };
            }
            "--granularity" => {
                let value = args.next().ok_or_else(|| {
                    "--granularity requires a value (fill-buffer or single-read)".to_string()
                })?;
                granularity = match value.as_str() {
                    "fill-buffer" => ReadGranularity::FillBuffer,
                    "single-read" => ReadGranularity::SingleRead,
                    other => {
                        return Err(format!(
                            "unknown granularity {other:?} (expected fill-buffer or single-read)"
                        ))
                    }
                };
            }
            "--readers" => mode = Mode::Readers,
            "--writers" => mode = Mode::Writers,
            _ => match mode {
                Mode::None => {
                    return Err(format!(
                        "unexpected argument {arg:?} before --readers/--writers"
                    ))
                }
                Mode::Readers => readers.push(arg),
                Mode::Writers => writers.push(arg),
            },
        }
    }
    Ok((engine, granularity, readers, writers))
}

/// Fold `names` into a single `Codec`, first name applied first, closest
/// to the raw bytes. An empty list folds down to a transparent
/// `identity()` — the base every real chain builds on top of, rather
/// than a separate zero/one/many special case.
fn compose(names: &[String]) -> Result<Box<dyn Codec>, String> {
    let mut composed: Box<dyn Codec> = Box::new(rust_codecs_core::identity::identity());
    for name in names.iter().rev() {
        let codec = make_codec(name)?;
        composed = Box::new(Chain::new(codec, composed, vec![0u8; STAGING]));
    }
    Ok(composed)
}

fn run_io<R: Read, W: Write>(
    reader_names: &[String],
    writer_names: &[String],
    granularity: ReadGranularity,
    input: R,
    output: W,
) -> Result<W, String> {
    let reader_codec = compose(reader_names)?;
    let writer_codec = compose(writer_names)?;

    let mut reader = CodecReader::new(input, reader_codec, vec![0u8; STAGING])
        .with_read_granularity(granularity);
    let mut writer = CodecWriter::new(output, writer_codec, vec![0u8; STAGING]);

    io::copy(&mut reader, &mut writer).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())
}

/// Same end-to-end behavior as `run_io`, but drives the composed chain
/// directly via `stream_to_stream` over `StdSource`/`StdSink` instead
/// of wrapping it in `CodecReader`/`CodecWriter` and driving it with
/// `std::io::copy`. The reader and writer stacks are folded into a
/// single `Chain` first, since `stream_to_stream` drives one codec
/// between one `Source` and one `Sink`.
fn run_io_stream<R: Read, W: Write>(
    reader_names: &[String],
    writer_names: &[String],
    input: R,
    output: W,
) -> Result<W, String> {
    let reader_codec = compose(reader_names)?;
    let writer_codec = compose(writer_names)?;
    let codec = Chain::new(reader_codec, writer_codec, vec![0u8; STAGING]);

    let mut source = StdSource::new(input, vec![0u8; STAGING]);
    let mut sink = StdSink::new(output, vec![0u8; STAGING]);

    stream_to_stream(&mut source, codec, &mut sink).map_err(|e| format!("{e:?}"))?;
    Ok(sink.into_inner())
}

fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let (engine, granularity, reader_names, writer_names) = parse_args(args)?;
    match engine {
        Engine::Copy => {
            run_io(
                &reader_names,
                &writer_names,
                granularity,
                io::stdin(),
                io::stdout(),
            )?;
        }
        Engine::Stream => {
            run_io_stream(&reader_names, &writer_names, io::stdin(), io::stdout())?;
        }
    }
    Ok(())
}

/// Installs a no-op `SIGUSR1` handler so the retry-on-`EINTR` paths in
/// `StdSource`/`BufReadSource` (see
/// `core/src/sources_and_sinks/std_io/adapter.rs`) can be exercised
/// against a real blocking read: send the running process `SIGUSR1`
/// (`kill -USR1 <pid>`) while it's waiting on stdin.
///
/// Without a caught handler, `SIGUSR1`'s default disposition is to
/// terminate the process, so it would never reach the retry logic at
/// all — installing a handler, even one that does nothing, is what
/// makes the interrupted syscall return `EINTR` instead of killing the
/// process.
#[cfg(unix)]
fn install_sigusr1_handler() {
    extern "C" fn no_op(_signum: libc::c_int) {}
    // SAFETY: registers a plain C function pointer that does nothing;
    // safe to run in a signal handler context.
    unsafe {
        libc::signal(libc::SIGUSR1, no_op as libc::sighandler_t);
    }
}

#[cfg(not(unix))]
fn install_sigusr1_handler() {}

fn main() {
    install_sigusr1_handler();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "--help" || a == "-h") {
        print!("{}", usage());
        return;
    }

    if let Err(err) = run(args.into_iter()) {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::io::{Cursor, Write};
    use std::rc::Rc;

    use rust_codecs_core::rot13::rot13;
    use rust_codecs_core::sources_and_sinks::vec::{VecSink, VecSource};
    use rust_codecs_core::stream_to_stream;

    use rust_codecs_core::sources_and_sinks::std_io::ReadGranularity;

    use super::{compose, parse_args, run_io, run_io_stream, Engine};

    fn collect(codec: impl rust_codecs_core::Codec, bytes: &[u8]) -> Vec<u8> {
        let mut input = VecSource::new(bytes.to_vec());
        let mut output = VecSink::default();
        stream_to_stream(&mut input, codec, &mut output).unwrap();
        output.into_inner()
    }

    fn names(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn end_to_end_round_trip_from_module_doc() {
        // --readers identity identity rot13 nets out to a single rot13
        // on the raw bytes; --writers rot13 rot13 identity nets out to
        // the identity (the two rot13s cancel) — so the whole pipeline
        // is equivalent to running the input through rot13 once.
        let reader_names = names(&["identity", "identity", "rot13"]);
        let writer_names = names(&["rot13", "rot13", "identity"]);
        let input = b"hello";

        let output = run_io(
            &reader_names,
            &writer_names,
            ReadGranularity::FillBuffer,
            Cursor::new(input.to_vec()),
            Vec::new(),
        )
        .unwrap();

        let expected = collect(rot13(), input);
        assert_eq!(output, expected);
    }

    #[test]
    fn stream_engine_matches_copy_engine() {
        // Same pipeline as `end_to_end_round_trip_from_module_doc`,
        // driven through `stream_to_stream`/`StdSource`/`StdSink`
        // instead of `std::io::copy`/`CodecReader`/`CodecWriter` — both
        // engines must agree on the bytes they produce.
        let reader_names = names(&["identity", "identity", "rot13"]);
        let writer_names = names(&["rot13", "rot13", "identity"]);
        let input = b"hello";

        let copy_output = run_io(
            &reader_names,
            &writer_names,
            ReadGranularity::FillBuffer,
            Cursor::new(input.to_vec()),
            Vec::new(),
        )
        .unwrap();
        let stream_output = run_io_stream(
            &reader_names,
            &writer_names,
            Cursor::new(input.to_vec()),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(stream_output, copy_output);
    }

    #[test]
    fn single_read_granularity_matches_fill_buffer() {
        // The granularity only changes how many `read()` calls it
        // takes to move the bytes through, not the bytes themselves.
        let reader_names = names(&["identity", "identity", "rot13"]);
        let writer_names = names(&["rot13", "rot13", "identity"]);
        let input = b"hello, world";

        let fill_buffer_output = run_io(
            &reader_names,
            &writer_names,
            ReadGranularity::FillBuffer,
            Cursor::new(input.to_vec()),
            Vec::new(),
        )
        .unwrap();
        let single_read_output = run_io(
            &reader_names,
            &writer_names,
            ReadGranularity::SingleRead,
            Cursor::new(input.to_vec()),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(single_read_output, fill_buffer_output);
    }

    #[test]
    fn parse_args_reads_engine_flag() {
        let (engine, _, readers, writers) =
            parse_args(names(&["--engine", "stream", "--readers", "rot13"]).into_iter()).unwrap();
        assert_eq!(engine, Engine::Stream);
        assert_eq!(readers, names(&["rot13"]));
        assert!(writers.is_empty());

        let (engine, _, _, _) = parse_args(names(&["--readers", "rot13"]).into_iter()).unwrap();
        assert_eq!(engine, Engine::Copy);
    }

    #[test]
    fn parse_args_reads_granularity_flag() {
        let (_, granularity, _, _) =
            parse_args(names(&["--granularity", "single-read", "--readers", "rot13"]).into_iter())
                .unwrap();
        assert_eq!(granularity, ReadGranularity::SingleRead);

        let (_, granularity, _, _) =
            parse_args(names(&["--readers", "rot13"]).into_iter()).unwrap();
        assert_eq!(granularity, ReadGranularity::FillBuffer);
    }

    #[test]
    fn parse_args_rejects_unknown_engine() {
        let err = parse_args(names(&["--engine", "bogus"]).into_iter()).unwrap_err();
        assert!(err.contains("bogus"));
    }

    #[test]
    fn parse_args_rejects_unknown_granularity() {
        let err = parse_args(names(&["--granularity", "bogus"]).into_iter()).unwrap_err();
        assert!(err.contains("bogus"));
    }

    /// A `Write` sink that shares its buffer via `Rc<RefCell<_>>`, so a
    /// clone kept by the test can inspect what's arrived without going
    /// through `CodecWriter::finish` (which consumes the writer).
    #[derive(Clone, Default)]
    struct SharedSink(Rc<RefCell<Vec<u8>>>);

    impl Write for SharedSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.borrow_mut().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn writer_stack_is_interactive() {
        // A single `--writers rot13` still runs through `compose`'s
        // Chain-over-identity base. Writing one line and flushing must
        // deliver the transformed bytes to the sink right away — the
        // return-clean guarantee `Chain` provides — without ever
        // calling `finish`.
        let writer_codec = compose(&names(&["rot13"])).unwrap();
        let sink = SharedSink::default();
        let mut writer = rust_codecs_core::sources_and_sinks::std_io::CodecWriter::new(
            sink.clone(),
            writer_codec,
            vec![0u8; 64],
        );

        writer.write_all(b"hi\n").unwrap();
        writer.flush().unwrap();

        let expected = collect(rot13(), b"hi\n");
        assert_eq!(sink.0.borrow().as_slice(), expected.as_slice());
    }
}
