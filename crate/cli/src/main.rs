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

use std::io::{self, BufRead, Read, Write};

use rust_codecs_core::sources_and_sinks::std_io::{
    BufReadCodecReader, BufReadSource, CodecWriter, StdSink,
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
  cargo run -p cli -- [--engine copy|stream] [--no-buffering] [--readers CODEC...] [--writers CODEC...]

Codecs: {names}

--engine selects which copy path drives the chain: `copy` (default)
wraps the chain in BufReadCodecReader/CodecWriter and drives it with
std::io::copy; `stream` drives the same chain directly via
stream_to_stream over BufReadSource/StdSink, with no Read/Write adapter
in between. BufReadCodecReader's read() returns as soon as a pull from the
wrapped reader produced output, instead of chasing a full buffer. So
it does not wait for enough input to fill std::io::copy's buffer on
an interactive pipe (e.g. a terminal). A codec that holds bytes back,
for example base64 with an incomplete group, can still delay its
output until more input arrives.

--no-buffering makes the tool interactive per character: on a terminal it
turns off line buffering (echo stays on), so each key press goes through the
codecs and appears on stdout at once, and it flushes stdout after every
write. The terminal is restored on exit and on Ctrl-C. Ctrl-D still ends
the input (it is translated to EOF), so codecs that hold bytes back
(base64) emit their tail then.

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
    /// `BufReadCodecReader`/`CodecWriter` (a `Read`/`Write` adapter
    /// pair) driven by `std::io::copy`.
    Copy,
    /// `BufReadSource`/`StdSink` driven directly by `stream_to_stream`,
    /// with no `Read`/`Write` adapter in between.
    Stream,
}

/// Parsed command line.
#[derive(Debug)]
struct Args {
    engine: Engine,
    readers: Vec<String>,
    writers: Vec<String>,
    /// `--no-buffering`: eager, per-character passthrough.
    no_buffering: bool,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut mode = Mode::None;
    let mut engine = Engine::Copy;
    let mut no_buffering = false;
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
            "--no-buffering" => no_buffering = true,
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
    Ok(Args {
        engine,
        readers,
        writers,
        no_buffering,
    })
}

/// Fold `names` into a single `Codec`, first name applied first, closest
/// to the raw bytes. The last codec is the base of the fold. An empty
/// list gives a transparent `identity()`.
fn compose(names: &[String]) -> Result<Box<dyn Codec>, String> {
    let mut codecs = names.iter().rev().map(|name| make_codec(name));
    let mut composed = match codecs.next() {
        Some(codec) => codec?,
        None => return Ok(Box::new(rust_codecs_core::identity::identity())),
    };
    for codec in codecs {
        composed = Box::new(
            Chain::new(codec?, composed, vec![0u8; STAGING]).expect("STAGING is non-zero"),
        );
    }
    Ok(composed)
}

fn run_io<R: BufRead, W: Write>(
    reader_names: &[String],
    writer_names: &[String],
    input: R,
    output: W,
) -> Result<W, String> {
    let reader_codec = compose(reader_names)?;
    let writer_codec = compose(writer_names)?;

    let mut reader = BufReadCodecReader::new(input, reader_codec);
    let mut writer =
        CodecWriter::new(output, writer_codec, vec![0u8; STAGING]).expect("STAGING is non-zero");

    io::copy(&mut reader, &mut writer).map_err(|e| e.to_string())?;
    let (mut output, _) = writer.finish().map_err(|e| e.to_string())?;
    output.flush().map_err(|e| e.to_string())?;
    Ok(output)
}

/// Same end-to-end behavior as `run_io`, but drives the composed codec
/// directly via `stream_to_stream` over `BufReadSource`/`StdSink` instead
/// of wrapping it in `BufReadCodecReader`/`CodecWriter` and driving it with
/// `std::io::copy`. The reader names and the writer names make one list,
/// because `stream_to_stream` drives one codec between one `Source` and
/// one `Sink`.
fn run_io_stream<R: BufRead, W: Write>(
    reader_names: &[String],
    writer_names: &[String],
    input: R,
    output: W,
) -> Result<W, String> {
    let names: Vec<String> = reader_names.iter().chain(writer_names).cloned().collect();
    let codec = compose(&names)?;

    let mut source = BufReadSource::new(input);
    let mut sink = StdSink::new(output, vec![0u8; STAGING]).expect("STAGING is non-zero");

    stream_to_stream(&mut source, codec, &mut sink).map_err(|e| error_chain(&e))?;
    let mut output = sink.into_inner();
    output.flush().map_err(|e| e.to_string())?;
    Ok(output)
}

/// Print an error and all its sources, separated by ": ".
fn error_chain(error: &dyn std::error::Error) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(inner) = source {
        text.push_str(": ");
        text.push_str(&inner.to_string());
        source = inner.source();
    }
    text
}

/// Flushes after every write. `io::stdout()` is a `LineWriter`, which
/// holds bytes back until a newline, defeating per-character output.
struct FlushEachWrite<W: Write>(W);

impl<W: Write> Write for FlushEachWrite<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.0.write(buf)?;
        self.0.flush()?;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let Args {
        engine,
        readers,
        writers,
        no_buffering,
    } = parse_args(args)?;
    let input = io::stdin().lock();
    if no_buffering {
        // Keep the guard alive until the end of `run`: it restores the tty.
        let tty = eager_tty::EagerTty::enable();
        let output = FlushEachWrite(io::stdout());
        if tty.is_some() {
            // In non-canonical mode Ctrl-D is a plain 0x04 byte, so
            // translate it to EOF here.
            drive(engine, &readers, &writers, EofOnCtrlD::new(input), output)
        } else {
            drive(engine, &readers, &writers, input, output)
        }
    } else {
        drive(engine, &readers, &writers, input, io::stdout())
    }
}

fn drive<R: BufRead, W: Write>(
    engine: Engine,
    readers: &[String],
    writers: &[String],
    input: R,
    output: W,
) -> Result<(), String> {
    match engine {
        Engine::Copy => run_io(readers, writers, input, output).map(drop),
        Engine::Stream => run_io_stream(readers, writers, input, output).map(drop),
    }
}

/// Ends the input at the first Ctrl-D (0x04) byte. A tty in
/// non-canonical mode delivers Ctrl-D as an ordinary byte instead of
/// signalling EOF, so this restores the usual way to end input.
struct EofOnCtrlD<R> {
    inner: R,
    done: bool,
}

impl<R> EofOnCtrlD<R> {
    const CTRL_D: u8 = 0x04;

    fn new(inner: R) -> Self {
        Self { inner, done: false }
    }
}

impl<R: BufRead> Read for EofOnCtrlD<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = {
            let buf = self.fill_buf()?;
            let n = buf.len().min(out.len());
            out[..n].copy_from_slice(&buf[..n]);
            n
        };
        self.consume(n);
        Ok(n)
    }
}

impl<R: BufRead> BufRead for EofOnCtrlD<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.done {
            return Ok(&[]);
        }
        // Two short borrows: returning the first one on one path and
        // mutating `inner` on another is rejected by the borrow checker.
        let pos = self
            .inner
            .fill_buf()?
            .iter()
            .position(|&b| b == Self::CTRL_D);
        match pos {
            Some(0) => {
                self.done = true;
                self.inner.consume(1);
                Ok(&[])
            }
            Some(i) => Ok(&self.inner.fill_buf()?[..i]),
            None => self.inner.fill_buf(),
        }
    }

    fn consume(&mut self, amount: usize) {
        self.inner.consume(amount);
    }
}

/// Puts the terminal on stdin into non-canonical mode (echo stays on) for the
/// lifetime of the guard, so each key press reaches the program at once. A no-op when stdin is not a tty
/// (or off unix). The old settings come back on drop and on
/// SIGINT/SIGTERM.
#[cfg(unix)]
mod eager_tty {
    use std::cell::UnsafeCell;
    use std::sync::atomic::{AtomicBool, Ordering};

    struct Saved(UnsafeCell<libc::termios>);
    // SAFETY: written once before `SAVED_SET` is published, then only read.
    unsafe impl Sync for Saved {}
    static SAVED: Saved = Saved(UnsafeCell::new(unsafe { std::mem::zeroed() }));
    static SAVED_SET: AtomicBool = AtomicBool::new(false);

    pub struct EagerTty;

    extern "C" fn restore_and_die(signum: libc::c_int) {
        // Async-signal-safe calls only.
        unsafe {
            restore();
            libc::signal(signum, libc::SIG_DFL);
            libc::raise(signum);
        }
    }

    unsafe fn restore() {
        if SAVED_SET.load(Ordering::Acquire) {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, SAVED.0.get());
        }
    }

    impl EagerTty {
        pub fn enable() -> Option<Self> {
            // SAFETY: plain termios calls on fd 0; `SAVED` is written
            // before the handlers that read it are installed.
            unsafe {
                if libc::isatty(libc::STDIN_FILENO) != 1 {
                    return None;
                }
                let mut t: libc::termios = std::mem::zeroed();
                if libc::tcgetattr(libc::STDIN_FILENO, &mut t) != 0 {
                    return None;
                }
                *SAVED.0.get() = t;
                SAVED_SET.store(true, Ordering::Release);
                libc::signal(
                    libc::SIGINT,
                    restore_and_die as *const () as libc::sighandler_t,
                );
                libc::signal(
                    libc::SIGTERM,
                    restore_and_die as *const () as libc::sighandler_t,
                );
                t.c_lflag &= !libc::ICANON;
                t.c_cc[libc::VMIN] = 1;
                t.c_cc[libc::VTIME] = 0;
                if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSANOW, &t) != 0 {
                    return None;
                }
            }
            Some(Self)
        }
    }

    impl Drop for EagerTty {
        fn drop(&mut self) {
            // SAFETY: see `enable`.
            unsafe { restore() }
        }
    }
}

#[cfg(not(unix))]
mod eager_tty {
    pub struct EagerTty;
    impl EagerTty {
        pub fn enable() -> Option<Self> {
            None
        }
    }
}

/// Installs a no-op `SIGUSR1` handler so the retry-on-`EINTR` path in
/// `BufReadSource` (see
/// `core/src/sources_and_sinks/std_io/adapter.rs`) can be exercised
/// against a real blocking read: send the running process `SIGUSR1`
/// (`kill -USR1 <pid>`) while it's waiting on stdin.
///
/// Without a caught handler, `SIGUSR1`'s default disposition is to
/// terminate the process, so it would never reach the retry logic at
/// all.
///
/// The handler is installed with `sigaction` and without
/// `SA_RESTART`. glibc's `signal()` sets `SA_RESTART`, and then the
/// kernel restarts the interrupted `read` itself, so user space never
/// sees `EINTR`.
#[cfg(unix)]
fn install_sigusr1_handler() {
    extern "C" fn no_op(_signum: libc::c_int) {}
    // SAFETY: `action` is zeroed, then its mask is set to empty and its
    // handler to a plain C function that does nothing, which is safe to
    // run in a signal handler context. `sa_flags` stays 0, so no
    // `SA_RESTART`.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = no_op as *const () as libc::sighandler_t;
        libc::sigemptyset(&mut action.sa_mask);
        if libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut()) == -1 {
            eprintln!(
                "warning: cannot install the SIGUSR1 handler: {}",
                std::io::Error::last_os_error()
            );
        }
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
        // driven through `stream_to_stream`/`BufReadSource`/`StdSink`
        // instead of `std::io::copy`/`BufReadCodecReader`/`CodecWriter` — both
        // engines must agree on the bytes they produce.
        let reader_names = names(&["identity", "identity", "rot13"]);
        let writer_names = names(&["rot13", "rot13", "identity"]);
        let input = b"hello";

        let copy_output = run_io(
            &reader_names,
            &writer_names,
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
    fn parse_args_reads_engine_flag() {
        let a =
            parse_args(names(&["--engine", "stream", "--readers", "rot13"]).into_iter()).unwrap();
        assert_eq!(a.engine, Engine::Stream);
        assert_eq!(a.readers, names(&["rot13"]));
        assert!(a.writers.is_empty());
        assert!(!a.no_buffering);

        let a = parse_args(names(&["--readers", "rot13"]).into_iter()).unwrap();
        assert_eq!(a.engine, Engine::Copy);
    }

    #[test]
    fn parse_args_reads_no_buffering_flag() {
        let a = parse_args(names(&["--no-buffering", "--writers", "rot13"]).into_iter()).unwrap();
        assert!(a.no_buffering);
        assert_eq!(a.writers, names(&["rot13"]));
    }

    #[test]
    fn parse_args_rejects_unknown_engine() {
        let err = parse_args(names(&["--engine", "bogus"]).into_iter()).unwrap_err();
        assert!(err.contains("bogus"));
    }

    #[test]
    fn ctrl_d_ends_input() {
        let mut r = super::EofOnCtrlD::new(Cursor::new(b"ab\x04cd".to_vec()));
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut r, &mut out).unwrap();
        assert_eq!(out, b"ab");

        // Ctrl-D as the very first byte: immediate EOF.
        let mut r = super::EofOnCtrlD::new(Cursor::new(b"\x04ab".to_vec()));
        let mut out = Vec::new();
        std::io::Read::read_to_end(&mut r, &mut out).unwrap();
        assert!(out.is_empty());
    }

    #[test]
    fn compose_handles_one_name_and_no_names() {
        let input = b"Hello, World! 123";

        let one = compose(&names(&["rot13"])).unwrap();
        assert_eq!(collect(one, input), collect(rot13(), input));

        let none = compose(&[]).unwrap();
        assert_eq!(collect(none, input), input);
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
        // `--writers rot13 identity` runs through a `Chain` in
        // `compose`. After one write and a flush, the sink has the
        // transformed bytes, and `finish` was not called.
        //
        // `rot13` holds no bytes back, so the write alone delivers them.
        // This test does not show that `flush` delivers held bytes. The
        // core test `flush_delivers_already_produced_output_without_finalizing_the_codec`
        // checks that.
        let writer_codec = compose(&names(&["rot13", "identity"])).unwrap();
        let sink = SharedSink::default();
        let mut writer = rust_codecs_core::sources_and_sinks::std_io::CodecWriter::new(
            sink.clone(),
            writer_codec,
            vec![0u8; 64],
        )
        .unwrap();

        writer.write_all(b"hi\n").unwrap();
        writer.flush().unwrap();

        let expected = collect(rot13(), b"hi\n");
        assert_eq!(sink.0.borrow().as_slice(), expected.as_slice());
    }
}
