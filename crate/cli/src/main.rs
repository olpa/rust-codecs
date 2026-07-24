//! Test harness for chaining codecs: builds a `CodecReader` chain from
//! `--readers` and a `CodecWriter` chain from `--writers`, then copies
//! stdin through the readers, through the writers, to stdout.
//!
//! ```text
//! echo hello | cargo run -p cli -- --readers identity identity rot13 --writers rot13 rot13 identity
//! ```
//!
//! Reader codecs apply in the order listed (first listed runs on the raw
//! bytes first). Writer codecs also apply in the order listed (first
//! listed runs first, closest to the incoming bytes, before reaching
//! stdout).

use std::io::{self, Read};

use rust_codecs_core::io::{CodecReader, CodecWriter, FinishWrite};
use rust_codecs_core::Codec;

/// Single source of truth for known codec names: `make_codec`, the
/// "unknown codec" error, and `--help`'s codec list all read from
/// this instead of repeating the name list.
const CODECS: &[(&str, fn() -> Box<dyn Codec>)] = &[
    ("identity", || Box::new(rust_codecs_core::identity::identity())),
    ("rot13", || Box::new(rust_codecs_core::rot13::rot13())),
    ("base64-enc", || Box::new(rust_codecs_core::base64::base64_enc())),
    ("base64-dec", || Box::new(rust_codecs_core::base64::base64_dec())),
    ("json-enc", || Box::new(rust_codecs_core::json::json_enc())),
];

fn usage() -> String {
    let names = CODECS.iter().map(|(name, _)| *name).collect::<Vec<_>>().join(", ");
    format!(
        "\
Test harness for chaining codecs: builds a CodecReader chain from
--readers and a CodecWriter chain from --writers, then copies stdin
through the readers, through the writers, to stdout.

Usage:
  cargo run -p cli -- [--readers CODEC...] [--writers CODEC...]

Codecs: {names}

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
    CODECS.iter().find(|(n, _)| *n == name).map(|(_, ctor)| ctor()).ok_or_else(|| {
        let names = CODECS.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ");
        format!("unknown codec {name:?} (expected one of: {names})")
    })
}

enum Mode {
    None,
    Readers,
    Writers,
}

fn parse_args(args: impl Iterator<Item = String>) -> Result<(Vec<String>, Vec<String>), String> {
    let mut mode = Mode::None;
    let mut readers = Vec::new();
    let mut writers = Vec::new();
    for arg in args {
        match arg.as_str() {
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
    Ok((readers, writers))
}

fn run(args: impl Iterator<Item = String>) -> Result<(), String> {
    let (reader_names, writer_names) = parse_args(args)?;

    let mut reader: Box<dyn Read> = Box::new(io::stdin());
    for name in &reader_names {
        let codec = make_codec(name)?;
        reader = Box::new(CodecReader::new(reader, codec));
    }

    let mut writer: Box<dyn FinishWrite> = Box::new(io::stdout());
    for name in writer_names.iter().rev() {
        let codec = make_codec(name)?;
        writer = Box::new(CodecWriter::new(writer, codec));
    }

    io::copy(&mut reader, &mut writer).map_err(|e| e.to_string())?;
    writer.finish_boxed().map_err(|e| e.to_string())?;
    Ok(())
}

fn main() {
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
