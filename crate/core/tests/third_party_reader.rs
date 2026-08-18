//! Smoke test that `Pump` and `sources_and_sinks::shared_io` are
//! actually usable from outside the crate — this file only sees
//! `rust_codecs_core`'s public API, exactly like a third-party
//! `Source`/`Sink` backend crate would. It rebuilds a minimal
//! `CodecReader`-equivalent over `SliceSource`, the pattern
//! `CREATING-IO-BACKENDS.md` documents.

#![cfg(feature = "rot13")]

use core::convert::Infallible;

use rust_codecs_core::rot13::rot13;
use rust_codecs_core::sources_and_sinks::shared_io::{pump_read, ReadGranularity};
use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::{DriveError, Pump, Source, EndCapableCodec};

/// A minimal incremental reader, built the same way
/// `std_io`/`embedded_io`'s `CodecReader` is: a `Source` plus a
/// `Pump`, driven one bounded call at a time by `pump_read`.
struct MinimalReader<I: Source, C: EndCapableCodec> {
    input: I,
    pump: Pump<C>,
}

impl<I: Source, C: EndCapableCodec> MinimalReader<I, C> {
    fn new(input: I, codec: C) -> Self {
        Self { input, pump: Pump::new(codec) }
    }

    fn read(&mut self, buf: &mut [u8]) -> Result<usize, DriveError<I::Error, Infallible>> {
        pump_read(&mut self.pump, &mut self.input, buf, ReadGranularity::FillBuffer)
    }
}

#[test]
fn third_party_backend_can_reuse_pump_and_shared_io() {
    let mut reader = MinimalReader::new(SliceSource::new(b"Uryyb, jbeyq!"), rot13());

    let mut out = [0u8; 32];
    let mut pos = 0;
    loop {
        let n = reader.read(&mut out[pos..]).unwrap();
        if n == 0 {
            break;
        }
        pos += n;
    }

    assert_eq!(&out[..pos], b"Hello, world!");
}
