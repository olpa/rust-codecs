//! Smoke test that `sources_and_sinks::shared_io::test_support` is
//! actually usable from outside the crate, behind the `test-support`
//! feature — exactly like a third-party `Source`/`Sink` backend crate
//! would use it in its own test suite, instead of reimplementing the
//! same doubles.

#![cfg(feature = "test-support")]

use core::convert::Infallible;

use rust_codecs_core::sources_and_sinks::shared_io::pump_read;
use rust_codecs_core::sources_and_sinks::shared_io::test_support::{CountingReader, EarlyEnd};
use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::{Pump, Source};

#[test]
fn early_end_latches_after_the_codec_ends_in_band() {
    let mut input = SliceSource::new(b"Hello World");
    let mut pump = Pump::new(EarlyEnd { limit: 3, done: 0 });

    let mut out = [0u8; 8];
    let mut pos = 0;
    loop {
        let n = pump_read(&mut pump, &mut input, &mut out[pos..]).unwrap();
        if n == 0 {
            break;
        }
        pos += n;
    }

    assert_eq!(&out[..pos], b"Hel");
}

#[test]
fn counting_reader_tracks_calls_made_on_the_wrapped_source() {
    struct OneShot<'a> {
        data: &'a [u8],
        served: bool,
    }

    impl Source for OneShot<'_> {
        type Error = Infallible;

        fn chunk(&mut self) -> Result<Option<&[u8]>, Self::Error> {
            Ok((!self.served).then_some(self.data))
        }

        fn consume(&mut self, amount: usize) {
            assert_eq!(amount, self.data.len());
            self.served = true;
        }
    }

    let mut reader = CountingReader {
        inner: OneShot {
            data: b"abc",
            served: false,
        },
        reads: 0,
    };
    assert_eq!(reader.inner.chunk().unwrap(), Some(b"abc".as_slice()));
    reader.reads += 1;
    assert_eq!(reader.reads, 1);
}
