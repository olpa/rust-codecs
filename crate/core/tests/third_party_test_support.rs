//! Smoke test that `sources_and_sinks::shared_io::test_support` is
//! actually usable from outside the crate, behind the `test-support`
//! feature — exactly like a third-party `Source`/`Sink` backend crate
//! would use it in its own test suite, instead of reimplementing the
//! same doubles.

#![cfg(feature = "test-support")]

use rust_codecs_core::sources_and_sinks::shared_io::end_capable_pump_read;
use rust_codecs_core::sources_and_sinks::shared_io::test_support::EarlyEnd;
use rust_codecs_core::sources_and_sinks::slice::SliceSource;
use rust_codecs_core::Pump;

#[test]
fn early_end_latches_after_the_codec_ends_in_band() {
    let mut input = SliceSource::new(b"Hello World");
    let mut pump = Pump::new(EarlyEnd { limit: 3, done: 0 });

    let mut out = [0u8; 8];
    let mut pos = 0;
    loop {
        let n = end_capable_pump_read(&mut pump, &mut input, &mut out[pos..]).unwrap();
        if n == 0 {
            break;
        }
        pos += n;
    }

    assert_eq!(&out[..pos], b"Hel");
}
