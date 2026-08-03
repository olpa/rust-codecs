//! The codec trust boundary shared by drivers and combinators.
//!
//! [`Codec::process`](crate::Codec::process) reports only the counts
//! not already implied by its outcome: all input was consumed, all
//! output was filled, or the stream ended. `transfer` validates that
//! report and normalizes it into exact progress on both sides.

use crate::{Codec, Error, Outcome};

/// Why one transfer between the current input and output windows stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StepEnd {
    /// The complete input window was consumed.
    InputExhausted,
    /// The complete output window was filled.
    OutputExhausted,
    /// The codec ended its stream in-band.
    StreamEnd,
}

/// Exact progress made by one validated [`Codec::process`] call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Step {
    pub(crate) consumed: usize,
    pub(crate) written: usize,
    pub(crate) end: StepEnd,
}

/// Transfer between the current windows until the codec reaches the
/// boundary guaranteed by its contract.
pub(crate) fn transfer<C: Codec + ?Sized>(
    codec: &mut C,
    input: &[u8],
    output: &mut [u8],
) -> Result<Step, Error> {
    let input_len = input.len();
    let output_len = output.len();
    let outcome = codec
        .process(input, output)?
        .validated(input_len, output_len)?;

    Ok(match outcome {
        Outcome::InputConsumed { written } => Step {
            consumed: input_len,
            written,
            end: StepEnd::InputExhausted,
        },
        Outcome::OutputFilled { consumed } => Step {
            consumed,
            written: output_len,
            end: StepEnd::OutputExhausted,
        },
        Outcome::StreamEnd { consumed, written } => Step {
            consumed,
            written,
            end: StepEnd::StreamEnd,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::{transfer, Step, StepEnd};
    use crate::{Codec, Drain, Error, ErrorKind, Outcome};

    struct Reports(Outcome);

    impl Codec for Reports {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Ok(self.0)
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn input_exhaustion_implies_all_input_was_consumed() {
        let mut codec = Reports(Outcome::InputConsumed { written: 2 });
        let mut output = [0; 5];

        assert_eq!(
            transfer(&mut codec, b"abc", &mut output),
            Ok(Step {
                consumed: 3,
                written: 2,
                end: StepEnd::InputExhausted,
            })
        );
    }

    #[test]
    fn output_exhaustion_implies_all_output_was_written() {
        let mut codec = Reports(Outcome::OutputFilled { consumed: 2 });
        let mut output = [0; 5];

        assert_eq!(
            transfer(&mut codec, b"abc", &mut output),
            Ok(Step {
                consumed: 2,
                written: 5,
                end: StepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn stream_end_preserves_both_explicit_counts() {
        let mut codec = Reports(Outcome::StreamEnd {
            consumed: 2,
            written: 4,
        });
        let mut output = [0; 5];

        assert_eq!(
            transfer(&mut codec, b"abc", &mut output),
            Ok(Step {
                consumed: 2,
                written: 4,
                end: StepEnd::StreamEnd
            })
        );
    }

    #[test]
    fn degenerate_windows_remain_well_defined() {
        let mut input_done = Reports(Outcome::InputConsumed { written: 0 });
        assert_eq!(
            transfer(&mut input_done, b"", &mut []),
            Ok(Step {
                consumed: 0,
                written: 0,
                end: StepEnd::InputExhausted,
            })
        );

        let mut output_done = Reports(Outcome::OutputFilled { consumed: 0 });
        assert_eq!(
            transfer(&mut output_done, b"abc", &mut []),
            Ok(Step {
                consumed: 0,
                written: 0,
                end: StepEnd::OutputExhausted,
            })
        );
    }

    #[test]
    fn overclaims_are_rejected_at_the_shared_boundary() {
        let violation = Error::new(ErrorKind::ContractViolation, 0, 0);

        let mut input_done = Reports(Outcome::InputConsumed { written: 6 });
        assert_eq!(
            transfer(&mut input_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut output_done = Reports(Outcome::OutputFilled { consumed: 4 });
        assert_eq!(
            transfer(&mut output_done, b"abc", &mut [0; 5]),
            Err(violation)
        );

        let mut ended = Reports(Outcome::StreamEnd {
            consumed: 4,
            written: 6,
        });
        assert_eq!(transfer(&mut ended, b"abc", &mut [0; 5]), Err(violation));
    }

    struct Fails;

    impl Codec for Fails {
        fn process(&mut self, _input: &[u8], _output: &mut [u8]) -> Result<Outcome, Error> {
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        }

        fn finish(&mut self, _output: &mut [u8]) -> Result<Drain, Error> {
            Ok(Drain::Done { written: 0 })
        }
    }

    #[test]
    fn codec_errors_are_preserved() {
        assert_eq!(
            transfer(&mut Fails, b"abc", &mut [0; 5]),
            Err(Error::new(ErrorKind::Corrupt, 1, 2))
        );
    }
}
