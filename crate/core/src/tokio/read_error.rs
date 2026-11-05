//! `ReadError`: the failure type of `TokioReader`.

/// A failure of [`TokioReader::next_item`](super::TokioReader::next_item).
#[derive(Debug)]
pub enum ReadError<ES, ED> {
    /// The source failed.
    Source(ES),
    /// The decoder failed.
    Decoder(ED),
    /// The source ended in the middle of an object.
    UnexpectedEnd,
}

impl<ES: core::fmt::Display, ED: core::fmt::Display> core::fmt::Display for ReadError<ES, ED> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Source(e) => e.fmt(f),
            Self::Decoder(e) => e.fmt(f),
            Self::UnexpectedEnd => f.write_str("the source ended in the middle of an object"),
        }
    }
}

impl<ES, ED> core::error::Error for ReadError<ES, ED>
where
    ES: core::error::Error + 'static,
    ED: core::error::Error + 'static,
{
    fn source(&self) -> Option<&(dyn core::error::Error + 'static)> {
        match self {
            Self::Source(e) => Some(e),
            Self::Decoder(e) => Some(e),
            Self::UnexpectedEnd => None,
        }
    }
}
