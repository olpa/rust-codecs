//! Concrete [`Codec`](crate::Codec) implementations, one per feature.

pub mod identity;
pub mod boundary_as_codec;

#[cfg(any(feature = "rot13", test))]
#[cfg_attr(docsrs, doc(cfg(feature = "rot13")))]
pub mod rot13;

#[cfg(feature = "base64")]
#[cfg_attr(docsrs, doc(cfg(feature = "base64")))]
pub mod base64_dec;
#[cfg(feature = "base64")]
#[cfg_attr(docsrs, doc(cfg(feature = "base64")))]
pub mod base64_enc;
#[cfg(feature = "base64")]
mod base64_shared;

#[cfg(feature = "json")]
#[cfg_attr(docsrs, doc(cfg(feature = "json")))]
pub mod json_enc;

#[cfg(feature = "compcol")]
#[cfg_attr(docsrs, doc(cfg(feature = "compcol")))]
pub mod compcol;

#[cfg(test)]
pub(crate) mod test_support;
