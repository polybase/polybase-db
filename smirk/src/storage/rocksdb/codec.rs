use serde::{Deserialize, Serialize};
use thiserror::Error;

/// An error encountered when encoding data to its database format
#[derive(Debug, Error)]
#[error("encode error: {0}")]
pub struct EncodeError(rmp_serde::encode::Error);

/// An error encountered when decoding data in its database format
#[derive(Debug, Error)]
#[error("decode error: {0}")]
pub struct DecodeError(rmp_serde::decode::Error);

pub(super) fn encode<'a, T>(t: &'a T) -> Result<Vec<u8>, EncodeError>
where
    T: Serialize,
{
    rmp_serde::encode::to_vec(t).map_err(EncodeError)
}

pub(super) fn decode<T: for<'a> Deserialize<'a> + 'static>(bytes: &[u8]) -> Result<T, DecodeError> {
    rmp_serde::decode::from_slice(bytes).map_err(DecodeError)
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::{proptest, Arbitrary};

    use crate::hash::Digest;

    use super::*;

    #[derive(Debug, Deserialize, Serialize, Arbitrary, PartialEq, Eq)]
    struct CoolCustomType {
        foo: String,
        bar: Vec<u8>,
        coords: [(i32, i32); 10],
    }

    #[proptest]
    fn encode_decode_bijective(key: Digest, value: CoolCustomType) {
        let bytes = encode(&(&key, &value)).unwrap();
        let (key_again, value_again): (Digest, CoolCustomType) = decode(&bytes).unwrap();

        prop_assert_eq!(key, key_again);
        prop_assert_eq!(value, value_again);
    }
}
