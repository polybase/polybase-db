use std::fmt::Display;

use miden_crypto::{
    hash::rpo::{Rpo256, RpoDigest},
    utils::{Deserializable, Serializable, SliceReader},
    Felt,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Hash(RpoDigest);

impl std::hash::Hash for Hash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_bytes().hash(state);
    }
}

mod serde_impl {
    use miden_crypto::utils::{Deserializable, Serializable, SliceReader};
    use serde::{de::Visitor, Deserializer, Serializer};

    use super::*;

    impl Serialize for Hash {
        fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            let mut bytes = vec![0; 32];
            self.0.write_into(&mut bytes);
            serializer.serialize_bytes(&bytes)
        }
    }

    impl<'de> Deserialize<'de> for Hash {
        fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>,
        {
            struct V;
            impl Visitor<'_> for V {
                fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                    formatter.write_str("bytes representing a rescue-prime optimized hash")
                }

                type Value = RpoDigest;

                fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
                where
                    E: serde::de::Error,
                {
                    if v.len() != 32 {
                        return Err(E::custom(format!(
                            "wrong number of bytes - expected 32, found {}",
                            v.len()
                        )));
                    }

                    let mut reader = SliceReader::new(v);
                    RpoDigest::read_from(&mut reader)
                        .map_err(|e| E::custom(format!("deserialization error: {e}")))
                }
            }

            deserializer.deserialize_bytes(V).map(Hash)
        }
    }
}

impl Display for Hash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Hash({})", hex::encode(self.0.as_bytes()))
    }
}

impl Hash {
    pub const NULL: Hash = Hash(RpoDigest::new([Felt::new(0); 4]));

    /// The length of this hash in bytes
    pub const LEN: usize = 32;

    /// The bytes of this hash
    #[inline]
    pub fn to_bytes(&self) -> [u8; Self::LEN] {
        // to_bytes is a more appropriate name, since this method copies into a new slice
        self.0.as_bytes()
    }

    #[inline]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; 32];
        self.0.write_into(&mut bytes);
        bytes
    }

    #[inline]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != 32 {
            return None;
        }

        let mut reader = SliceReader::new(bytes);
        RpoDigest::read_from(&mut reader).ok().map(Hash)
    }

    /// Hash the given bytes
    #[inline]
    pub fn calculate(bytes: &[u8]) -> Self {
        Self(Rpo256::hash(bytes))
    }

    #[inline]
    pub fn digest(&self) -> RpoDigest {
        self.0
    }

    /// Replace `self` with `rpo256(this + other)`
    #[inline]
    pub fn merge(&mut self, other: &Hash) {
        self.0 = Rpo256::merge(&[self.0, other.0]);
    }
}

impl From<RpoDigest> for Hash {
    fn from(value: RpoDigest) -> Self {
        Self(value)
    }
}

pub struct MerklePath {
    /// The components of the path, with the root at the end
    components: Vec<Hash>,
}

impl MerklePath {
    /// Create a new [`MerklePath`] from the given components
    ///
    /// The components should be the hashes that form the path, with the root of the tree at the
    /// end
    #[inline]
    pub fn new(components: Vec<Hash>) -> Self {
        Self { components }
    }

    /// Get a slice of hashes representing the components of the path
    #[inline]
    pub fn components(&self) -> &[Hash] {
        &self.components
    }

    /// Get a mutable slice of hashes representing the components of the path
    #[inline]
    pub fn components_mut(&mut self) -> &mut [Hash] {
        &mut self.components
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_hash_is_all_zeroes() {
        assert_eq!(Hash::NULL.to_bytes(), [0; 32]);
    }
}
