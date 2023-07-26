use serde::{de::Visitor, Deserializer, Serializer};
use serde::{Deserialize, Serialize};

use super::Digest;

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let bytes = self.to_bytes();
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct V;
        impl Visitor<'_> for V {
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("bytes representing a rescue-prime optimized hash")
            }

            type Value = Digest;

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes = v
                    .try_into()
                    .map_err(|_| E::custom(format!("incorrect number of bytes: {}", v.len())))?;

                Digest::from_bytes(bytes).ok_or(E::custom("deserialization error"))
            }
        }

        deserializer.deserialize_bytes(V)
    }
}
