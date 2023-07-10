use super::*;

use miden_crypto::utils::{Deserializable, Serializable, SliceReader};
use serde::{de::Visitor, Deserializer, Serializer};
use serde::{Deserialize, Serialize};

impl Serialize for Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = vec![0; 32];
        self.0.write_into(&mut bytes);
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

        deserializer.deserialize_bytes(V).map(Digest)
    }
}
