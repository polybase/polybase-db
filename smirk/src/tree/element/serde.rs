use halo2_proofs::pasta::group::ff::PrimeField;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::{Base, Element};

impl Serialize for Element {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let bytes = self.0.to_repr();
        serializer.serialize_bytes(&bytes)
    }
}

impl<'de> Deserialize<'de> for Element {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V;

        impl Visitor<'_> for V {
            type Value = Element;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("32 bytes representing a pallas base")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let bytes = v
                    .try_into()
                    .map_err(|_| E::custom("incorrect number of bytes"))?;

                let base = Base::from_repr_vartime(bytes)
                    .ok_or_else(|| E::custom("failed to parse base"))?;

                Ok(Element(base))
            }
        }

        deserializer.deserialize_bytes(V)
    }
}
