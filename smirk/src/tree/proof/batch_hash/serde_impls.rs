use miden_prover::ExecutionProof;
use serde::{de::Visitor, Deserializer, Serializer};

pub(super) fn serialize<S>(proof: &ExecutionProof, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_bytes(&proof.to_bytes())
}

pub(super) fn deserialize<'de, D>(d: D) -> Result<ExecutionProof, D::Error>
where
    D: Deserializer<'de>,
{
    struct V;
    impl Visitor<'_> for V {
        type Value = ExecutionProof;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("bytes representing an execution proof")
        }

        fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            ExecutionProof::from_bytes(v).map_err(E::custom)
        }
    }

    d.deserialize_bytes(V)
}
