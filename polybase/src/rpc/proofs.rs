use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(test, derive(test_strategy::Arbitrary))]
#[non_exhaustive]
#[serde(tag = "kind")]
#[serde(rename_all = "kebab-case")]
pub enum Proof {
    MerkleRootHash {
        #[serde(with = "hex")]
        bytes: [u8; 32],
    },
    MerkleProof {
        #[serde(with = "hex")]
        bytes: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::{from_value, json};
    use test_strategy::proptest;

    #[test]
    fn proof_serialization_format() {
        let proof: Proof = from_value(json!({
            "kind": "merkle-proof",
            "bytes": "010203",
        }))
        .unwrap();

        let expected = Proof::MerkleProof {
            bytes: vec![1, 2, 3],
        };

        assert_eq!(proof, expected)
    }

    #[proptest]
    fn serialization_format_round_trip(proof: Proof) {
        let string = serde_json::to_string(&proof).unwrap();
        let proof_again: Proof = serde_json::from_str(&string).unwrap();

        assert_eq!(proof, proof_again);
    }
}
