use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PublicKey {
    /// Key type. Always `EC` for now.
    kty: String,
    /// Curve. Always `secp256k1` for now.
    crv: String,
    /// Algorithm. Always `ES256K` for now.
    alg: String,
    /// Public key use. Always `sig` for now.
    #[serde(rename = "use")]
    use_: String,
    /// X coordinate of the key.
    #[serde(
        serialize_with = "to_url_safe_base64",
        deserialize_with = "from_url_safe_base64"
    )]
    x: Vec<u8>,
    /// Y coordinate of the key.
    #[serde(
        serialize_with = "to_url_safe_base64",
        deserialize_with = "from_url_safe_base64"
    )]
    y: Vec<u8>,
}

fn to_url_safe_base64<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&base64::engine::general_purpose::URL_SAFE.encode(bytes))
}

fn from_url_safe_base64<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    base64::engine::general_purpose::URL_SAFE
        .decode(s.as_bytes())
        .map_err(serde::de::Error::custom)
}

impl PublicKey {
    pub fn es256k(x: [u8; 32], y: [u8; 32]) -> Result<Self, secp256k1::Error> {
        let mut pk = Vec::with_capacity(65);
        let prefix = 4u8;
        pk.push(prefix);
        pk.extend_from_slice(&x);
        pk.extend_from_slice(&y);

        // Verify that the public key is valid.
        secp256k1::PublicKey::from_slice(&pk)?;

        Ok(Self {
            kty: "EC".to_string(),
            crv: "secp256k1".to_string(),
            alg: "ES256K".to_string(),
            use_: "sig".to_string(),
            x: x.to_vec(),
            y: y.to_vec(),
        })
    }

    #[cfg(test)]
    pub(crate) fn random() -> Self {
        // Generate a random key pair. Has to be valid. From secp256k1

        let mut rng = rand::thread_rng();
        let secp = secp256k1::Secp256k1::new();
        let (_, pk) = secp.generate_keypair(&mut rng);

        Self::from_secp256k1_key(&pk).unwrap()
    }

    pub fn from_secp256k1_key(key: &secp256k1::PublicKey) -> Result<Self, secp256k1::Error> {
        let uncompressed = key.serialize_uncompressed();
        let x = uncompressed[1..33].try_into().unwrap();
        let y = uncompressed[33..65].try_into().unwrap();

        Self::es256k(x, y)
    }

    pub(crate) fn to_indexable(&self) -> Vec<u8> {
        let mut v = Vec::new();

        v.extend_from_slice(self.kty.as_bytes());
        v.extend_from_slice(b"|");
        v.extend_from_slice(self.crv.as_bytes());
        v.extend_from_slice(b"|");
        v.extend_from_slice(self.alg.as_bytes());
        v.extend_from_slice(b"|");
        v.extend_from_slice(self.use_.as_bytes());
        v.extend_from_slice(b"|");
        v.extend_from_slice(&self.x);
        v.extend_from_slice(b"|");
        v.extend_from_slice(&self.y);

        v
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_public_key() {
        super::PublicKey::random();
    }
}
