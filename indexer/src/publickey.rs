use base64::Engine;
use serde::{Deserialize, Serialize};

pub type Result<T> = std::result::Result<T, PublicKeyError>;

#[derive(Debug, thiserror::Error)]
pub enum PublicKeyError {
    #[error("missing field {name:?}")]
    MissingField { name: &'static str },

    #[error("invalid type, expected object")]
    InvalidTypeExpectedObject,

    #[error("invalid type for field {field:?}, expected string")]
    InvalidTypeExpectedString { field: &'static str },

    #[error("invalid value {value:?} for field {field:?}")]
    InvalidValue { field: &'static str, value: String },

    #[error("from utf8 error")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),

    #[error("base64 decode error")]
    Base64DecodeError(#[from] base64::DecodeError),

    #[error("secp256k1 error")]
    Secp256k1Error(#[from] secp256k1::Error),
}

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

fn to_url_safe_base64<S>(bytes: &[u8], serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&base64::engine::general_purpose::URL_SAFE.encode(bytes))
}

fn from_url_safe_base64<'de, D>(deserializer: D) -> std::result::Result<Vec<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;
    base64::engine::general_purpose::URL_SAFE
        .decode(s.as_bytes())
        .map_err(serde::de::Error::custom)
}

impl PublicKey {
    pub fn es256k(x: [u8; 32], y: [u8; 32]) -> std::result::Result<Self, secp256k1::Error> {
        Self::validate_secp256k1_key(&x, &y)?;

        Ok(Self {
            kty: "EC".to_string(),
            crv: "secp256k1".to_string(),
            alg: "ES256K".to_string(),
            use_: "sig".to_string(),
            x: x.to_vec(),
            y: y.to_vec(),
        })
    }

    fn validate_secp256k1_key(x: &[u8], y: &[u8]) -> std::result::Result<(), secp256k1::Error> {
        let mut pk = Vec::with_capacity(65);
        let prefix = 4u8;
        pk.push(prefix);
        pk.extend_from_slice(x);
        pk.extend_from_slice(y);

        secp256k1::PublicKey::from_slice(&pk).map(|_| ())
    }

    #[cfg(test)]
    pub(crate) fn random() -> Self {
        let mut rng = rand::thread_rng();
        let secp = secp256k1::Secp256k1::new();
        let (_, pk) = secp.generate_keypair(&mut rng);

        Self::from_secp256k1_key(&pk).unwrap()
    }

    pub fn from_secp256k1_key(
        key: &secp256k1::PublicKey,
    ) -> std::result::Result<Self, secp256k1::Error> {
        let uncompressed = key.serialize_uncompressed();
        let x = uncompressed[1..33].try_into().unwrap();
        let y = uncompressed[33..65].try_into().unwrap();

        Self::es256k(x, y)
    }

    pub fn to_secp256k1_key(&self) -> std::result::Result<secp256k1::PublicKey, secp256k1::Error> {
        let mut pk = Vec::with_capacity(65);
        let prefix = 4u8;
        pk.push(prefix);
        pk.extend_from_slice(&self.x);
        pk.extend_from_slice(&self.y);

        secp256k1::PublicKey::from_slice(&pk)
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

    pub(crate) fn from_indexable(v: &[u8]) -> Result<Self> {
        let mut parts = v.split(|b| *b == b'|');

        let kty = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "kty" })?;
        let crv = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "crv" })?;
        let alg = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "alg" })?;
        let use_ = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "use" })?;
        let x = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "x" })?;
        let y = parts
            .next()
            .ok_or(PublicKeyError::MissingField { name: "y" })?;

        Ok(Self {
            kty: String::from_utf8(kty.to_vec())?,
            crv: String::from_utf8(crv.to_vec())?,
            alg: String::from_utf8(alg.to_vec())?,
            use_: String::from_utf8(use_.to_vec())?,
            x: x.to_vec(),
            y: y.to_vec(),
        })
    }

    pub fn to_hex(&self) -> Result<String> {
        let key = self.to_secp256k1_key()?;
        let bytes = key.serialize_uncompressed();
        // Remove the prefix 0x04
        let bytes = &bytes[1..];

        let mut s = hex::encode(bytes);
        s.insert_str(0, "0x");

        Ok(s)
    }
}

impl TryFrom<serde_json::Value> for PublicKey {
    type Error = PublicKeyError;

    fn try_from(value: serde_json::Value) -> Result<Self> {
        match value {
            serde_json::Value::Object(mut o) => {
                let kty_v = o
                    .remove("kty")
                    .ok_or(PublicKeyError::MissingField { name: "kty" })?;
                let crv_v = o
                    .remove("crv")
                    .ok_or(PublicKeyError::MissingField { name: "crv" })?;
                let alg_v = o
                    .remove("alg")
                    .ok_or(PublicKeyError::MissingField { name: "alg" })?;
                let use_v = o
                    .remove("use")
                    .ok_or(PublicKeyError::MissingField { name: "use" })?;
                let x_v = o
                    .remove("x")
                    .ok_or(PublicKeyError::MissingField { name: "x" })?;
                let y_v = o
                    .remove("y")
                    .ok_or(PublicKeyError::MissingField { name: "y" })?;

                let kty = match kty_v {
                    serde_json::Value::String(s) => s,
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "kty" }),
                };

                if kty != "EC" {
                    return Err(PublicKeyError::InvalidValue {
                        field: "kty",
                        value: kty,
                    });
                }

                let crv = match crv_v {
                    serde_json::Value::String(s) => s,
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "crv" }),
                };

                if crv != "secp256k1" {
                    return Err(PublicKeyError::InvalidValue {
                        field: "crv",
                        value: crv,
                    });
                }

                let alg = match alg_v {
                    serde_json::Value::String(s) => s,
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "alg" }),
                };

                if alg != "ES256K" {
                    return Err(PublicKeyError::InvalidValue {
                        field: "alg",
                        value: alg,
                    });
                }

                let use_ = match use_v {
                    serde_json::Value::String(s) => s,
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "use" }),
                };

                if use_ != "sig" {
                    return Err(PublicKeyError::InvalidValue {
                        field: "use",
                        value: use_,
                    });
                }

                let x = match &x_v {
                    serde_json::Value::String(s) => {
                        base64::engine::general_purpose::URL_SAFE.decode(s.as_bytes())?
                    }
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "x" }),
                };

                if x.len() != 32 {
                    return Err(PublicKeyError::InvalidValue {
                        field: "x",
                        value: x_v.to_string(),
                    });
                }

                let y = match &y_v {
                    serde_json::Value::String(s) => {
                        base64::engine::general_purpose::URL_SAFE.decode(s.as_bytes())?
                    }
                    _ => return Err(PublicKeyError::InvalidTypeExpectedString { field: "y" }),
                };

                if y.len() != 32 {
                    return Err(PublicKeyError::InvalidValue {
                        field: "y",
                        value: y_v.to_string(),
                    });
                }

                PublicKey::validate_secp256k1_key(&x, &y)?;

                Ok(Self {
                    kty,
                    crv,
                    alg,
                    use_,
                    x,
                    y,
                })
            }
            _ => Err(PublicKeyError::InvalidTypeExpectedObject),
        }
    }
}

impl From<PublicKey> for serde_json::Value {
    fn from(pk: PublicKey) -> Self {
        let mut o = serde_json::Map::new();

        o.insert("kty".to_string(), serde_json::Value::String(pk.kty));
        o.insert("crv".to_string(), serde_json::Value::String(pk.crv));
        o.insert("alg".to_string(), serde_json::Value::String(pk.alg));
        o.insert("use".to_string(), serde_json::Value::String(pk.use_));
        o.insert(
            "x".to_string(),
            serde_json::Value::String(base64::engine::general_purpose::URL_SAFE.encode(&pk.x)),
        );
        o.insert(
            "y".to_string(),
            serde_json::Value::String(base64::engine::general_purpose::URL_SAFE.encode(&pk.y)),
        );

        serde_json::Value::Object(o)
    }
}

impl TryFrom<[u8; 64]> for PublicKey {
    type Error = PublicKeyError;

    fn try_from(bytes: [u8; 64]) -> Result<Self> {
        let x = <[u8; 32]>::try_from(&bytes[0..32]).unwrap();
        let y = <[u8; 32]>::try_from(&bytes[32..64]).unwrap();

        Self::es256k(x, y).map_err(PublicKeyError::from)
    }
}

impl Default for PublicKey {
    fn default() -> Self {
        let mut compressed = [0u8; 33];
        compressed[0] = 2;
        compressed[32] = 1;

        let key = secp256k1::PublicKey::from_slice(&compressed).unwrap();
        Self::from_secp256k1_key(&key).unwrap()
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_public_key() {
        super::PublicKey::random();
    }
}
