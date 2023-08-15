use secp256k1::ecdsa::{RecoverableSignature, RecoveryId};
use sha3::Digest;
use std::{
    future::ready,
    time::{SystemTime, UNIX_EPOCH},
};

use actix_web::{http::header::CONTENT_LENGTH, FromRequest};
use futures::{future::LocalBoxFuture, StreamExt};
use schema::publickey::PublicKey;
use serde::de::DeserializeOwned;

use crate::errors::http::HTTPError;
use indexer::auth_user::AuthUser;

pub type Result<T> = std::result::Result<T, AuthError>;

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("user error")]
    User(#[from] AuthUserError),

    #[error("actix web http header to str error")]
    ToStr(#[from] actix_web::http::header::ToStrError),

    #[error("actix web payload error")]
    Payload(#[from] actix_web::error::PayloadError),

    #[error("secp256k1 error")]
    Secp256k1(#[from] secp256k1::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum AuthUserError {
    #[error("missing = in key=value pair")]
    MissingEquals,

    #[error("public key must start with 0x")]
    PublicKeyMustStartWith0x,

    #[error("signature must start with 0x")]
    SignatureMustStartWith0x,

    #[error("signature must be 65 bytes")]
    SignatureMustBe65Bytes,

    #[error("missing signature")]
    MissingSignature,

    #[error("missing timestamp")]
    MissingTimestamp,

    #[error("missing version")]
    MissingVersion,

    #[error("missing hash")]
    MissingHash,

    #[error("unknown key {key:?}")]
    UnknownKey { key: String },

    #[error("public key does not match key recovered from signature")]
    SignaturePublicKeyMismatch,

    #[error("signature expired")]
    SignatureExpired,

    #[error("failed to decode hex parameter {parameter:?}")]
    FailedToDecodeHexParameter {
        parameter: String,
        source: hex::FromHexError,
    },

    #[error("failed to decode timestamp")]
    FailedToDecodeTimestamp(#[source] std::num::ParseIntError),

    #[error("invalid recovery id {n:?}")]
    InvalidRecoveryId { n: u8, source: secp256k1::Error },

    #[error("failed to decode public key")]
    FailedToDecodePublicKey(#[source] secp256k1::Error),

    #[error("invalid signature")]
    InvalidSignature(#[source] secp256k1::Error),

    #[error("failed to recover public key")]
    FailedToRecoverPublicKey(#[source] secp256k1::Error),

    #[error("serde_json error")]
    FailedToParseBody(#[source] serde_json::Error),
}

const TIME_TOLERANCE: u64 = 5 * 60; // 5 minutes

pub(crate) struct Auth {
    pub(crate) public_key: PublicKey,
}

impl From<Auth> for AuthUser {
    fn from(auth: Auth) -> Self {
        Self::new(auth.public_key)
    }
}

#[derive(Debug, PartialEq)]
struct Signature {
    public_key: Option<PublicKey>,
    sig: RecoverableSignature,
    /// Unix timestamp in *milliseconds*.
    timestamp: u64,
    version: String,
    hash: String,
}

impl Signature {
    // Deserialize parses the signature in the k=v,k2=v2,... format.
    fn deserialize(header_value: &str) -> Result<Self> {
        let mut public_key = None;
        let mut signature = None;
        let mut timestamp = None;
        let mut version = None;
        let mut hash = None;

        for kv in header_value.split(',') {
            let Some((k, v)) = kv.split_once('=') else {
                return Err(AuthUserError::MissingEquals.into());
            };

            match k {
                "pk" => {
                    let original_v = v;
                    let v = v.trim_start_matches("0x");
                    if v == original_v {
                        return Err(AuthUserError::PublicKeyMustStartWith0x.into());
                    }

                    let hex = hex::decode(v).map_err(|source| {
                        AuthUserError::FailedToDecodeHexParameter {
                            parameter: "pk".to_string(),
                            source,
                        }
                    })?;
                    let pk = secp256k1::PublicKey::from_slice(&hex)
                        .map_err(AuthUserError::FailedToDecodePublicKey)?;
                    let pk = PublicKey::from_secp256k1_key(&pk)
                        .map_err(AuthUserError::FailedToDecodePublicKey)?;

                    public_key = Some(pk)
                }
                "sig" => {
                    let original_v = v;
                    let v = v.trim_start_matches("0x");
                    if v == original_v {
                        return Err(AuthUserError::SignatureMustStartWith0x.into());
                    }

                    let hex = hex::decode(v).map_err(|source| {
                        AuthUserError::FailedToDecodeHexParameter {
                            parameter: "sig".to_string(),
                            source,
                        }
                    })?;
                    if hex.len() != 65 {
                        return Err(AuthUserError::SignatureMustBe65Bytes.into());
                    }

                    let rec_id = if hex[64] >= 27 { hex[64] - 27 } else { hex[64] };

                    let recoverable_signature = RecoverableSignature::from_compact(
                        &hex[0..64],
                        RecoveryId::from_i32(rec_id as i32).map_err(|source| {
                            AuthUserError::InvalidRecoveryId { n: rec_id, source }
                        })?,
                    )
                    .map_err(AuthUserError::InvalidSignature)?;

                    signature = Some(recoverable_signature)
                }
                "t" => {
                    // Example t from explorer: 1677023964425000
                    let v: u64 = v.parse().map_err(AuthUserError::FailedToDecodeTimestamp)?;
                    timestamp = Some(v)
                }
                "v" => version = Some(v.to_string()),
                "h" => hash = Some(v.to_string()),
                x => return Err(AuthUserError::UnknownKey { key: x.to_string() }.into()),
            }
        }

        let signature = signature.ok_or(AuthError::User(AuthUserError::MissingSignature))?;
        let timestamp = timestamp.ok_or(AuthError::User(AuthUserError::MissingTimestamp))?;
        let version = version.ok_or(AuthError::User(AuthUserError::MissingVersion))?;
        let hash = hash.ok_or(AuthError::User(AuthUserError::MissingHash))?;

        Ok(Self {
            public_key,
            sig: signature,
            timestamp,
            version,
            hash,
        })
    }

    fn verify(&self, body: &[u8]) -> Result<PublicKey> {
        let timestamp = self.timestamp.to_string();
        let timestamp_body_len = (timestamp.len() + 1 + body.len()).to_string();
        let message_parts = &[
            "\u{19}Ethereum Signed Message:\n".as_bytes(),
            timestamp_body_len.as_bytes(),
            timestamp.as_bytes(),
            b".",
            body,
        ];

        let mut hasher = sha3::Keccak256::new();
        for part in message_parts {
            hasher.update(part);
        }

        let message_hash = hasher.finalize();

        let sig_pk = self
            .sig
            .recover(&secp256k1::Message::from_slice(&message_hash)?)
            .map_err(|source| AuthError::User(AuthUserError::FailedToRecoverPublicKey(source)))?;
        let sig_pk = PublicKey::from_secp256k1_key(&sig_pk)?;

        if self.public_key.as_ref().map_or(false, |pk| *pk != sig_pk) {
            return Err(AuthUserError::SignaturePublicKeyMismatch.into());
        }

        Ok(sig_pk)
    }

    fn from_req(req: &actix_web::HttpRequest) -> Result<Option<Self>> {
        let Some(signature) = req
            .headers()
            .get("X-Polybase-Signature") else { return Ok(None); };

        let signature = signature.to_str()?;

        let signature = Signature::deserialize(signature)?;

        #[allow(clippy::unwrap_used)] // this should never error
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if signature.timestamp / 1000 + TIME_TOLERANCE < now {
            return Err(AuthUserError::SignatureExpired.into());
        }

        Ok(Some(signature))
    }
}

pub(crate) struct SignedJSON<T: DeserializeOwned> {
    pub(crate) data: T,
    pub(crate) auth: Option<Auth>,
}

impl<T: DeserializeOwned + 'static> FromRequest for SignedJSON<T> {
    type Error = HTTPError;
    type Future = LocalBoxFuture<'static, std::result::Result<Self, HTTPError>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let sig = match Signature::from_req(req) {
            Ok(sig) => sig,
            Err(e) => return Box::pin(ready(Err(e.into()))),
        };

        let length = req
            .headers()
            .get(&CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok());

        let mut payload = payload.take();
        Box::pin(async move {
            let mut body = Vec::with_capacity(length.unwrap_or(0));
            while let Some(chunk) = payload.next().await {
                body.extend_from_slice(&chunk.map_err(AuthError::from)?);
            }

            Ok(Self {
                data: serde_json::from_slice(if body.is_empty() { b"null" } else { &body })
                    .map_err(AuthUserError::FailedToParseBody)
                    .map_err(AuthError::from)?,
                auth: sig
                    .map(|sig| {
                        if std::option_env!("DEV_SKIP_SIGNATURE_VERIFICATION") == Some("1")
                            && sig.public_key.is_some()
                        {
                            #[allow(clippy::unwrap_used)]
                            // we know public_key is Some, and this is a dev-only feature
                            return Ok(Auth {
                                public_key: sig.public_key.unwrap(),
                            });
                        }

                        Ok::<_, AuthError>(Auth {
                            public_key: sig.verify(&body)?,
                        })
                    })
                    .transpose()?,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_deserialize() {
        let signature =
            Signature::deserialize("pk=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d93456836c0,sig=0x043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d9345683600,t=1234,v=1,h=deadbeef").unwrap();

        assert_eq!(
            signature,
            Signature {
                public_key: Some(
                    PublicKey::es256k(
                        [
                            55, 5, 166, 151, 44, 128, 244, 74, 195, 56, 195, 191, 137, 23, 137,
                            151, 115, 235, 46, 17, 159, 197, 45, 161, 60, 2, 249, 141, 166, 171,
                            227, 58
                        ],
                        [
                            49, 170, 29, 96, 15, 117, 61, 1, 227, 132, 141, 87, 179, 129, 57, 90,
                            210, 180, 58, 88, 196, 14, 227, 217, 81, 3, 109, 147, 69, 104, 54, 192
                        ]
                    )
                    .unwrap()
                ),
                sig: RecoverableSignature::from_compact(
                    &hex::decode("043705a6972c80f44ac338c3bf8917899773eb2e119fc52da13c02f98da6abe33a31aa1d600f753d01e3848d57b381395ad2b43a58c40ee3d951036d93456836")
                        .unwrap(),
                    RecoveryId::from_i32(0).unwrap()
                )
                .unwrap(),
                timestamp: 1234,
                version: "1".to_string(),
                hash: "deadbeef".to_string()
            }
        );
        assert_eq!(signature.timestamp, 1234);
        assert_eq!(signature.version, "1");
        assert_eq!(signature.hash, "deadbeef");
    }

    #[test]
    fn test_signature_verify() {
        let (private, public) = secp256k1::generate_keypair(&mut rand::thread_rng());
        let public = PublicKey::from_secp256k1_key(&public).unwrap();

        let body = r#"{ "message": "hello world" }"#;
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            .to_string();

        let message_content = format!("{timestamp}.{body}");
        let message_content_length = message_content.len().to_string();

        let message_parts = &[
            "\u{19}Ethereum Signed Message:\n".as_bytes(),
            message_content_length.as_bytes(),
            message_content.as_bytes(),
        ];

        let mut hasher = sha3::Keccak256::new();
        for part in message_parts {
            hasher.update(part);
        }

        let message_hash = hasher.finalize();
        let message = secp256k1::Message::from_slice(&message_hash).unwrap();

        let sig = secp256k1::global::SECP256K1.sign_ecdsa_recoverable(&message, &private);

        let signature = Signature {
            public_key: Some(public.clone()),
            sig,
            timestamp: timestamp.parse().unwrap(),
            version: "0".to_owned(),
            hash: "eth-personal-sign".to_owned(),
        };

        assert_eq!(signature.verify(body.as_bytes()).unwrap(), public);
    }
}
