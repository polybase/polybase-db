use secp256k1::ecdsa::{RecoverableSignature, RecoveryId};
use sha3::Digest;
use std::{
    future::ready,
    time::{SystemTime, UNIX_EPOCH},
};

use actix_web::{http::header::CONTENT_LENGTH, FromRequest};
use futures::{future::LocalBoxFuture, StreamExt};
use indexer::PublicKey;
use serde::de::DeserializeOwned;

const TIME_TOLERANCE: u64 = 5 * 60; // 5 minutes

pub(crate) struct Auth {
    pub(crate) public_key: PublicKey,
}

impl From<Auth> for indexer::AuthUser {
    fn from(auth: Auth) -> Self {
        Self::new(auth.public_key)
    }
}

#[derive(Debug, PartialEq)]
struct Signature {
    public_key: Option<PublicKey>,
    sig: RecoverableSignature,
    timestamp: u64,
    version: String,
    hash: String,
}

impl Signature {
    // Deserialize parses the signature in the k=v,k2=v2,... format.
    fn deserialize(header_value: &str) -> Result<Self, String> {
        let mut public_key = None;
        let mut signature = None;
        let mut timestamp = None;
        let mut version = None;
        let mut hash = None;

        for kv in header_value.split(',') {
            let mut kv = kv.split('=');
            let k = kv.next().ok_or("missing key")?;
            let v = kv.next().ok_or("missing value")?;
            if kv.next().is_some() {
                return Err("too many values".to_string());
            }

            match k {
                "pk" => {
                    let original_v = v;
                    let v = v.trim_start_matches("0x");
                    if v == original_v {
                        return Err("public key must start with 0x".to_string());
                    }

                    let hex =
                        hex::decode(v).map_err(|e| format!("invalid public key hex: {e:?}"))?;
                    let pk = secp256k1::PublicKey::from_slice(&hex)
                        .map_err(|e| format!("invalid public key: {e:?}"))?;
                    let pk = PublicKey::from_secp256k1_key(&pk)
                        .map_err(|e| format!("failed to parse public key: {e:?}"))?;

                    public_key = Some(pk)
                }
                "sig" => {
                    let original_v = v;
                    let v = v.trim_start_matches("0x");
                    if v == original_v {
                        return Err("signature must start with 0x".to_string());
                    }

                    let hex = hex::decode(v).map_err(|_| "invalid signature hex")?;
                    if hex.len() != 65 {
                        return Err("invalid signature length".to_string());
                    }

                    let recoverable_signature = RecoverableSignature::from_compact(
                        &hex[0..64],
                        RecoveryId::from_i32(hex[64] as i32)
                            .map_err(|e| format!("invalid signature recovery id: {e:?}"))?,
                    )
                    .map_err(|e| format!("invalid compact signature: {e:?}"))?;

                    signature = Some(recoverable_signature)
                }
                "t" => {
                    // TODO: is v in seconds or milliseconds? check explorer signing message
                    let v: u64 = v.parse().map_err(|e| format!("invalid timestamp: {e:?}"))?;
                    timestamp = Some(v)
                }
                "v" => version = Some(v.to_string()),
                "h" => hash = Some(v.to_string()),
                _ => return Err("invalid key".to_string()),
            }
        }

        let signature = signature.ok_or("missing signature")?;
        let timestamp = timestamp.ok_or("missing timestamp")?;
        let version = version.ok_or("missing version")?;
        let hash = hash.ok_or("missing hash")?;

        Ok(Self {
            public_key,
            sig: signature,
            timestamp,
            version,
            hash,
        })
    }

    fn verify(
        &self,
        body: &[u8],
    ) -> Result<PublicKey, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let timestamp = self.timestamp.to_string();
        let timestamp_body_len = (timestamp.len() + 1 + body.len()).to_string();
        let message_parts = &[
            &[19][..],
            b"Ethereum Signed Message:\n",
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
            .recover(&secp256k1::Message::from_slice(&message_hash)?)?;
        let sig_pk = PublicKey::from_secp256k1_key(&sig_pk)?;

        if self.public_key.as_ref().map_or(false, |pk| *pk != sig_pk) {
            return Err("invalid signature".into());
        }

        Ok(sig_pk)
    }

    fn from_req(req: &actix_web::HttpRequest) -> Result<Option<Self>, actix_web::Error> {
        let Some(signature) = req
            .headers()
            .get("X-Polybase-Signature") else { return Ok(None); };

        let signature = signature
            .to_str()
            .map_err(|_| actix_web::error::ErrorBadRequest("invalid signature"))?;

        let signature = Signature::deserialize(signature)
            .map_err(|s| actix_web::error::ErrorBadRequest(format!("invalid signature: {s}")))?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        if signature.timestamp + TIME_TOLERANCE < now {
            return Err(actix_web::error::ErrorBadRequest("signature expired"));
        }

        Ok(Some(signature))
    }
}

pub(crate) struct SignedJSON<T: DeserializeOwned> {
    pub(crate) data: T,
    pub(crate) auth: Option<Auth>,
}

impl<T: DeserializeOwned + 'static> FromRequest for SignedJSON<T> {
    type Error = actix_web::Error;
    type Future = LocalBoxFuture<'static, Result<Self, Self::Error>>;

    fn from_request(
        req: &actix_web::HttpRequest,
        payload: &mut actix_web::dev::Payload,
    ) -> Self::Future {
        let sig = match Signature::from_req(req) {
            Ok(sig) => sig,
            Err(e) => return Box::pin(ready(Err(e))),
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
                body.extend_from_slice(&chunk?);
            }

            Ok(Self {
                data: serde_json::from_slice(if body.is_empty() { b"null" } else { &body })?,
                auth: sig
                    .map(|sig| {
                        if std::option_env!("DEV_SKIP_SIGNATURE_VERIFICATION") == Some("1") {
                            return Ok(Auth {
                                public_key: sig.public_key.unwrap(),
                            });
                        }

                        Ok::<_, actix_web::Error>(Auth {
                            public_key: sig.verify(&body).map_err(|e| {
                                actix_web::error::ErrorUnauthorized(format!(
                                    "failed to validate signature: {e}"
                                ))
                            })?,
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
}
