use crate::api::{Error, ErrorData, Signature, Signer};
use std::time::SystemTime;

use super::{Server, ServerConfig};

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Account {
    id: String,
}

#[tokio::test]
async fn valid_pk() {
    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let whitelist = Some(vec![public_key.to_hex().unwrap()]);

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    let server = Server::setup_and_wait(Some(ServerConfig {
        whitelist,
        ..Default::default()
    }))
    .await;

    let res = server
        .create_collection::<Account>("test/Account", schema, Some(&signer))
        .await
        .unwrap();

    assert_eq!(res.id, "test/Account".to_string(),);
}

#[tokio::test]
async fn mismatch_pk() {
    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    // Key signing the request
    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();

    // Key to be used in whitelist
    let (_, alt_public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let alt_public_key = indexer::PublicKey::from_secp256k1_key(&alt_public_key).unwrap();
    let whitelist = Some(vec![alt_public_key.to_hex().unwrap()]);

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    let server = Server::setup_and_wait(Some(ServerConfig {
        whitelist,
        ..Default::default()
    }))
    .await;

    let res = server
        .create_collection::<Account>("test/Account", schema, Some(&signer))
        .await
        .unwrap_err();

    assert_eq!(
        res,
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                message: "public key not included in allowed whitelist".to_string(),
                reason: "unauthorized".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn missing_pk() {
    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    // Key signing the request
    let (_, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let whitelist = Some(vec![public_key.to_hex().unwrap()]);

    let server = Server::setup_and_wait(Some(ServerConfig {
        whitelist,
        ..Default::default()
    }))
    .await;

    let res = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap_err();

    assert_eq!(
        res,
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                message: "public key not included in allowed whitelist".to_string(),
                reason: "unauthorized".to_string(),
            }
        }
    );
}
