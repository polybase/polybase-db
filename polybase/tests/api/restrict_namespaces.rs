use crate::api::{Error, ErrorData, Signature, Signer};
use std::time::SystemTime;

use super::{Server, ServerConfig};

#[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct Account {
    id: String,
}

#[tokio::test]
async fn valid() {
    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let pk_hex = public_key.to_hex().unwrap();

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    let server = Server::setup_and_wait(Some(ServerConfig {
        restrict_namespaces: true,
        ..Default::default()
    }))
    .await;

    let collection_id = format!("pk/{}/Account", pk_hex);
    let res = server
        .create_collection::<Account>(collection_id.as_str(), schema, Some(&signer))
        .await
        .unwrap();

    assert_eq!(res.id, collection_id.to_string());
}

#[tokio::test]
async fn invalid_prefix() {
    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let pk_hex = public_key.to_hex().unwrap();

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    let server = Server::setup_and_wait(Some(ServerConfig {
        restrict_namespaces: true,
        ..Default::default()
    }))
    .await;

    let collection_id = format!("other/{}/Account", pk_hex);

    let res = server
        .create_collection::<Account>(collection_id.as_str(), schema, Some(&signer))
        .await
        .unwrap_err();

    assert_eq!(
        res,
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                message: format!("namespace is invalid, must be in format pk/<public_key_hex>/<CollectionName> got {}", collection_id.as_str()),
                reason: "unauthorized".to_string(),
            }
        }
    );
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
    let pk_hex: String = alt_public_key.to_hex().unwrap();

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    let collection_id = format!("pk/{}/Account", pk_hex);
    let server = Server::setup_and_wait(Some(ServerConfig {
        restrict_namespaces: true,
        ..Default::default()
    }))
    .await;

    let res = server
        .create_collection::<Account>(collection_id.as_str(), schema, Some(&signer))
        .await
        .unwrap_err();

    assert_eq!(
        res,
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                message: format!("namespace is invalid, must be in format pk/<public_key_hex>/<CollectionName> got {}", collection_id.as_str()),
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

    let server = Server::setup_and_wait(Some(ServerConfig {
        restrict_namespaces: true,
        ..ServerConfig::default()
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
                message: "anonymous namespaces are not allowed, sign your request".to_string(),
                reason: "unauthorized".to_string(),
            }
        }
    );
}
