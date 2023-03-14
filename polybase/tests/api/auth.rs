use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::api::{Error, ErrorData, ForeignRecordReference, ListQuery, Signature, Signer};

use super::Server;

#[tokio::test]
async fn get_slash_signature_is_not_required() {
    let server = Server::setup_and_wait().await;

    let res = server
        .client
        .get(server.base_url.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(res.status(), 200);
}

#[tokio::test]
async fn get_collection_collection_records_with_invalid_signature_different_public_key() {
    let server = Server::setup_and_wait().await;

    let (private_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());

    let (_, another_public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let another_public_key = indexer::PublicKey::from_secp256k1_key(&another_public_key).unwrap();

    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(another_public_key.clone());
        signature
    });

    let res = server
        .list_records::<serde_json::Value>("Collection", ListQuery::default(), Some(&signer))
        .await
        .unwrap_err();

    assert_eq!(
        res,
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                message: "public key does not match key recovered from signature".to_string(),
                reason: "auth/invalid-signature".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn public_key_is_optional() {
    let server = Server::setup_and_wait().await;

    let (private_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = None;
        signature
    });

    let res = server
        .list_records::<serde_json::Value>("Collection", ListQuery::default(), Some(&signer))
        .await
        .unwrap();

    assert_eq!(res.data.len(), 0);
}

#[tokio::test]
async fn collection_with_auth() {
    let schema = r#"
@public
collection People {
    id: string; 
    name?: string; 
    publicKey?: PublicKey;

    constructor (id: string, name: string) {
        this.id = id;
        this.name = name;
        if (ctx.publicKey) this.publicKey = ctx.publicKey;
    }

    update (name: string) {
        if (this.publicKey != ctx.publicKey) {
            error('invalid owner');
        }

        this.name = name;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct People {
        id: String,
        name: Option<String>,
        public_key: Option<indexer::PublicKey>,
    }

    let server = Server::setup_and_wait().await;

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let signer =
        Signer::from(move |body: &str| Signature::create(&private_key, SystemTime::now(), body));

    let collection = server
        .create_collection::<People>("test/People", schema, Some(&signer))
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(json!(["0", "John"]), Some(&signer))
            .await
            .unwrap(),
        People {
            id: "0".to_string(),
            name: Some("John".to_string()),
            public_key: Some(public_key.clone()),
        }
    );

    assert_eq!(
        collection.get("0", None).await.unwrap(),
        People {
            id: "0".to_string(),
            name: Some("John".to_string()),
            public_key: Some(public_key.clone()),
        }
    );

    // Try to update record 0 with a different (unauthorized) key
    let (another_private_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let another_signer =
        move |body: &str| Signature::create(&another_private_key, SystemTime::now(), body);

    assert_eq!(
        collection
            .call("0", "update", json!(["Tom"]), Some(&another_signer.into()))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "failed-precondition".to_string(),
                reason: "function/collection-error".to_string(),
                message: "collection function error: invalid owner".to_string(),
            }
        }
    );

    assert_eq!(
        collection.get("0", None).await.unwrap(),
        People {
            id: "0".to_string(),
            name: Some("John".to_string()),
            public_key: Some(public_key.clone()),
        }
    );

    // Update record 0 with the authorized key
    assert_eq!(
        collection
            .call("0", "update", json!(["Tom"]), Some(&signer))
            .await
            .unwrap()
            .unwrap(),
        People {
            id: "0".to_string(),
            name: Some("Tom".to_string()),
            public_key: Some(public_key.clone()),
        }
    );

    assert_eq!(
        collection.get("0", None).await.unwrap(),
        People {
            id: "0".to_string(),
            name: Some("Tom".to_string()),
            public_key: Some(public_key.clone()),
        }
    );
}

#[tokio::test]
async fn read_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
collection Account {
    id: string;
    balance: number;
    @read
    owner: PublicKey;

    constructor (id: string, balance: number) {
        this.id = id;
        this.balance = balance;

        if (ctx.publicKey) {
            this.owner = ctx.publicKey;
        } else {
            error('no public key');
        }
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
        owner: Option<indexer::PublicKey>,
    }

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let signer =
        Signer::from(move |body: &str| Signature::create(&private_key, SystemTime::now(), body));

    let collection = server
        .create_collection::<Account>("test/Account", schema, Some(&signer))
        .await
        .unwrap();

    let account_id1_10 = collection
        .create(json!(["id1", 10]), Some(&signer))
        .await
        .unwrap();

    assert_eq!(
        account_id1_10,
        Account {
            id: "id1".to_string(),
            balance: 10.0,
            owner: Some(indexer::PublicKey::from_secp256k1_key(&public_key).unwrap()),
        }
    );

    // Trying to get the record with the same key succeeds
    assert_eq!(
        collection.get("id1", Some(&signer)).await.unwrap(),
        account_id1_10
    );

    let (another_private_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let another_signer = Signer::from(move |body: &str| {
        Signature::create(&another_private_key, SystemTime::now(), body)
    });

    // Trying to get the record with a different key fails
    assert_eq!(
        collection
            .get("id1", Some(&another_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // Trying to get the record without auth fails
    assert_eq!(
        collection.get("id1", None).await.unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // Listing records with the same key succeeds
    assert_eq!(
        collection
            .list(ListQuery::default(), Some(&signer))
            .await
            .unwrap()
            .into_record_data(),
        vec![account_id1_10.clone()]
    );

    // Listing records with a different key returns 0 records
    assert_eq!(
        collection
            .list(ListQuery::default(), Some(&another_signer))
            .await
            .unwrap()
            .into_record_data(),
        vec![]
    );
}

#[tokio::test]
async fn call_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
collection Account {
    id: string;
    balance: number;
    @read
    manager: PublicKey;
    @read
    owner: PublicKey;

    constructor (id: string, balance: number, manager: PublicKey) {
        this.id = id;
        this.balance = balance;
        this.manager = manager;

        if (ctx.publicKey) {
            this.owner = ctx.publicKey;
        } else {
            error('no public key');
        }
    }

    @call(manager)
    reset () {
        this.balance = 0;
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
        manager: indexer::PublicKey,
        owner: indexer::PublicKey,
    }

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let (owner_private_key, owner_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let owner_public_key = indexer::PublicKey::from_secp256k1_key(&owner_public_key).unwrap();
    let owner_signer = Signer::from(move |body: &str| {
        Signature::create(&owner_private_key, SystemTime::now(), body)
    });

    let (manager_private_key, manager_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let manager_public_key = indexer::PublicKey::from_secp256k1_key(&manager_public_key).unwrap();
    let manager_signer = Signer::from(move |body: &str| {
        Signature::create(&manager_private_key, SystemTime::now(), body)
    });

    let account_id1_10 = collection
        .create(
            json!(["id1", 10, manager_public_key.clone()]),
            Some(&owner_signer),
        )
        .await
        .unwrap();

    assert_eq!(
        account_id1_10,
        Account {
            id: "id1".to_string(),
            balance: 10.0,
            manager: manager_public_key.clone(),
            owner: owner_public_key.clone(),
        }
    );

    // Calling reset with a non-manager (owner) key fails
    assert_eq!(
        collection
            .call("id1", "reset", json!([]), Some(&owner_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "you do not have permission to call this function".to_string(),
            }
        }
    );

    // Fails with a non-manager, non-owner (no read access) key
    let (no_access_private_key, _) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let no_access_signer = Signer::from(move |body: &str| {
        Signature::create(&no_access_private_key, SystemTime::now(), body)
    });

    assert_eq!(
        collection
            .call("id1", "reset", json!([]), Some(&no_access_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // Fails with no key
    assert_eq!(
        collection
            .call("id1", "reset", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // Make sure account hasn't changed
    assert_eq!(
        collection.get("id1", Some(&owner_signer)).await.unwrap(),
        account_id1_10
    );

    // Calling reset with a manager key succeeds
    let account_id1_0 = {
        let mut account_id1_10 = account_id1_10;
        account_id1_10.balance = 0.0;
        account_id1_10
    };

    assert_eq!(
        collection
            .call("id1", "reset", json!([]), Some(&manager_signer))
            .await
            .unwrap()
            .unwrap(),
        account_id1_0
    );

    assert_eq!(
        collection.get("id1", Some(&owner_signer)).await.unwrap(),
        account_id1_0
    );
}

#[tokio::test]
async fn delegate_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
collection Account {
    id: string;
    balance: number;
    @read
    owner: User;
    @read
    reader: User;

    constructor (id: string, balance: number, owner: User, reader: User) {
        this.id = id;
        this.balance = balance;
        this.owner = owner;
        this.reader = reader;
    }

    @call(owner)
    reset () {
        this.balance = 0;
    }
}

@read
collection User {
    id: string;
    name: string;
    @delegate
    pk: PublicKey;

    constructor (id: string, name: string) {
        this.id = id;
        this.name = name;
        this.pk = ctx.publicKey;
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
        owner: ForeignRecordReference,
        reader: ForeignRecordReference,
    }

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct User {
        id: String,
        name: String,
        pk: indexer::PublicKey,
    }

    let user_collection = server
        .create_collection::<User>("test/User", schema, None)
        .await
        .unwrap();

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let (owner_private_key, owner_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());

    let owner_public_key = indexer::PublicKey::from_secp256k1_key(&owner_public_key).unwrap();
    let owner_signer = Signer::from(move |body: &str| {
        Signature::create(&owner_private_key, SystemTime::now(), body)
    });

    let (reader_private_key, reader_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let reader_public_key = indexer::PublicKey::from_secp256k1_key(&reader_public_key).unwrap();
    let reader_signer = Signer::from(move |body: &str| {
        Signature::create(&reader_private_key, SystemTime::now(), body)
    });

    let user_id1_john = user_collection
        .create(json!(["id1", "John"]), Some(&owner_signer))
        .await
        .unwrap();

    assert_eq!(
        user_id1_john,
        User {
            id: "id1".to_string(),
            name: "John".to_string(),
            pk: owner_public_key.clone(),
        }
    );

    let user_id2_tom = user_collection
        .create(json!(["id2", "Tom"]), Some(&reader_signer))
        .await
        .unwrap();

    assert_eq!(
        user_id2_tom,
        User {
            id: "id2".to_string(),
            name: "Tom".to_string(),
            pk: reader_public_key.clone(),
        }
    );

    let account_id1_10_user_id1 = account_collection
        .create(
            json!([
                "id1",
                10,
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: user_id1_john.id.clone(),
                },
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: user_id2_tom.id.clone(),
                }
            ]),
            Some(&owner_signer),
        )
        .await
        .unwrap();

    assert_eq!(
        account_id1_10_user_id1,
        Account {
            id: "id1".to_string(),
            balance: 10.0,
            owner: ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: user_id1_john.id.clone(),
            },
            reader: ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: user_id2_tom.id.clone(),
            }
        }
    );

    // Reset call fails with a non-owner (reader) key
    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), Some(&reader_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "you do not have permission to call this function".to_string(),
            }
        }
    );

    // Reset call succeeds with an owner key
    let account_id1_0_user_id1 = {
        let mut account_id1_10_user_id1 = account_id1_10_user_id1;
        account_id1_10_user_id1.balance = 0.0;
        account_id1_10_user_id1
    };

    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), Some(&owner_signer))
            .await
            .unwrap()
            .unwrap(),
        account_id1_0_user_id1
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_0_user_id1
    );
}

#[tokio::test]
async fn collection_read_any_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
// Anyone can read this collection, but not call it's methods
@read
collection Account {
    id: string;

    constructor (id: string) {
        this.id = id;
    }

    reset () {
        this.balance = 0;
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        account_collection
            .create(json!(["id1"]), None)
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
        }
    );

    assert_eq!(
        account_collection.get("id1", None).await.unwrap(),
        Account {
            id: "id1".to_string(),
        }
    );

    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "you do not have permission to call this function".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn collection_call_any_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
// Anyone can call this collection's methods, but not read it
// In practice, this allows anyone with @read access to call any method in the collection,
// unless the method has a @call directive
@call
collection Account {
    id: string;
    balance: number;
    @read
    owner: PublicKey;
    @read
    manager: PublicKey;

    constructor (id: string, balance: number, manager: PublicKey) {
        this.id = id;
        this.balance = balance;
        this.owner = ctx.publicKey;
        this.manager = manager;
    }

    // Only manager can call this method
    @call(manager)
    reset() {
        this.balance = 0;
    }

    // Both manager and owner can call this method,
    // because they have @read access to the record,
    // and the method has no @call directive
    setBalanceToOne() {
        this.balance = 1;
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
        owner: indexer::PublicKey,
        manager: indexer::PublicKey,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let (owner_private_key, owner_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let owner_public_key = indexer::PublicKey::from_secp256k1_key(&owner_public_key).unwrap();
    let owner_signer = Signer::from(move |body: &str| {
        Signature::create(&owner_private_key, SystemTime::now(), body)
    });

    let (manager_private_key, manager_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let manager_public_key = indexer::PublicKey::from_secp256k1_key(&manager_public_key).unwrap();
    let manager_signer = Signer::from(move |body: &str| {
        Signature::create(&manager_private_key, SystemTime::now(), body)
    });

    let account_id1_10 = account_collection
        .create(
            json!(["id1", 10, manager_public_key.clone()]),
            Some(&owner_signer),
        )
        .await
        .unwrap();

    assert_eq!(
        account_id1_10,
        Account {
            id: "id1".to_string(),
            balance: 10.0,
            owner: owner_public_key.clone(),
            manager: manager_public_key.clone(),
        }
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_10
    );

    // reset call fails without read permission to the record (no auth)
    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // reset call fails with a non-manager (owner) key
    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), Some(&owner_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "you do not have permission to call this function".to_string(),
            }
        }
    );

    // Make sure record is unchanged
    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_10
    );

    // reset call succeeds with a manager key
    let account_id1_0 = Account {
        id: "id1".to_string(),
        balance: 0.0,
        owner: owner_public_key.clone(),
        manager: manager_public_key.clone(),
    };
    assert_eq!(
        account_collection
            .call("id1", "reset", json!([]), Some(&manager_signer))
            .await
            .unwrap()
            .unwrap(),
        account_id1_0
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_0
    );

    // setBalanceToOne call fails without read permission to the record (no auth)
    assert_eq!(
        account_collection
            .call("id1", "setBalanceToOne", json!([]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_0
    );

    // setBalanceToOne call succeeds with a manager key
    let account_id1_1 = Account {
        id: "id1".to_string(),
        balance: 1.0,
        owner: owner_public_key.clone(),
        manager: manager_public_key.clone(),
    };

    assert_eq!(
        account_collection
            .call("id1", "setBalanceToOne", json!([]), Some(&manager_signer))
            .await
            .unwrap()
            .unwrap(),
        account_id1_1
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_1
    );

    // setBalanceToOne call succeeds with an owner key
    assert_eq!(
        account_collection
            .call("id1", "setBalanceToOne", json!([]), Some(&owner_signer))
            .await
            .unwrap()
            .unwrap(),
        account_id1_1
    );

    assert_eq!(
        account_collection
            .get("id1", Some(&owner_signer))
            .await
            .unwrap(),
        account_id1_1
    );
}

#[tokio::test]
async fn array_of_records_auth() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
collection Account {
    id: string;
    @read
    managers: Manager[];

    constructor (id: string, managers: Manager[]) {
        this.id = id;
        this.managers = managers;
    }
}

@read
collection Manager {
    id: string;
    @delegate
    publicKey: PublicKey;

    constructor (id: string) {
        this.id = id;
        this.publicKey = ctx.publicKey;
    }
}    
    "#;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        managers: Vec<ForeignRecordReference>,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Manager {
        id: String,
        public_key: indexer::PublicKey,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let manager_collection = server
        .create_collection::<Manager>("test/Manager", schema, None)
        .await
        .unwrap();

    let (manager_1_private_key, _manager_1_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let (manager_2_private_key, _manager_2_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());
    let (manager_3_private_key, _manager_3_public_key) =
        secp256k1::generate_keypair(&mut rand::thread_rng());

    let manager_1_signer = Signer::from(move |body: &str| {
        Signature::create(&manager_1_private_key, SystemTime::now(), body)
    });
    let manager_2_signer = Signer::from(move |body: &str| {
        Signature::create(&manager_2_private_key, SystemTime::now(), body)
    });
    let manager_3_signer = Signer::from(move |body: &str| {
        Signature::create(&manager_3_private_key, SystemTime::now(), body)
    });

    manager_collection
        .create(json!(["manager1"]), Some(&manager_1_signer))
        .await
        .unwrap();

    manager_collection
        .create(json!(["manager2"]), Some(&manager_2_signer))
        .await
        .unwrap();

    manager_collection
        .create(json!(["manager3"]), Some(&manager_3_signer))
        .await
        .unwrap();

    let account_1 = account_collection
        .create(
            json!([
                "account1",
                [
                    ForeignRecordReference {
                        collection_id: manager_collection.id.clone(),
                        id: "manager1".to_string(),
                    },
                    ForeignRecordReference {
                        collection_id: manager_collection.id.clone(),
                        id: "manager2".to_string(),
                    },
                ]
            ]),
            None,
        )
        .await
        .unwrap();

    assert_eq!(
        account_1,
        Account {
            id: "account1".to_string(),
            managers: vec![
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "manager1".to_string(),
                },
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "manager2".to_string(),
                },
            ]
        }
    );

    // get succeeds with manager key 1
    assert_eq!(
        account_collection
            .get("account1", Some(&manager_1_signer))
            .await
            .unwrap(),
        account_1
    );

    // get succeeds with manager key 2
    assert_eq!(
        account_collection
            .get("account1", Some(&manager_2_signer))
            .await
            .unwrap(),
        account_1
    );

    // get fails with manager key 3
    assert_eq!(
        account_collection
            .get("account1", Some(&manager_3_signer))
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );

    // get fails with no key
    assert_eq!(
        account_collection.get("account1", None).await.unwrap_err(),
        Error {
            error: ErrorData {
                code: "permission-denied".to_string(),
                reason: "unauthorized".to_string(),
                message: "unauthorized read".to_string(),
            }
        }
    );
}
