use crate::api::{ListQuery, Server, Signature, Signer};
use serde_json::json;
use std::time::SystemTime;

#[tokio::test]
async fn schema_index_update() {
    let server: std::sync::Arc<Server> = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    hello: string;
    active: boolean;

    constructor (id: string) {
        this.id = id;
        this.hello = "hello";
        this.active = true;
    }
}
    "#;

    let schema2 = r#"
@public
collection Account {
    id: string;
    hello: string;
    active: boolean;

    @index(hello, active);

    constructor (id: string) {
        this.id = id;
        this.hello = "hello";
        this.active = true;
    }
}
    "#;

    let (private_key, public_key) = secp256k1::generate_keypair(&mut rand::thread_rng());
    let public_key = indexer_db_adaptor::publickey::PublicKey::from_secp256k1_key(&public_key).unwrap();
    let signer = Signer::from(move |body: &str| {
        let mut signature = Signature::create(&private_key, SystemTime::now(), body);
        signature.public_key = Some(public_key.clone());
        signature
    });

    #[derive(Debug, PartialEq, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        hello: String,
        active: bool,
    }

    let collection = server
        // use signer, so we can update the collection
        .create_collection::<Account>("ns/Account", schema, Some(&signer))
        .await
        .unwrap();

    collection.create(json!(["id1"]), None).await.unwrap();

    let res = collection
        .list(
            ListQuery {
                where_query: Some(json!({
                    "hello": "hello",
                })),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.data.len(), 1);

    let create = collection.create(json!(["id2"]), None);
    let update_schema = server.update_collection::<Account>("ns/Account", schema2, Some(&signer));

    let (_, _) = tokio::join!(create, update_schema);

    let res = collection
        .list(
            ListQuery {
                where_query: Some(json!({
                    "hello": "hello",
                    "active": true,
                })),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(res.data.len(), 2);
}
