use serde_json::json;

use crate::api::{ForeignRecordReference, Server};

#[tokio::test]
async fn store_other_collection_records() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    balance: number;
    owner: User;
    managers: User[];

    constructor (id: string, balance: number, owner: User) {
        this.id = id;
        this.balance = balance;
        this.owner = owner;
        this.managers = [];
    }

    addManager (manager: User) {
        this.managers.push(manager);
    }
}

@public
collection User {
    id: string;
    name: string;

    constructor (id: string, name: string) {
        this.id = id;
        this.name = name;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
        owner: ForeignRecordReference,
        managers: Vec<ForeignRecordReference>,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct User {
        id: String,
        name: String,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let user_collection = server
        .create_collection::<User>("test/User", schema, None)
        .await
        .unwrap();

    assert_eq!(
        user_collection
            .create(json!(["id1", "John"]), None)
            .await
            .unwrap(),
        User {
            id: "id1".to_string(),
            name: "John".to_string(),
        }
    );

    assert_eq!(
        user_collection
            .create(json!(["id2", "Tom"]), None)
            .await
            .unwrap(),
        User {
            id: "id2".to_string(),
            name: "Tom".to_string(),
        }
    );

    assert_eq!(
        account_collection
            .create(
                json!(["id1", 100, { "collectionId": &user_collection.id, "id": "id1" }]),
                None
            )
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            balance: 100.0,
            owner: ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: "id1".to_string(),
            },
            managers: vec![],
        }
    );

    assert_eq!(
        account_collection
            .call(
                "id1",
                "addManager",
                json!([{ "collectionId": &user_collection.id, "id": "id2" }]),
                None
            )
            .await
            .unwrap()
            .unwrap(),
        Account {
            id: "id1".to_string(),
            balance: 100.0,
            owner: ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: "id1".to_string(),
            },
            managers: vec![ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: "id2".to_string(),
            }],
        }
    );
}

#[tokio::test]
async fn from_array_argument() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    managers: User[];

    constructor (id: string, managers: User[]) {
        this.id = id;
        this.managers = managers;
    }
}

@public
collection User {
    id: string;

    constructor (id: string) {
        this.id = id;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        managers: Vec<ForeignRecordReference>,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct User {
        id: String,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let user_collection = server
        .create_collection::<User>("test/User", schema, None)
        .await
        .unwrap();

    assert_eq!(
        user_collection.create(json!(["id1"]), None).await.unwrap(),
        User {
            id: "id1".to_string(),
        }
    );

    assert_eq!(
        user_collection.create(json!(["id2"]), None).await.unwrap(),
        User {
            id: "id2".to_string(),
        }
    );

    assert_eq!(
        account_collection
            .create(
                json!(["id1", [
                    { "collectionId": &user_collection.id, "id": "id1" },
                    { "collectionId": &user_collection.id, "id": "id2" },
                ]]),
                None
            )
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            managers: vec![
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: "id1".to_string(),
                },
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: "id2".to_string(),
                }
            ],
        }
    );
}
