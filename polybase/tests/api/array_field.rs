use serde_json::json;

use crate::api::{ForeignRecordReference, Server};

#[tokio::test]
async fn collection_array_field() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
@public
collection Account {
    id: string;
    friends: string[];

    constructor (id: string, friends: string[]) {
        this.id = id;
        this.friends = friends;
    }

    addFriend(friend: string) {
        this.friends.push(friend);
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        friends: Vec<String>,
    }

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(json!(["id1", ["id2", "id3"]]), None)
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            friends: vec!["id2".to_string(), "id3".to_string()],
        }
    );

    assert_eq!(
        collection
            .call("id1", "addFriend", json!(["id4"]), None)
            .await
            .unwrap()
            .unwrap(),
        Account {
            id: "id1".to_string(),
            friends: vec!["id2".to_string(), "id3".to_string(), "id4".to_string()],
        }
    );

    assert_eq!(
        collection.get("id1", None).await.unwrap(),
        Account {
            id: "id1".to_string(),
            friends: vec!["id2".to_string(), "id3".to_string(), "id4".to_string()],
        }
    );
}

#[tokio::test]
async fn array_of_records_field() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
@public
collection Account {
    id: string;
    managers: Manager[];

    constructor (id: string, managers: Manager[]) {
        this.id = id;
        this.managers = managers;
    }
}

@public
collection Manager {
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
    struct Manager {
        id: String,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let manager_collection = server
        .create_collection::<Manager>("test/Manager", schema, None)
        .await
        .unwrap();

    assert_eq!(
        manager_collection
            .create(json!(["id1"]), None)
            .await
            .unwrap(),
        Manager {
            id: "id1".to_string(),
        }
    );

    assert_eq!(
        manager_collection
            .create(json!(["id2"]), None)
            .await
            .unwrap(),
        Manager {
            id: "id2".to_string(),
        }
    );

    assert_eq!(
        account_collection
            .create(
                json!([
                    "id1",
                    [
                        ForeignRecordReference {
                            collection_id: manager_collection.id.clone(),
                            id: "id1".to_string()
                        },
                        ForeignRecordReference {
                            collection_id: manager_collection.id.clone(),
                            id: "id2".to_string()
                        }
                    ]
                ]),
                None
            )
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            managers: vec![
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "id1".to_string(),
                },
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "id2".to_string(),
                },
            ],
        }
    );

    assert_eq!(
        account_collection.get("id1", None).await.unwrap(),
        Account {
            id: "id1".to_string(),
            managers: vec![
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "id1".to_string(),
                },
                ForeignRecordReference {
                    collection_id: manager_collection.id.clone(),
                    id: "id2".to_string(),
                },
            ],
        }
    );
}
