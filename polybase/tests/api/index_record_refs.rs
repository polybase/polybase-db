use serde_json::json;

use crate::api::{ForeignRecordReference, ListQuery, Server};

#[tokio::test]
async fn record_ref_index() {
    let schema = r#"
@public
collection Account {
    id: string;
    user?: User;

    @index(user);

    constructor (id: string, user?: User) {
        this.id = id;
        this.user = user;
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
    struct User {
        id: String,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        user: Option<ForeignRecordReference>,
    }

    let server = Server::setup_and_wait(None).await;

    let user_collection = server
        .create_collection::<User>("test/User", schema, None)
        .await
        .unwrap();

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let user = user_collection.create(json!(["0"]), None).await.unwrap();
    assert_eq!(
        user,
        User {
            id: "0".to_string(),
        }
    );

    let account = account_collection
        .create(
            json!([
                "0",
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: user.id.clone()
                }
            ]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        account,
        Account {
            id: "0".to_string(),
            user: Some(ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: user.id.clone()
            }),
        }
    );

    let user_2 = user_collection.create(json!(["1"]), None).await.unwrap();
    assert_eq!(
        user_2,
        User {
            id: "1".to_string(),
        }
    );

    let account_2 = account_collection
        .create(
            json!([
                "1",
                ForeignRecordReference {
                    collection_id: user_collection.id.clone(),
                    id: user_2.id.clone()
                }
            ]),
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        account_2,
        Account {
            id: "1".to_string(),
            user: Some(ForeignRecordReference {
                collection_id: user_collection.id.clone(),
                id: user_2.id.clone()
            }),
        }
    );

    let account_3 = account_collection.create(json!(["2"]), None).await.unwrap();
    assert_eq!(
        account_3,
        Account {
            id: "2".to_string(),
            user: None,
        }
    );

    assert_eq!(
        account_collection
            .list(
                ListQuery {
                    where_query: Some(
                        json!({"user": {"collectionId": &user_collection.id, "id": &user.id}}),
                    ),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![account]
    );

    assert_eq!(
        account_collection
            .list(
                ListQuery {
                    where_query: Some(
                        json!({"user": {"collectionId": &user_collection.id, "id": &user_2.id}}),
                    ),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![account_2]
    );
}
