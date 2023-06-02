use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn collection_boolean_field() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    name: string;
    isActive: boolean;

    @index(name, isActive);

    constructor (id: string, name: string, isActive: boolean) {
        this.id = id;
        this.name = name;
        this.isActive = isActive;
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        name: String,
        is_active: bool,
    }

    let collection = server
        .create_collection::<Account>("ns/Account", schema, None)
        .await
        .unwrap();

    let account_id1_john_true = collection
        .create(json!(["id1", "John", true]), None)
        .await
        .unwrap();

    assert_eq!(
        account_id1_john_true,
        Account {
            id: "id1".to_string(),
            name: "John".to_string(),
            is_active: true,
        }
    );

    let account_id2_john_false = collection
        .create(json!(["id2", "John", false]), None)
        .await
        .unwrap();

    assert_eq!(
        account_id2_john_false,
        Account {
            id: "id2".to_string(),
            name: "John".to_string(),
            is_active: false,
        }
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    where_query: Some(json!({ "name": "John", "isActive": true })),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![account_id1_john_true.clone()]
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    where_query: Some(json!({ "name": "John", "isActive": false })),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![account_id2_john_false.clone()]
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    sort: Some(json!([["isActive", "desc"]])),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![
            account_id1_john_true.clone(),
            account_id2_john_false.clone()
        ]
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    sort: Some(json!([["isActive", "asc"]])),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![
            account_id2_john_false.clone(),
            account_id1_john_true.clone()
        ]
    );
}
