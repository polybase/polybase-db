use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn collection_nested_field() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
@public
collection Account {
    id: string;
    info: {
        name: string;
    };

    @index(info.name);

    constructor (id: string, name: string) {
        this.id = id;
        this.info = { name: name };
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        info: Info,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Info {
        name: String,
    }

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(json!(["id1", "John"]), None)
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            info: Info {
                name: "John".to_string(),
            },
        }
    );

    assert_eq!(
        collection
            .create(json!(["id2", "Tom"]), None)
            .await
            .unwrap(),
        Account {
            id: "id2".to_string(),
            info: Info {
                name: "Tom".to_string(),
            },
        }
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    where_query: Some(json!({
                        "info.name": "John",
                    })),
                    ..Default::default()
                },
                None
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![Account {
            id: "id1".to_string(),
            info: Info {
                name: "John".to_string(),
            },
        }]
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    sort: Some(json!([["info.name", "desc"]])),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![
            Account {
                id: "id2".to_string(),
                info: Info {
                    name: "Tom".to_string(),
                },
            },
            Account {
                id: "id1".to_string(),
                info: Info {
                    name: "John".to_string(),
                },
            },
        ]
    );

    assert_eq!(
        collection
            .list(
                ListQuery {
                    sort: Some(json!([["info.name", "asc"]])),
                    ..Default::default()
                },
                None,
            )
            .await
            .unwrap()
            .into_record_data(),
        vec![
            Account {
                id: "id1".to_string(),
                info: Info {
                    name: "John".to_string(),
                },
            },
            Account {
                id: "id2".to_string(),
                info: Info {
                    name: "Tom".to_string(),
                },
            },
        ]
    );
}
