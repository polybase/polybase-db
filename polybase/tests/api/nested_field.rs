use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn collection_nested_field() {
    let server = Server::setup_and_wait(None).await;

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

#[tokio::test]
async fn nested_field_extraneous_fields() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    info: {
        name?: string;
    };

    constructor (id: string, info: map<string, string>) {
        this.id = id;
        this.info = info;
    }
}"#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        info: Info,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    #[serde_with::skip_serializing_none]
    struct Info {
        name: Option<String>,
    }

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection
            .create(
                json!(["id1", {
                    "surname": "Doe",
                    "age": "30",
                }]),
                None
            )
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            info: Info { name: None },
        }
    );
}
