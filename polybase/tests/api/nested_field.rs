use serde_json::json;

use crate::api::{ErrorData, ListQuery, Server};

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
async fn nested_field_extranous_fields() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    info: {
        name: string;
    };

    constructor (id: string, info: map<string, string>) {
        this.id = id;
        this.info = info;
    }
}"#;

    let col = server
        .create_collection_untyped("test/Account", schema, None)
        .await
        .unwrap();

    let err = col
        .create(
            json!(["id1", {
                "name": "John",
                "surname": "Doe",
                "age": "30",
            }]),
            None,
        )
        .await
        .unwrap_err();

    assert_eq!(err.error.code, "invalid-argument");
    assert_eq!(err.error.reason, "record/invalid-field");
    assert!(matches!(
        err.error.message.as_str(),
        "unexpected fields: info.age, info.surname" | "unexpected fields: info.surname, info.age"
    ));
}
