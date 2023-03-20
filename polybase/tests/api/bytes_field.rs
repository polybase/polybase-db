use base64::Engine;
use serde_json::json;

use crate::api::{Error, ErrorData, Server};

#[tokio::test]
async fn bytes_field() {
    let server = Server::setup_and_wait().await;

    let schema = r#"
@public
collection Account {
    id: string;
    data: bytes;

    constructor (id: string, data: bytes) {
        this.id = id;
        this.data = data;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        data: String,
    }

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let hello_base64 = base64::engine::general_purpose::STANDARD.encode(b"hello");
    assert_eq!(
        collection
            .create(json!(["id1", hello_base64.clone()]), None)
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            data: hello_base64.clone(),
        }
    );

    assert_eq!(
        collection.get("id1", None).await.unwrap(),
        Account {
            id: "id1".to_string(),
            data: hello_base64.clone(),
        }
    );

    // Fails if given invalid base64
    assert_eq!(
        collection
            .create(json!(["id2", "hello"]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "function/invalid-args".to_string(),
                message: r#"invalid argument type for parameter "data": base64 decode error"#
                    .to_string(),
            },
        },
    );
}
