use serde_json::json;

use crate::api::{Error, ErrorData, Server};

#[tokio::test]
async fn call() {
    let schema = r#"
@public
collection Account {
    id: string;
    balance: number;

    constructor (id: string, balance: number) {
        this.id = id;
        this.balance = balance;
    }

    function transfer(b: record, amount: number) {
        this.balance -= amount;
        b.balance += amount;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
    }

    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection.create(json!(["0", 10.0]), None).await.unwrap(),
        Account {
            id: "0".to_string(),
            balance: 10.0,
        }
    );

    assert_eq!(
        collection.create(json!(["1", 100.0]), None).await.unwrap(),
        Account {
            id: "1".to_string(),
            balance: 100.0,
        }
    );

    assert_eq!(
        collection
            .call("0", "transfer", json!([{"id": "1"}, 5.0]), None)
            .await
            .unwrap()
            .unwrap(),
        Account {
            id: "0".to_string(),
            balance: 5.0,
        },
    );

    assert_eq!(
        collection.get("1", None).await.unwrap(),
        Account {
            id: "1".to_string(),
            balance: 105.0,
        },
    );
}

#[tokio::test]
async fn with_optional_parameters() {
    let schema = r#"
@public
collection Account {
    id: string;
    name?: string;

    constructor (id: string, name?: string) {
        this.id = id;
        this.name = name;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        name: Option<String>,
    }

    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    assert_eq!(
        collection.create(json!(["0"]), None).await.unwrap(),
        Account {
            id: "0".to_string(),
            name: None,
        }
    );

    assert_eq!(
        collection
            .create(json!(["1", "Alice"]), None)
            .await
            .unwrap(),
        Account {
            id: "1".to_string(),
            name: Some("Alice".to_string()),
        }
    );

    // Fails with 0 arguments
    assert_eq!(
        collection.create(json!([]), None).await.unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "function/invalid-args".to_string(),
                message: "incorrect number of arguments, expected 1, got 0".to_string(),
            }
        }
    );

    // Fails with 3 arguments (one extra)
    assert_eq!(
        collection
            .create(json!(["2", "Bob", "extra"]), None)
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "function/invalid-args".to_string(),
                message: "incorrect number of arguments, expected 2, got 3".to_string(),
            }
        }
    );
}

#[tokio::test]
async fn timeouts_after_5_seconds() {
    let schema = r#"
@public
collection Account {
    id: string;
    balance: number;

    constructor (id: string, balance: number) {
        while (true) {}
    }
}
    "#;

    let server = Server::setup_and_wait(None).await;

    let collection = server
        .create_collection::<serde_json::Value>("test/Account", schema, None)
        .await
        .unwrap();

    let result = collection.create(json!(["0", 10.0]), None).await;

    assert_eq!(
        result.unwrap_err(),
        Error {
            error: ErrorData {
                code: "failed-precondition".to_string(),
                reason: "function/javascript-exception".to_string(),
                message: "function timed out".to_string(),
            }
        }
    );
}
