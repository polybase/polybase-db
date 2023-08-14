use serde_json::json;

use crate::api::{Error, ErrorData, Server};

#[tokio::test]
async fn calling_other_collection_functions() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    balance: number;

    constructor (id: string, balance: number) {
        this.id = id;
        this.balance = balance;
    }

    emptyOtherAccount(otherAccount: OtherAccountCol) {
        this.balance = this.balance + otherAccount.balance;
        otherAccount.balance = 0;
    }
}

@public
collection OtherAccountCol {
    id: string;
    balance: number;

    constructor (id: string, balance: number) {
        this.id = id;
        this.balance = balance;
    }
}
    "#;

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        balance: f64,
    }

    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct OtherAccountCol {
        id: String,
        balance: f64,
    }

    let account_collection = server
        .create_collection::<Account>("test/Account", schema, None)
        .await
        .unwrap();

    let other_collection = server
        .create_collection::<OtherAccountCol>("test/OtherAccountCol", schema, None)
        .await
        .unwrap();

    assert_eq!(
        account_collection
            .create(json!(["id1", 10.0]), None)
            .await
            .unwrap(),
        Account {
            id: "id1".to_string(),
            balance: 10.0,
        }
    );

    assert_eq!(
        other_collection
            .create(json!(["id1", 123.0]), None)
            .await
            .unwrap(),
        OtherAccountCol {
            id: "id1".to_string(),
            balance: 123.0,
        }
    );

    assert_eq!(
        account_collection
            .call(
                "id1",
                "emptyOtherAccount",
                json!([{"collectionId": &other_collection.id, "id": "id1"}]),
                None
            )
            .await
            .unwrap()
            .unwrap(),
        Account {
            id: "id1".to_string(),
            balance: 133.0,
        }
    );

    assert_eq!(
        other_collection.get("id1", None).await.unwrap(),
        OtherAccountCol {
            id: "id1".to_string(),
            balance: 0.0,
        }
    );

    assert_eq!(
        account_collection
            .call(
                "id1",
                "emptyOtherAccount",
                // This should fail because the collectionId is not the id of OtherCollection
                json!([{"collectionId": &account_collection.id, "id": "id1"}]),
                None
            )
            .await
            .unwrap_err(),
        Error {
            error: ErrorData {
                code: "invalid-argument".to_string(),
                reason: "function/invalid-args".to_string(),
                message: r#"invalid argument type for parameter "otherAccount": foreign record reference has incorrect collection id, expected: "OtherAccountCol", got: "Account""#.to_string(),
            },
        },
    );
}
