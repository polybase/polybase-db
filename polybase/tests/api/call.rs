use serde_json::json;

use crate::api::Server;

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

    let server = Server::setup_and_wait().await;

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
