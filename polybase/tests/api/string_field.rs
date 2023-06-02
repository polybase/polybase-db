use serde_json::json;

use crate::api::{ListQuery, Server};

#[tokio::test]
async fn collection_string_field() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
    hello: string;
    world: boolean;

    @index(name, isActive);

    constructor (id: string) {
        this.id = id;
        this.hello = "hello";
        this.world = 'world';
    }
}
    "#;

    #[derive(Debug, PartialEq, Clone, serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Account {
        id: String,
        hello: String,
        world: String,
    }

    let collection = server
        .create_collection::<Account>("ns/Account", schema, None)
        .await
        .unwrap();

    let account_id1_john_true = collection.create(json!(["id1"]), None).await.unwrap();

    assert_eq!(
        account_id1_john_true,
        Account {
            id: "id1".to_string(),
            hello: "hello".to_string(),
            world: "world".to_string(),
        }
    );
}
