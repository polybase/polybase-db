use super::Server;
use serde_json::json;

#[tokio::test]
async fn change_collection_ref_to_object() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;

    constructor (id: string) {
        this.id = id;
    }
}"#;

    let col = server
        .create_collection_untyped("ns/Account", schema, None)
        .await
        .unwrap();

    col.create(json!(["id1"]), None).await.unwrap();

    let schema_with_col_ref = r#"
@public
collection Account {
    id: string;
    x: SomeCol;

    constructor (id: string) {
        this.id = id;
    }
}
"#;

    let col = server
        .update_collection_untyped("ns/Account", schema_with_col_ref, None)
        .await
        .unwrap();

    assert_eq!(
        col.get("id1", None).await.unwrap(),
        json!({
            "id": "id1",
            "x": {
                "collectionId": "",
                "id": "",
            },
        })
    );

    let schema_with_object = r#"
@public
collection Account {
    id: string;
    x: {
        name: string;
    };

    constructor (id: string) {
        this.id = id;
    }
}"#;

    let col = server
        .update_collection_untyped("ns/Account", schema_with_object, None)
        .await
        .unwrap();

    assert_eq!(
        col.get("id1", None).await.unwrap(),
        json!({
            "id": "id1",
            "x": {
                "name": "",
            },
        })
    );
}
