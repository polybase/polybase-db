use crate::api::{ListQuery, Server};

#[tokio::test]
async fn collection_collection_records() {
    let server = Server::setup_and_wait(None).await;

    let schema = r#"
@public
collection Account {
    id: string;
}
    "#;

    server
        .create_collection_untyped("test/Account", schema, None)
        .await
        .unwrap();

    let collection_collection = server.collection_untyped("Collection");

    let records = collection_collection
        .list(ListQuery::default(), None)
        .await
        .unwrap();

    assert_eq!(records.data.len(), 1);
    assert_eq!(records.data[0].data.get("id").unwrap(), "test/Account");
    assert_eq!(records.data[0].data.get("code").unwrap(), schema);
}
