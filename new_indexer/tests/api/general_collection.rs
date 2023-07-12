use async_trait::async_trait;
use rand::Rng;
use serde::Deserialize;
use serde_json::json;
use tokio::sync::oneshot;
use tonic::Request;

use new_indexer::indexer::{
    indexer_client::IndexerClient, Collection, CollectionRecord, CreateCollectionRecordRequest,
    CreateCollectionRequest, GetCollectionRecordRequest, GetCollectionRequest,
    ListCollectionRecordsRequest, ListCollectionsRequest, ShutdownRequest, ShutdownResponse,
};

use crate::api;

#[tokio::test]
async fn test_db() -> Result<(), Box<dyn std::error::Error>> {
    api::create_db("polybase_test_db").await?;
    api::drop_db("polybase_test_db").await?;

    Ok(())
}

async fn setup(
    test_db_name: &str,
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    api::create_db(&test_db_name).await?;

    tokio::spawn(async move {
        if let Err(err) = api::start_indexer_service(shutdown_rx).await {
            eprintln!("Error starting indexer service: {}", err);
        }
    });

    // wait for service to become active
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    Ok(())
}

async fn teardown(
    test_db_name: &str,
    shutdown_tx: oneshot::Sender<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    // shutdown indexer service
    let _ = shutdown_tx.send(());

    // drop the test database
    api::drop_db(&test_db_name).await?;

    Ok(())
}

#[tokio::test]
async fn test_create_collection() -> Result<(), Box<dyn std::error::Error>> {
    let test_db_name = format!(
        "test_create_collection_{}",
        rand::thread_rng().gen_range(1..1000)
    );

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    setup(&test_db_name, shutdown_rx).await;

    let collection: Collection = serde_json::from_str(include_str!("create_coll_minimal.json"))?;

    // create the collection
    let indexer_service_addr = format!("http://{}", api::INDEXER_SERVER_ADDR);
    let mut indexer_client = IndexerClient::connect(indexer_service_addr).await?;

    let req = Request::new(CreateCollectionRequest {
        collection: Some(collection),
    });

    let created_collection = indexer_client
        .create_collection(req)
        .await?
        .into_inner()
        .collection;

    println!("created_collection = {created_collection:#?}");

    // shutdown indexer
    let req = Request::new(ShutdownRequest {});
    let _ = indexer_client.shutdown(req).await?;

    teardown(&test_db_name, shutdown_tx).await?;

    Ok(())
}

#[tokio::test]
async fn test_create_collection_record() {}

#[tokio::test]
async fn test_get_collection() {}

#[tokio::test]
async fn test_get_collection_record() {}

#[tokio::test]
async fn test_list_collections() {}

#[tokio::test]
async fn test_list_collection_records() {}
