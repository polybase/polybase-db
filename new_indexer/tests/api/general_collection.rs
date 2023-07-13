use rand::{rngs::OsRng, RngCore};
use tokio::sync::oneshot;
use tonic::Request;

use new_indexer::indexer::{
    indexer_client::IndexerClient, Collection, CreateCollectionRequest, ListCollectionsRequest,
    ShutdownRequest,
};

use crate::api;

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

fn get_random_number() -> u32 {
    let mut rng = OsRng;
    rng.next_u32()
}

#[tokio::test]
async fn test_collection() -> Result<(), Box<dyn std::error::Error>> {
    let test_db_name = format!("test_create_collection_{}", get_random_number());

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let _ = setup(&test_db_name, shutdown_rx).await;

    let collection: Collection = serde_json::from_str(include_str!("create_coll_minimal.json"))?;

    // create the collection
    let indexer_service_addr = format!("http://{}", api::INDEXER_SERVER_ADDR);
    let mut indexer_client = IndexerClient::connect(indexer_service_addr).await?;

    let create_coll_req = Request::new(CreateCollectionRequest {
        collection: Some(collection),
    });

    let created_collection = indexer_client
        .create_collection(create_coll_req)
        .await?
        .into_inner()
        .collection;

    println!("created_collection = {created_collection:?}");

    test_list_collections(&mut indexer_client).await?;

    // shutdown indexer
    let req = Request::new(ShutdownRequest {});
    let _ = indexer_client.shutdown(req).await?;

    let _ = teardown(&test_db_name, shutdown_tx).await?;

    Ok(())
}

async fn test_list_collections(
    indexer_client: &mut IndexerClient<tonic::transport::Channel>,
) -> Result<(), Box<dyn std::error::Error>> {
    //let collection: Collection = serde_json::from_str(include_str!("create_coll_minimal.json"))?;

    // create the collection

    //let create_coll_req = Request::new(CreateCollectionRequest {
    //    collection: Some(collection),
    //});

    //let _ = indexer_client
    //    .create_collection(create_coll_req)
    //    .await?
    //    .into_inner()
    //    .collection;

    // list the collections

    let list_colls_req = Request::new(ListCollectionsRequest {});

    let (_, collections, _) = indexer_client
        .list_collections(list_colls_req)
        .await?
        .into_parts();

    println!("collections = {collections:?}");

    Ok(())
}

#[tokio::test]
async fn test_list_collection_records() {}
