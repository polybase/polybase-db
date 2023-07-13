mod general_collection;

use sqlx::postgres::PgPoolOptions;
use tokio::sync::oneshot;
use tonic::transport::Server;

use new_indexer::{
    db::postgres::PostgresDB, indexer::indexer_server::IndexerServer, IndexerService,
    PolybaseIndexer,
};

const POSTGRES_TEST_SERVER: &str =
    "postgres://polybase_test_user:polybase_test_password@127.0.0.1:9000";

pub async fn create_db(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = PgPoolOptions::new()
        .max_connections(100)
        .connect(POSTGRES_TEST_SERVER)
        .await?;

    // create the test db
    sqlx::query(&format!("create database {}", db_name))
        .execute(&conn)
        .await?;

    conn.close().await;

    // set env for the application code to pick up connection to this database
    std::env::set_var(
        "DATABASE_URL",
        format!("{}/{}?schema=public", POSTGRES_TEST_SERVER, db_name),
    );

    // run migrations
    let test_db_conn = PgPoolOptions::new()
        .max_connections(100)
        .connect(&format!(
            "{}/{}?schema=public",
            POSTGRES_TEST_SERVER, db_name
        ))
        .await?;

    sqlx::migrate!("migrations/postgres")
        .run(&test_db_conn)
        .await?;

    test_db_conn.close().await;

    println!("Test db server started");

    Ok(())
}

pub async fn drop_db(db_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = PgPoolOptions::new()
        .max_connections(100)
        .connect(POSTGRES_TEST_SERVER)
        .await?;

    sqlx::query(&format!("drop database {}", db_name))
        .execute(&conn)
        .await?;

    conn.close().await;

    // remove the env var for the next test
    std::env::remove_var("DATABASE_URL");

    println!("Test db server shutdown");

    Ok(())
}

pub const INDEXER_SERVER_ADDR: &str = "[::1]:9003";

async fn start_indexer_service(
    shutdown_rx: oneshot::Receiver<()>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Setup the Indexer service
    let addr = INDEXER_SERVER_ADDR.parse()?;

    let indexer = PolybaseIndexer::<PostgresDB>::new().await;
    let postgres_service = IndexerService { indexer };

    let service = IndexerServer::new(postgres_service);
    let server = Server::builder().add_service(service).serve(addr);

    println!("Indexer service listening on {}", addr);

    tokio::select! {
        _ = shutdown_rx => {
            println!("Shutdown signal received");
        }
        result = server => {
            if let Err(err) = result {
                eprintln!("Server error: {}", err);
            }
        }
    }

    Ok(())
}
