use chrono::prelude::{DateTime, Utc};
use futures::StreamExt;
use indexer::where_query::WhereQuery;
use indexer_rocksdb::{adaptor, RocksDBAdaptor};
use schema::record::{RecordRoot, RecordValue};
use schema::COLLECTION_SCHEMA;
use std::collections::HashMap;
use tracing::{info, warn};

const VERSION_SYSTEM_KEY: &str = "database_version";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Adaptor error")]
    Adaptor(#[from] adaptor::Error),

    #[error("Record error")]
    Record(#[from] schema::record::RecordError),
}

pub(crate) async fn check_for_migration(store: &RocksDBAdaptor) -> Result<()> {
    let version = get_migration_version(store).await?;

    match version {
        0 => migrate_to_v1(store).await,
        _ => {
            info!(
                version = version,
                "Database is up to date, no migration needed"
            );
            Ok(())
        }
    }
}

/// Migration can fail half way through, but because we only commit the version at the end
/// it will restart the migration process.
async fn migrate_to_v1(store: &RocksDBAdaptor) -> Result<()> {
    info!("Migrating database to v1");

    // Loop through all collections
    let mut collections = store
        ._list("Collection", None, WhereQuery(HashMap::new()), &[], false)
        .await?
        .collect::<Vec<_>>()
        .await;

    let schema = COLLECTION_SCHEMA.clone();

    for col in collections.iter_mut() {
        let record_id = col.id()?.to_string();

        // Get createdAt (last updated)
        let meta = store.get_metadata(&record_id).await?;
        if let Some(meta) = meta {
            let dt: DateTime<Utc> = meta.last_record_updated_at.into();
            col.insert(
                "createdAt".to_string(),
                RecordValue::String(format!("{}", dt.format("%+"))),
            );
        }

        match store.set("Collection", &record_id, col, &schema).await {
            Ok(_) => {}
            Err(e) => warn!(
                "error migrating collection: {} with err {}, record {:?}",
                record_id, e, col
            ),
        };
    }

    // Udpate to v1
    set_migration_version(store, 1).await?;

    store.store_commit().await?;

    info!("Migration to v1 complete");

    Ok(())
}

/// Gets the migration version
async fn get_migration_version(store: &RocksDBAdaptor) -> Result<u64> {
    let record = store._get_system_record(VERSION_SYSTEM_KEY).await?;
    match record.and_then(|mut r| r.remove(VERSION_SYSTEM_KEY)) {
        Some(RecordValue::Number(b)) => Ok(b as u64),
        _ => Ok(0),
    }
}

// Sets the migration version
async fn set_migration_version(store: &RocksDBAdaptor, version: u64) -> Result<()> {
    let value = RecordValue::Number(version as f64);
    let mut record = RecordRoot::new();
    record.insert(VERSION_SYSTEM_KEY.to_string(), value);

    Ok(store
        ._set_system_record(VERSION_SYSTEM_KEY, &record)
        .await?)
}
