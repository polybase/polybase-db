use tracing::info;

use crate::collection::{
    collection_ast_from_json, Collection, CollectionError, CollectionUserError, RocksDBCollection,
};
use crate::record::{IndexValue, RecordError, RecordRoot, RecordValue};
use crate::{db::Database, store};
use crate::{index, json_to_record, keys, proto, record_to_json};
use prost::Message;
use std::collections::{HashMap, HashSet};

const VERSION_SYSTEM_KEY: &str = "database_version";

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("collection error")]
    Collection(#[from] CollectionError),

    #[error("collection error")]
    CollectionUser(#[from] CollectionUserError),

    #[error("store error")]
    RocksDBStore(#[from] store::RocksDBStoreError),

    #[error("record error")]
    Record(#[from] RecordError),

    #[error("index error")]
    Index(#[from] index::IndexError),

    #[error("keys error")]
    Keys(#[from] keys::KeysError),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),

    #[error("prost decode error")]
    ProstDecodeError(#[from] prost::DecodeError),

    #[error("RocksDB error")]
    RocksDBError(#[from] rocksdb::Error),
}

pub(crate) async fn check_for_migration(
    store: &store::RocksDBStore,
    migration_batch_size: usize,
) -> Result<()> {
    let version = get_migration_version(store).await?;

    match version {
        0 => migrate_to_v1(store, migration_batch_size).await,
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
async fn migrate_to_v1(store: &store::RocksDBStore, migration_batch_size: usize) -> Result<()> {
    info!("Migrating database to v1");

    // Delete index
    info!("Deleting all indexes");
    delete_all_index_records(store, migration_batch_size).await?;
    store.commit().await?;

    // Apply indexes
    info!("Reapplying indexes");
    apply_indexes(store, migration_batch_size).await?;

    // Save version
    set_migration_version(store, 1).await?;

    // Save changes
    info!("Commiting changes");
    store.commit().await?;

    info!("Migration to v1 complete");

    Ok(())
}

/// Gets the migration version
async fn get_migration_version(store: &store::RocksDBStore) -> Result<u64> {
    let system_key = keys::Key::new_system_data(VERSION_SYSTEM_KEY.to_string())?;
    let record = store.get(&system_key).await?;
    match record.and_then(|mut r| r.remove(VERSION_SYSTEM_KEY)) {
        Some(RecordValue::Number(b)) => Ok(b as u64),
        _ => Ok(0),
    }
}

// Sets the migration version
async fn set_migration_version(store: &store::RocksDBStore, version: u64) -> Result<()> {
    let system_key = keys::Key::new_system_data(VERSION_SYSTEM_KEY.to_string())?;
    let value = RecordValue::Number(version as f64);
    let mut record = RecordRoot::new();
    record.insert(VERSION_SYSTEM_KEY.to_string(), value);

    Ok(store
        .set(&system_key, &store::Value::DataValue(&record))
        .await?)
}

/// Get a Collection instance for given collection_id
async fn get_collection(store: &store::RocksDBStore, id: String) -> Result<RocksDBCollection<'_>> {
    Ok(RocksDBCollection::load(store, id).await?)
}

/// Delete all index records (except for `id` index)
async fn delete_all_index_records(
    store: &store::RocksDBStore,
    migration_batch_size: usize,
) -> Result<()> {
    let mut opts = rocksdb::ReadOptions::default();
    opts.set_iterate_lower_bound([2]);
    opts.set_iterate_upper_bound([3]);

    // Get the index cids for every collections `id` index, as we don't want to delete those
    // records, as it would be very hard to recover if we lost these
    let keep_cids = get_collection_cids(store).await?;

    let mut i = 0;
    for entry in store.db.iterator_opt(rocksdb::IteratorMode::Start, opts) {
        let (key, _) = entry?;
        let index_key = keys::Key::deserialize(&key)?;

        match &index_key {
            keys::Key::Index { cid, .. } => {
                // Don't delete the `id` index
                if keep_cids.contains(cid.as_ref()) {
                    continue;
                }
                // Delete any other index key
                store.delete(&index_key).await?;
            }
            _ => continue,
        }

        // Commit every X records
        if i % migration_batch_size == 0 && i > 0 {
            info!(count = i, "Commit index delete");
            store.commit().await?;
        }

        i += 1;
    }

    Ok(())
}

/// For every collection, build all indexes
async fn apply_indexes(store: &store::RocksDBStore, migration_batch_size: usize) -> Result<()> {
    let collection_ids = get_collection_ids(store).await?;
    info!(
        count = collection_ids.len(),
        "Building indexes for all collections"
    );
    for collection_id in collection_ids {
        info!(
            collection_id = collection_id.as_str(),
            "Building indexes for collection"
        );
        build_collection_indexes(store, collection_id, migration_batch_size).await?;

        // Commiting here to release the memory
        store.commit().await?;
    }
    Ok(())
}

/// Get a list of all collections
async fn get_collection_ids(store: &store::RocksDBStore) -> Result<Vec<String>> {
    // Get a list of all collections
    let start_key = keys::Key::new_index(
        "Collection".to_string(),
        &[&["id"]],
        &[keys::Direction::Ascending],
        vec![],
    )?;
    let end_key = start_key.clone().wildcard();

    let mut collections = vec!["Collection".to_string()];

    for entry in store.list(&start_key, &end_key, false)? {
        let (key, _) = entry?;
        let index_key = keys::Key::deserialize(&key)?;
        let collection_id = match index_key {
            keys::Key::Index { values, .. } => match values.get(0) {
                Some(val) => match val.as_ref() {
                    IndexValue::String(s) => s.clone(),
                    _ => continue,
                },
                _ => continue,
            },
            _ => continue,
        };
        collections.push(collection_id.to_string());
    }

    Ok(collections)
}

/// Gets the cid of every collection, including Collection (we use this to exclude the `id` index)
async fn get_collection_cids(store: &store::RocksDBStore) -> Result<HashSet<Vec<u8>>> {
    let mut collections = HashSet::new();

    for collection_id in get_collection_ids(store).await? {
        let collection_cid = match keys::Key::new_index(
            collection_id,
            &[&["id"]],
            &[keys::Direction::Ascending],
            vec![],
        )? {
            keys::Key::Index { cid, .. } => cid,
            _ => return Err(Error::Keys(keys::KeysError::InvalidKeyType { n: 0u8 })),
        };
        collections.insert(collection_cid.to_vec());
    }

    Ok(collections)
}

/// Loop through every record for a collection, and rebuild the index for each record, mostly copied
/// from `Collection::rebuild`
async fn build_collection_indexes(
    store: &store::RocksDBStore,
    collection_id: String,
    migration_batch_size: usize,
) -> Result<()> {
    // Get the id index
    let start_key = keys::Key::new_index(
        collection_id.to_string(),
        &[&["id"]],
        &[keys::Direction::Ascending],
        vec![],
    )?;

    let collection_collection = get_collection(store, "Collection".to_string()).await?;
    let collection = get_collection(store, collection_id).await?;

    let meta = collection_collection
        .get(collection.id().to_string(), None)
        .await?;
    let Some(meta) = meta else {
            return Err(CollectionUserError::CollectionNotFound { name: collection.id().to_string() })?;
        };

    let collection_ast = match meta.get("ast") {
        Some(RecordValue::String(ast)) => {
            collection_ast_from_json(ast, collection.name().as_str())?
        }
        _ => return Err(CollectionError::CollectionRecordMissingAST)?,
    };

    let mut i = 0;
    let end_key = start_key.clone().wildcard();
    for entry in store.list(&start_key, &end_key, false)? {
        let (_, value) = entry?;
        let index_record = proto::IndexRecord::decode(&value[..])?;
        let data_key = keys::Key::deserialize(&index_record.id)?;
        let data = store.get(&data_key).await?;
        let Some(data) = data else {
            continue;
        };
        let Some(RecordValue::String(id)) = data.get("id") else {
            return Err(CollectionError::RecordMissingID)?;
        };
        let id = id.clone();

        let json_data = record_to_json(data)?;
        let new_data: HashMap<String, RecordValue> =
            json_to_record(&collection_ast, json_data, true)?;

        collection.set(id, &new_data).await?;

        // Commit every 1k records
        if i % migration_batch_size == 0 && i > 0 {
            info!(count = i, "Commit index set");
            store.commit().await?;
        }

        i += 1;
    }

    Ok(())
}
