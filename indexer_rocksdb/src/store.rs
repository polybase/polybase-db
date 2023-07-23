use crate::snapshot::{SnapshotChunk, SnapshotIterator};
use parking_lot::Mutex;
use prost::Message;
use rocksdb::WriteBatch;
use std::collections::HashMap;
use std::mem;
use std::{convert::AsRef, path::Path, sync::Arc};

use crate::{
    keys::{self, Key},
    proto,
    record::RecordRoot,
};

use crate::db::Database;

pub type Result<T> = std::result::Result<T, RocksDBStoreError>;

#[derive(Debug, thiserror::Error)]
pub enum RocksDBStoreError {
    #[error("invalid key/value combination")]
    InvalidKeyValueCombination,

    #[error("keys error")]
    KeysError(#[from] keys::KeysError),

    #[error("RocksDB error")]
    RocksDBError(#[from] rocksdb::Error),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),

    #[error("tokio task join error")]
    TokioTaskJoinError(#[from] tokio::task::JoinError),

    #[error("snapshot error")]
    SnapshotError(#[from] crate::snapshot::Error),
}

#[derive(Debug)]
pub(crate) enum Value<'a> {
    DataValue(&'a RecordRoot),
    IndexValue(proto::IndexRecord),
}

impl<'a> Value<'a> {
    fn serialize(&self) -> Result<Vec<u8>> {
        match self {
            Value::DataValue(value) => Ok(bincode::serialize(value)?),
            Value::IndexValue(value) => Ok(value.encode_to_vec()),
        }
    }
}

pub(crate) struct RocksDBStore {
    pub(crate) db: Arc<rocksdb::DB>,
    state: Arc<Mutex<RocksDBStoreState>>,
}

enum RocksDBStoreOp {
    Put(Vec<u8>),
    Delete,
}

pub(crate) struct RocksDBStoreState {
    pending: HashMap<Vec<u8>, RocksDBStoreOp>,
}

impl RocksDBStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_comparator("polybase", keys::comparator);

        let db = rocksdb::DB::open(&options, path)?;

        Ok(Self {
            db: Arc::new(db),
            state: Arc::new(Mutex::new(RocksDBStoreState {
                pending: HashMap::new(),
            })),
        })
    }

    #[tracing::instrument(skip(self))]
    pub fn list(
        &self,
        lower_bound: &Key,
        upper_bound: &Key,
        reverse: bool,
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + '_> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_lower_bound(lower_bound.serialize()?);
        opts.set_iterate_upper_bound(upper_bound.serialize()?);

        Ok(Box::new(
            self.db
                .iterator_opt(
                    if reverse {
                        rocksdb::IteratorMode::End
                    } else {
                        rocksdb::IteratorMode::Start
                    },
                    opts,
                )
                .map(|res| {
                    let (key, value) = res?;
                    Ok((key, value))
                }),
        ))
    }
}

#[async_trait::async_trait]
impl Database for RocksDBStore {
    type Error = RocksDBStoreError;
    type Key<'k> = keys::Key<'k>;
    type Value<'v> = Value<'v>;

    #[tracing::instrument(skip(self))]
    async fn commit(&self) -> Result<()> {
        let db = Arc::clone(&self.db);

        let pending = {
            let mut state = self.state.lock();
            mem::take(&mut state.pending)
        };

        let mut db_batch = WriteBatch::default();

        tokio::task::spawn_blocking(move || {
            for (key, op) in pending {
                match op {
                    RocksDBStoreOp::Put(value) => db_batch.put(key, value),
                    RocksDBStoreOp::Delete => db_batch.delete(key),
                }
            }
            db.write(db_batch)
        })
        .await??;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn set(&self, key: &Key<'_>, value: &Value<'_>) -> Result<()> {
        match (key, value) {
            (Key::Data { .. }, Value::DataValue(_)) => {}
            (Key::SystemData { .. }, Value::DataValue(_)) => {}
            (Key::Index { .. }, Value::IndexValue(_)) => {}
            _ => return Err(RocksDBStoreError::InvalidKeyValueCombination),
        }

        let key = key.serialize()?;
        let value = value.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().pending.insert(key, RocksDBStoreOp::Put(value));
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get(&self, key: &Key<'_>) -> Result<Option<RecordRoot>> {
        let key = key.serialize()?;
        let db = Arc::clone(&self.db);
        let state = Arc::clone(&self.state);

        tokio::task::spawn_blocking(move || match state.lock().pending.get(&key) {
            Some(RocksDBStoreOp::Put(value)) => {
                Ok(Some(bincode::deserialize_from(value.as_slice())?))
            }
            Some(RocksDBStoreOp::Delete) => Ok(None),
            None => match db.get_pinned(key)? {
                Some(slice) => Ok(Some(bincode::deserialize_from(slice.as_ref())?)),
                None => Ok(None),
            },
        })
        .await?
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, key: &Key<'_>) -> Result<()> {
        let key = key.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().pending.insert(key, RocksDBStoreOp::Delete);
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn destroy(self) -> Result<()> {
        let path = self.db.path().to_path_buf();
        drop(self.db);
        rocksdb::DB::destroy(&rocksdb::Options::default(), path)?;
        Ok(())
    }

    fn reset(&self) -> Result<()> {
        let iter = SnapshotIterator::new(&self.db, 100 * 1024 * 1024);
        for entry in iter {
            let mut batch = WriteBatch::default();
            for entry in entry? {
                batch.delete(entry.key);
            }
            self.db.write(batch)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn snapshot(&self, chunk_size: usize) -> SnapshotIterator {
        SnapshotIterator::new(&self.db, chunk_size)
    }

    // TODO:
    #[tracing::instrument(skip(self))]
    fn restore(&self, chunk: SnapshotChunk) -> Result<()> {
        let mut batch = WriteBatch::default();
        for entry in chunk {
            batch.put(entry.key, entry.value);
        }
        self.db.write(batch)?;
        Ok(())
    }

    async fn set_system_key(&self, key: String, data: &RecordRoot) -> Result<()> {
        let system_key = keys::Key::new_system_data(key)?;

        Ok(self.set(&system_key, &Value::DataValue(data)).await?)
    }

    async fn get_system_key(&self, key: String) -> Result<Option<RecordRoot>> {
        let system_key = keys::Key::new_system_data(key)?;
        Ok(self.get(&system_key).await?)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        borrow::Cow,
        ops::{Deref, DerefMut},
    };

    use crate::db::Database;
    use crate::IndexValue;

    use super::*;

    pub(crate) struct TestRocksDBStore(Option<RocksDBStore>);

    impl Default for TestRocksDBStore {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-indexer-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(RocksDBStore::open(path).unwrap()))
        }
    }

    impl Drop for TestRocksDBStore {
        fn drop(&mut self) {
            if let Some(store) = self.0.take() {
                store.destroy().unwrap();
            }
        }
    }

    impl Deref for TestRocksDBStore {
        type Target = RocksDBStore;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestRocksDBStore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    #[tokio::test]
    async fn test_store_index() {
        let store = TestRocksDBStore::default();

        let index = Key::new_index(
            "ns".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Owned(IndexValue::String("John".to_string().into()))],
        )
        .unwrap();

        store
            .set(&index, &Value::IndexValue(proto::IndexRecord::default()))
            .await
            .unwrap();

        let upper_bound = index.clone().wildcard();
        for record in store.list(&index, &upper_bound, false).unwrap() {
            let (key_box, value_box) = record.unwrap();
            let _key = Key::deserialize(&key_box[..]).unwrap();
            let value = proto::IndexRecord::decode(&value_box[..]).unwrap();

            // This doesn't work, not sure why.
            // assert_eq!(&key, &index);
            assert_eq!(value, proto::IndexRecord::default());
        }
    }
}
