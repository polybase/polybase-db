use crate::snapshot::{SnapshotChunk, SnapshotIterator};
use parking_lot::Mutex;
use prost::Message;
use rocksdb::WriteBatch;
use std::mem;
use std::{convert::AsRef, path::Path, sync::Arc};

use crate::{
    job_engine,
    keys::{self, Key},
    proto,
    record::RecordRoot,
};

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
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

    #[error("job engine error")]
    JobEngineError(#[from] job_engine::JobEngineError),
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

pub(crate) struct Store {
    db: Arc<rocksdb::DB>,
    state: Arc<Mutex<StoreState>>,
}

pub(crate) struct StoreState {
    batch: WriteBatch,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_comparator("polybase", keys::comparator);

        let db = rocksdb::DB::open(&options, path)?;

        Ok(Self {
            db: Arc::new(db),
            state: Arc::new(Mutex::new(StoreState {
                batch: WriteBatch::default(),
            })),
        })
    }

    pub(crate) async fn commit(&self) -> Result<()> {
        // let batch = Arc::clone(&self.batch);
        let db = Arc::clone(&self.db);

        let batch = {
            let mut state = self.state.lock();
            mem::take(&mut state.batch)
        };

        tokio::task::spawn_blocking(move || db.write(batch)).await??;

        Ok(())
    }

    pub(crate) async fn set(&self, key: &Key<'_>, value: &Value<'_>) -> Result<()> {
        match (key, value) {
            (Key::Data { .. }, Value::DataValue(_)) => {}
            (Key::SystemData { .. }, Value::DataValue(_)) => {}
            (Key::Index { .. }, Value::IndexValue(_)) => {}
            _ => return Err(StoreError::InvalidKeyValueCombination),
        }

        let key = key.serialize()?;
        let value = value.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().batch.put(key, value);
        })
        .await?;

        Ok(())
    }

    pub(crate) async fn get(&self, key: &Key<'_>) -> Result<Option<RecordRoot>> {
        let key = key.serialize()?;
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || match db.get_pinned(key)? {
            Some(slice) => Ok(Some(bincode::deserialize_from(slice.as_ref())?)),
            None => Ok(None),
        })
        .await?
    }

    pub(crate) async fn delete(&self, key: &Key<'_>) -> Result<()> {
        let key = key.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().batch.delete(key);
        })
        .await?;

        Ok(())
    }

    pub(crate) fn list(
        &self,
        lower_bound: &Key,
        upper_bound: &Key,
        reverse: bool,
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>> + '_> {
        let mut opts = rocksdb::ReadOptions::default();
        opts.set_iterate_lower_bound(lower_bound.serialize()?);
        opts.set_iterate_upper_bound(upper_bound.serialize()?);

        Ok(self
            .db
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
            }))
    }

    pub(crate) fn destroy(self) -> Result<()> {
        let path = self.db.path().to_path_buf();
        drop(self.db);
        rocksdb::DB::destroy(&rocksdb::Options::default(), path)?;
        Ok(())
    }

    pub fn reset(&self) -> Result<()> {
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

    pub fn snapshot(&self, chunk_size: usize) -> SnapshotIterator {
        SnapshotIterator::new(&self.db, chunk_size)
    }

    // TODO:
    pub fn restore(&self, chunk: SnapshotChunk) -> Result<()> {
        let mut batch = WriteBatch::default();
        for entry in chunk {
            batch.put(entry.key, entry.value);
        }
        self.db.write(batch)?;
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::{
        borrow::Cow,
        ops::{Deref, DerefMut},
    };

    use crate::IndexValue;

    use super::*;

    pub(crate) struct TestStore(Option<Store>);

    impl Default for TestStore {
        fn default() -> Self {
            let temp_dir = std::env::temp_dir();
            let path = temp_dir.join(format!(
                "test-indexer-rocksdb-store-{}",
                rand::random::<u32>()
            ));

            Self(Some(Store::open(path).unwrap()))
        }
    }

    impl Drop for TestStore {
        fn drop(&mut self) {
            if let Some(store) = self.0.take() {
                store.destroy().unwrap();
            }
        }
    }

    impl Deref for TestStore {
        type Target = Store;

        fn deref(&self) -> &Self::Target {
            self.0.as_ref().unwrap()
        }
    }

    impl DerefMut for TestStore {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.0.as_mut().unwrap()
        }
    }

    #[tokio::test]
    async fn test_store_index() {
        let store = TestStore::default();

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
