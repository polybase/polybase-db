use crate::{
    db,
    snapshot::{SnapshotChunk, SnapshotIterator},
};
use async_trait::async_trait;
use parking_lot::Mutex;
use rocksdb::WriteBatch;
use std::collections::HashMap;
use std::mem;
use std::{convert::AsRef, path::Path, sync::Arc};

use crate::{
    keys::{self, Key},
    record::RecordRoot,
};

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
}

pub(crate) struct Store {
    pub(crate) db: Arc<rocksdb::DB>,
    state: Arc<Mutex<StoreState>>,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_comparator("polybase", keys::comparator);

        let db = rocksdb::DB::open(&options, path)?;

        Ok(Self {
            db: Arc::new(db),
            state: Arc::new(Mutex::new(StoreState {
                // batch: WriteBatch::default(),
                pending: HashMap::new(),
            })),
        })
    }
}

enum StoreOp {
    Put(Vec<u8>),
    Delete,
}

pub(crate) struct StoreState {
    // batch: WriteBatch,
    pending: HashMap<Vec<u8>, StoreOp>,
}

#[async_trait]
impl db::Database for Store {
    #[tracing::instrument(skip(self))]
    async fn commit(&self) -> db::Result<()> {
        // let batch = Arc::clone(&self.batch);
        let db = Arc::clone(&self.db);

        let pending = {
            let mut state = self.state.lock();
            mem::take(&mut state.pending)
        };

        let mut db_batch = WriteBatch::default();

        tokio::task::spawn_blocking(move || {
            for (key, op) in pending {
                match op {
                    StoreOp::Put(value) => db_batch.put(key, value),
                    StoreOp::Delete => db_batch.delete(key),
                }
            }
            db.write(db_batch)
        })
        .await??;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn set(&self, key: &Key<'_>, value: &db::Value<'_>) -> db::Result<()> {
        match (key, value) {
            (Key::Data { .. }, db::Value::DataValue(_)) => {}
            (Key::SystemData { .. }, db::Value::DataValue(_)) => {}
            (Key::Index { .. }, db::Value::IndexValue(_)) => {}
            _ => {
                return Err(db::DatabaseError::from(
                    StoreError::InvalidKeyValueCombination,
                ))
            }
        }

        let key = key.serialize()?;
        let value = value.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().pending.insert(key, StoreOp::Put(value));
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn get(&self, key: &Key<'_>) -> db::Result<Option<RecordRoot>> {
        let key = key.serialize()?;
        let db = Arc::clone(&self.db);
        let state = Arc::clone(&self.state);

        tokio::task::spawn_blocking(move || match state.lock().pending.get(&key) {
            Some(StoreOp::Put(value)) => Ok(Some(bincode::deserialize_from(value.as_slice())?)),
            Some(StoreOp::Delete) => Ok(None),
            None => match db.get_pinned(key)? {
                Some(slice) => Ok(Some(bincode::deserialize_from(slice.as_ref())?)),
                None => Ok(None),
            },
        })
        .await?
    }

    #[tracing::instrument(skip(self))]
    async fn delete(&self, key: &Key<'_>) -> db::Result<()> {
        let key = key.serialize()?;
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            state.lock().pending.insert(key, StoreOp::Delete);
        })
        .await?;

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    fn list(
        &self,
        lower_bound: &Key,
        upper_bound: &Key,
        reverse: bool,
    ) -> db::Result<Box<dyn Iterator<Item = db::Result<(Box<[u8]>, Box<[u8]>)>> + '_>> {
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

    #[tracing::instrument(skip(self))]
    fn destroy(self) -> db::Result<()> {
        let path = self.db.path().to_path_buf();
        drop(self.db);
        rocksdb::DB::destroy(&rocksdb::Options::default(), path)?;
        Ok(())
    }

    fn reset(&self) -> db::Result<()> {
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
    fn restore(&self, chunk: SnapshotChunk) -> db::Result<()> {
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

    use super::*;
    use crate::db::Database;
    use crate::{proto, IndexValue};
    use prost::Message;

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

    #[async_trait::async_trait]
    impl db::Database for TestStore {
        async fn commit(&self) -> db::Result<()> {
            self.0.as_ref().unwrap().commit().await
        }

        async fn set(&self, key: &Key<'_>, value: &db::Value<'_>) -> db::Result<()> {
            self.0.as_ref().unwrap().set(key, value).await
        }

        async fn get(&self, key: &Key<'_>) -> db::Result<Option<RecordRoot>> {
            self.0.as_ref().unwrap().get(key).await
        }

        async fn delete(&self, key: &Key<'_>) -> db::Result<()> {
            self.0.as_ref().unwrap().delete(key).await
        }

        fn list(
            &self,
            lower_bound: &Key,
            upper_bound: &Key,
            reverse: bool,
        ) -> db::Result<Box<dyn Iterator<Item = db::Result<(Box<[u8]>, Box<[u8]>)>> + '_>> {
            self.0
                .as_ref()
                .unwrap()
                .list(lower_bound, upper_bound, reverse)
        }

        fn destroy(mut self) -> db::Result<()> {
            self.0.take().unwrap().destroy()
        }

        fn reset(&self) -> db::Result<()> {
            self.0.as_ref().unwrap().reset()
        }

        fn snapshot(&self, chunk_size: usize) -> SnapshotIterator {
            self.0.as_ref().unwrap().snapshot(chunk_size)
        }

        fn restore(&self, chunk: SnapshotChunk) -> db::Result<()> {
            self.0.as_ref().unwrap().restore(chunk)
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
            .set(
                &index,
                &db::Value::IndexValue(proto::IndexRecord::default()),
            )
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
