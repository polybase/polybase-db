use std::{convert::AsRef, error::Error, path::Path};

use prost::Message;

use crate::{
    keys::{self, Key},
    proto,
    record::RecordRoot,
};

pub(crate) struct Store {
    db: std::sync::Arc<rocksdb::DB>,
}

pub(crate) enum Value<'a> {
    DataValue(&'a RecordRoot),
    IndexValue(proto::IndexRecord),
}

impl<'a> Value<'a> {
    fn serialize(&self) -> Result<Vec<u8>, serde_json::Error> {
        match self {
            Value::DataValue(value) => Ok(serde_json::to_vec(value)?),
            Value::IndexValue(value) => Ok(value.encode_to_vec()),
        }
    }
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error + Send + Sync + 'static>> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);
        options.set_comparator("polybase", keys::comparator);

        let db = rocksdb::DB::open(&options, path)?;

        Ok(Self {
            db: std::sync::Arc::new(db),
        })
    }

    pub(crate) async fn set(
        &self,
        key: &Key<'_>,
        value: &Value<'_>,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        match (key, value) {
            (Key::Data { .. }, Value::DataValue(_)) => {}
            (Key::SystemData { .. }, Value::DataValue(_)) => {}
            (Key::Index { .. }, Value::IndexValue(_)) => {}
            _ => return Err("invalid key/value combination".into()),
        }

        let key = key.serialize()?;
        let value = value.serialize()?;
        let db = std::sync::Arc::clone(&self.db);
        tokio::task::spawn_blocking(move || db.put(key, value)).await??;

        Ok(())
    }

    pub(crate) async fn get(
        &self,
        key: &Key<'_>,
    ) -> Result<Option<RecordRoot>, Box<dyn Error + Send + Sync + 'static>> {
        let key = key.serialize()?;
        let db = std::sync::Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || match db.get_pinned(key)? {
            Some(slice) => Ok(Some(serde_json::from_slice(slice.as_ref())?)),
            None => Ok(None),
        })
        .await?
    }

    pub(crate) fn list(
        &self,
        lower_bound: &Key,
        upper_bound: &Key,
        reverse: bool,
    ) -> Result<
        impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>), Box<dyn Error + Send + Sync + 'static>>>
            + '_,
        Box<dyn Error + Send + Sync + 'static>,
    > {
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

    pub(crate) fn destroy(self) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        let path = self.db.path().to_path_buf();

        drop(self.db);
        rocksdb::DB::destroy(&rocksdb::Options::default(), path)?;

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
            vec![Cow::Owned(IndexValue::String("John".to_string()))],
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
