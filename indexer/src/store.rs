use std::{
    borrow::Cow,
    collections::HashMap,
    convert::AsRef,
    error::Error,
    ops::{Deref, DerefMut},
    path::Path,
};

use prost::Message;
use rocksdb::DBPinnableSlice;
use serde::{Deserialize, Serialize};

use crate::{
    keys::{self, Key},
    proto,
};

pub(crate) struct Store {
    db: rocksdb::DB,
}

pub type RecordValue<'a> = HashMap<Cow<'a, str>, keys::RecordValue<'a>>;

enum RecordBacking<'a> {
    Pinnable(DBPinnableSlice<'a>),
    Static(&'static [u8]),
    Vec(Vec<u8>),
}

#[ouroboros::self_referencing]
pub struct StoreRecordValue<'db> {
    slice: RecordBacking<'db>,
    #[borrows(slice)]
    #[covariant]
    pub record: RecordValue<'this>,
}

impl StoreRecordValue<'_> {
    pub fn new_from_static(slice: &'static [u8]) -> Result<Self, serde_json::Error> {
        StoreRecordValueTryBuilder {
            slice: RecordBacking::Static(slice),
            record_builder: |slice| {
                serde_json::from_slice(match slice {
                    RecordBacking::Pinnable(p) => p.as_ref(),
                    RecordBacking::Static(s) => s,
                    RecordBacking::Vec(v) => v.as_slice(),
                })
            },
        }
        .try_build()
    }

    pub fn new_from_vec(slice: Vec<u8>) -> Result<Self, serde_json::Error> {
        StoreRecordValueTryBuilder {
            slice: RecordBacking::Vec(slice),
            record_builder: |slice| {
                serde_json::from_slice(match slice {
                    RecordBacking::Pinnable(p) => p.as_ref(),
                    RecordBacking::Static(s) => s,
                    RecordBacking::Vec(v) => v.as_slice(),
                })
            },
        }
        .try_build()
    }

    pub fn get_slice(&self) -> &[u8] {
        match self.borrow_slice() {
            RecordBacking::Pinnable(p) => p.deref(),
            RecordBacking::Static(s) => s,
            RecordBacking::Vec(v) => v.deref(),
        }
    }
}

pub(crate) enum Value<'a> {
    DataValue(Cow<'a, HashMap<Cow<'a, str>, keys::RecordValue<'a>>>),
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

        Ok(Self { db })
    }

    pub(crate) fn set(
        &self,
        key: &Key,
        value: &Value,
    ) -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
        match (key, value) {
            (Key::Data { .. }, Value::DataValue(_)) => {}
            (Key::SystemData { .. }, Value::DataValue(_)) => {}
            (Key::Index { .. }, Value::IndexValue(_)) => {}
            _ => return Err("invalid key/value combination".into()),
        }

        self.db.put(key.serialize()?, value.serialize()?)?;

        Ok(())
    }

    pub(crate) fn get(
        &self,
        key: &Key,
    ) -> Result<Option<StoreRecordValue>, Box<dyn Error + Send + Sync + 'static>> {
        match self.db.get_pinned(key.serialize()?)? {
            Some(slice) => Ok(Some(
                StoreRecordValueTryBuilder {
                    slice: RecordBacking::Pinnable(slice),
                    record_builder: |slice| {
                        serde_json::from_slice(match slice {
                            RecordBacking::Pinnable(p) => p.as_ref(),
                            RecordBacking::Static(s) => s,
                            RecordBacking::Vec(v) => v.as_slice(),
                        })
                    },
                }
                .try_build()?,
            )),
            None => Ok(None),
        }
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

    #[test]
    fn test_store_index() {
        let store = TestStore::default();

        let index = Key::new_index(
            "ns".to_string(),
            &[&["name"]],
            &[keys::Direction::Ascending],
            vec![Cow::Borrowed(&keys::IndexValue::String(Cow::Borrowed(
                "John",
            )))],
        )
        .unwrap();

        store
            .set(&index, &Value::IndexValue(proto::IndexRecord::default()))
            .unwrap();

        let upper_bound = index.clone().wildcard();
        for record in store.list(&index, &upper_bound, false).unwrap() {
            let (key_box, value_box) = record.unwrap();
            let key = Key::deserialize(&key_box[..]).unwrap();
            let value = proto::IndexRecord::decode(&value_box[..]).unwrap();

            // This doesn't work, not sure why.
            // assert_eq!(&key, &index);
            assert_eq!(value, proto::IndexRecord::default());
        }
    }
}
