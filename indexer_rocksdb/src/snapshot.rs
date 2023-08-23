use indexer::adaptor::SnapshotValue;
use rocksdb::{IteratorMode, DB};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("RocksDB error")]
    RocksDBError(#[from] rocksdb::Error),

    #[error("bincode error")]
    BincodeError(#[from] bincode::Error),
}

pub type SnapshotChunk = Vec<SnapshotValue>;

pub struct SnapshotIterator<'a> {
    chunk_size: usize,
    iter: rocksdb::DBIteratorWithThreadMode<'a, rocksdb::DB>,
}

impl<'a> SnapshotIterator<'a> {
    pub fn new(db: &'a DB, chunk_size: usize) -> Self {
        SnapshotIterator {
            chunk_size,
            iter: db.iterator(IteratorMode::Start),
        }
    }
}

impl<'a> Iterator for SnapshotIterator<'a> {
    type Item = Result<Vec<SnapshotValue>>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut batch = Vec::new();
        let mut bytes = 0;
        while bytes < self.chunk_size {
            match self.iter.next() {
                Some(Ok((key, value))) => {
                    bytes += key.len() + value.len();
                    batch.push(SnapshotValue { key, value });
                }
                Some(Err(e)) => return Some(Err(e.into())),
                None => break,
            }
        }
        if batch.is_empty() {
            None
        } else {
            Some(Ok(batch))
        }
    }
}
