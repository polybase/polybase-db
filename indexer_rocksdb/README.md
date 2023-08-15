# indexer_rocksdb

This is a concrete implementation of the generic interface provided by the `indexer` crate. Please refer to the README in that crate for more details.

In particular, this crate provides existing functionality (NOTE: aside from the `list` function which is commented out for now) as concrete implementations of the 
traits from the `indexer` crate:

  - Concrete `RocksDBIndexer` implementing the `Indexer` trait.
  - Concrete `RocksSDBStore` implementing the `Database` trait, and
  - Concrete `RocksDBCollection` implementing the `Collection` trait.`

## Build & Run

Right now, the unit tests (aside from the one for `list`ing collections) work as is:

```
  $ cargo test

```