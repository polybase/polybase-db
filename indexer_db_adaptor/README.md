# indexer_db_adaptor

WIP.

This crate defines the generic parts of the new indexer. In particular, it defines the following important traits:

  - `Indexer` in src/indexer.rs, representing an abstract Indexer.
  - `Database` in src/db.rs, representing an abstract and generic Database (store).
  - `Collection` in src/collection.rs, representing an abstract Collection

See the `indexer_rocksdb` crate for a concrete implementation of this crate.

Note: For the `Collection` types in particular, more generic functionality may be put as a default implementation in the trait itself.