# `smirk` - Persistent Merkle Tree

`smirk` = "stable `merk`"

This library provides `MerkleTree`, a Merkle tree that uses the [Rescue-Prime Optimized][rpo]
hash function, with a map-like API. There is also a [`Storage`] API for persisting the tree in
[rocksdb][db]

```rust
# use smirk::{MerkleTree, smirk, storage::Storage};
let mut tree = MerkleTree::new();
tree.insert(1, "hello");
tree.insert(2, "world");

// or you can use the macro to create a new tree
let tree = smirk! {
  1 => "hello",
  2 => "world",
};

assert_eq!(tree.get(&1), Some(&"hello"));
assert_eq!(tree.get(&2), Some(&"world"));
assert_eq!(tree.get(&3), None);
```

You can persist trees with the [`Storage`] API:
```rust,no_run
# use std::path::Path;
# use smirk::{smirk, storage::Storage};
let path = Path::new("path/for/rocksdb");
let storage = Storage::open(path).unwrap();

let tree = smirk! {
    1 => 123,
    2 => 234,
};

storage.store_tree(&tree).unwrap();
let tree_again = storage.load_tree().unwrap().unwrap();

assert_eq!(tree, tree_again);
```

Any type that implements [`Serialize`] and [`Deserialize`] can be used

```rust,no_run
# use std::path::Path;
# use serde::{Serialize, Deserialize};
# use smirk::{smirk, storage::Storage, hash::{Hashable, Digest}};
#[derive(Debug, Serialize, Deserialize)]
struct MyCoolType {
    foo: i32,
    bar: String,
}

impl Hashable for MyCoolType {
    fn hash(&self) -> Digest {
        [self.foo.hash(), self.bar.hash()].into_iter().collect()
    }
}

let path = Path::new("path/for/rocksdb");
let storage = Storage::open(path).unwrap();

let tree = smirk! {
    1 => MyCoolType { foo: 123, bar: "hello".to_string() },
    2 => MyCoolType { foo: 234, bar: "world".to_string() },
};

storage.store_tree(&tree).unwrap();
let tree_again = storage.load_tree().unwrap().unwrap();

assert_eq!(tree, tree_again);
```

Types provided by this library implement [`Arbitrary`], for use with [`proptest`], gated behind
the `proptest` feature flag.


## Todo

 - benchmarks
 - batch update API for storage
 - use a slab allocator internally

[rpo]: https://eprint.iacr.org/2022/1577.pdf
[db]: https://github.com/facebook/rocksdb

[`Storage`]: storage::Storage
[`Arbitrary`]: proptest::prelude::Arbitrary
[`Serialize`]: serde::Serialize
[`Deserialize`]: serde::Deserialize
