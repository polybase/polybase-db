# RBMerkle (Red Black Merkle Tree)

In memory Red Black Merkle Tree, allows for fast merkle tree inserts and updates.


## Features

 - ✅ **Self balancing** - the tree will always be 
 - ✅ **O(log n) read/writes** due to the self balancing nature of the tree
 - ✅ **Hash** adheres to winter-crypto `Hasher`


## Benchmarks

```
cargo bench
```

|          | `Insert`           | `Insert + Hash`                      |
|:---------|:--------------------------|:------------------------------------ |
| **`10k`** | `2 ms`     | `220.83 ms`      |
| **`100k`** | `36 ms`    | `2.2788 ms`      |


## TODO

 - Concurrent hashing of tree
 - Hashing only changed nodes
 - More tests