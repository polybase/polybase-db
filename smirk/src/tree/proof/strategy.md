## Old strategy

Given a tree, plus a set of updates, we can prove the new root hash by:
 - rebalance the tree and recalculate the hash in Rust
 - pass the old root hash to miden via stack inputs, exact tree structure and new batch via advice
 - miden rebalances the tree, return the new hash via stack output
 - rust code verifies hashes match

## Observation

With a slight tweak to the rebalancing alg, we can make it so:

> For any tree size `n`, there is exactly 1 canonical tree structure

Essentially, the exact keys/values are meaningless, except that the key ordering essentially "assigns" KV pairs to nodes (and ofc the KV hash determines the hash of the node)

## New strategy

Broadly the same except, instead of rebalancing the tree in miden, we essentially implement the following function in miden:
```rust
fn create_ord_to_depth_map(size: usize) -> HashMap<usize, usize> {
    // for a tree of size `size`, create a map that maps "index according to node `Ord` impl" to "depth"
}
```
The miden equivalent of `HashMap<usize, usize>` will be the advice map.

Essentially, we're moving some complexity from miden to rust.
We're also (afaict) moving some overhead from miden to rust, which is probably overall good for performance.

Security-wise, this seems equivalent to me, since we need to know the keys to generate the proof, and if the keys hashes aren't equivalent, we won't be able to generate the correct root hashes
