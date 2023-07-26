use rocksdb::{Transaction, TransactionDB};
use serde::{Deserialize, Serialize};

use crate::{hash::Hashable, MerkleTree, TreeNode};

use super::{Error, Storage};

/// An error encountered when encoding data to its database format
#[derive(Debug, thiserror::Error)]
#[error("encode error: {0}")]
pub struct EncodeError(rmp_serde::encode::Error);

/// An error encountered when decoding data in its database format
#[derive(Debug, thiserror::Error)]
#[error("decode error: {0}")]
pub struct DecodeError(rmp_serde::decode::Error);

#[derive(Debug, Serialize, Deserialize)]
struct NodeFormat {
    value: Vec<u8>,
    left: Option<Vec<u8>>,
    right: Option<Vec<u8>>,
}

/// Note - this function doesn't actually write, the caller needs to call `tx.commit()`
pub(super) fn write_tree_to_tx<K, V>(
    tx: &Transaction<TransactionDB>,
    tree: &MerkleTree<K, V>,
) -> Result<(), Error>
where
    K: Serialize,
    V: Serialize,
{
    let root_value = tree
        .inner
        .as_deref()
        .map(|node| encode(&node.key))
        .transpose()?
        .unwrap_or(vec![]);

    tx.put(Storage::ROOT_KEY, &root_value)?;

    for node in tree.iter() {
        let (key, value) = encode_single_node(node)?;
        println!("writing to {}", hex::encode(&key));
        tx.put(&key, &value)?;
    }

    Ok(())
}

pub(super) fn load_node<K, V>(
    tx: &Transaction<TransactionDB>,
    key: &[u8],
) -> Result<TreeNode<K, V>, Error>
where
    K: for<'de> Deserialize<'de> + Hashable + Ord,
    V: for<'de> Deserialize<'de> + Hashable,
{
    let value_bytes = tx
        .get(key)?
        .ok_or_else(|| Error::KeyMissing(key.to_vec()))?;

    let NodeFormat { value, left, right } = decode(&value_bytes)?;

    let value = decode(&value)?;
    let key = decode(key)?;

    let left = left.map(|key| load_node(tx, &key)).transpose()?;
    let right = right.map(|key| load_node(tx, &key)).transpose()?;

    Ok(TreeNode::new(key, value, left, right))
}

fn encode_single_node<K, V>(node: &TreeNode<K, V>) -> Result<(Vec<u8>, Vec<u8>), Error>
where
    K: Serialize,
    V: Serialize,
{
    let enc = |node: &TreeNode<K, V>| encode(&node.key);

    let value = encode(&node.value)?;
    let left = node.left.as_deref().map(enc).transpose()?;
    let right = node.right.as_deref().map(enc).transpose()?;

    let value = NodeFormat { value, left, right };

    let value_bytes = encode(&value)?;
    let key_bytes = encode(&node.key)?;

    Ok((key_bytes, value_bytes))
}

fn encode<T: Serialize>(t: &T) -> Result<Vec<u8>, EncodeError> {
    rmp_serde::encode::to_vec(t).map_err(EncodeError)
}

fn decode<'de, 'a: 'de, T: Deserialize<'de>>(bytes: &'a [u8]) -> Result<T, DecodeError> {
    rmp_serde::decode::from_slice(bytes).map_err(DecodeError)
}

#[cfg(test)]
mod tests {
    use proptest::prop_assert_eq;
    use test_strategy::{proptest, Arbitrary};

    use crate::hash::Digest;

    use super::*;

    #[derive(Debug, Deserialize, Serialize, Arbitrary, PartialEq, Eq)]
    struct CoolCustomType {
        foo: String,
        bar: Vec<u8>,
        coords: [(i32, i32); 10],
    }

    #[proptest]
    fn encode_decode_bijective(key: Digest, value: CoolCustomType) {
        let bytes = encode(&(&key, &value)).unwrap();
        let (key_again, value_again): (Digest, CoolCustomType) = decode(&bytes).unwrap();

        prop_assert_eq!(key, key_again);
        prop_assert_eq!(value, value_again);
    }
}
