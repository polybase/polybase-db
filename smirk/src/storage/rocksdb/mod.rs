use std::{collections::HashMap, fmt::Debug, path::Path};

use serde::{Deserialize, Serialize};

use crate::{hash::Digest, tree::TreeNode, MerkleTree};

use super::{Error, Storage};

pub struct RocksDb {
    inner: rocksdb::TransactionDB,
}

impl Debug for RocksDb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RocksDb")
    }
}

impl RocksDb {
    pub fn open(path: &Path) -> Result<Self, rocksdb::Error> {
        let inner = rocksdb::TransactionDB::open_default(path)?;

        Ok(Self { inner })
    }
}

/// Struct to represent structure of the tree without storing the actual data
///
/// json was chosen as an "obviously bad" encoding - we should decide on a proper representation
#[derive(Debug, Clone, Serialize, Deserialize)]
struct NodeStructure {
    hash: Digest,
    left: Option<Box<NodeStructure>>,
    right: Option<Box<NodeStructure>>,
}

impl Storage for RocksDb {
    // fn store_tree<T: Serialize>(&self, tree: &MerkleTree<T>) -> Result<(), Error> {
    //     let tx = self.inner.transaction();
    //     for elem in tree.depth_first() {
    //         let key = Digest::calculate(elem.as_ref());
    //         let value = elem.as_ref();
    //
    //         tx.put(&key.to_bytes(), value)?;
    //     }
    //
    //     let structure_bytes =
    //         serde_json::to_string(&tree.inner.as_ref().map(|node| get_structure(node)))?;
    //
    //     tx.put("structure".as_bytes(), structure_bytes)?;
    //
    //     tx.commit()?;
    //
    //     Ok(())
    // }
    //
    // fn load_tree<T: Clone + From<Vec<u8>>>(&self) -> Result<Option<MerkleTree<T>>, Error> {
    //     let tx = self.inner.transaction();
    //
    //     let Some(structure) = tx.get("structure".as_bytes())? else { return Ok(None) };
    //     let structure = serde_json::from_str(
    //         &String::from_utf8(structure).expect("we're not actually going to use json"),
    //     )?;
    //
    //     let Some(structure) = structure else { return Ok(Some(MerkleTree { inner: None })) };
    //
    //     let mut data = HashMap::new();
    //
    //     for result in tx.iterator(rocksdb::IteratorMode::Start).into_iter() {
    //         let (key, value) = result?;
    //         let Some(hash) = Digest::decode(&key) else { continue };
    //         let value = value.to_vec().into();
    //
    //         data.insert(hash, value);
    //     }
    //
    //     let mut tree = rebuild_tree(structure, &data)?;
    //     tree.update_height();
    //
    //     Ok(Some(MerkleTree {
    //         inner: Some(Box::new(tree)),
    //     }))
    // }
}

// fn rebuild_tree<T: Clone>(
//     structure: NodeStructure,
//     data: &HashMap<Digest, T>,
// ) -> Result<TreeNode<T>, Error> {
//     let this = data
//         .get(&structure.hash)
//         .ok_or(Error::MissingKeyReferenced(structure.hash))?;
//
//     let left = structure
//         .left
//         .map(|structure| rebuild_tree(*structure, data))
//         .transpose()?
//         .map(Box::new);
//
//     let right = structure
//         .right
//         .map(|structure| rebuild_tree(*structure, data))
//         .transpose()?
//         .map(Box::new);
//
//     Ok(TreeNode {
//         value: this.clone(),
//         left,
//         right,
//         height: 0,
//     })
// }
//
// fn get_structure<T: AsRef<[u8]>>(node: &TreeNode<T>) -> NodeStructure {
//     NodeStructure {
//         hash: node.hash(),
//         left: node.left.as_ref().map(|node| Box::new(get_structure(node))),
//         right: node
//             .right
//             .as_ref()
//             .map(|node| Box::new(get_structure(node))),
//     }
// }
