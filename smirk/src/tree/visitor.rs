use super::{MerkleTree, TreeNode};

/// A trait for types which can visit nodes in a Merkle tree
///
/// Note: currently only immutable access is given to prevent invalidating memoized hashes
pub trait Visitor<K, V> {
    /// The function to be called on each node
    fn visit(&mut self, key: &K, value: &V);
}

impl<Vis, K, V> Visitor<K, V> for &mut Vis
where
    Vis: Visitor<K, V>,
{
    fn visit(&mut self, key: &K, value: &V) {
        Vis::visit(self, key, value);
    }
}

impl<K, V> MerkleTree<K, V> {
    /// Apply a visitor to all the nodes in a tree
    ///
    /// The visitor will run on `self`, then `left` (if it is `Some`), then `right` (if it is `Some`)
    pub fn visit<Vis: Visitor<K, V>>(&self, mut visitor: Vis) {
        if let Some(inner) = &self.inner {
            inner.visit(&mut visitor);
        }
    }
}

impl<K, V> TreeNode<K, V> {
    fn visit<Vis: Visitor<K, V>>(&self, visitor: &mut Vis) {
        visitor.visit(&self.key, &self.value);

        if let Some(left) = &self.left {
            left.visit(visitor);
        }

        if let Some(right) = &self.right {
            right.visit(visitor);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counter_example() {
        struct Counter(usize);

        impl<K, V> Visitor<K, V> for Counter {
            fn visit(&mut self, _: &K, _: &V) {
                self.0 += 1;
            }
        }

        let tree = MerkleTree::from_iter([(1, 1), (2, 2), (3, 3)]);
        let mut counter = Counter(0);
        tree.visit(&mut counter);

        assert_eq!(counter.0, 3);
    }

    #[test]
    fn sum_example() {
        struct Sum(i32);

        impl Visitor<i32, i32> for Sum {
            fn visit(&mut self, key: &i32, _value: &i32) {
                self.0 += *key;
            }
        }

        let tree = MerkleTree::from_iter([(1, 1), (2, 2), (3, 3)]);
        let mut sum = Sum(0);
        tree.visit(&mut sum);

        assert_eq!(sum.0, 6);
    }
}
