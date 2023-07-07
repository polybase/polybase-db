use super::{MerkleTree, TreeNode};

pub trait Visitor<T> {
    fn visit(&mut self, item: &T);
}

impl<T, V> Visitor<T> for &mut V
where
    V: Visitor<T>,
{
    fn visit(&mut self, item: &T) {
        V::visit(self, item)
    }
}

impl<T> MerkleTree<T> {
    pub fn visit<V: Visitor<T>>(&self, mut visitor: V) {
        if let Some(inner) = &self.inner {
            inner.visit(&mut visitor);
        }
    }
}

impl<T> TreeNode<T> {
    pub fn visit<V: Visitor<T>>(&self, visitor: &mut V) {
        visitor.visit(&self.value);

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

        impl<T> Visitor<T> for Counter {
            fn visit(&mut self, _item: &T) {
                self.0 += 1;
            }
        }

        let tree = MerkleTree::from_iter([1, 2, 3]);
        let mut counter = Counter(0);
        tree.visit(&mut counter);

        assert_eq!(counter.0, 3);
    }

    #[test]
    fn sum_example() {
        struct Sum(i32);

        impl Visitor<i32> for Sum {
            fn visit(&mut self, item: &i32) {
                self.0 += *item;
            }
        }

        let tree = MerkleTree::from_iter([1, 2, 3]);
        let mut sum = Sum(0);
        tree.visit(&mut sum);

        assert_eq!(sum.0, 6);

    }
}
