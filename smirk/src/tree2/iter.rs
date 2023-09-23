use super::*;

impl<K, V> FromIterator<(K, V)> for Tree2<K, V>
where
    K: Hashable,
    V: Hashable,
{
    fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
        let vec: Vec<_> = iter.into_iter().collect();
        let mut tree = Tree2::with_capacity(vec.len());
        todo!();
        tree
    }
}

pub struct IntoIter<K, V> {
    nodes: Vec<Node2<K, V>>,
}

impl<K, V> IntoIterator for Tree2<K, V> {
    type Item = (K, V);
    type IntoIter = IntoIter<K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let nodes = self.node.into_iter().map(|b| *b).collect();
        IntoIter { nodes }
    }
}

impl<K, V> Iterator for IntoIter<K, V> {
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.nodes.pop()?;
        match next {
            Node2::EmptyLeaf => None,
            Node2::Leaf { key, value, .. } => Some((key, value)),
            Node2::Parent { left, right, .. } => {
                if let Some(right) = right {
                    self.nodes.push(*right);
                }
                if let Some(left) = left {
                    self.nodes.push(*left);
                }
                self.next()
            }
        }
    }
}

pub struct Iter<'a, K, V> {
    nodes: Vec<&'a Node2<K, V>>,
}

impl<'a, K, V> IntoIterator for &'a Tree2<K, V> {
    type Item = (&'a K, &'a V);
    type IntoIter = Iter<'a, K, V>;

    fn into_iter(self) -> Self::IntoIter {
        let nodes = self.node.as_deref().into_iter().collect();
        Iter { nodes }
    }
}

impl<'a, K, V> Iterator for Iter<'a, K, V> {
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.nodes.pop()?;
        match next {
            Node2::EmptyLeaf => None,
            Node2::Leaf { key, value, .. } => Some((key, value)),
            Node2::Parent { left, right, .. } => {
                if let Some(right) = right {
                    self.nodes.push(right.as_ref());
                }
                if let Some(left) = left {
                    self.nodes.push(left.as_ref());
                }
                self.next()
            }
        }
    }
}

impl<K, V> Tree2<K, V> {
    pub fn iter(&self) -> Iter<K, V> {
        self.into_iter()
    }

    /// This shouldn't be public, becasue we need to make sure we update the hashes after granting
    /// mutable access to the nodes
    fn iter_mut(&self) -> Box<dyn Iterator<Item = &mut (K, V)>> {
        todo!()
    }
}


#[cfg(test)]
mod tests {
    use test_strategy::proptest;

    use super::*;

    #[proptest]
    fn to_from_iter_round_trip(mut items: Vec<(i32, String)>) {
        items.sort_unstable_by_key(|item| item.0);

        let tree: Tree2<_, _> = items.clone().into_iter().collect();
        let items_again: Vec<_> = tree.into_iter().collect();

        assert_eq!(items, items_again);
    }
}
