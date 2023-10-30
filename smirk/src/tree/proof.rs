use crate::{Element, Insert, Proof, Tree};

impl<const DEPTH: usize> Tree<DEPTH> {
    /// Insert a value into the tree, and return a cryptographic proof that the insert was correct.
    ///
    /// In particular, the proof proves:
    /// given a tree with publicly-known root hash `A`, and a private element to insert, that the
    /// tree has root hash `B` after the element is inserted.
    ///
    /// ```rust
    /// # use smirk::*;
    /// let tree = Tree::<64>::new();
    ///
    /// let proof = tree.insert_and_prove(Element::ONE).unwrap();
    /// ```
    ///
    /// This method returns an `Option` because it is not possible to prove an insert for a value
    /// that is in the tree already.
    ///
    /// The effect of this function on the tree is identical to the effect of [`Tree::insert`].
    ///
    /// If you don't need the proof, [`Tree::insert`] will be **much** faster, since proof
    /// generation is very slow
    pub fn insert_and_prove(&mut self, element: Element) -> Option<Proof<Insert>> {
        todo!()
    }
}
