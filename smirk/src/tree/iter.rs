use std::{collections::btree_set, iter::Copied};

use crate::{Element, Tree};

impl<const DEPTH: usize> FromIterator<Element> for Tree<DEPTH> {
    fn from_iter<T: IntoIterator<Item = Element>>(iter: T) -> Self {
        let mut tree = Self::new();
        tree.insert_all(iter.into_iter());
        tree
    }
}

#[derive(Debug)]
pub struct Elements<'a> {
    inner: Copied<btree_set::Iter<'a, Element>>,
}

impl<'a> Iterator for Elements<'a> {
    type Item = Element;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<const DEPTH: usize> Tree<DEPTH> {
    /// Get an iterator over the elements in this set
    ///
    /// ```rust
    /// # use smirk::*;
    /// let mut tree = Tree::<64>::new();
    ///
    /// tree.insert(Element::from(1));
    /// tree.insert(Element::from(2));
    /// tree.insert(Element::from(3));
    ///
    /// let vec: Vec<Element> = tree.elements().collect();
    ///
    /// assert_eq!(vec, vec![
    ///   Element::from(1),
    ///   Element::from(2),
    ///   Element::from(3),
    /// ]);
    /// ```
    #[inline]
    #[doc(alias = "iter")]
    pub fn elements(&self) -> Elements {
        let inner = self.elements.iter().copied();
        Elements { inner }
    }
}

impl<const N: usize> Extend<Element> for Tree<N> {
    fn extend<T: IntoIterator<Item = Element>>(&mut self, iter: T) {
        self.insert_all(iter.into_iter());
    }
}
