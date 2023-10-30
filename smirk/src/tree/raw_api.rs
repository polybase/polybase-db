use crate::{Element, Tree};

impl<const DEPTH: usize> Tree<DEPTH> {
    pub(crate) fn raw_insert(&mut self, element: Element, update_hashes: bool) -> bool {
        assert_ne!(
            element,
            Element::NULL_HASH,
            "`Element::NULL_HASH` is used to represent absent values, so cannot be inserted"
        );

        let changed = self.elements.insert(element);

        if changed {
            // if the tree has depth n, we need n-1 bits, since there are n-1 left/right decisions
            let bits = element.lsb(DEPTH - 1);
            self.tree.insert(element, &bits);
        }

        if update_hashes {
            self.tree.recalculate_hashes();
        }

        changed
    }
}
