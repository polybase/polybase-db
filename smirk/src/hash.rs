use std::sync::OnceLock;

use halo2_gadgets::poseidon::primitives::{Hash, P128Pow5T3};

use crate::Element;

/// The hash of an empty tree with a given depth
///
/// This function can be defined recursively:
///  - `empty_tree_hash(1) = Element::NULL_HASH`
///  - `empty_tree_hash(n) = hash_merge(empty_tree_hash(n - 1), empty_tree_hash(n - 1))`
///
/// # Panics
///
/// Panics if `depth` is 0, since there is no such thing as a tree with depth 0
pub fn empty_tree_hash(depth: usize) -> Element {
    static CACHE: OnceLock<Vec<Element>> = OnceLock::new();

    assert_ne!(depth, 0, "the smallest possible tree has depth 1");

    let cache = CACHE.get_or_init(|| {
        const COMPUTE_DEPTH: usize = 128;

        let mut vec = Vec::with_capacity(COMPUTE_DEPTH);
        vec.push(Element::NULL_HASH);

        for _ in 1..COMPUTE_DEPTH {
            let hash = *vec.last().unwrap();
            let new_hash = hash_merge(hash, hash);
            vec.push(new_hash);
        }

        vec
    });

    cache[depth - 1]
}

/// Hash two elements together
///
/// This function is used to calculate the hash of a parent node from the hash of its children,
/// i.e.: `parent_hash = hash_merge(left_hash, right_hash)`
///
/// ```rust
/// # use smirk::*;
/// let a = hash_merge(Element::from(1), Element::from(2));
/// let b = hash_merge(Element::from(1), Element::from(3));
/// let c = hash_merge(Element::from(3), Element::from(2));
///
/// assert_ne!(a, b);
/// assert_ne!(a, c);
/// assert_ne!(b, c);
/// ```
/// This operation is not symmetric:
/// ```rust
/// # use smirk::*;
/// let a = Element::from(1);
/// let b = Element::from(2);
///
/// let ab = hash_merge(a, b);
/// let ba = hash_merge(b, a);
///
/// assert_ne!(ab, ba);
/// ```
#[inline]
#[must_use]
pub fn hash_merge(a: Element, b: Element) -> Element {
    let hash = Hash::<_, P128Pow5T3, _, 3, 2>::init().hash([a.0, b.0]);
    Element(hash)
}
