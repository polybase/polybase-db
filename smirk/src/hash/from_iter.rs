use std::{borrow::Borrow, fmt::Debug};

use super::Digest;

impl<H> FromIterator<H> for Digest
where
    H: Borrow<Digest> + Debug,
{
    fn from_iter<T: IntoIterator<Item = H>>(iter: T) -> Self {
        // we actually don't want to do an optimization for the len = 1 case, since that's slower
        // in miden
        
        let mut hash = Digest::NULL;

        for h in iter {
            hash.merge(h.borrow());
        }

        hash
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Cow, rc::Rc, sync::Arc};

    use crate::hash::Hashable;

    use super::*;

    #[test]
    fn can_collect_various_types() {
        let mut d = Digest::calculate(&[]);

        let _: Digest = [d].into_iter().collect();
        let _: Digest = [&d].into_iter().collect();
        let _: Digest = [&mut d].into_iter().collect();
        let _: Digest = [Box::new(d)].into_iter().collect();
        let _: Digest = [Cow::Owned(d)].into_iter().collect();
        let _: Digest = [Rc::new(d)].into_iter().collect();
        let _: Digest = [Arc::new(d)].into_iter().collect();
    }

    #[test]
    fn collecting_empty_hash_is_null() {
        let hash: Digest = Vec::<Digest>::new().into_iter().collect();
        assert_eq!(hash, Digest::NULL);
    }

    #[test]
    fn collecting_single_hash_is_unchanged() {
        let hash: Digest = vec![1.hash()].iter().collect();
        assert_eq!(hash, 1.hash());
    }

    #[test]
    fn collecting_multiple_hashes() {
        let hash: Digest = [1.hash(), "hello".hash(), [1u8, 2, 3].hash()]
            .iter()
            .collect();

        let mut expected = 1.hash();
        expected.merge(&"hello".hash());
        expected.merge(&[1u8, 2, 3].hash());

        assert_eq!(hash, expected);
    }
}
