use std::{backtrace::Backtrace, borrow::Borrow, fmt::Debug};

use super::Digest;

impl<H> FromIterator<H> for Digest
where
    H: Borrow<Digest> + Debug,
{
    fn from_iter<T: IntoIterator<Item = H>>(iter: T) -> Self {
        let vec: Vec<_> = iter.into_iter().collect();
        let mut iter = vec.iter();

        let Some(hash) = iter.next() else { return Digest::NULL };
        let mut hash = *hash.borrow();

        for new_hash in iter {
            hash.merge(new_hash.borrow());
        }

        if format!("{hash}")
            .contains("e54944d3c80d00cc318e861d5d56c76a2b1bf9e7638422c0ec636e48ae8b4c0f")
        {
            let bt = Backtrace::capture();
            println!("{bt}");
            println!("{vec:?}");
            // panic!("uh oh");
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
