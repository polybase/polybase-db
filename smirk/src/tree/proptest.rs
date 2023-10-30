use super::*;
use ::proptest::{arbitrary::StrategyFor, prelude::*, strategy::Map};

impl<const DEPTH: usize> Arbitrary for Tree<DEPTH> {
    type Parameters = ();
    type Strategy = Map<StrategyFor<BTreeSet<Element>>, fn(BTreeSet<Element>) -> Self>;

    fn arbitrary_with((): Self::Parameters) -> Self::Strategy {
        any::<BTreeSet<Element>>().prop_map(|set| set.into_iter().collect())
    }
}
