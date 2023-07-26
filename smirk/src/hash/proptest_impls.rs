use super::{Digest, RpoDigest};

use miden_crypto::Felt;
use proptest::{arbitrary::StrategyFor, prelude::*, strategy::Map};

impl Arbitrary for Digest {
    type Parameters = ();
    type Strategy = Map<StrategyFor<[u64; 4]>, fn([u64; 4]) -> Self>;

    fn arbitrary_with(_: Self::Parameters) -> Self::Strategy {
        any::<[u64; 4]>().prop_map(|nums| Digest(RpoDigest::new(nums.map(Felt::new))))
    }
}
