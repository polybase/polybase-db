use crate::proposal::{Proposal, ProposalHash};
use std::cmp::Ordering;
use std::collections::HashMap;

/// Responsible for storing proposals temporarily in the cache.
/// Provides untility methods for easily traversing proposals.
#[derive(Debug)]
pub struct ProposalCache {
    /// Hash of last confirmed proposal, we can work backwards from here to get
    /// all other confirmed proposals in the cache.
    last_confirmed_proposal_hash: ProposalHash,

    /// List of proposals cached in memory
    proposals: HashMap<ProposalHash, Proposal>,

    /// Max confirmed height seen across all received proposals
    max_height: usize,

    /// Config for the proposal cache
    cache_size: usize,
}

impl ProposalCache {
    pub fn new(last_confirmed_proposal: Proposal, cache_size: usize) -> Self {
        let proposal_hash = last_confirmed_proposal.hash().clone();
        let max_height: usize = last_confirmed_proposal.height();
        let mut proposals = HashMap::new();

        // Add last confirmed proposal to pending proposals
        proposals.insert(proposal_hash.clone(), last_confirmed_proposal);

        ProposalCache {
            last_confirmed_proposal_hash: proposal_hash,
            proposals,
            max_height,
            cache_size,
        }
    }

    /// Confirmed height
    pub fn height(&self) -> usize {
        // We can use unwrap because last_confirmed_proposal_hash must always be set
        #[allow(clippy::unwrap_used)]
        self.proposals
            .get(&self.last_confirmed_proposal_hash)
            .unwrap()
            .height()
    }

    /// Max height seen across all proposals
    pub fn max_height(&self) -> usize {
        self.max_height
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.proposals.len()
    }

    /// Check if a proposal exists in the cache
    pub fn contains(&self, hash: &ProposalHash) -> bool {
        self.proposals.contains_key(hash)
    }

    /// Insert a proposal into the cache
    pub fn insert(&mut self, proposal: Proposal) {
        if proposal.height() > self.max_height {
            self.max_height = proposal.height();
        }

        self.proposals.insert(proposal.hash().clone(), proposal);
    }

    /// Get a proposal by hash (mutable)
    pub fn get_mut(&mut self, proposal_hash: &ProposalHash) -> Option<&mut Proposal> {
        self.proposals.get_mut(proposal_hash)
    }

    /// Get the last confirmed proposal
    pub fn last_confirmed_proposal(&self) -> &Proposal {
        // We can use unwrap because last_confirmed_proposal_hash must always be set
        #[allow(clippy::unwrap_used)]
        self.proposals
            .get(&self.last_confirmed_proposal_hash)
            .unwrap()
    }

    /// Returns all confirmed proposals from height to confirmed proposal
    pub fn confirmed_proposals_from(&self, from_height: usize) -> Vec<&Proposal> {
        // Start with the last confirmed proposal and work backwards
        // We can use unwrap because last_confirmed_proposal_hash must always be set
        #[allow(clippy::unwrap_used)]
        let mut proposal = self
            .proposals
            .get(&self.last_confirmed_proposal_hash)
            .unwrap();

        let mut proposals = vec![proposal];

        // Loop through to get the next proposal by looking at the chain of proposals
        while proposal.height() >= from_height {
            if let Some(p) = self.proposals.get(proposal.last_hash()) {
                proposals.push(p);
                proposal = p
            } else {
                return proposals;
            }
        }

        proposals
    }

    /// Returns next pending proposal to be processed - either as a commit or accept. If there is a
    /// gap in the chain or no pending commits exist, then None is returned.
    pub fn next_pending_proposal(&self, offset: usize) -> Option<&Proposal> {
        // Get the first proposal, by looking for the proposal with the highest height
        // and skip
        let mut proposal = self.max_proposal()?;

        // Loop through to get the next proposal by looking at the chain of proposals
        while proposal.height() > self.height() + 1 + offset {
            proposal = self.proposals.get(proposal.last_hash())?;
        }

        Some(proposal)
    }

    /// Proposal with largest height, skip which is valid proposal in the store
    /// (it includes the last confirmed proposal in the tree)
    pub fn max_proposal(&self) -> Option<&Proposal> {
        self.proposals
            .values()
            .filter(|proposal| proposal.height() == self.max_height)
            .max_by(|a, b| a.skips().cmp(&b.skips()))
    }

    /// Confirm a proposal, all subsequent proposals must now
    /// include this proposal in the tree.
    pub fn confirm(&mut self, proposal_hash: ProposalHash) {
        self.last_confirmed_proposal_hash = proposal_hash;
        self.purge();
    }

    /// Check if a hash is a decendent of another hash
    fn is_decendent(&self, decendent_hash: &ProposalHash, parent_hash: &ProposalHash) -> bool {
        let mut proposal = match self.proposals.get(parent_hash) {
            Some(p) => p,
            None => return false,
        };

        loop {
            if proposal.hash() == decendent_hash {
                return true;
            }

            proposal = match self.proposals.get(proposal.last_hash()) {
                Some(p) => p,
                None => return false,
            };
        }
    }

    /// Remove redundant proposals from the cache
    fn purge(&mut self) {
        let confirmed_height = self.height();
        let confirmed_hash = self.last_confirmed_proposal_hash.clone();

        let keys_to_remove = self
            .proposals
            .iter()
            .filter(|(_, p)| match confirmed_height.partial_cmp(&p.height()) {
                Some(Ordering::Greater) => {
                    if p.height() + self.cache_size < confirmed_height {
                        return true;
                    }
                    false
                }
                Some(Ordering::Less) => !self.is_decendent(&confirmed_hash, p.hash()),
                Some(Ordering::Equal) => p.hash() != &self.last_confirmed_proposal_hash,
                None => true,
            })
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();

        for key in keys_to_remove {
            self.proposals.remove(&key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer::PeerId;
    use crate::proposal::{ProposalHash, ProposalManifest};

    fn create_peers() -> [PeerId; 3] {
        let p1 = PeerId::new(vec![1u8]);
        let p2 = PeerId::new(vec![2u8]);
        let p3 = PeerId::new(vec![3u8]);
        [p1, p2, p3]
    }

    fn create_proposal(
        height: usize,
        skips: usize,
        last_proposal_hash: ProposalHash,
    ) -> (Proposal, ProposalHash) {
        let m = ProposalManifest {
            last_proposal_hash,
            height,
            skips,
            leader_id: PeerId::new(vec![1u8]),
            changes: vec![],
            peers: create_peers().to_vec(),
        };
        let m_hash = m.hash();
        (Proposal::new(m), m_hash)
    }

    #[test]
    fn test_new_cache() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let cache = ProposalCache::new(genesis.clone(), 1000);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.height(), 0);
        assert_eq!(cache.max_height(), 0);
        assert_eq!(cache.last_confirmed_proposal(), &genesis);
        assert!(cache.contains(&genesis_hash), "contains genesis hash");
    }

    #[test]
    fn test_insert() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let mut cache = ProposalCache::new(genesis, 1000);

        let (p1, _) = create_proposal(1, 0, genesis_hash);
        cache.insert(p1);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.height(), 0);
        assert_eq!(cache.max_height(), 1);
    }

    #[test]
    fn test_confirm_proposal() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let mut cache = ProposalCache::new(genesis, 1000);

        let (p1, p1_hash) = create_proposal(1, 0, genesis_hash);
        cache.insert(p1.clone());
        cache.confirm(p1_hash);

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.height(), 1);
        assert_eq!(cache.max_height(), 1);
        assert_eq!(cache.last_confirmed_proposal(), &p1);
    }

    #[test]
    fn test_is_decendent() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let mut cache = ProposalCache::new(genesis, 1000);

        let (p1, p1_hash) = create_proposal(1, 0, genesis_hash.clone());
        let (p2, p2_hash) = create_proposal(2, 0, p1_hash.clone());
        let (p3, p3_hash) = create_proposal(3, 0, genesis_hash.clone());
        cache.insert(p1);
        cache.insert(p2);
        cache.insert(p3);

        assert!(
            cache.is_decendent(&genesis_hash, &p2_hash),
            "genesis is decendent of p2"
        );

        assert!(
            cache.is_decendent(&genesis_hash, &p3_hash),
            "genesis is decendent of p3"
        );

        assert!(
            !cache.is_decendent(&p2_hash, &genesis_hash),
            "p2 is not decendent of genesis"
        );

        assert!(
            !cache.is_decendent(&p1_hash, &p3_hash),
            "p1 not is decendent of p3"
        );
    }

    #[test]
    fn test_purge_proposals() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let mut cache = ProposalCache::new(genesis, 1000);

        let (p1a, p1a_hash) = create_proposal(1, 0, genesis_hash);
        let (p1b, p1b_hash) = create_proposal(1, 1, ProposalHash::new(vec![1u8]));
        // let (p2a, p2a_hash) = create_proposal(2, 0, p1_hash.clone());
        // let (p2b, p2b_hash) = create_proposal(2, 1, p1_hash);

        cache.insert(p1a);
        cache.insert(p1b);

        assert_eq!(cache.len(), 3);

        cache.purge();

        assert_eq!(cache.len(), 2);
        assert!(cache.contains(&p1a_hash), "p1a should  not be purged");
        assert!(!cache.contains(&p1b_hash), "p1b should be purged");

        let mut last_hash = p1a_hash;
        for i in 2..1010 {
            let (p, h) = create_proposal(i, 0, last_hash);
            last_hash = h.clone();
            cache.insert(p);
        }

        cache.confirm(last_hash);

        assert_eq!(cache.len(), 1001);
    }

    #[test]
    fn test_next_pending_proposal() {
        let (genesis, genesis_hash) = create_proposal(0, 0, ProposalHash::genesis());
        let mut cache = ProposalCache::new(genesis, 1000);

        let (p1, p1_hash) = create_proposal(1, 0, genesis_hash);
        let (p2a, p2a_hash) = create_proposal(2, 0, p1_hash.clone());
        let (p2b, p2b_hash) = create_proposal(2, 1, p1_hash);
        let (p3a, _) = create_proposal(3, 0, p2a_hash.clone());
        let (p3b, _) = create_proposal(3, 1, p2b_hash);

        cache.insert(p1.clone());
        cache.insert(p2a);
        cache.insert(p2b);
        cache.insert(p3a.clone());
        cache.insert(p3b);

        assert_eq!(cache.len(), 6);
        assert_eq!(cache.height(), 0);
        assert_eq!(cache.max_height(), 3);
        assert_eq!(cache.next_pending_proposal(0), Some(&p1));

        cache.confirm(p2a_hash);

        assert_eq!(cache.next_pending_proposal(0), Some(&p3a));
    }
}
