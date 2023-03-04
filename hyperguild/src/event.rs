use crate::change::Change;
use crate::proposal::manifest::ProposalManifest;
use libp2p_core::PeerId;
use serde::{de, Deserialize, Deserializer, Serialize};

pub enum NetworkEvent {
    PeerJoin { peerId: PeerId },
    PeerLeave { peerId: PeerId },
    PeerEvent { peerId: PeerId, event: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum GuildEvent {
    /// Propose a commit
    Proposal {
        proposal_manifest: ProposalManifest,
        proposal_hash: Vec<u8>,
    },

    /// Accept a proposal, this is a vote this node
    /// to become the next leader (and create a proposal)
    Accept {
        proposal_hash: Vec<u8>,
    },

    /// Add a set of pending txns to the queue
    Pending {
        changes: Vec<Change>,
    },

    // Share consensus state with other members this is usually
    // just used on join, so nodes can quickly catch up if needed
    State {
        root: Vec<u8>,
    },

    Snapshot {
        from: Vec<u8>,
    },

    Restore {
        from: Vec<u8>,
        data: Vec<u8>,
    },
}

#[derive(Debug)]
struct Peer(PeerId);

impl Serialize for Peer {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let v: Vec<u8> = self.0.into();
        serializer.serialize_bytes(v.as_slice())
    }
}

impl<'de> Deserialize<'de> for Peer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct PeerVisitor;

        impl<'de> de::Visitor<'de> for PeerVisitor {
            type Value = Peer;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Peer")
            }

            fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(Peer(PeerId::from_bytes(&v).unwrap()))
            }
        }

        deserializer.deserialize_bytes(PeerVisitor)
    }
}
