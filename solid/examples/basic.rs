use futures::StreamExt;
use solid::config::SolidConfig;
use solid::event::SolidEvent;
use solid::peer::PeerId;
use solid::proposal::ProposalManifest;
use solid::Solid;
use std::time::Duration;
use std::vec;

use tracing::info;
use tracing_subscriber::{filter::EnvFilter, layer::SubscriberExt};

#[tokio::main]
async fn main() {
    let local_peer_id = PeerId::random();

    // Logging
    let stdout_tracer = tracing_subscriber::fmt::layer().compact();
    let filter = EnvFilter::try_new("warn")
        .unwrap()
        .add_directive("basic=info".parse().unwrap());
    let subscriber = tracing_subscriber::registry()
        .with(stdout_tracer)
        .with(filter);

    tracing::subscriber::set_global_default(subscriber).unwrap();

    // Create a new solid instance
    let mut solid = Solid::genesis(
        local_peer_id.clone(),
        vec![local_peer_id.clone()],
        SolidConfig::default(),
    );

    // Start the service
    solid.run();

    // Start
    loop {
        tokio::select! {
            Some(event) = solid.next() => {
                match event {
                    // Node should send accept for an active proposal
                    // to another peer
                    SolidEvent::Accept { accept } => {
                        info!(height = &accept.height, skips = &accept.skips, to = &accept.leader_id.prefix(), hash = accept.proposal_hash.to_string(), "Send accept");
                    }

                    // Node should create and send a new proposal
                    SolidEvent::Propose {
                        last_proposal_hash,
                        height,
                        skips,
                    } => {
                        // Get changes from the pending changes cache
                        let txns = vec![];

                        // Simulate delay
                        tokio::time::sleep(Duration::from_secs(1)).await;

                        // Create the proposl manfiest
                        let manifest = ProposalManifest {
                            last_proposal_hash,
                            skips,
                            height,
                            leader_id: local_peer_id.clone(),
                            txns,
                            peers: vec![local_peer_id.clone()]
                        };
                        let proposal_hash = manifest.hash();

                        info!(hash = proposal_hash.to_string(), height = height, skips = skips, "Propose");

                        // Add proposal to own register, this will trigger an accept
                        solid.receive_proposal(manifest.clone());
                    }

                    // Commit a confirmed proposal changes
                    SolidEvent::Commit { manifest } => {
                        info!(hash = manifest.hash().to_string(), height = manifest.height, skips = manifest.skips, "Commit");
                    }

                    SolidEvent::OutOfSync {
                        height,
                        max_seen_height,
                        accepts_sent,
                    } => {
                        info!(local_height = height, accepts_sent = accepts_sent, max_seen_height = max_seen_height, "Out of sync");
                    }

                    SolidEvent::OutOfDate {
                        local_height,
                        proposal_height,
                        proposal_hash,
                        peer_id,
                    } => {
                        info!(local_height = local_height, proposal_height = proposal_height, proposal_hash = proposal_hash.to_string(), from = peer_id.prefix(), "Out of date proposal");
                    }

                    SolidEvent::DuplicateProposal { proposal_hash } => {
                        info!(hash = proposal_hash.to_string(), "Duplicate proposal");
                    }
                }
            }
        }
    }
}
