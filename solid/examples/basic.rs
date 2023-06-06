#[macro_use]
extern crate slog;
extern crate slog_async;
extern crate slog_term;

use futures::StreamExt;
use slog::{Drain, Level};
use solid::config::SolidConfig;
use solid::event::SolidEvent;
use solid::peer::PeerId;
use solid::proposal::ProposalManifest;
use solid::Solid;
use std::time::Duration;
use std::vec;

#[tokio::main]
async fn main() {
    let local_peer_id = PeerId::random();

    // Logging
    let decorator = slog_term::PlainDecorator::new(std::io::stdout());
    let drain = slog_term::CompactFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let drain = slog::LevelFilter::new(drain, Level::Info).fuse();
    let logger = slog::Logger::root(drain, o!());

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
                        info!(logger, "Send accept"; "height" => &accept.height, "skips" => &accept.skips, "to" => &accept.leader_id.prefix(), "hash" => accept.proposal_hash.to_string());
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

                        info!(logger, "Propose"; "hash" => proposal_hash.to_string(), "height" => height, "skips" => skips);

                        // Add proposal to own register, this will trigger an accept
                        solid.receive_proposal(manifest.clone());
                    }

                    // Commit a confirmed proposal changes
                    SolidEvent::Commit { manifest } => {
                        info!(logger, "Commit"; "hash" => manifest.hash().to_string(), "height" => manifest.height, "skips" => manifest.skips);
                    }

                    SolidEvent::OutOfSync {
                        height,
                        max_seen_height,
                        accepts_sent,
                    } => {
                        info!(logger, "Out of sync"; "local_height" => height, "accepts_sent" => accepts_sent, "max_seen_height" => max_seen_height);
                    }

                    SolidEvent::OutOfDate {
                        local_height,
                        proposal_height,
                        proposal_hash,
                        peer_id,
                    } => {
                        info!(logger, "Out of date proposal"; "local_height" => local_height, "proposal_height" => proposal_height, "proposal_hash" => proposal_hash.to_string(), "from" => peer_id.prefix());
                    }

                    SolidEvent::DuplicateProposal { proposal_hash } => {
                        info!(logger, "Duplicate proposal"; "hash" => proposal_hash.to_string());
                    }
                }
            }
        }
    }
}
