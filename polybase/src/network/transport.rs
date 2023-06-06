use futures::future::Either;
use libp2p::{
    core::{
        muxing::StreamMuxerBox,
        transport::{Boxed, OrTransport},
        upgrade,
    },
    dns,
    identity::Keypair,
    noise, tcp, yamux, PeerId, Transport,
};
use libp2p_quic as quic;

type BoxedTransport = Boxed<(PeerId, StreamMuxerBox)>;

/// Create the transports for the swarm, we use TCP/IP and quic.
pub fn create_transport(keypair: &Keypair) -> BoxedTransport {
    // Set up an encrypted DNS-enabled TCP Transport over the yamux protocol.
    #[allow(clippy::expect_used)]
    let tcp_transport = tcp::tokio::Transport::new(tcp::Config::default().nodelay(true))
        .upgrade(upgrade::Version::V1Lazy)
        .authenticate(noise::Config::new(keypair).expect("signing libp2p-noise static keypair"))
        .multiplex(yamux::Config::default())
        .timeout(std::time::Duration::from_secs(20))
        .boxed();

    #[allow(clippy::expect_used)]
    let dns_tcp_transport =
        dns::TokioDnsConfig::system(tcp_transport).expect("Failed to create DNS transport");

    let quic_transport = quic::tokio::Transport::new(quic::Config::new(keypair));
    OrTransport::new(quic_transport, dns_tcp_transport)
        .map(|either_output, _| match either_output {
            Either::Left((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
            Either::Right((peer_id, muxer)) => (peer_id, StreamMuxerBox::new(muxer)),
        })
        .boxed()
}
