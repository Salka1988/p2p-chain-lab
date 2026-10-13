use std::{future::Future, time::Duration};

use async_trait::async_trait;
use libp2p::{Multiaddr, identity::Keypair};
use p2p_chain_lib_chain::{
    block::{Block, BlockHash},
    store::ChainStore,
};
use p2p_chain_lib_network::{
    PeerAddress, PeerId,
    peer_score::{PeerScoreActor, PeerScoreHandle},
    service::{NetworkConfig, NetworkHandle, NetworkService},
};
use p2p_chain_lib_ports::{NetworkEvent, PeerReputationPort};
use p2p_chain_lib_protocol::messages::{ChainTip, Transaction, TransactionId};
use p2p_chain_lib_sync::SyncService;
use sha2::{Digest, Sha256};
use tokio::{
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_util::sync::CancellationToken;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct NoopReputation;

#[async_trait]
impl PeerReputationPort for NoopReputation {
    async fn report_good_peer(&self, _peer_id: PeerId, _reason: String) -> anyhow::Result<()> {
        Ok(())
    }

    async fn report_bad_peer(&self, _peer_id: PeerId, _reason: String) -> anyhow::Result<()> {
        Ok(())
    }
}

struct TestNetwork {
    handle: NetworkHandle,
    event_rx: mpsc::Receiver<NetworkEvent>,
    cancellation_token: CancellationToken,
    network_task: JoinHandle<anyhow::Result<()>>,
    peer_score_task: JoinHandle<anyhow::Result<()>>,
}

impl TestNetwork {
    async fn start(name: &str, bootstrap_peers: Vec<PeerAddress>) -> anyhow::Result<Self> {
        Self::start_with_limits(name, bootstrap_peers, 64, 16).await
    }

    async fn start_with_limits(
        name: &str,
        bootstrap_peers: Vec<PeerAddress>,
        max_gossip_events_per_second: u16,
        max_block_requests_per_second: u16,
    ) -> anyhow::Result<Self> {
        let (event_tx, event_rx) = mpsc::channel(64);
        let (peer_score_command_tx, peer_score_command_rx) = mpsc::channel(64);
        let (peer_score_event_tx, peer_score_event_rx) = mpsc::channel(64);
        let peer_score = PeerScoreHandle::new(peer_score_command_tx);
        let cancellation_token = CancellationToken::new();

        let peer_score_task = {
            let cancellation_token = cancellation_token.child_token();
            tokio::spawn(async move {
                PeerScoreActor::new(peer_score_command_rx, peer_score_event_tx)
                    .run(cancellation_token)
                    .await
            })
        };

        let (service, handle) = NetworkService::new(
            NetworkConfig {
                listen_addr: PeerAddress::from("/ip4/127.0.0.1/tcp/0"),
                bootstrap_peers,
                node_key_seed: Some(name.to_string()),
                validator_id: None,
                validators: Vec::new(),
                max_gossip_events_per_second,
                max_block_requests_per_second,
            },
            event_tx,
            peer_score,
            peer_score_event_rx,
        )?;

        let network_task = {
            let cancellation_token = cancellation_token.child_token();
            tokio::spawn(async move {
                let result = service.run(cancellation_token).await;
                if let Err(error) = &result {
                    eprintln!("network service failed: {error:?}");
                }
                result
            })
        };

        Ok(Self {
            handle,
            event_rx,
            cancellation_token,
            network_task,
            peer_score_task,
        })
    }

    async fn listening_addr(&mut self) -> anyhow::Result<PeerAddress> {
        self.wait_for_event(|event| match event {
            NetworkEvent::Listening(address) if address.as_str().starts_with("/ip4/127.0.0.1/") => {
                Some(address)
            }
            _ => None,
        })
        .await
    }

    async fn stop(self) -> anyhow::Result<()> {
        self.cancellation_token.cancel();
        self.network_task.await??;
        self.peer_score_task.await??;
        Ok(())
    }

    async fn wait_for_event<T>(
        &mut self,
        mut select: impl FnMut(NetworkEvent) -> Option<T>,
    ) -> anyhow::Result<T> {
        with_timeout(async {
            loop {
                let event = tokio::select! {
                    event = self.event_rx.recv() => event
                        .ok_or_else(|| anyhow::anyhow!("network event channel closed"))?,
                    _ = wait_finished(&mut self.network_task) => {
                        anyhow::bail!("network service stopped before expected event");
                    }
                };

                if let Some(value) = select(event) {
                    return Ok(value);
                }
            }
        })
        .await
    }
}

async fn wait_finished<T>(task: &mut JoinHandle<T>) {
    if task.is_finished() {
        return;
    }

    std::future::pending::<()>().await;
}

async fn with_timeout<T>(future: impl Future<Output = anyhow::Result<T>>) -> anyhow::Result<T> {
    timeout(TEST_TIMEOUT, future)
        .await
        .map_err(|_| anyhow::anyhow!("timed out waiting for network integration event"))?
}

async fn connected_pair() -> anyhow::Result<(TestNetwork, TestNetwork, PeerId)> {
    let first_seed = "integration-node-a";
    let mut first = TestNetwork::start(first_seed, Vec::new()).await?;
    let first_addr = first.listening_addr().await?;
    let first_peer_id = deterministic_peer_id(first_seed)?;
    let first_bootstrap_addr = with_peer_id(first_addr, first_peer_id)?;
    let mut second = TestNetwork::start("integration-node-b", vec![first_bootstrap_addr]).await?;

    let first_peer_id = second
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(peer_id) => Some(peer_id),
            _ => None,
        })
        .await?;

    first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(_) => Some(()),
            _ => None,
        })
        .await?;

    Ok((first, second, first_peer_id))
}

fn deterministic_peer_id(seed: &str) -> anyhow::Result<libp2p::PeerId> {
    let secret_key = Sha256::digest(seed.as_bytes());
    Ok(Keypair::ed25519_from_bytes(secret_key.to_vec())?
        .public()
        .to_peer_id())
}

fn with_peer_id(address: PeerAddress, peer_id: libp2p::PeerId) -> anyhow::Result<PeerAddress> {
    let address: Multiaddr = address.as_str().parse()?;
    let address = address
        .with_p2p(peer_id)
        .map_err(|address| anyhow::anyhow!("address already has different peer id: {address}"))?;
    Ok(PeerAddress::from(address.to_string()))
}

fn tx(id: u8) -> Transaction {
    Transaction {
        id: TransactionId::from_u8(id),
        payload: vec![id, id + 1],
    }
}

fn block(hash: u8, height: u64) -> Block {
    Block {
        hash: BlockHash::from_u8(hash),
        parent_hash: None,
        height,
        producer: "validator".to_string(),
        signature: vec![1],
        body: vec![hash, hash + 1],
    }
}

#[tokio::test]
async fn disconnects_peer_that_exceeds_block_request_limit() -> anyhow::Result<()> {
    let first_seed = "rate-limit-node-a";
    let mut first = TestNetwork::start_with_limits(first_seed, Vec::new(), 64, 1).await?;
    let first_addr = first.listening_addr().await?;
    let first_bootstrap_addr = with_peer_id(first_addr, deterministic_peer_id(first_seed)?)?;
    let mut second = TestNetwork::start("rate-limit-node-b", vec![first_bootstrap_addr]).await?;

    let first_peer_id = second
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(peer_id) => Some(peer_id),
            _ => None,
        })
        .await?;
    let second_peer_id = first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(peer_id) => Some(peer_id),
            _ => None,
        })
        .await?;

    second
        .handle
        .request_block(first_peer_id.clone(), BlockHash::from_u8(1))
        .await?;
    second
        .handle
        .request_block(first_peer_id, BlockHash::from_u8(2))
        .await?;

    first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerDisconnected(peer_id) if peer_id == second_peer_id => Some(()),
            _ => None,
        })
        .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn rejects_a_rate_limited_peer_after_it_reconnects() -> anyhow::Result<()> {
    let first_seed = "reconnect-rate-limit-node-a";
    let second_seed = "reconnect-rate-limit-node-b";
    let mut first = TestNetwork::start_with_limits(first_seed, Vec::new(), 64, 1).await?;
    let first_addr = first.listening_addr().await?;
    let first_bootstrap_addr = with_peer_id(first_addr, deterministic_peer_id(first_seed)?)?;
    let mut second = TestNetwork::start(second_seed, vec![first_bootstrap_addr.clone()]).await?;

    let first_peer_id = second
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(peer_id) => Some(peer_id),
            _ => None,
        })
        .await?;
    let second_peer_id = first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerConnected(peer_id) => Some(peer_id),
            _ => None,
        })
        .await?;

    second
        .handle
        .request_block(first_peer_id.clone(), BlockHash::from_u8(1))
        .await?;
    second
        .handle
        .request_block(first_peer_id, BlockHash::from_u8(2))
        .await?;
    first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerDisconnected(peer_id) if peer_id == second_peer_id => Some(()),
            _ => None,
        })
        .await?;
    second.stop().await?;

    let replacement = TestNetwork::start(second_seed, vec![first_bootstrap_addr]).await?;
    first
        .wait_for_event(|event| match event {
            NetworkEvent::PeerDisconnected(peer_id) if peer_id == second_peer_id => Some(()),
            _ => None,
        })
        .await?;

    replacement.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn transaction_broadcast_reaches_connected_peer() -> anyhow::Result<()> {
    let (first, mut second, _) = connected_pair().await?;
    let expected = tx(7);

    repeat_until_event(
        || first.handle.broadcast_transaction(expected.clone()),
        &mut second,
        |event| match event {
            NetworkEvent::TransactionReceived { tx, .. } if tx == expected => Some(()),
            _ => None,
        },
    )
    .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn block_announcement_reaches_connected_peer() -> anyhow::Result<()> {
    let (first, mut second, _) = connected_pair().await?;
    let block = block(9, 42);
    let expected_hash = block.hash.clone();

    repeat_until_event(
        || first.handle.broadcast_block(block.clone()),
        &mut second,
        |event| match event {
            NetworkEvent::BlockAnnouncementReceived { announcement, .. }
                if announcement.block_hash == expected_hash && announcement.height == 42 =>
            {
                Some(())
            }
            _ => None,
        },
    )
    .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn block_broadcast_reaches_connected_peer() -> anyhow::Result<()> {
    let (first, mut second, _) = connected_pair().await?;
    let expected = block(10, 0);

    repeat_until_event(
        || first.handle.broadcast_block(expected.clone()),
        &mut second,
        |event| match event {
            NetworkEvent::BlockReceived { block, .. } if block == expected => Some(()),
            _ => None,
        },
    )
    .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn chain_tip_broadcast_reaches_connected_peer() -> anyhow::Result<()> {
    let (first, mut second, _) = connected_pair().await?;
    let expected = ChainTip {
        block_hash: BlockHash::from_u8(12),
        height: 42,
    };

    repeat_until_event(
        || first.handle.broadcast_chain_tip(expected.clone()),
        &mut second,
        |event| match event {
            NetworkEvent::ChainTipReceived { tip, .. } if tip == expected => Some(()),
            _ => None,
        },
    )
    .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn late_joiner_requests_an_advertised_chain_tip() -> anyhow::Result<()> {
    let (mut first, mut second, first_peer_id) = connected_pair().await?;
    let expected = block(13, 0);
    let tip = ChainTip {
        block_hash: expected.hash.clone(),
        height: expected.height,
    };

    let (tip_peer_id, received_tip) = repeat_until_event(
        || first.handle.broadcast_chain_tip(tip.clone()),
        &mut second,
        |event| match event {
            NetworkEvent::ChainTipReceived { peer_id, tip } => Some((peer_id, tip)),
            _ => None,
        },
    )
    .await?;
    assert_eq!(tip_peer_id, first_peer_id);
    assert_eq!(received_tip, tip);

    let sync = SyncService::new(second.handle.clone(), ChainStore::new(), NoopReputation);
    sync.on_chain_tip(tip_peer_id, received_tip).await?;
    let mut workers = tokio::task::JoinSet::new();
    workers.spawn(async move { sync.run_requests().await });

    let request_peer_id = first
        .wait_for_event(|event| match event {
            NetworkEvent::BlockRequestReceived {
                peer_id,
                block_hash,
            } if block_hash == expected.hash => Some(peer_id),
            _ => None,
        })
        .await?;
    first
        .handle
        .send_block(request_peer_id, expected.clone())
        .await?;

    let received = second
        .wait_for_event(|event| match event {
            NetworkEvent::BlockReceived { block, .. } if block.hash == expected.hash => Some(block),
            _ => None,
        })
        .await?;
    assert_eq!(received, expected);

    workers.shutdown().await;
    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn request_response_fetches_block_from_connected_peer() -> anyhow::Result<()> {
    let (mut first, mut second, first_peer_id) = connected_pair().await?;
    let expected = block(11, 5);

    second
        .handle
        .request_block(first_peer_id.clone(), expected.hash.clone())
        .await?;

    let request_peer_id = first
        .wait_for_event(|event| match event {
            NetworkEvent::BlockRequestReceived {
                peer_id,
                block_hash,
            } if block_hash == expected.hash => Some(peer_id),
            _ => None,
        })
        .await?;

    first
        .handle
        .send_block(request_peer_id, expected.clone())
        .await?;

    let received = second
        .wait_for_event(|event| match event {
            NetworkEvent::BlockReceived { block, .. } if block.hash == expected.hash => Some(block),
            _ => None,
        })
        .await?;

    assert_eq!(received, expected);

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

#[tokio::test]
async fn request_response_reports_missing_block() -> anyhow::Result<()> {
    let (mut first, mut second, first_peer_id) = connected_pair().await?;
    let block_hash = BlockHash::from_u8(99);

    second
        .handle
        .request_block(first_peer_id, block_hash.clone())
        .await?;

    let request_peer_id = first
        .wait_for_event(|event| match event {
            NetworkEvent::BlockRequestReceived {
                peer_id,
                block_hash: requested_hash,
            } if requested_hash == block_hash => Some(peer_id),
            _ => None,
        })
        .await?;
    first
        .handle
        .send_block_not_found(request_peer_id, block_hash.clone())
        .await?;

    second
        .wait_for_event(|event| match event {
            NetworkEvent::BlockNotFound {
                block_hash: received_hash,
                ..
            } if received_hash == block_hash => Some(()),
            _ => None,
        })
        .await?;

    second.stop().await?;
    first.stop().await?;
    Ok(())
}

async fn repeat_until_event<T, S, Fut, F>(
    mut send: S,
    network: &mut TestNetwork,
    mut select: F,
) -> anyhow::Result<T>
where
    S: FnMut() -> Fut,
    Fut: Future<Output = anyhow::Result<()>>,
    F: FnMut(NetworkEvent) -> Option<T>,
{
    with_timeout(async {
        loop {
            send().await?;

            match timeout(Duration::from_millis(200), network.event_rx.recv()).await {
                Ok(Some(event)) => {
                    if let Some(value) = select(event) {
                        return Ok(value);
                    }
                }
                Ok(None) => anyhow::bail!("network event channel closed"),
                Err(_) => {}
            }

            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
}
