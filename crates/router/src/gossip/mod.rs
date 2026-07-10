use common::gossip_proto::gossip_service_client::GossipServiceClient;
use common::gossip_proto::{GossipMessage, MemberStatus as ProtoMemberStatus};
use common::{HashRing, NodeId, NodeInfo};
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

use crate::health::HealthMonitor;

/// RouterGossipClient is a passive observer that learns cluster membership
/// by periodically exchanging gossip with seed nodes.
pub struct RouterGossipClient {
    hash_ring: Arc<RwLock<HashRing>>,
    health_monitor: Arc<HealthMonitor>,
    seed_addrs: Vec<String>,
}

impl RouterGossipClient {
    pub fn new(
        hash_ring: Arc<RwLock<HashRing>>,
        health_monitor: Arc<HealthMonitor>,
        seed_addrs: Vec<String>,
    ) -> Self {
        Self {
            hash_ring,
            health_monitor,
            seed_addrs,
        }
    }

    /// Main gossip loop: periodically exchange gossip with seed nodes.
    pub async fn run(&self) {
        info!(
            "Router gossip client started, observing {} seed nodes",
            self.seed_addrs.len()
        );

        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut rng = rand::rngs::StdRng::from_entropy();

        loop {
            interval.tick().await;

            // Pick 1-2 random seed nodes
            let count = self.seed_addrs.len().min(2);
            let targets: Vec<_> = self
                .seed_addrs
                .choose_multiple(&mut rng, count)
                .cloned()
                .collect();

            for addr in targets {
                if let Err(e) = self.exchange_gossip(&addr).await {
                    tracing::debug!("Gossip exchange with {} failed: {}", addr, e);
                }
            }
        }
    }

    /// Exchange gossip with a single node.
    async fn exchange_gossip(
        &self,
        addr: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))?
            .connect()
            .await?;
        let mut client = GossipServiceClient::new(channel);

        // Send an empty gossip message (router has no members to share)
        let request = GossipMessage {
            members: vec![],
            sender_id: "router".to_string(),
        };

        let response = client.gossip_exchange(request).await?.into_inner();

        // Process response: update hash ring and health monitor
        for member in response.members {
            let node_id = match NodeId::parse(&member.node_id) {
                Ok(id) => id,
                Err(_) => continue,
            };

            let status = match ProtoMemberStatus::try_from(member.status) {
                Ok(s) => s,
                Err(_) => continue,
            };

            match status {
                ProtoMemberStatus::Alive => {
                    self.health_monitor.mark_alive(node_id.clone()).await;

                    // Check if node is already in ring
                    let ring = self.hash_ring.read().await;
                    let node_exists = ring.nodes().iter().any(|n| n.id == node_id);
                    drop(ring);

                    if !node_exists {
                        let node_info = NodeInfo {
                            id: node_id.clone(),
                            addr: member.addr.clone(),
                        };
                        info!("Adding node {} to ring via gossip", node_id);
                        self.hash_ring.write().await.add_node(node_info);
                    }
                }
                ProtoMemberStatus::Joining => {
                    // Mark as joining in health monitor to exclude from preference list
                    self.health_monitor.mark_joining(node_id.clone()).await;
                    tracing::debug!("Node {} is joining (excluded from routing)", node_id);
                }
                ProtoMemberStatus::Suspect => {
                    self.health_monitor.record_failure(node_id.clone()).await;
                }
                ProtoMemberStatus::Dead => {
                    // Mark as dead in health monitor
                    self.health_monitor.record_failure(node_id.clone()).await;
                    self.health_monitor.record_failure(node_id.clone()).await;
                    self.health_monitor.record_failure(node_id.clone()).await;

                    // Remove from ring if present
                    let mut ring = self.hash_ring.write().await;
                    if ring.nodes().iter().any(|n| n.id == node_id) {
                        warn!("Removing dead node {} from ring via gossip", node_id);
                        ring.remove_node(&node_id);
                    }
                }
            }
        }

        Ok(())
    }
}
