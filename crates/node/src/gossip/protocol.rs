use common::gossip_proto::gossip_service_client::GossipServiceClient;
use common::gossip_proto::{GossipMessage, MemberEntry as ProtoMemberEntry, MemberStatus as ProtoMemberStatus};
use common::NodeId;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use std::sync::Arc;
use std::time::{Duration, Instant};
use super::state::{MemberStatus, MembershipTable};

const GOSSIP_INTERVAL: Duration = Duration::from_millis(1000);
const SUSPECT_TIMEOUT: Duration = Duration::from_secs(3);
const DEAD_TIMEOUT: Duration = Duration::from_secs(5);

pub struct GossipProtocol {
    table: Arc<MembershipTable>,
    local_id: NodeId,
}

impl GossipProtocol {
    pub fn new(table: Arc<MembershipTable>, local_id: NodeId) -> Self {
        Self { table, local_id }
    }

    /// Main gossip loop — runs forever
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(GOSSIP_INTERVAL);
        loop {
            interval.tick().await;
            self.gossip_round().await;
            self.check_timeouts();
        }
    }

    /// Pick random peers and exchange gossip
    async fn gossip_round(&self) {
        let peers = self.table.alive_peers();
        if peers.is_empty() {
            return;
        }

        // Pick 1-3 random peers
        let mut rng = StdRng::from_entropy();
        let count = peers.len().min(rand::Rng::gen_range(&mut rng, 1..=3usize));
        let targets: Vec<_> = peers.choose_multiple(&mut rng, count).cloned().collect();

        for target in targets {
            let msg = self.build_gossip_message();
            match self.send_gossip(&target.info.addr, msg).await {
                Ok(response) => {
                    self.table.touch(&target.info.id);
                    self.process_gossip_response(response);
                }
                Err(e) => {
                    tracing::debug!("Gossip to {} failed: {}", target.info.addr, e);
                    // Don't mark as suspect on a single failure — wait for timeout
                }
            }
        }
    }

    /// Check for suspect/dead timeouts
    fn check_timeouts(&self) {
        let members = self.table.all_members();
        let now = Instant::now();

        for member in &members {
            if member.info.id == self.local_id {
                continue;
            }

            let elapsed = now.duration_since(member.last_seen);

            match member.status {
                MemberStatus::Alive => {
                    if elapsed > SUSPECT_TIMEOUT {
                        tracing::warn!(
                            "Node {} suspected (no response for {:.1}s)",
                            member.info.id,
                            elapsed.as_secs_f64()
                        );
                        self.table.mark_suspect(&member.info.id);
                    }
                }
                MemberStatus::Joining => {
                    // Joining nodes also get monitored for heartbeats
                    if elapsed > SUSPECT_TIMEOUT {
                        tracing::warn!(
                            "Joining node {} suspected (no response for {:.1}s)",
                            member.info.id,
                            elapsed.as_secs_f64()
                        );
                        self.table.mark_suspect(&member.info.id);
                    }
                }
                MemberStatus::Suspect => {
                    if elapsed > DEAD_TIMEOUT {
                        tracing::error!(
                            "Node {} marked DEAD (no response for {:.1}s)",
                            member.info.id,
                            elapsed.as_secs_f64()
                        );
                        self.table.mark_dead(&member.info.id);
                    }
                }
                MemberStatus::Dead => {
                    // Already dead, nothing to do
                }
            }
        }
    }

    /// Build a GossipMessage from local membership table
    fn build_gossip_message(&self) -> GossipMessage {
        build_gossip_message(&self.table)
    }

    /// Process a gossip response, merging remote state
    fn process_gossip_response(&self, response: GossipMessage) {
        let entries: Vec<_> = response
            .members
            .iter()
            .map(|m| {
                let status = match ProtoMemberStatus::try_from(m.status) {
                    Ok(ProtoMemberStatus::Alive) => MemberStatus::Alive,
                    Ok(ProtoMemberStatus::Suspect) => MemberStatus::Suspect,
                    Ok(ProtoMemberStatus::Dead) => MemberStatus::Dead,
                    Ok(ProtoMemberStatus::Joining) => MemberStatus::Joining,
                    Err(_) => MemberStatus::Alive,
                };
                (
                    NodeId::parse(&m.node_id).unwrap_or_else(|_| NodeId::new()),
                    m.addr.clone(),
                    status,
                    m.incarnation,
                )
            })
            .collect();

        self.table.merge(entries);
    }

    /// Send gossip to a peer via gRPC
    async fn send_gossip(
        &self,
        addr: &str,
        msg: GossipMessage,
    ) -> Result<GossipMessage, Box<dyn std::error::Error + Send + Sync>> {
        let channel = tonic::transport::Channel::from_shared(format!("http://{}", addr))?
            .connect()
            .await?;
        let mut client = GossipServiceClient::new(channel);
        let response = client.gossip_exchange(msg).await?;
        Ok(response.into_inner())
    }
}

/// Build a GossipMessage from a membership table (shared helper)
pub fn build_gossip_message(table: &MembershipTable) -> GossipMessage {
    let members = table.all_members();
    let proto_members: Vec<ProtoMemberEntry> = members
        .iter()
        .map(|entry| ProtoMemberEntry {
            node_id: entry.info.id.to_string(),
            addr: entry.info.addr.clone(),
            status: match entry.status {
                MemberStatus::Alive => ProtoMemberStatus::Alive as i32,
                MemberStatus::Suspect => ProtoMemberStatus::Suspect as i32,
                MemberStatus::Dead => ProtoMemberStatus::Dead as i32,
                MemberStatus::Joining => ProtoMemberStatus::Joining as i32,
            },
            incarnation: entry.incarnation,
            last_seen_ms: entry.last_seen.elapsed().as_millis() as u64,
        })
        .collect();

    GossipMessage {
        members: proto_members,
        sender_id: table.local_id().to_string(),
    }
}
