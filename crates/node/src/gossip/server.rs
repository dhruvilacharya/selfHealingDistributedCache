use common::gossip_proto::gossip_service_server::GossipService;
use common::gossip_proto::{GossipMessage, MemberStatus as ProtoMemberStatus};
use common::NodeId;
use std::sync::Arc;
use tonic::{Request, Response, Status};

use super::protocol::build_gossip_message;
use super::state::{MemberStatus, MembershipTable};

pub struct GossipServiceImpl {
    table: Arc<MembershipTable>,
}

impl GossipServiceImpl {
    pub fn new(table: Arc<MembershipTable>) -> Self {
        Self { table }
    }
}

#[tonic::async_trait]
impl GossipService for GossipServiceImpl {
    async fn gossip_exchange(
        &self,
        request: Request<GossipMessage>,
    ) -> Result<Response<GossipMessage>, Status> {
        let remote_msg = request.into_inner();

        // Process incoming gossip: merge remote state
        let entries: Vec<_> = remote_msg
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

        // Update last_seen for the sender
        if let Ok(sender_id) = NodeId::parse(&remote_msg.sender_id) {
            self.table.touch(&sender_id);
        }

        // Build and return our local view
        let response = build_gossip_message(&self.table);
        Ok(Response::new(response))
    }
}
