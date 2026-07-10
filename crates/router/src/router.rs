use crate::client::NodeClientPool;
use crate::replication::coordinator::ReplicationCoordinator;
use common::cache_proto::*;
use common::{HashRing, NodeId};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Router {
    hash_ring: Arc<RwLock<HashRing>>,
    client_pool: Arc<NodeClientPool>,
    coordinator: ReplicationCoordinator,
}

impl Router {
    pub fn new(
        hash_ring: Arc<RwLock<HashRing>>,
        client_pool: Arc<NodeClientPool>,
        coordinator: ReplicationCoordinator,
    ) -> Self {
        Self {
            hash_ring,
            client_pool,
            coordinator,
        }
    }

    /// Route a Set request: use replication coordinator unless an explicit target is given
    pub async fn set(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
        target: Option<&NodeId>,
    ) -> Result<SetResponse, Box<dyn std::error::Error + Send + Sync>> {
        if target.is_some() {
            // Explicit target override — bypass replication, write directly
            let addr = self.resolve_addr(&key, target).await?;
            let mut client = self.client_pool.get_client(&addr).await?;
            let request = tonic::Request::new(SetRequest {
                key,
                value,
                ttl_ms,
            });
            let response = client.set(request).await?;
            Ok(response.into_inner())
        } else {
            // Use replication coordinator
            self.coordinator.set(key, value, ttl_ms).await
        }
    }

    /// Route a Get request: use replication coordinator unless an explicit target is given
    pub async fn get(
        &self,
        key: &str,
        target: Option<&NodeId>,
    ) -> Result<GetResponse, Box<dyn std::error::Error + Send + Sync>> {
        if target.is_some() {
            let addr = self.resolve_addr(key, target).await?;
            let mut client = self.client_pool.get_client(&addr).await?;
            let request = tonic::Request::new(GetRequest {
                key: key.to_string(),
            });
            let response = client.get(request).await?;
            Ok(response.into_inner())
        } else {
            self.coordinator.get(key).await
        }
    }

    /// Route a Delete request: use replication coordinator unless an explicit target is given
    pub async fn delete(
        &self,
        key: &str,
        target: Option<&NodeId>,
    ) -> Result<DeleteResponse, Box<dyn std::error::Error + Send + Sync>> {
        if target.is_some() {
            let addr = self.resolve_addr(key, target).await?;
            let mut client = self.client_pool.get_client(&addr).await?;
            let request = tonic::Request::new(DeleteRequest {
                key: key.to_string(),
            });
            let response = client.delete(request).await?;
            Ok(response.into_inner())
        } else {
            self.coordinator.delete(key).await
        }
    }

    /// Resolve the address for a key using an explicit target override
    async fn resolve_addr(
        &self,
        _key: &str,
        target: Option<&NodeId>,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        let ring = self.hash_ring.read().await;
        if let Some(id) = target {
            let node = ring
                .nodes()
                .iter()
                .find(|n| n.id == *id)
                .ok_or("target node not found in ring")?;
            Ok(node.addr.clone())
        } else {
            Err("resolve_addr called without target".into())
        }
    }

    /// Access the hash ring (for membership updates)
    pub fn hash_ring(&self) -> &Arc<RwLock<HashRing>> {
        &self.hash_ring
    }
}
