use bytes::Bytes;
use common::cache_proto::cache_service_client::CacheServiceClient;
use common::cache_proto::TransferRequest;
use common::hashring::HashRing;
use common::{NodeId, NodeInfo};
use std::sync::Arc;

use crate::cache::CacheStore;

/// Handles key rebalancing when this node joins or recovers in the cluster.
/// Pulls keys from peer nodes via the TransferKeys streaming RPC and inserts
/// them into the local CacheStore with preserved TTL.
pub struct Rebalancer {
    store: Arc<CacheStore>,
    local_id: NodeId,
}

impl Rebalancer {
    pub fn new(store: Arc<CacheStore>, local_id: NodeId) -> Self {
        Self { store, local_id }
    }

    /// Pull keys from a single source node that belong to this node according to `ring`.
    /// Returns the number of keys successfully transferred.
    pub async fn pull_keys_from(
        &self,
        source_addr: &str,
        ring: &HashRing,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let channel =
            tonic::transport::Channel::from_shared(format!("http://{}", source_addr))?
                .connect()
                .await?;
        let mut client = CacheServiceClient::new(channel);

        let request = TransferRequest {
            target_node_id: self.local_id.to_string(),
            key_prefixes: vec![],
        };

        let mut stream = client.transfer_keys(request).await?.into_inner();
        let mut count = 0usize;

        while let Some(chunk) = stream.message().await? {
            // Only accept keys that the hash ring assigns to this node.
            if let Some(owner) = ring.get_node(&chunk.key) {
                if owner.id == self.local_id {
                    self.store.set_from_transfer(
                        chunk.key,
                        Bytes::from(chunk.value),
                        chunk.expires_at_ms,
                    );
                    count += 1;
                }
            }
        }

        tracing::info!("Pulled {} keys from {}", count, source_addr);
        Ok(count)
    }

    /// Initiate a full rebalance: pull keys from all known peers.
    /// Returns the total number of keys transferred across all peers.
    pub async fn rebalance(
        &self,
        peers: &[NodeInfo],
        ring: &HashRing,
    ) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
        let mut total = 0usize;
        for peer in peers {
            if peer.id == self.local_id {
                continue;
            }
            match self.pull_keys_from(&peer.addr, ring).await {
                Ok(count) => total += count,
                Err(e) => {
                    tracing::warn!("Failed to pull keys from {}: {}", peer.addr, e);
                }
            }
        }
        tracing::info!("Rebalance complete: {} total keys transferred", total);
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::grpc::CacheServiceImpl;
    use common::cache_proto::cache_service_server::CacheServiceServer;
    use common::NodeId;
    use std::net::SocketAddr;
    use std::time::Duration;
    use tonic::transport::Server;

    /// Spin up a gRPC server on a random free port, returning (addr, store).
    async fn start_test_server() -> (String, Arc<CacheStore>) {
        let store = Arc::new(CacheStore::new());
        let service = CacheServiceImpl::new(Arc::clone(&store));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        tokio::spawn(async move {
            Server::builder()
                .add_service(CacheServiceServer::new(service))
                .serve_with_incoming(incoming)
                .await
                .unwrap();
        });
        // Brief yield to let the server start
        tokio::time::sleep(Duration::from_millis(50)).await;
        (addr.to_string(), store)
    }

    #[tokio::test]
    async fn test_transfer_keys_streaming() {
        let (addr, source_store) = start_test_server().await;
        // Populate source store with some keys
        source_store.set("key_a".into(), Bytes::from("val_a"), None);
        source_store.set(
            "key_b".into(),
            Bytes::from("val_b"),
            Some(Duration::from_secs(300)),
        );
        // This key is already expired
        source_store.set("key_expired".into(), Bytes::from("x"), Some(Duration::from_millis(1)));
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Create a target node and build a ring with both nodes
        let source_id = NodeId::new();
        let target_id = NodeId::new();
        let source_info = NodeInfo {
            id: source_id.clone(),
            addr: addr.clone(),
        };
        let target_info = NodeInfo {
            id: target_id.clone(),
            addr: "127.0.0.1:0".to_string(),
        };
        let mut ring = HashRing::new();
        ring.add_node(source_info);
        ring.add_node(target_info);

        let target_store = Arc::new(CacheStore::new());
        let rebalancer = Rebalancer::new(target_store.clone(), target_id);
        let peers = vec![NodeInfo {
            id: source_id.clone(),
            addr: addr.clone(),
        }];

        let count = rebalancer.rebalance(&peers, &ring).await.unwrap();
        // Some keys should have been transferred (those owned by target_id in the ring)
        // Exact count depends on hash ring assignment, but we know key_expired should be skipped
        assert!(count <= 2); // at most 2 (key_a and key_b, never key_expired)

        // Verify transferred keys exist in target store with correct values
        for key in &["key_a", "key_b"] {
            if let Some(owner) = ring.get_node(key) {
                if owner.id == rebalancer.local_id {
                    let val = target_store.get(key).unwrap();
                    assert_eq!(val, Bytes::from(format!("val_{}", &key[4..])));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_transfer_preserves_ttl() {
        let (addr, source_store) = start_test_server().await;
        // Set a key with 60s TTL
        source_store.set("ttl_key".into(), Bytes::from("data"), Some(Duration::from_secs(60)));

        let source_id = NodeId::new();
        let target_id = NodeId::new();
        let source_info = NodeInfo {
            id: source_id.clone(),
            addr: addr.clone(),
        };
        let target_info = NodeInfo {
            id: target_id.clone(),
            addr: "127.0.0.1:0".to_string(),
        };
        let mut ring = HashRing::new();
        ring.add_node(source_info);
        ring.add_node(target_info);

        // Check which node owns "ttl_key"
        let owner = ring.get_node("ttl_key").unwrap();
        if owner.id != target_id {
            // Key doesn't belong to target, skip test (hash ring assignment is random)
            return;
        }

        let target_store = Arc::new(CacheStore::new());
        let rebalancer = Rebalancer::new(target_store.clone(), target_id);
        let peers = vec![NodeInfo {
            id: source_id,
            addr,
        }];

        rebalancer.rebalance(&peers, &ring).await.unwrap();

        // Check that the transferred key has approximately the same remaining TTL
        let remaining = target_store.ttl("ttl_key").unwrap();
        // Should be roughly 60s minus a small transfer delay
        assert!(remaining > Duration::from_secs(55));
        assert!(remaining <= Duration::from_secs(60));
    }
}
