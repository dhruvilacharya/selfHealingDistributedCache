use crate::client::NodeClientPool;
use crate::health::HealthMonitor;
use crate::replication::config::ReplicationConfig;
use common::cache_proto::*;
use common::hashring::HashRing;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, warn};

pub struct ReplicationCoordinator {
    hash_ring: Arc<RwLock<HashRing>>,
    client_pool: Arc<NodeClientPool>,
    config: ReplicationConfig,
    health_monitor: Arc<HealthMonitor>,
}

impl ReplicationCoordinator {
    pub fn new(
        hash_ring: Arc<RwLock<HashRing>>,
        client_pool: Arc<NodeClientPool>,
        config: ReplicationConfig,
        health_monitor: Arc<HealthMonitor>,
    ) -> Self {
        Self {
            hash_ring,
            client_pool,
            config,
            health_monitor,
        }
    }

    /// Build a filtered preference list: request extra nodes to account for dead ones,
    /// then keep only the alive ones up to `replication_factor`.
    async fn alive_preference_list(&self, key: &str) -> Vec<common::NodeInfo> {
        // Ask the ring for extra candidates so we can skip dead ones and still
        // fill up to replication_factor.
        let extra = self.config.replication_factor + 2;
        let ring = self.hash_ring.read().await;
        let candidates = ring.get_preference_list(key, extra);
        drop(ring);

        let mut alive_nodes = Vec::new();
        for node in candidates {
            if self.health_monitor.is_alive(&node.id).await {
                alive_nodes.push(node);
            } else {
                debug!("Skipping dead node for key '{}': {}", key, node.id);
            }
            if alive_nodes.len() >= self.config.replication_factor {
                break;
            }
        }
        alive_nodes
    }

    /// Write to primary + replicate to preference list.
    pub async fn set(
        &self,
        key: String,
        value: Vec<u8>,
        ttl_ms: Option<u64>,
    ) -> Result<SetResponse, Box<dyn std::error::Error + Send + Sync>> {
        let pref_list = self.alive_preference_list(&key).await;

        if pref_list.is_empty() {
            return Err("No alive nodes available for write".into());
        }

        // Write to all alive nodes in the preference list.
        let mut results = Vec::new();
        for node in &pref_list {
            let mut success = false;
            for attempt in 0..self.config.max_retries {
                match self.client_pool.get_client(&node.addr).await {
                    Ok(mut client) => {
                        let request = SetRequest {
                            key: key.clone(),
                            value: value.clone(),
                            ttl_ms,
                        };
                        match client.set(request).await {
                            Ok(resp) => {
                                success = resp.into_inner().success;
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Set to {} failed (attempt {}/{}): {}",
                                    node.addr,
                                    attempt + 1,
                                    self.config.max_retries,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Connect to {} failed (attempt {}/{}): {}",
                            node.addr,
                            attempt + 1,
                            self.config.max_retries,
                            e
                        );
                    }
                }
            }
            results.push(success);
        }

        // Success if at least the primary (first) wrote successfully.
        if results.first().copied().unwrap_or(false) {
            Ok(SetResponse { success: true })
        } else {
            Err("Primary write failed".into())
        }
    }

    /// Read from primary, fall back to replicas.
    pub async fn get(
        &self,
        key: &str,
    ) -> Result<GetResponse, Box<dyn std::error::Error + Send + Sync>> {
        let pref_list = self.alive_preference_list(key).await;

        if pref_list.is_empty() {
            return Err("No alive nodes available for read".into());
        }

        // Try each alive node in the preference list.
        for node in &pref_list {
            for attempt in 0..self.config.max_retries {
                match self.client_pool.get_client(&node.addr).await {
                    Ok(mut client) => {
                        let request = GetRequest {
                            key: key.to_string(),
                        };
                        match client.get(request).await {
                            Ok(resp) => {
                                let inner = resp.into_inner();
                                if inner.found {
                                    return Ok(inner);
                                }
                                // Key not found on this replica, try next node
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Get from {} failed (attempt {}/{}): {}",
                                    node.addr,
                                    attempt + 1,
                                    self.config.max_retries,
                                    e
                                );
                            }
                        }
                    }
                    Err(e) => {
                        warn!(
                            "Connect to {} failed (attempt {}/{}): {}",
                            node.addr,
                            attempt + 1,
                            self.config.max_retries,
                            e
                        );
                    }
                }
            }
        }

        // Key not found on any alive replica.
        Ok(GetResponse {
            value: None,
            found: false,
        })
    }

    /// Delete from all alive replicas.
    pub async fn delete(
        &self,
        key: &str,
    ) -> Result<DeleteResponse, Box<dyn std::error::Error + Send + Sync>> {
        let pref_list = self.alive_preference_list(key).await;

        if pref_list.is_empty() {
            return Err("No alive nodes available for delete".into());
        }

        let mut any_deleted = false;
        for node in &pref_list {
            for attempt in 0..self.config.max_retries {
                match self.client_pool.get_client(&node.addr).await {
                    Ok(mut client) => {
                        let request = DeleteRequest {
                            key: key.to_string(),
                        };
                        match client.delete(request).await {
                            Ok(resp) => {
                                if resp.into_inner().deleted {
                                    any_deleted = true;
                                }
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    "Delete from {} failed (attempt {}/{}): {}",
                                    node.addr,
                                    attempt + 1,
                                    self.config.max_retries,
                                    e
                                );
                            }
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
            }
        }

        Ok(DeleteResponse {
            deleted: any_deleted,
        })
    }
}
