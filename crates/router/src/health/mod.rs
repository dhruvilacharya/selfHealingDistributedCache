use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use common::cache_proto::HeartbeatRequest;
use common::NodeId;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::client::NodeClientPool;

/// Health status of a node as seen by the router.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeHealth {
    Alive,
    Suspect,
    Dead,
    Joining,
}

impl std::fmt::Display for NodeHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NodeHealth::Alive => write!(f, "Alive"),
            NodeHealth::Suspect => write!(f, "Suspect"),
            NodeHealth::Dead => write!(f, "Dead"),
            NodeHealth::Joining => write!(f, "Joining"),
        }
    }
}

struct NodeHealthEntry {
    health: NodeHealth,
    last_checked: Instant,
    consecutive_failures: u32,
}

/// Tracks health of all cluster nodes. Thread-safe via internal RwLock.
pub struct HealthMonitor {
    nodes: RwLock<HashMap<NodeId, NodeHealthEntry>>,
    /// Number of consecutive failures before escalating Suspect -> Dead.
    failure_threshold: u32,
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            nodes: RwLock::new(HashMap::new()),
            failure_threshold: 3,
        }
    }

    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Record a successful heartbeat from a node.
    pub async fn mark_alive(&self, node_id: NodeId) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(
            node_id,
            NodeHealthEntry {
                health: NodeHealth::Alive,
                last_checked: Instant::now(),
                consecutive_failures: 0,
            },
        );
    }

    /// Mark a node as Joining (during rebalance).
    pub async fn mark_joining(&self, node_id: NodeId) {
        let mut nodes = self.nodes.write().await;
        nodes.insert(
            node_id,
            NodeHealthEntry {
                health: NodeHealth::Joining,
                last_checked: Instant::now(),
                consecutive_failures: 0,
            },
        );
    }

    /// Record a failed heartbeat. Transitions: Alive->Suspect, Suspect->Dead (after threshold).
    pub async fn record_failure(&self, node_id: NodeId) {
        let mut nodes = self.nodes.write().await;
        let entry = nodes.entry(node_id.clone()).or_insert(NodeHealthEntry {
            health: NodeHealth::Alive,
            last_checked: Instant::now(),
            consecutive_failures: 0,
        });

        entry.consecutive_failures += 1;
        entry.last_checked = Instant::now();

        match entry.health {
            NodeHealth::Alive => {
                entry.health = NodeHealth::Suspect;
                warn!(
                    "Node {} is now Suspect ({} consecutive failures)",
                    node_id, entry.consecutive_failures
                );
            }
            NodeHealth::Joining => {
                // Joining nodes can also fail heartbeats, treat like Alive
                entry.health = NodeHealth::Suspect;
                warn!(
                    "Joining node {} is now Suspect ({} consecutive failures)",
                    node_id, entry.consecutive_failures
                );
            }
            NodeHealth::Suspect => {
                if entry.consecutive_failures >= self.failure_threshold {
                    entry.health = NodeHealth::Dead;
                    error!(
                        "Node {} is now Dead ({} consecutive failures)",
                        node_id, entry.consecutive_failures
                    );
                } else {
                    debug!(
                        "Node {} still Suspect ({}/{})",
                        node_id, entry.consecutive_failures, self.failure_threshold
                    );
                }
            }
            NodeHealth::Dead => {
                // Already dead, nothing to escalate to
            }
        }
    }

    /// Check whether a node is considered alive (Alive or Suspect, not Dead or Joining).
    /// Unknown nodes are assumed alive.
    /// Joining nodes are excluded from routing to prevent writes during rebalance.
    pub async fn is_alive(&self, node_id: &NodeId) -> bool {
        let nodes = self.nodes.read().await;
        match nodes.get(node_id) {
            Some(entry) => entry.health != NodeHealth::Dead && entry.health != NodeHealth::Joining,
            None => true,
        }
    }

    /// Return all node IDs currently marked Dead.
    pub async fn dead_nodes(&self) -> Vec<NodeId> {
        let nodes = self.nodes.read().await;
        nodes
            .iter()
            .filter(|(_, e)| e.health == NodeHealth::Dead)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Return a snapshot of all node health statuses.
    pub async fn all_health(&self) -> HashMap<NodeId, NodeHealth> {
        let nodes = self.nodes.read().await;
        nodes.iter().map(|(id, e)| (id.clone(), e.health)).collect()
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

/// Background task that periodically probes nodes via Heartbeat RPC.
pub async fn start_health_checker(
    health_monitor: Arc<HealthMonitor>,
    node_addrs: Vec<(NodeId, String)>,
    client_pool: Arc<NodeClientPool>,
    check_interval: Duration,
) {
    tokio::spawn(async move {
        info!(
            "Health checker started, monitoring {} nodes every {:?}",
            node_addrs.len(),
            check_interval
        );
        let mut interval = tokio::time::interval(check_interval);
        loop {
            interval.tick().await;

            for (node_id, addr) in &node_addrs {
                match client_pool.get_client(addr).await {
                    Ok(mut client) => {
                        let request =
                            tonic::Request::new(HeartbeatRequest {
                                node_id: node_id.to_string(),
                            });
                        match client.heartbeat(request).await {
                            Ok(_) => {
                                health_monitor.mark_alive(node_id.clone()).await;
                                debug!("Heartbeat OK from node {} ({})", node_id, addr);
                            }
                            Err(e) => {
                                debug!(
                                    "Heartbeat failed for node {} ({}): {}",
                                    node_id, addr, e
                                );
                                health_monitor.record_failure(node_id.clone()).await;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(
                            "Could not connect to node {} ({}): {}",
                            node_id, addr, e
                        );
                        health_monitor.record_failure(node_id.clone()).await;
                    }
                }
            }

            // Log summary of dead nodes
            let dead = health_monitor.dead_nodes().await;
            if !dead.is_empty() {
                warn!("Dead nodes: {:?}", dead.iter().map(|id| id.to_string()).collect::<Vec<_>>());
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mark_alive() {
        let monitor = HealthMonitor::new();
        let node_id = NodeId::new();

        monitor.mark_alive(node_id.clone()).await;
        assert!(monitor.is_alive(&node_id).await);

        let health = monitor.all_health().await;
        assert_eq!(health.get(&node_id), Some(&NodeHealth::Alive));
    }

    #[tokio::test]
    async fn test_mark_joining() {
        let monitor = HealthMonitor::new();
        let node_id = NodeId::new();

        monitor.mark_joining(node_id.clone()).await;
        // Joining nodes should NOT be considered alive for routing
        assert!(!monitor.is_alive(&node_id).await);

        let health = monitor.all_health().await;
        assert_eq!(health.get(&node_id), Some(&NodeHealth::Joining));
    }

    #[tokio::test]
    async fn test_joining_to_alive_transition() {
        let monitor = HealthMonitor::new();
        let node_id = NodeId::new();

        // Start as Joining
        monitor.mark_joining(node_id.clone()).await;
        assert!(!monitor.is_alive(&node_id).await);

        // Transition to Alive
        monitor.mark_alive(node_id.clone()).await;
        assert!(monitor.is_alive(&node_id).await);
    }

    #[tokio::test]
    async fn test_record_failure_escalation() {
        let monitor = HealthMonitor::new().with_failure_threshold(3);
        let node_id = NodeId::new();

        // Start as Alive
        monitor.mark_alive(node_id.clone()).await;
        assert!(monitor.is_alive(&node_id).await);

        // First failure -> Suspect
        monitor.record_failure(node_id.clone()).await;
        assert!(monitor.is_alive(&node_id).await); // Suspect is still routable
        let health = monitor.all_health().await;
        assert_eq!(health.get(&node_id), Some(&NodeHealth::Suspect));

        // More failures -> Dead after threshold
        monitor.record_failure(node_id.clone()).await;
        monitor.record_failure(node_id.clone()).await;
        assert!(!monitor.is_alive(&node_id).await);
        let health = monitor.all_health().await;
        assert_eq!(health.get(&node_id), Some(&NodeHealth::Dead));
    }

    #[tokio::test]
    async fn test_unknown_node_assumed_alive() {
        let monitor = HealthMonitor::new();
        let unknown_node = NodeId::new();

        // Unknown nodes are assumed alive
        assert!(monitor.is_alive(&unknown_node).await);
    }

    #[tokio::test]
    async fn test_dead_nodes_list() {
        let monitor = HealthMonitor::new().with_failure_threshold(2);
        let node1 = NodeId::new();
        let node2 = NodeId::new();

        monitor.mark_alive(node1.clone()).await;
        monitor.mark_alive(node2.clone()).await;

        // Kill node1
        monitor.record_failure(node1.clone()).await;
        monitor.record_failure(node1.clone()).await;
        monitor.record_failure(node1.clone()).await;

        let dead = monitor.dead_nodes().await;
        assert_eq!(dead.len(), 1);
        assert!(dead.contains(&node1));
        assert!(!dead.contains(&node2));
    }
}
