use common::{NodeId, NodeInfo};
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Instant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberStatus {
    Alive,
    Suspect,
    Dead,
    Joining,
}

impl std::fmt::Display for MemberStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemberStatus::Alive => write!(f, "Alive"),
            MemberStatus::Suspect => write!(f, "Suspect"),
            MemberStatus::Dead => write!(f, "Dead"),
            MemberStatus::Joining => write!(f, "Joining"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MemberEntry {
    pub info: NodeInfo,
    pub status: MemberStatus,
    pub incarnation: u64,
    pub last_seen: Instant,
}

pub struct MembershipTable {
    members: RwLock<HashMap<NodeId, MemberEntry>>,
    local_id: NodeId,
}

impl MembershipTable {
    pub fn new(local_id: NodeId, seed_nodes: Vec<NodeInfo>) -> Self {
        let mut members = HashMap::new();
        let now = Instant::now();

        let is_joining = !seed_nodes.is_empty();

        // Add self as Alive (standalone) or Joining (has seeds)
        let self_info = seed_nodes
            .iter()
            .find(|n| n.id == local_id)
            .cloned()
            .unwrap_or(NodeInfo {
                id: local_id.clone(),
                addr: String::new(),
            });

        members.insert(
            local_id.clone(),
            MemberEntry {
                info: self_info,
                status: if is_joining { MemberStatus::Joining } else { MemberStatus::Alive },
                incarnation: 0,
                last_seen: now,
            },
        );

        // Add seed nodes
        for node in seed_nodes {
            if node.id != local_id {
                members.insert(
                    node.id.clone(),
                    MemberEntry {
                        info: node,
                        status: MemberStatus::Alive,
                        incarnation: 0,
                        last_seen: now,
                    },
                );
            }
        }

        Self {
            members: RwLock::new(members),
            local_id,
        }
    }

    /// Get all alive members (excluding self) - includes Joining nodes for gossip propagation
    pub fn alive_peers(&self) -> Vec<MemberEntry> {
        let members = self.members.read().unwrap();
        members
            .iter()
            .filter(|(id, entry)| {
                **id != self.local_id && 
                (entry.status == MemberStatus::Alive || entry.status == MemberStatus::Joining)
            })
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// Get only routable peers (Alive, not Joining) for hash ring construction
    pub fn routable_peers(&self) -> Vec<MemberEntry> {
        let members = self.members.read().unwrap();
        members
            .iter()
            .filter(|(id, entry)| **id != self.local_id && entry.status == MemberStatus::Alive)
            .map(|(_, entry)| entry.clone())
            .collect()
    }

    /// Get all members regardless of status
    pub fn all_members(&self) -> Vec<MemberEntry> {
        let members = self.members.read().unwrap();
        members.values().cloned().collect()
    }

    /// Mark a node as Suspect
    pub fn mark_suspect(&self, node_id: &NodeId) -> bool {
        let mut members = self.members.write().unwrap();
        if let Some(entry) = members.get_mut(node_id) {
            if entry.status == MemberStatus::Alive {
                tracing::warn!(
                    "Node {} transitioning from Alive to Suspect",
                    node_id
                );
                entry.status = MemberStatus::Suspect;
                return true;
            }
        }
        false
    }

    /// Mark a node as Dead
    pub fn mark_dead(&self, node_id: &NodeId) -> bool {
        let mut members = self.members.write().unwrap();
        if let Some(entry) = members.get_mut(node_id) {
            if entry.status == MemberStatus::Alive || entry.status == MemberStatus::Suspect {
                tracing::error!(
                    "Node {} transitioning from {} to Dead",
                    node_id,
                    entry.status
                );
                entry.status = MemberStatus::Dead;
                return true;
            }
        }
        false
    }

    /// Mark a node as Alive (refutation)
    pub fn mark_alive(&self, node_id: &NodeId, incarnation: u64) -> bool {
        let mut members = self.members.write().unwrap();
        if let Some(entry) = members.get_mut(node_id) {
            if incarnation >= entry.incarnation && entry.status == MemberStatus::Suspect {
                tracing::info!(
                    "Node {} refuted suspect status, incarnation {} -> {}",
                    node_id,
                    entry.incarnation,
                    incarnation
                );
                entry.status = MemberStatus::Alive;
                entry.incarnation = incarnation;
                return true;
            }
        }
        false
    }

    /// Update last_seen for a node
    pub fn touch(&self, node_id: &NodeId) {
        let mut members = self.members.write().unwrap();
        if let Some(entry) = members.get_mut(node_id) {
            entry.last_seen = Instant::now();
        }
    }

    /// Get dead node IDs
    pub fn dead_nodes(&self) -> Vec<NodeId> {
        let members = self.members.read().unwrap();
        members
            .iter()
            .filter(|(_, entry)| entry.status == MemberStatus::Dead)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Check if a node is alive
    pub fn is_alive(&self, node_id: &NodeId) -> bool {
        let members = self.members.read().unwrap();
        members
            .get(node_id)
            .map(|entry| entry.status == MemberStatus::Alive)
            .unwrap_or(false)
    }

    /// Get the local node ID
    pub fn local_id(&self) -> &NodeId {
        &self.local_id
    }

    /// Get the local node's current incarnation
    pub fn local_incarnation(&self) -> u64 {
        let members = self.members.read().unwrap();
        members
            .get(&self.local_id)
            .map(|e| e.incarnation)
            .unwrap_or(0)
    }

    /// Get local node's address
    pub fn local_addr(&self) -> String {
        let members = self.members.read().unwrap();
        members
            .get(&self.local_id)
            .map(|e| e.info.addr.clone())
            .unwrap_or_default()
    }

    /// Mark self as Alive after rebalance completes
    pub fn mark_self_alive(&self) {
        let mut members = self.members.write().unwrap();
        if let Some(entry) = members.get_mut(&self.local_id) {
            entry.incarnation += 1;
            entry.status = MemberStatus::Alive;
            tracing::info!(
                "Node {} transitioned to Alive (incarnation {})",
                self.local_id,
                entry.incarnation
            );
        }
    }

    /// Check if self is currently in Joining state
    pub fn is_self_joining(&self) -> bool {
        let members = self.members.read().unwrap();
        members
            .get(&self.local_id)
            .map(|e| e.status == MemberStatus::Joining)
            .unwrap_or(false)
    }

    /// Merge remote gossip state into local state
    /// Rule: higher incarnation wins; for same incarnation, more recent last_seen wins
    /// Dead > Suspect > Alive > Joining precedence when incarnation is equal
    pub fn merge(&self, remote_entries: Vec<(NodeId, String, MemberStatus, u64)>) {
        let mut members = self.members.write().unwrap();
        for (node_id, addr, remote_status, remote_incarnation) in remote_entries {
            if node_id == self.local_id {
                // If someone suspects us, refute by bumping incarnation
                if remote_status == MemberStatus::Suspect
                    || remote_status == MemberStatus::Dead
                {
                    if let Some(local) = members.get_mut(&self.local_id) {
                        if remote_incarnation >= local.incarnation {
                            local.incarnation = remote_incarnation + 1;
                            local.status = MemberStatus::Alive;
                            tracing::info!(
                                "Refuted gossip: bumped incarnation to {}",
                                local.incarnation
                            );
                        }
                    }
                }
                continue;
            }

            if let Some(local) = members.get(&node_id) {
                // Merge rule: higher incarnation wins
                if remote_incarnation > local.incarnation {
                    // Remote has newer info
                    members.insert(
                        node_id.clone(),
                        MemberEntry {
                            info: NodeInfo {
                                id: node_id,
                                addr,
                            },
                            status: remote_status,
                            incarnation: remote_incarnation,
                            last_seen: Instant::now(),
                        },
                    );
                } else if remote_incarnation == local.incarnation {
                    // Same incarnation: Dead > Suspect > Alive > Joining
                    let should_update = match (local.status, remote_status) {
                        // Dead always wins
                        (MemberStatus::Alive, MemberStatus::Dead) => true,
                        (MemberStatus::Suspect, MemberStatus::Dead) => true,
                        (MemberStatus::Joining, MemberStatus::Dead) => true,
                        // Suspect beats Alive and Joining
                        (MemberStatus::Alive, MemberStatus::Suspect) => true,
                        (MemberStatus::Joining, MemberStatus::Suspect) => true,
                        // Alive beats Joining
                        (MemberStatus::Joining, MemberStatus::Alive) => true,
                        _ => false,
                    };
                    if should_update {
                        members.insert(
                            node_id.clone(),
                            MemberEntry {
                                info: NodeInfo {
                                    id: node_id.clone(),
                                    addr,
                                },
                                status: remote_status,
                                incarnation: remote_incarnation,
                                last_seen: Instant::now(),
                            },
                        );
                    }
                }
                // else: local has newer incarnation, ignore remote
            } else {
                // New node we haven't seen before
                members.insert(
                    node_id.clone(),
                    MemberEntry {
                        info: NodeInfo {
                            id: node_id.clone(),
                            addr,
                        },
                        status: remote_status,
                        incarnation: remote_incarnation,
                        last_seen: Instant::now(),
                    },
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_membership_initial() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id.clone(), seeds);

        // Node should start as Joining since seed nodes were provided
        assert!(table.is_self_joining());
        assert!(table.is_alive(&NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap()));
        assert_eq!(table.alive_peers().len(), 1);  // peer is Alive, not including self
        assert_eq!(table.all_members().len(), 2);
    }

    #[test]
    fn test_mark_suspect() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id.clone(), seeds);

        assert!(table.mark_suspect(&peer_id));
        assert!(!table.is_alive(&peer_id));

        // Can't mark suspect again (already suspect)
        assert!(!table.mark_suspect(&peer_id));
    }

    #[test]
    fn test_mark_dead() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id, seeds);

        // Alive -> Dead directly
        assert!(table.mark_dead(&peer_id));
        assert!(!table.is_alive(&peer_id));
        assert_eq!(table.dead_nodes().len(), 1);

        // Can't mark dead again
        assert!(!table.mark_dead(&peer_id));
    }

    #[test]
    fn test_refutation() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id.clone(), seeds);

        // Simulate a remote node suspecting us (incarnation 0)
        let remote_entries = vec![(
            local_id.clone(),
            "127.0.0.1:5001".to_string(),
            MemberStatus::Suspect,
            0,
        )];
        table.merge(remote_entries);

        // We should have refuted by bumping incarnation
        assert!(table.is_alive(&local_id));
        assert_eq!(table.local_incarnation(), 1);

        // Simulate another suspect with incarnation 1
        let remote_entries2 = vec![(
            local_id.clone(),
            "127.0.0.1:5001".to_string(),
            MemberStatus::Dead,
            1,
        )];
        table.merge(remote_entries2);

        // Should refute again
        assert!(table.is_alive(&local_id));
        assert_eq!(table.local_incarnation(), 2);
    }

    #[test]
    fn test_merge_higher_incarnation() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id, seeds);

        // Remote says peer is Dead with incarnation 5 (we have incarnation 0)
        let remote_entries = vec![(
            peer_id.clone(),
            "127.0.0.1:5002".to_string(),
            MemberStatus::Dead,
            5,
        )];
        table.merge(remote_entries);

        assert!(!table.is_alive(&peer_id));
        assert_eq!(table.dead_nodes().len(), 1);
    }

    #[test]
    fn test_merge_dead_wins() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];

        let table = MembershipTable::new(local_id, seeds);

        // Local has peer as Alive, incarnation 0
        // Remote says peer is Dead, incarnation 0 (same)
        // Dead > Alive, so we should update
        let remote_entries = vec![(
            peer_id.clone(),
            "127.0.0.1:5002".to_string(),
            MemberStatus::Dead,
            0,
        )];
        table.merge(remote_entries);

        assert!(!table.is_alive(&peer_id));
        assert_eq!(table.dead_nodes().len(), 1);

        // Now test Suspect > Alive
        let local_id2 = NodeId::parse("33333333-3333-3333-3333-333333333333").unwrap();
        let peer_id2 = NodeId::parse("44444444-4444-4444-4444-444444444444").unwrap();
        let seeds2 = vec![
            NodeInfo {
                id: local_id2.clone(),
                addr: "127.0.0.1:5003".to_string(),
            },
            NodeInfo {
                id: peer_id2.clone(),
                addr: "127.0.0.1:5004".to_string(),
            },
        ];

        let table2 = MembershipTable::new(local_id2, seeds2);

        // Remote says peer is Suspect with same incarnation
        let remote_entries2 = vec![(
            peer_id2.clone(),
            "127.0.0.1:5004".to_string(),
            MemberStatus::Suspect,
            0,
        )];
        table2.merge(remote_entries2);

        // Peer should be suspect now
        assert!(!table2.is_alive(&peer_id2));
        assert_eq!(table2.dead_nodes().len(), 0);
    }

    #[test]
    fn test_merge_joining_precedence() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        
        // Test Alive > Joining
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
        ];
        let table = MembershipTable::new(local_id.clone(), seeds);
        
        // Add peer as Joining
        table.merge(vec![(
            peer_id.clone(),
            "127.0.0.1:5002".to_string(),
            MemberStatus::Joining,
            0,
        )]);
        
        // Now remote says Alive at same incarnation
        table.merge(vec![(
            peer_id.clone(),
            "127.0.0.1:5002".to_string(),
            MemberStatus::Alive,
            0,
        )]);
        
        assert!(table.is_alive(&peer_id));
        
        // Test Dead > Joining
        let peer_id2 = NodeId::parse("33333333-3333-3333-3333-333333333333").unwrap();
        table.merge(vec![(
            peer_id2.clone(),
            "127.0.0.1:5003".to_string(),
            MemberStatus::Joining,
            0,
        )]);
        
        table.merge(vec![(
            peer_id2.clone(),
            "127.0.0.1:5003".to_string(),
            MemberStatus::Dead,
            0,
        )]);
        
        assert_eq!(table.dead_nodes().len(), 1);
        
        // Test Suspect > Joining
        let peer_id3 = NodeId::parse("44444444-4444-4444-4444-444444444444").unwrap();
        table.merge(vec![(
            peer_id3.clone(),
            "127.0.0.1:5004".to_string(),
            MemberStatus::Joining,
            0,
        )]);
        
        table.merge(vec![(
            peer_id3.clone(),
            "127.0.0.1:5004".to_string(),
            MemberStatus::Suspect,
            0,
        )]);
        
        assert!(!table.is_alive(&peer_id3));
    }

    #[test]
    fn test_routable_peers_excludes_joining() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id1 = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        let peer_id2 = NodeId::parse("33333333-3333-3333-3333-333333333333").unwrap();
        
        let seeds = vec![
            NodeInfo {
                id: local_id.clone(),
                addr: "127.0.0.1:5001".to_string(),
            },
        ];
        
        let table = MembershipTable::new(local_id.clone(), seeds);
        
        // Add one Alive and one Joining peer
        table.merge(vec![
            (peer_id1.clone(), "127.0.0.1:5002".to_string(), MemberStatus::Alive, 0),
            (peer_id2.clone(), "127.0.0.1:5003".to_string(), MemberStatus::Joining, 0),
        ]);
        
        // alive_peers should include both
        assert_eq!(table.alive_peers().len(), 2);
        
        // routable_peers should only include Alive
        let routable = table.routable_peers();
        assert_eq!(routable.len(), 1);
        assert_eq!(routable[0].info.id, peer_id1);
    }

    #[test]
    fn test_joining_node_startup() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        let peer_id = NodeId::parse("22222222-2222-2222-2222-222222222222").unwrap();
        
        // Node with seed nodes should start as Joining
        let seeds = vec![
            NodeInfo {
                id: peer_id.clone(),
                addr: "127.0.0.1:5002".to_string(),
            },
        ];
        
        let table = MembershipTable::new(local_id.clone(), seeds);
        assert!(table.is_self_joining());
        
        // Mark self as alive after rebalance
        table.mark_self_alive();
        assert!(!table.is_self_joining());
        assert!(table.is_alive(&local_id));
        assert_eq!(table.local_incarnation(), 1); // incarnation bumped
    }

    #[test]
    fn test_standalone_node_startup() {
        let local_id = NodeId::parse("11111111-1111-1111-1111-111111111111").unwrap();
        
        // Node with no seeds should start as Alive
        let seeds = vec![];
        let table = MembershipTable::new(local_id.clone(), seeds);
        
        assert!(!table.is_self_joining());
        assert!(table.is_alive(&local_id));
    }
}
