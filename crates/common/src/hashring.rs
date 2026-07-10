use std::collections::BTreeMap;

use crate::{NodeId, NodeInfo};

const DEFAULT_VNODE_COUNT: usize = 150;

pub struct HashRing {
    ring: BTreeMap<u64, NodeId>,
    nodes: Vec<NodeInfo>,
    vnode_count: usize,
}

impl HashRing {
    pub fn new() -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
            vnode_count: DEFAULT_VNODE_COUNT,
        }
    }

    pub fn with_vnode_count(vnode_count: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            nodes: Vec::new(),
            vnode_count,
        }
    }

    /// Hash a string to a u64 position using xxh3
    fn hash_key(key: &str) -> u64 {
        xxhash_rust::xxh3::xxh3_64(key.as_bytes())
    }

    /// Hash a virtual node identifier to a ring position
    fn hash_vnode(node_id: &NodeId, vnode_index: usize) -> u64 {
        let key = format!("{}-{}", node_id, vnode_index);
        xxhash_rust::xxh3::xxh3_64(key.as_bytes())
    }

    /// Add a node to the ring with virtual nodes
    pub fn add_node(&mut self, info: NodeInfo) {
        for i in 0..self.vnode_count {
            let pos = Self::hash_vnode(&info.id, i);
            self.ring.insert(pos, info.id.clone());
        }
        self.nodes.push(info);
    }

    /// Remove a node and all its virtual nodes from the ring
    pub fn remove_node(&mut self, node_id: &NodeId) {
        self.ring.retain(|_, id| id != node_id);
        self.nodes.retain(|n| n.id != *node_id);
    }

    /// Find the node responsible for a key (clockwise lookup)
    pub fn get_node(&self, key: &str) -> Option<&NodeInfo> {
        if self.ring.is_empty() {
            return None;
        }
        let hash = Self::hash_key(key);
        // Find first ring position >= hash
        if let Some((_, node_id)) = self.ring.range(hash..).next() {
            self.nodes.iter().find(|n| n.id == *node_id)
        } else {
            // Wrap around to the start of the ring
            let (_, node_id) = self.ring.iter().next().unwrap();
            self.nodes.iter().find(|n| n.id == *node_id)
        }
    }

    /// Get the preference list for a key — N distinct physical nodes walking clockwise
    pub fn get_preference_list(&self, key: &str, replica_count: usize) -> Vec<NodeInfo> {
        if self.ring.is_empty() {
            return vec![];
        }
        let hash = Self::hash_key(key);
        let mut result = Vec::new();
        let mut seen = std::collections::HashSet::new();

        // Walk clockwise from the key's hash position, wrapping around
        let iter = self.ring.range(hash..).chain(self.ring.iter());

        for (_, node_id) in iter {
            if seen.insert(node_id.clone()) {
                if let Some(info) = self.nodes.iter().find(|n| n.id == *node_id) {
                    result.push(info.clone());
                }
                if result.len() >= replica_count {
                    break;
                }
            }
        }
        result
    }

    /// Get all nodes in the ring
    pub fn nodes(&self) -> &[NodeInfo] {
        &self.nodes
    }

    /// Check if ring contains a node
    pub fn contains_node(&self, node_id: &NodeId) -> bool {
        self.nodes.iter().any(|n| n.id == *node_id)
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn make_node(name: &str) -> NodeInfo {
        NodeInfo {
            id: NodeId(Uuid::new_v4()),
            addr: format!("127.0.0.1:{}", name),
        }
    }

    #[test]
    fn test_distribution_is_even() {
        let mut ring = HashRing::new();
        let nodes: Vec<NodeInfo> = (0..5).map(|i| make_node(&format!("500{}", i))).collect();
        for n in &nodes {
            ring.add_node(n.clone());
        }

        let mut counts = std::collections::HashMap::new();
        let total = 100_000;
        for i in 0..total {
            let key = format!("key-{}", i);
            let node = ring.get_node(&key).unwrap();
            *counts.entry(node.addr.clone()).or_insert(0) += 1;
        }

        println!("Distribution stats ({} keys, 5 nodes):", total);
        for (addr, count) in &counts {
            let pct = (*count as f64) / (total as f64) * 100.0;
            println!("  {}: {} keys ({:.1}%)", addr, count, pct);
        }

        // Each of 5 nodes should get 15-25% of keys
        for (_, count) in &counts {
            let pct = (*count as f64) / (total as f64) * 100.0;
            assert!(pct > 15.0 && pct < 25.0, "Distribution not even: {}%", pct);
        }
    }

    #[test]
    fn test_add_node_minimal_reshuffle() {
        let mut ring = HashRing::new();
        let nodes: Vec<NodeInfo> = (0..5).map(|i| make_node(&format!("500{}", i))).collect();
        for n in &nodes {
            ring.add_node(n.clone());
        }

        // Map 100k keys to nodes
        let before: Vec<_> = (0..100_000)
            .map(|i| {
                let key = format!("key-{}", i);
                (key.clone(), ring.get_node(&key).unwrap().id.clone())
            })
            .collect();

        // Add a 6th node
        let new_node = make_node("5005");
        ring.add_node(new_node);

        // Check how many keys moved
        let mut moved = 0;
        for (key, old_node_id) in &before {
            let new_node_id = &ring.get_node(key).unwrap().id;
            if old_node_id != new_node_id {
                moved += 1;
            }
        }

        let move_pct = (moved as f64) / 100_000.0 * 100.0;
        println!("Keys moved after adding 6th node: {} ({:.1}%)", moved, move_pct);
        // Should move roughly 1/6 ≈ 16.7%, assert less than 25%
        assert!(move_pct < 25.0, "Too many keys moved: {}%", move_pct);
    }

    #[test]
    fn test_preference_list_distinct_nodes() {
        let mut ring = HashRing::new();
        for i in 0..5 {
            ring.add_node(make_node(&format!("500{}", i)));
        }

        let prefs = ring.get_preference_list("test-key", 3);
        assert_eq!(prefs.len(), 3);
        // All should be distinct nodes
        let ids: std::collections::HashSet<_> = prefs.iter().map(|n| n.id.clone()).collect();
        assert_eq!(ids.len(), 3);
    }

    #[test]
    fn test_remove_node() {
        let mut ring = HashRing::new();
        let node = make_node("5001");
        ring.add_node(node.clone());
        assert!(ring.contains_node(&node.id));
        ring.remove_node(&node.id);
        assert!(!ring.contains_node(&node.id));
    }

    #[test]
    fn test_empty_ring() {
        let ring = HashRing::new();
        assert!(ring.get_node("any-key").is_none());
    }
}
