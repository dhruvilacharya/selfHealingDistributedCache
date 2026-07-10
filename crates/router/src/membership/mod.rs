use common::NodeInfo;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub nodes: Vec<NodeConfigEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeConfigEntry {
    pub id: String,   // UUID string
    pub addr: String, // host:port
}

impl ClusterConfig {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let content = std::fs::read_to_string(path)?;
        let config: ClusterConfig = serde_json::from_str(&content)?;
        Ok(config)
    }

    pub fn to_node_infos(&self) -> Vec<NodeInfo> {
        self.nodes
            .iter()
            .map(|n| NodeInfo {
                id: common::NodeId::parse(&n.id).expect("invalid UUID in config"),
                addr: n.addr.clone(),
            })
            .collect()
    }
}
