pub mod cache_proto {
    tonic::include_proto!("cache");
}

pub mod gossip_proto {
    tonic::include_proto!("gossip");
}

pub mod hashring;
pub use hashring::HashRing;

use std::fmt;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct NodeId(pub Uuid);

impl NodeId {
    pub fn new() -> Self {
        NodeId(Uuid::new_v4())
    }

    pub fn from_uuid(id: Uuid) -> Self {
        NodeId(id)
    }

    pub fn parse(s: &str) -> Result<Self, uuid::Error> {
        Ok(NodeId(Uuid::parse_str(s)?))
    }
}

impl Default for NodeId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct NodeInfo {
    pub id: NodeId,
    pub addr: String,
}
