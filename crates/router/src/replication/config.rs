#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    pub replication_factor: usize,
    /// Number of times to retry a failed RPC to a single node before moving to the next.
    pub max_retries: usize,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            replication_factor: 3,
            max_retries: 1,
        }
    }
}
