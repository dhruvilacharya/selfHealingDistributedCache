use common::cache_proto::cache_service_client::CacheServiceClient;
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::transport::Channel;

pub struct NodeClientPool {
    clients: RwLock<HashMap<String, CacheServiceClient<Channel>>>,
}

impl NodeClientPool {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Connect to a node and cache the client
    pub async fn get_client(
        &self,
        addr: &str,
    ) -> Result<CacheServiceClient<Channel>, tonic::transport::Error> {
        let clients = self.clients.read().await;
        if let Some(client) = clients.get(addr) {
            return Ok(client.clone());
        }
        drop(clients);

        let channel = Channel::from_shared(format!("http://{}", addr))
            .expect("invalid address")
            .connect()
            .await?;
        let client = CacheServiceClient::new(channel);

        let mut clients = self.clients.write().await;
        clients.insert(addr.to_string(), client.clone());
        Ok(client)
    }
}

impl Default for NodeClientPool {
    fn default() -> Self {
        Self::new()
    }
}
