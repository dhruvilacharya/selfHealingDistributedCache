use crate::cache::CacheStore;
use bytes::Bytes;
use common::cache_proto::cache_service_server::CacheService;
use common::cache_proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, HeartbeatRequest, HeartbeatResponse,
    ReplicateRequest, ReplicateResponse, SetRequest, SetResponse, TransferChunk, TransferRequest,
};
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct CacheServiceImpl {
    store: Arc<CacheStore>,
}

impl CacheServiceImpl {
    pub fn new(store: Arc<CacheStore>) -> Self {
        Self { store }
    }
}

#[tonic::async_trait]
impl CacheService for CacheServiceImpl {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        let ttl = req.ttl_ms.map(Duration::from_millis);
        self.store.set(req.key, Bytes::from(req.value), ttl);
        Ok(Response::new(SetResponse { success: true }))
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        match self.store.get(&req.key) {
            Some(value) => Ok(Response::new(GetResponse {
                value: Some(value.to_vec()),
                found: true,
            })),
            None => Ok(Response::new(GetResponse {
                value: None,
                found: false,
            })),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        let deleted = self.store.delete(&req.key);
        Ok(Response::new(DeleteResponse { deleted }))
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse { ok: true }))
    }

    async fn replicate_write(
        &self,
        request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        let req = request.into_inner();
        let ttl = req.ttl_ms.map(Duration::from_millis);
        self.store.set(req.key, Bytes::from(req.value), ttl);
        Ok(Response::new(ReplicateResponse { success: true }))
    }

    type TransferKeysStream = ReceiverStream<Result<TransferChunk, Status>>;

    async fn transfer_keys(
        &self,
        request: Request<TransferRequest>,
    ) -> Result<Response<Self::TransferKeysStream>, Status> {
        let _req = request.into_inner();
        // Collect all non-expired entries for transfer.
        let entries = self.store.entries_for_transfer();
        let (tx, rx) = tokio::sync::mpsc::channel(128);
        tokio::spawn(async move {
            for (key, value, expires_at_ms) in entries {
                let chunk = TransferChunk {
                    key,
                    value,
                    expires_at_ms,
                };
                if tx.send(Ok(chunk)).await.is_err() {
                    break; // receiver dropped
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
