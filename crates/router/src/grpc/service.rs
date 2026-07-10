use crate::router::Router;
use common::cache_proto::cache_service_server::CacheService;
use common::cache_proto::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, HeartbeatRequest, HeartbeatResponse,
    ReplicateRequest, ReplicateResponse, SetRequest, SetResponse, TransferChunk, TransferRequest,
};
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct RouterServiceImpl {
    router: Arc<Router>,
}

impl RouterServiceImpl {
    pub fn new(router: Arc<Router>) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl CacheService for RouterServiceImpl {
    async fn set(&self, request: Request<SetRequest>) -> Result<Response<SetResponse>, Status> {
        let req = request.into_inner();
        match self.router.set(req.key, req.value, req.ttl_ms, None).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::internal(format!("router set failed: {}", e))),
        }
    }

    async fn get(&self, request: Request<GetRequest>) -> Result<Response<GetResponse>, Status> {
        let req = request.into_inner();
        match self.router.get(&req.key, None).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::internal(format!("router get failed: {}", e))),
        }
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let req = request.into_inner();
        match self.router.delete(&req.key, None).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(Status::internal(format!("router delete failed: {}", e))),
        }
    }

    async fn heartbeat(
        &self,
        _request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse { ok: true }))
    }

    async fn replicate_write(
        &self,
        _request: Request<ReplicateRequest>,
    ) -> Result<Response<ReplicateResponse>, Status> {
        Err(Status::unimplemented(
            "replicate_write not supported on router",
        ))
    }

    type TransferKeysStream = ReceiverStream<Result<TransferChunk, Status>>;

    async fn transfer_keys(
        &self,
        _request: Request<TransferRequest>,
    ) -> Result<Response<Self::TransferKeysStream>, Status> {
        Err(Status::unimplemented(
            "transfer_keys not supported on router",
        ))
    }
}
