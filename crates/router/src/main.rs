use clap::Parser;
use common::cache_proto::cache_service_server::CacheServiceServer;
use common::HashRing;
use router::client::NodeClientPool;
use router::grpc::RouterServiceImpl;
use router::health::{start_health_checker, HealthMonitor};
use router::membership::ClusterConfig;
use router::replication::config::ReplicationConfig;
use router::replication::coordinator::ReplicationCoordinator;
use router::router::Router;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "cache-router", about = "Self-Healing Distributed Cache Router")]
struct Args {
    /// Listen address for the router, e.g. "127.0.0.1:6000"
    #[arg(long)]
    addr: String,

    /// Path to cluster configuration JSON file
    #[arg(long)]
    config: String,

    /// Optional target node UUID for explicit targeting
    #[arg(long)]
    target_node: Option<String>,

    /// Replication factor (number of replicas per key, default: 3)
    #[arg(long, default_value = "3")]
    replication_factor: usize,

    /// Maximum number of retries per node for a failed RPC (default: 1)
    #[arg(long, default_value = "1")]
    max_retries: usize,

    /// Health check interval in milliseconds (default: 1000)
    #[arg(long, default_value = "1000")]
    health_check_interval_ms: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!("Router starting on {}", args.addr);

    // Load cluster config
    let cluster_config = ClusterConfig::load(&args.config)?;
    info!("Loaded {} nodes from config", cluster_config.nodes.len());

    // Build the consistent hash ring from cluster config
    let members = cluster_config.to_node_infos();
    let mut hash_ring = HashRing::new();
    for member in &members {
        info!("  Node {} at {}", member.id, member.addr);
        hash_ring.add_node(member.clone());
    }
    let hash_ring = Arc::new(RwLock::new(hash_ring));

    // Create client pool
    let client_pool = Arc::new(NodeClientPool::new());

    // Create health monitor and start background health checker
    let health_monitor = Arc::new(HealthMonitor::new());
    let node_addrs: Vec<_> = members
        .iter()
        .map(|n| (n.id.clone(), n.addr.clone()))
        .collect();
    start_health_checker(
        Arc::clone(&health_monitor),
        node_addrs,
        Arc::clone(&client_pool),
        Duration::from_millis(args.health_check_interval_ms),
    )
    .await;

    // Create replication config and coordinator
    let replication_config = ReplicationConfig {
        replication_factor: args.replication_factor,
        max_retries: args.max_retries,
    };
    info!(
        "Replication factor: {}, max retries: {}",
        replication_config.replication_factor, replication_config.max_retries
    );

    let coordinator = ReplicationCoordinator::new(
        Arc::clone(&hash_ring),
        Arc::clone(&client_pool),
        replication_config,
        Arc::clone(&health_monitor),
    );

    // Create router with coordinator
    let router = Arc::new(Router::new(hash_ring, client_pool, coordinator));

    // Create gRPC service
    let service = RouterServiceImpl::new(router);

    // Start tonic server
    let addr = args.addr.parse()?;
    info!("Router gRPC server listening on {}", addr);

    tonic::transport::Server::builder()
        .add_service(CacheServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}
