use clap::Parser;
use common::cache_proto::cache_service_server::CacheServiceServer;
use common::gossip_proto::gossip_service_server::GossipServiceServer;
use common::NodeId;
use dashmap::DashSet;
use node::cache::{start_expiry_sweeper, CacheStore};
use node::gossip::protocol::GossipProtocol;
use node::gossip::server::GossipServiceImpl;
use node::gossip::state::MembershipTable;
use node::grpc::CacheServiceImpl;
use node::rebalance::Rebalancer;
use node::resp::server::start_resp_server;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Server;
use tracing::info;

#[derive(Parser, Debug)]
#[command(name = "cache-node", about = "Self-Healing Distributed Cache Node")]
struct Args {
    /// gRPC address to listen on, e.g. "127.0.0.1:5001"
    #[arg(long)]
    addr: String,

    /// Optional UUID for this node; generated if not provided
    #[arg(long)]
    id: Option<String>,

    /// RESP (Redis-compatible) TCP address, defaults to "127.0.0.1:6379"
    #[arg(long, default_value = "127.0.0.1:6379")]
    resp_addr: String,

    /// Comma-separated list of seed node addresses (host:port), e.g. "127.0.0.1:5002,127.0.0.1:5003"
    #[arg(long, default_value = "")]
    seed_nodes: String,
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

    let node_id = match &args.id {
        Some(id_str) => NodeId::parse(id_str)?,
        None => NodeId::new(),
    };

    info!("Starting cache node {} on {}", node_id, args.addr);
    println!("Node ID: {}", node_id);

    // Create the cache store and start the background expiry sweeper.
    let store = Arc::new(CacheStore::new());
    let _sweeper = start_expiry_sweeper(Arc::clone(&store), Duration::from_secs(1));

    // Parse seed nodes from CLI
    let seed_addrs: Vec<String> = args
        .seed_nodes
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.trim().to_string())
        .collect();
    let seed_nodes: Vec<common::NodeInfo> = seed_addrs
        .iter()
        .map(|addr| {
            // Each seed node needs an addr. Use a synthetic ID since we don't know remote IDs yet.
            common::NodeInfo {
                id: NodeId::new(), // Will be updated on first gossip exchange
                addr: addr.clone(),
            }
        })
        .collect();
    let is_joining = !seed_nodes.is_empty();

    let table = Arc::new(MembershipTable::new(node_id.clone(), seed_nodes));
    let gossip = GossipProtocol::new(table.clone(), node_id.clone());

    // Spawn gossip protocol as a background task
    tokio::spawn(async move {
        gossip.run().await;
    });

    let grpc_addr = args.addr.parse()?;
    let tombstones = Arc::new(DashSet::new());
    let cache_service = CacheServiceImpl::new(Arc::clone(&store), table.clone(), tombstones.clone());
    let gossip_service = GossipServiceImpl::new(table.clone());

    let resp_addr = args.resp_addr.clone();
    let resp_store = Arc::clone(&store);

    // If this is a joining node, trigger gossip-triggered rebalance.
    if is_joining {
        let rebalancer = Rebalancer::new(
            Arc::clone(&store),
            node_id.clone(),
            table.clone(),
            tombstones.clone(),
        );
        tokio::spawn(async move {
            match rebalancer.wait_and_rebalance().await {
                Ok(count) => info!("Rebalance complete: {} keys transferred", count),
                Err(e) => tracing::error!("Rebalance failed: {}", e),
            }
        });
    }

    // Run gRPC, RESP servers concurrently, with graceful shutdown on Ctrl-C.
    let grpc_future = Server::builder()
        .add_service(CacheServiceServer::new(cache_service))
        .add_service(GossipServiceServer::new(gossip_service))
        .serve(grpc_addr);

    let resp_future = start_resp_server(&resp_addr, resp_store);

    tokio::select! {
        result = grpc_future => {
            if let Err(e) = result {
                tracing::error!("gRPC server error: {}", e);
            }
        }
        result = resp_future => {
            if let Err(e) = result {
                tracing::error!("RESP server error: {}", e);
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("Shutting down node {}", node_id);
        }
    }

    Ok(())
}
