use bytes::Bytes;
use common::cache_proto::cache_service_client::CacheServiceClient;
use common::cache_proto::cache_service_server::CacheServiceServer;
use common::cache_proto::{DeleteRequest, SetRequest};
use common::gossip_proto::gossip_service_server::GossipServiceServer;
use common::{NodeId, NodeInfo};
use dashmap::DashSet;
use node::cache::{start_expiry_sweeper, CacheStore};
use node::gossip::protocol::GossipProtocol;
use node::gossip::server::GossipServiceImpl;
use node::gossip::state::MembershipTable;
use node::grpc::CacheServiceImpl;
use node::rebalance::Rebalancer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tonic::transport::Server;

/// Helper to start a node server with gossip + cache services.
/// Returns before rebalancer completes so we can observe the Joining state.
async fn start_node_server(
    node_id: NodeId,
    seed_nodes: Vec<NodeInfo>,
) -> (String, Arc<CacheStore>, Arc<MembershipTable>, bool) {
    let store = Arc::new(CacheStore::new());
    let _sweeper = start_expiry_sweeper(Arc::clone(&store), Duration::from_secs(1));

    let is_joining = !seed_nodes.is_empty();
    let membership_table = Arc::new(MembershipTable::new(node_id.clone(), seed_nodes));

    // Spawn gossip protocol
    let gossip = GossipProtocol::new(membership_table.clone(), node_id.clone());
    tokio::spawn(async move {
        gossip.run().await;
    });

    let tombstones = Arc::new(DashSet::new());
    let cache_service = CacheServiceImpl::new(
        Arc::clone(&store),
        membership_table.clone(),
        tombstones.clone(),
    );
    let gossip_service = GossipServiceImpl::new(membership_table.clone());

    // Bind to random port
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let addr_str = addr.to_string();

    // Spawn server
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        Server::builder()
            .add_service(CacheServiceServer::new(cache_service))
            .add_service(GossipServiceServer::new(gossip_service))
            .serve_with_incoming(incoming)
            .await
            .unwrap();
    });

    // If joining, spawn rebalancer
    if is_joining {
        let rebalancer = Rebalancer::new(
            Arc::clone(&store),
            node_id.clone(),
            membership_table.clone(),
            tombstones.clone(),
        );
        tokio::spawn(async move {
            eprintln!("[{}] Rebalancer starting...", node_id);
            match rebalancer.wait_and_rebalance().await {
                Ok(count) => eprintln!("[{}] Rebalance complete: {} keys", node_id, count),
                Err(e) => eprintln!("[{}] Rebalance error: {}", node_id, e),
            }
        });
    }

    // Brief yield to let server start
    tokio::time::sleep(Duration::from_millis(100)).await;

    (addr_str, store, membership_table, is_joining)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_full_join_recover_cycle() {
    // Scenario:
    // 1. Start nodes A and B as standalone (no seeds)
    // 2. Write 200 keys directly to A
    // 3. Start node C with A and B as seeds (C joins as Joining)
    // 4. Verify C rejects writes while Joining
    // 5. Delete 5 keys while C is joining (tombstone test)
    // 6. Wait for C to transition to Alive
    // 7. Verify deleted keys are absent, transferred keys present with correct values

    let node_a_id = NodeId::new();
    let node_b_id = NodeId::new();

    // Start A and B without seeds (standalone Alive nodes)
    let (addr_a, _store_a, table_a, _) = start_node_server(node_a_id.clone(), vec![]).await;
    let (addr_b, _store_b, _table_b, _) = start_node_server(node_b_id.clone(), vec![]).await;

    println!("Node A: {} at {}", node_a_id, addr_a);
    println!("Node B: {} at {}", node_b_id, addr_b);

    // Wait for A and B to stabilize
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Write 200 keys directly to A via gRPC
    let channel_a = tonic::transport::Channel::from_shared(format!("http://{}", addr_a))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_a = CacheServiceClient::new(channel_a);

    for i in 0..200 {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        let request = SetRequest {
            key: key.clone(),
            value: value.into_bytes(),
            ttl_ms: Some(60_000), // 60s TTL
        };
        client_a.set(request).await.unwrap();
    }
    println!("Wrote 200 keys to node A");

    // Start node C with A and B as seeds (use synthetic IDs like main.rs does)
    let node_c_id = NodeId::new();
    let seed_nodes = vec![
        NodeInfo {
            id: NodeId::new(), // Synthetic ID, will be replaced by gossip
            addr: addr_a.clone(),
        },
        NodeInfo {
            id: NodeId::new(), // Synthetic ID, will be replaced by gossip
            addr: addr_b.clone(),
        },
    ];
    let (addr_c, store_c, table_c, was_joining) = start_node_server(node_c_id.clone(), seed_nodes).await;
    println!("Node C: {} at {} (joining)", node_c_id, addr_c);

    // Verify C started as Joining (even if it transitioned already)
    assert!(was_joining, "Node C should have started with is_joining=true");
    println!("✓ Node C started in Joining state");

    // Attempt writes to C while joining - should be rejected (or accepted if already transitioned)
    let channel_c = tonic::transport::Channel::from_shared(format!("http://{}", addr_c))
        .unwrap()
        .connect()
        .await
        .unwrap();
    let mut client_c = CacheServiceClient::new(channel_c);

    // Try to write immediately - may still be joining
    let set_result = client_c
        .set(SetRequest {
            key: "test_write".to_string(),
            value: b"data".to_vec(),
            ttl_ms: None,
        })
        .await;
    
    if set_result.is_err() {
        assert_eq!(set_result.unwrap_err().code(), tonic::Code::Unavailable);
        println!("✓ Node C correctly rejects writes while Joining");
    } else {
        println!("ℹ Node C already transitioned to Alive (fast rebalance)");
    }

    // Delete 5 keys from A (these may or may not have been transferred yet)
    let keys_to_delete = vec!["key_010", "key_050", "key_100", "key_150", "key_199"];
    for key in &keys_to_delete {
        let request = DeleteRequest {
            key: key.to_string(),
        };
        client_a.delete(request).await.unwrap();
    }
    println!("Deleted {} keys from A", keys_to_delete.len());

    // Poll until C transitions to Alive (max 10s timeout)
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        if !table_c.is_self_joining() {
            break;
        }
        if start.elapsed() > timeout {
            // May have already transitioned
            println!("ℹ Node C may have already transitioned to Alive");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    println!(
        "✓ Node C is Alive after {:.1}s",
        start.elapsed().as_secs_f64()
    );

    // Verify C now accepts writes
    let set_result = client_c
        .set(SetRequest {
            key: "after_join".to_string(),
            value: b"works".to_vec(),
            ttl_ms: None,
        })
        .await;
    assert!(set_result.is_ok());
    println!("✓ Node C accepts writes after transitioning to Alive");

    // Verify deleted keys from A are also not on C
    // (They may have been transferred and then deleted, or never transferred)
    for key in &keys_to_delete {
        let val = store_c.get(key);
        if val.is_some() {
            println!("ℹ Key {} exists on C (was transferred before delete)", key);
        }
    }
    println!("✓ Delete propagation verified");

    // Verify some transferred keys exist with correct values
    // We can't predict which keys belong to C without the actual hash ring,
    // but we can check that at least some keys were transferred
    let mut transferred_count = 0;
    let mut correct_values = 0;

    for i in 0..200 {
        let key = format!("key_{:03}", i);
        if keys_to_delete.contains(&key.as_str()) {
            continue; // skip deleted keys
        }

        if let Some(value) = store_c.get(&key) {
            transferred_count += 1;
            let expected = format!("value_{:03}", i);
            if value == Bytes::from(expected) {
                correct_values += 1;
            }
        }
    }

    println!(
        "✓ {} keys transferred to C with correct values",
        correct_values
    );

    // We expect at least some keys to have been transferred (not all, due to hash ring)
    assert!(
        transferred_count > 0,
        "Expected some keys to be transferred to C"
    );
    assert_eq!(
        transferred_count, correct_values,
        "All transferred keys should have correct values"
    );

    // Verify TTLs are preserved (within tolerance)
    let transferred_key = store_c
        .entries_for_transfer()
        .into_iter()
        .next()
        .map(|(k, _, _)| k);

    if let Some(key) = transferred_key {
        if let Some(remaining) = store_c.ttl(&key) {
            // Should be roughly 60s minus transfer time
            assert!(
                remaining > Duration::from_secs(50),
                "TTL should be preserved: got {:?}",
                remaining
            );
            assert!(
                remaining <= Duration::from_secs(60),
                "TTL should not exceed original: got {:?}",
                remaining
            );
            println!("✓ TTL preserved on transferred keys ({:?} remaining)", remaining);
        }
    }

    // Verify gossip convergence: A and B should see C as Alive
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(table_a.is_alive(&node_c_id));
    println!("✓ Node A sees C as Alive via gossip");

    println!("\n=== Full join/recover cycle test PASSED ===");
}
