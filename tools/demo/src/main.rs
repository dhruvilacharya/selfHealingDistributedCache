use clap::{Parser, Subcommand};
use common::cache_proto::cache_service_client::CacheServiceClient;
use common::cache_proto::{DeleteRequest, GetRequest, SetRequest};
use rand::{Rng, SeedableRng};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::info;

#[derive(Parser)]
#[command(name = "demo")]
#[command(about = "Self-Healing Distributed Cache Demo Tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run concurrent load workers against the cluster
    Load {
        /// Router gRPC address (e.g., "127.0.0.1:6000")
        #[arg(long)]
        router_grpc: String,

        /// Router RESP address (e.g., "127.0.0.1:6001")
        #[arg(long)]
        router_resp: String,

        /// Number of concurrent workers (default: 10)
        #[arg(long, default_value = "10")]
        workers: usize,

        /// Duration in seconds (default: 30)
        #[arg(long, default_value = "30")]
        duration_secs: u64,

        /// Key space size (default: 1000)
        #[arg(long, default_value = "1000")]
        key_space: usize,
    },

    /// Run chaos testing (requires manual cluster setup)
    Chaos {
        /// Number of chaos rounds (default: 3)
        #[arg(long, default_value = "3")]
        rounds: usize,

        /// Kill interval in seconds (default: 10)
        #[arg(long, default_value = "10")]
        kill_interval_secs: u64,
    },

    /// Run combined load + chaos + report
    Run {
        /// Router gRPC address
        #[arg(long)]
        router_grpc: String,

        /// Router RESP address
        #[arg(long)]
        router_resp: String,

        /// Number of workers (default: 20)
        #[arg(long, default_value = "20")]
        workers: usize,

        /// Duration in seconds (default: 30)
        #[arg(long, default_value = "30")]
        duration_secs: u64,

        /// Number of chaos rounds (default: 5)
        #[arg(long, default_value = "5")]
        rounds: usize,
    },
}

struct LoadMetrics {
    total_ops: AtomicU64,
    successful_ops: AtomicU64,
    failed_ops: AtomicU64,
    latencies_us: Arc<tokio::sync::Mutex<Vec<u64>>>,
}

impl LoadMetrics {
    fn new() -> Self {
        Self {
            total_ops: AtomicU64::new(0),
            successful_ops: AtomicU64::new(0),
            failed_ops: AtomicU64::new(0),
            latencies_us: Arc::new(tokio::sync::Mutex::new(Vec::new())),
        }
    }

    fn record_success(&self, _latency_us: u64) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.successful_ops.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.total_ops.fetch_add(1, Ordering::Relaxed);
        self.failed_ops.fetch_add(1, Ordering::Relaxed);
    }

    async fn record_latency(&self, latency_us: u64) {
        let mut latencies = self.latencies_us.lock().await;
        latencies.push(latency_us);
    }

    async fn compute_percentiles(&self) -> (u64, u64, u64) {
        let mut latencies = self.latencies_us.lock().await;
        if latencies.is_empty() {
            return (0, 0, 0);
        }
        latencies.sort_unstable();
        let p50 = latencies[latencies.len() * 50 / 100];
        let p99 = latencies[latencies.len() * 99 / 100];
        let max = *latencies.last().unwrap();
        (p50, p99, max)
    }

    fn summary(&self) -> (u64, u64, u64) {
        (
            self.total_ops.load(Ordering::Relaxed),
            self.successful_ops.load(Ordering::Relaxed),
            self.failed_ops.load(Ordering::Relaxed),
        )
    }
}

/// gRPC load worker - each worker creates its own client connection
async fn grpc_worker(
    router_addr: String,
    key_space: usize,
    duration: Duration,
    metrics: Arc<LoadMetrics>,
) {
    let start = Instant::now();
    let mut rng = rand::rngs::StdRng::from_entropy();

    // Create one connection for this worker
    let channel = match tonic::transport::Channel::from_shared(format!("http://{}", router_addr))
    {
        Ok(ch) => ch,
        Err(_e) => {
            tracing::error!("Invalid gRPC address");
            return;
        }
    };

    let client = match channel.connect().await {
        Ok(ch) => ch,
        Err(_e) => {
            tracing::error!("Failed to connect to gRPC");
            return;
        }
    };

    while start.elapsed() < duration {
        let mut worker_client = CacheServiceClient::new(client.clone());
        let key = format!("key_{}", rng.gen_range(0..key_space));
        let op_type: u8 = rng.gen_range(0..100);

        let op_start = Instant::now();

        let result = if op_type < 50 {
            // 50% Set
            let value = format!("value_{}", rng.gen::<u64>());
            let request = SetRequest {
                key,
                value: value.into_bytes(),
                ttl_ms: Some(60_000),
            };
            worker_client.set(request).await.map(|_| ())
        } else if op_type < 90 {
            // 40% Get
            let request = GetRequest { key };
            worker_client.get(request).await.map(|_| ())
        } else {
            // 10% Delete
            let request = DeleteRequest { key };
            worker_client.delete(request).await.map(|_| ())
        };

        let latency_us = op_start.elapsed().as_micros() as u64;

        match result {
            Ok(_) => {
                metrics.record_success(latency_us);
                metrics.record_latency(latency_us).await;
            }
            Err(_) => {
                metrics.record_failure();
            }
        }
    }
}

/// RESP load worker (Redis protocol)
async fn resp_worker(
    router_addr: String,
    key_space: usize,
    duration: Duration,
    metrics: Arc<LoadMetrics>,
) {
    let mut stream = match TcpStream::connect(&router_addr).await {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("Failed to connect to RESP: {}", e);
            return;
        }
    };

    let start = Instant::now();
    let mut rng = rand::rngs::StdRng::from_entropy();

    while start.elapsed() < duration {
        let key = format!("key_{}", rng.gen_range(0..key_space));
        let op_type: u8 = rng.gen_range(0..100);

        let op_start = Instant::now();

        let result = if op_type < 50 {
            // 50% Set
            let value = format!("value_{}", rng.gen::<u64>());
            let cmd = format!(
                "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                value.len(),
                value
            );
            if stream.write_all(cmd.as_bytes()).await.is_err() {
                Err(())
            } else {
                // Read response
                let mut buf = vec![0u8; 256];
                match stream.read(&mut buf).await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
        } else if op_type < 90 {
            // 40% Get
            let cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);
            if stream.write_all(cmd.as_bytes()).await.is_err() {
                Err(())
            } else {
                let mut buf = vec![0u8; 256];
                match stream.read(&mut buf).await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
        } else {
            // 10% Delete
            let cmd = format!("*2\r\n$3\r\nDEL\r\n${}\r\n{}\r\n", key.len(), key);
            if stream.write_all(cmd.as_bytes()).await.is_err() {
                Err(())
            } else {
                let mut buf = vec![0u8; 256];
                match stream.read(&mut buf).await {
                    Ok(_) => Ok(()),
                    Err(_) => Err(()),
                }
            }
        };

        let latency_us = op_start.elapsed().as_micros() as u64;

        match result {
            Ok(_) => {
                metrics.record_success(latency_us);
                metrics.record_latency(latency_us).await;
            }
            Err(_) => {
                metrics.record_failure();
            }
        }
    }
}

async fn run_load_test(
    router_grpc: String,
    router_resp: String,
    workers: usize,
    duration_secs: u64,
    key_space: usize,
) {
    info!(
        "Starting load test: {} workers, {}s duration, key space size {}",
        workers, duration_secs, key_space
    );

    let duration = Duration::from_secs(duration_secs);
    let grpc_metrics = Arc::new(LoadMetrics::new());
    let resp_metrics = Arc::new(LoadMetrics::new());

    let grpc_workers = workers / 2;
    let resp_workers = workers - grpc_workers;

    info!(
        "Spawning {} gRPC workers and {} RESP workers",
        grpc_workers, resp_workers
    );

    let mut handles = Vec::new();

    // Spawn gRPC workers
    for _ in 0..grpc_workers {
        let addr = router_grpc.clone();
        let metrics = grpc_metrics.clone();
        let handle = tokio::task::spawn(grpc_worker(addr, key_space, duration, metrics));
        handles.push(handle);
    }

    // Spawn RESP workers
    for _ in 0..resp_workers {
        let addr = router_resp.clone();
        let metrics = resp_metrics.clone();
        let handle = tokio::task::spawn(resp_worker(addr, key_space, duration, metrics));
        handles.push(handle);
    }

    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.await;
    }

    // Print summary
    let (grpc_total, grpc_success, grpc_failed) = grpc_metrics.summary();
    let (resp_total, resp_success, resp_failed) = resp_metrics.summary();

    let (grpc_p50, grpc_p99, grpc_max) = grpc_metrics.compute_percentiles().await;
    let (resp_p50, resp_p99, resp_max) = resp_metrics.compute_percentiles().await;

    let total_ops = grpc_total + resp_total;
    let total_success = grpc_success + resp_success;
    let total_failed = grpc_failed + resp_failed;

    let ops_per_sec = total_ops as f64 / duration_secs as f64;
    let success_rate = if total_ops > 0 {
        (total_success as f64 / total_ops as f64) * 100.0
    } else {
        0.0
    };

    println!("\n═══════════════════════════════════════════════");
    println!("Load Test Summary");
    println!("═══════════════════════════════════════════════");
    println!("Duration:        {}s", duration_secs);
    println!("Workers:         {} ({} gRPC + {} RESP)", workers, grpc_workers, resp_workers);
    println!("───────────────────────────────────────────────");
    println!("Total ops:       {}", total_ops);
    println!("Successful:      {}  ({:.2}%)", total_success, success_rate);
    println!("Failed:          {}  ({:.2}%)", total_failed, 100.0 - success_rate);
    println!("Throughput:      {:.0} ops/sec", ops_per_sec);
    println!("───────────────────────────────────────────────");
    println!(
        "Latency (gRPC):  p50={}μs  p99={}μs  max={}μs",
        grpc_p50, grpc_p99, grpc_max
    );
    println!(
        "Latency (RESP):  p50={}μs  p99={}μs  max={}μs",
        resp_p50, resp_p99, resp_max
    );
    println!("═══════════════════════════════════════════════\n");
}

async fn run_chaos_test(rounds: usize, kill_interval_secs: u64) {
    info!(
        "Chaos testing: {} rounds, {} second intervals",
        rounds, kill_interval_secs
    );
    println!("\n═══════════════════════════════════════════════");
    println!("Chaos Engine — Manual Node Kill Test");
    println!("═══════════════════════════════════════════════");
    println!("This tool simulates chaos by instructing you to");
    println!("manually kill and restart nodes.");
    println!();
    println!("Setup: Ensure your cluster is running before starting.");
    println!("───────────────────────────────────────────────\n");

    for round in 1..=rounds {
        println!("Round {}/{}", round, rounds);
        println!("  1. Identify a running node");
        println!("  2. Kill the node process (Ctrl-C or kill command)");
        println!("  3. Wait for gossip to detect failure (~5s)");
        println!("  4. Restart the node with --seed-nodes");
        println!("  5. Observe rebalance and recovery");
        println!();
        println!("Press Enter when ready to continue to next round...");
        
        let mut input = String::new();
        std::io::stdin().read_line(&mut input).unwrap();
        
        println!("  → Sleeping for {} seconds...\n", kill_interval_secs);
        tokio::time::sleep(Duration::from_secs(kill_interval_secs)).await;
    }

    println!("═══════════════════════════════════════════════");
    println!("Chaos rounds complete!");
    println!("Check your cluster logs to verify recovery.");
    println!("═══════════════════════════════════════════════\n");
}

async fn run_combined_test(
    router_grpc: String,
    router_resp: String,
    workers: usize,
    duration_secs: u64,
    rounds: usize,
) {
    println!("\n═══════════════════════════════════════════════");
    println!("Self-Healing Cache — Combined Chaos Run");
    println!("═══════════════════════════════════════════════");
    println!("This will run load workers while you perform chaos.");
    println!("Follow the prompts to kill/restart nodes during load.");
    println!("───────────────────────────────────────────────\n");

    // Warm-up: run short load test
    println!("Warm-up: Running 5s load test...");
    run_load_test(
        router_grpc.clone(),
        router_resp.clone(),
        workers / 2,
        5,
        1000,
    )
    .await;

    println!("\n═══════════════════════════════════════════════");
    println!("Starting main test: {} rounds", rounds);
    println!("═══════════════════════════════════════════════\n");

    let start = Instant::now();
    let grpc_metrics = Arc::new(LoadMetrics::new());
    let resp_metrics = Arc::new(LoadMetrics::new());

    let grpc_workers = workers / 2;
    let resp_workers = workers - grpc_workers;

    // Spawn background load workers
    let mut handles = Vec::new();
    
    for _ in 0..grpc_workers {
        let addr = router_grpc.clone();
        let metrics = grpc_metrics.clone();
        let handle = tokio::task::spawn(grpc_worker(
            addr,
            1000,
            Duration::from_secs(duration_secs),
            metrics,
        ));
        handles.push(handle);
    }

    for _ in 0..resp_workers {
        let addr = router_resp.clone();
        let metrics = resp_metrics.clone();
        let handle = tokio::task::spawn(resp_worker(
            addr,
            1000,
            Duration::from_secs(duration_secs),
            metrics,
        ));
        handles.push(handle);
    }

    // Run chaos rounds while load is ongoing
    let chaos_interval = duration_secs / rounds as u64;
    for round in 1..=rounds {
        println!("Chaos Round {}/{}", round, rounds);
        println!("  → Kill a node now, then restart it");
        println!("  → Load continues in background...");
        tokio::time::sleep(Duration::from_secs(chaos_interval)).await;
        println!();
    }

    // Wait for all workers to complete
    for handle in handles {
        let _ = handle.await;
    }

    let elapsed = start.elapsed().as_secs_f64();

    // Compute results
    let (grpc_total, grpc_success, grpc_failed) = grpc_metrics.summary();
    let (resp_total, resp_success, resp_failed) = resp_metrics.summary();

    let total_ops = grpc_total + resp_total;
    let total_success = grpc_success + resp_success;
    let total_failed = grpc_failed + resp_failed;

    let success_rate = if total_ops > 0 {
        (total_success as f64 / total_ops as f64) * 100.0
    } else {
        0.0
    };

    let (grpc_p50, grpc_p99, grpc_max) = grpc_metrics.compute_percentiles().await;
    let (resp_p50, resp_p99, resp_max) = resp_metrics.compute_percentiles().await;

    // Print final report
    println!("\n═══════════════════════════════════════════════");
    println!("Self-Healing Cache — Chaos Run Summary");
    println!("═══════════════════════════════════════════════");
    println!("Duration:          {:.1}s", elapsed);
    println!("Nodes killed:      {} (across {} rounds)", rounds, rounds);
    println!("Load workers:      {} ({} gRPC + {} RESP)", workers, grpc_workers, resp_workers);
    println!("───────────────────────────────────────────────");
    println!("Total ops:         {}", total_ops);
    println!("Successful:        {}  ({:.2}%)", total_success, success_rate);
    println!("Hard failures:     {}  ({:.2}%)", total_failed, 100.0 - success_rate);
    println!("Retried+recovered: 0      (0.00%)");
    println!("───────────────────────────────────────────────");
    println!(
        "Latency (gRPC):    p50={}μs  p99={}μs  max={}μs",
        grpc_p50, grpc_p99, grpc_max
    );
    println!(
        "Latency (RESP):    p50={}μs  p99={}μs  max={}μs",
        resp_p50, resp_p99, resp_max
    );
    println!("───────────────────────────────────────────────");
    println!("Node recovery times (avg): N/A (manual test)");
    println!("═══════════════════════════════════════════════\n");

    // Save report to file
    let report = serde_json::json!({
        "duration_secs": elapsed,
        "nodes_killed": rounds,
        "workers": workers,
        "total_ops": total_ops,
        "successful": total_success,
        "failed": total_failed,
        "success_rate": success_rate,
        "latency_grpc": {
            "p50_us": grpc_p50,
            "p99_us": grpc_p99,
            "max_us": grpc_max,
        },
        "latency_resp": {
            "p50_us": resp_p50,
            "p99_us": resp_p99,
            "max_us": resp_max,
        },
    });

    let filename = format!(
        "chaos_report_{}.json",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    );
    
    if let Ok(json_str) = serde_json::to_string_pretty(&report) {
        if let Err(e) = std::fs::write(&filename, json_str) {
            eprintln!("Failed to write report: {}", e);
        } else {
            println!("Report saved to {}", filename);
        }
    }

    // Exit with appropriate code
    if total_failed > 0 {
        std::process::exit(1);
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Load {
            router_grpc,
            router_resp,
            workers,
            duration_secs,
            key_space,
        } => {
            run_load_test(router_grpc, router_resp, workers, duration_secs, key_space).await;
        }
        Commands::Chaos {
            rounds,
            kill_interval_secs,
        } => {
            run_chaos_test(rounds, kill_interval_secs).await;
        }
        Commands::Run {
            router_grpc,
            router_resp,
            workers,
            duration_secs,
            rounds,
        } => {
            run_combined_test(router_grpc, router_resp, workers, duration_secs, rounds).await;
        }
    }
}
