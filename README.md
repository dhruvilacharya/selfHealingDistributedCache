# Self-Healing Distributed Cache

A production-grade distributed cache system with automatic failure detection, gossip-based membership, and self-healing rebalancing capabilities.

## Features

### Core Capabilities
- **Distributed Key-Value Store**: In-memory cache with TTL support
- **Consistent Hashing**: Even key distribution with minimal reshuffling on topology changes
- **Replication**: Configurable replication factor for high availability
- **Dual Protocol Support**: gRPC for internal communication, Redis-compatible RESP for clients

### Self-Healing Architecture
- **Gossip Protocol**: SWIM-based failure detection and membership management
- **Automatic Rebalancing**: Joining nodes pull keys from existing nodes based on hash ring ownership
- **Joining State Isolation**: New nodes are excluded from writes during rebalance to prevent inconsistencies
- **Tombstone Tracking**: Prevents resurrection of deleted keys during key transfer
- **Dynamic Router Updates**: Router learns about new/dead nodes via gossip without restart

### Production Ready
- **Zero Hard Failures**: Designed to handle node failures without data loss
- **Chaos Testing**: Integrated demo tool for validating self-healing under load
- **Comprehensive Tests**: 74 unit/integration tests covering core functionality
- **Protocol Buffers**: Efficient wire format for all internal communication

## Quick Start

### Prerequisites
- Rust 1.70+ (install via [rustup](https://rustup.rs/))
- Protocol Buffers compiler (`protoc`):
  - macOS: `brew install protobuf`
  - Ubuntu: `apt-get install protobuf-compiler`
  - Or download from [protobuf releases](https://github.com/protocolbuffers/protobuf/releases)

### Build
```bash
cargo build --release
```

### Run a 3-Node Cluster + Router

**Terminal 1 - Node A (standalone)**:
```bash
cargo run --release -p node -- \
  --addr 127.0.0.1:5001 \
  --resp-addr 127.0.0.1:6379
```

**Terminal 2 - Node B (standalone)**:
```bash
cargo run --release -p node -- \
  --addr 127.0.0.1:5002 \
  --resp-addr 127.0.0.1:6380
```

**Terminal 3 - Node C (standalone)**:
```bash
cargo run --release -p node -- \
  --addr 127.0.0.1:5003 \
  --resp-addr 127.0.0.1:6381
```

**Terminal 4 - Router**:
```bash
# Create cluster config
cat > cluster.json <<EOF
{
  "nodes": [
    {"id": "$(uuidgen)", "addr": "127.0.0.1:5001"},
    {"id": "$(uuidgen)", "addr": "127.0.0.1:5002"},
    {"id": "$(uuidgen)", "addr": "127.0.0.1:5003"}
  ]
}
EOF

cargo run --release -p router -- \
  --addr 127.0.0.1:6000 \
  --config cluster.json \
  --replication-factor 2
```

**Terminal 5 - Test with redis-cli**:
```bash
redis-cli -p 6000
127.0.0.1:6000> SET mykey "hello world"
OK
127.0.0.1:6000> GET mykey
"hello world"
127.0.0.1:6000> DEL mykey
(integer) 1
```

### Add a Node Dynamically

**Terminal 6 - Node D (joining)**:
```bash
cargo run --release -p node -- \
  --addr 127.0.0.1:5004 \
  --resp-addr 127.0.0.1:6382 \
  --seed-nodes 127.0.0.1:5001,127.0.0.1:5002
```

Node D will:
1. Start in **Joining** state (rejects writes)
2. Wait for gossip to converge (~1s)
3. Pull keys it owns from existing nodes
4. Apply tombstones to prevent deleted key resurrection
5. Transition to **Alive** (starts accepting writes)
6. Router dynamically adds it to the ring via gossip

## Demo Tool

The demo tool provides load testing and chaos engineering capabilities.

### Load Test
```bash
cargo run --release -p demo -- load \
  --router-grpc 127.0.0.1:6000 \
  --router-resp 127.0.0.1:6000 \
  --workers 20 \
  --duration-secs 30
```

### Chaos Test (Interactive)
```bash
cargo run --release -p demo -- chaos \
  --rounds 5 \
  --kill-interval-secs 10
```

### Combined Load + Chaos
```bash
cargo run --release -p demo -- run \
  --router-grpc 127.0.0.1:6000 \
  --router-resp 127.0.0.1:6000 \
  --workers 20 \
  --duration-secs 60 \
  --rounds 5
```

This generates a `chaos_report_{timestamp}.json` with:
- Total operations and success rate
- Latency percentiles (p50/p99/max)
- Per-protocol breakdown (gRPC vs RESP)

## Architecture

### Components

```
┌─────────────┐
│   Router    │  ← Client requests (gRPC/RESP)
│  (Stateless)│  ← Gossip observer (learns topology)
└──────┬──────┘  ← Replication coordinator
       │
       ├─────────┬─────────┬─────────┐
       │         │         │         │
   ┌───▼───┐ ┌──▼────┐ ┌──▼────┐ ┌──▼────┐
   │Node A │ │Node B │ │Node C │ │Node D │
   │ Alive │ │ Alive │ │ Alive │ │Joining│
   └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘
       └─────────┴─────────┴─────────┘
          Gossip Protocol (SWIM)
```

### Gossip State Machine

```
   ┌─────────┐  seed_nodes     ┌─────────┐
   │ Standalone│────────────────▶│ Joining │
   │  (Alive) │   non-empty      │(no writes)│
   └─────────┘                  └────┬────┘
                                     │ rebalance
                                     │ complete
                                ┌────▼────┐
                                │  Alive  │
                                │ (normal)│
                                └────┬────┘
                                     │ no heartbeat
                                ┌────▼────┐      timeout
                                │ Suspect │──────────┐
                                └────┬────┘          │
                                     │ refutation    │
                                     │ or recovery   │
                                ┌────▼────┐          │
                                │  Dead   │◀─────────┘
                                └─────────┘
```

### Rebalance Flow

1. Node starts with seed addresses
2. Gossip protocol runs, membership converges
3. `routable_peers()` returns only Alive nodes
4. Rebalancer builds hash ring from live membership
5. For each key from each peer:
   - Pull via `TransferKeys` streaming RPC
   - Check ownership in hash ring
   - Store if owned, skip otherwise
6. Apply tombstones (keys deleted during transfer)
7. Call `mark_self_alive()` → gossip propagates Joining→Alive

## Testing

### Run All Tests
```bash
cargo test --workspace
```

### Run Specific Test Suites
```bash
# Gossip protocol tests
cargo test -p node gossip

# Rebalance tests
cargo test -p node rebalance

# Integration test (full join/recover cycle)
cargo test -p node test_full_join_recover_cycle

# Router health monitor tests
cargo test -p router health
```

### Integration Test Scenario
The `test_full_join_recover_cycle` test validates:
1. Nodes A and B start standalone
2. 200 keys written to A
3. Node C joins with A+B as seeds
4. C starts in Joining state
5. Writes to C rejected while Joining
6. Keys deleted from A during transfer
7. C transitions to Alive after rebalance
8. Transferred keys have correct values + TTLs
9. Deleted keys absent (tombstones applied)
10. Gossip converged (A sees C as Alive)

## Configuration

### Node CLI Options
```
--addr <ADDR>              gRPC listen address (e.g., 127.0.0.1:5001)
--resp-addr <ADDR>         RESP listen address (e.g., 127.0.0.1:6379)
--id <UUID>                Optional node UUID (generated if omitted)
--seed-nodes <ADDRS>       Comma-separated seed addresses for joining
```

### Router CLI Options
```
--addr <ADDR>                      Listen address
--config <PATH>                    Cluster config JSON path
--replication-factor <N>           Number of replicas per key (default: 3)
--max-retries <N>                  RPC retry limit (default: 1)
--health-check-interval-ms <MS>    Heartbeat interval (default: 1000)
```

## Protocol Details

### gRPC Services
- **CacheService**: Set/Get/Delete/Heartbeat/TransferKeys
- **GossipService**: GossipExchange (bidirectional membership sync)

### RESP Commands
- `SET key value` — Store key-value
- `GET key` — Retrieve value
- `DEL key` — Delete key
- `EXPIRE key seconds` — Set TTL
- `TTL key` — Get remaining TTL

### Gossip Messages
```protobuf
message GossipMessage {
  repeated MemberEntry members = 1;
  string sender_id = 2;
}

message MemberEntry {
  string node_id = 1;
  string addr = 2;
  MemberStatus status = 3;  // ALIVE | SUSPECT | DEAD | JOINING
  uint64 incarnation = 4;
  uint64 last_seen_ms = 5;
}
```

## Failure Modes & Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Node crash | Gossip heartbeat timeout (~3s → Suspect, ~5s → Dead) | Router removes from ring, traffic redirected |
| Network partition | Nodes mark each other Dead | Refutation via incarnation bump on reconnect |
| Slow node | Becomes Suspect, still routable | Self-refutes or transitions to Dead |
| Joining node crash | Times out, stays in Joining | Restart with same seed-nodes, rebalance resumes |
| Router crash | Stateless, no state loss | Restart, gossip re-populates ring |

## Performance

- **Throughput**: ~50k ops/sec single-node, ~150k ops/sec 3-node cluster (M1 Mac)
- **Latency**: p50 < 2ms, p99 < 15ms (local cluster)
- **Rebalance**: ~65 keys/sec transfer rate (includes TTL preservation)
- **Gossip Overhead**: ~1 KB/s per node (1s interval, 3-node fanout)


## Contributing

Contributions welcome! Please:
1. Run `cargo test --workspace` before submitting
2. Follow existing code style (rustfmt)
3. Add tests for new features
