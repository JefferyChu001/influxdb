# InfluxDB 3 Cluster Module

This module provides distributed clustering capabilities for InfluxDB 3, enabling horizontal scaling, high availability, and distributed query execution including multi-table JOINs.

## Features

### ✅ Implemented

1. **Node Registry and Discovery**
   - Node registration and heartbeat mechanism
   - Failure detection
   - Node capacity tracking

2. **Shard Management**
   - Hash-based data sharding
   - Replica management
   - Load balancing across nodes

3. **Metadata Store**
   - In-memory metadata store for testing
   - etcd integration for production deployments
   - Distributed metadata consistency

4. **Raft Consensus**
   - Raft-based replication for data consistency
   - Leader election
   - Log replication

5. **Write Replication**
   - Configurable consistency levels (ONE, QUORUM, ALL)
   - Write routing to appropriate shards
   - Clustered write buffer wrapper

6. **Distributed Query Planning**
   - Query analysis and shard determination
   - Distributed execution plan generation

7. **Multi-Table JOIN Support**
   - **Broadcast JOIN**: Optimized for small table × large table scenarios
   - **Shuffle JOIN**: Optimized for large table × large table scenarios
   - **JOIN Optimizer**: Automatic strategy selection based on table statistics

## Architecture

```
┌─────────────────────────────────────────┐
│           Client Layer                   │
└────────────────┬────────────────────────┘
                 │
┌────────────────▼────────────────────────┐
│      Coordinator Layer                   │
│  ┌──────────┐  ┌──────────┐            │
│  │  Query   │  │  Write   │            │
│  │  Router  │  │  Router  │            │
│  └──────────┘  └──────────┘            │
└────────────────┬────────────────────────┘
                 │
        ┌────────┼────────┐
        │        │        │
┌───────▼──┐ ┌──▼────┐ ┌─▼──────┐
│ Node 1   │ │ Node 2│ │ Node N │
│ ┌──────┐ │ │┌─────┐│ │┌──────┐│
│ │Shard │ │ ││Shard││ ││Shard ││
│ │Data  │ │ ││Data ││ ││Data  ││
│ └──────┘ │ │└─────┘│ │└──────┘│
└──────────┘ └───────┘ └────────┘
```

## Usage Example

```rust
use influxdb3_cluster::{
    meta_store::InMemoryMetaStore,
    node_registry::NodeRegistry,
    shard_manager::ShardManager,
    types::{NodeInfo, NodeRole, ConsistencyLevel},
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create metadata store
    let meta_store = Arc::new(InMemoryMetaStore::new());
    
    // Create node registry
    let node_registry = Arc::new(NodeRegistry::new(meta_store.clone()));
    
    // Register nodes
    let node = NodeInfo {
        node_id: NodeId::new(1),
        address: "192.168.1.10".to_string(),
        grpc_port: 8087,
        http_port: 8086,
        role: NodeRole::DataNode,
        // ... other fields
    };
    node_registry.register_node(node).await?;
    
    // Create shard manager with 16 shards and 3 replicas
    let shard_manager = Arc::new(ShardManager::new(16, 3, meta_store));
    
    // Create a shard
    let shard_id = shard_manager.create_shard(
        DbId::new(1),
        ShardRange::Hash { start: 0, end: 1000 },
        &node_registry
    ).await?;
    
    Ok(())
}
```

## Testing

Run all tests:
```bash
cargo test -p influxdb3_cluster
```

Run integration tests:
```bash
cargo test -p influxdb3_cluster --test integration_test
```

## Test Coverage

- ✅ Node registration and discovery (6 tests)
- ✅ Shard routing and management (4 tests)
- ✅ Write replication (1 test)
- ✅ Raft consensus (3 tests)
- ✅ JOIN optimization (2 tests)
- ✅ Integration tests (3 tests)

**Total: 20 unit tests + 3 integration tests**

## Configuration

The cluster can be configured with:

- `shard_count`: Number of shards (default: 16)
- `replication_factor`: Number of replicas per shard (default: 3)
- `consistency_level`: Write consistency (ONE, QUORUM, ALL)
- `broadcast_threshold`: Size threshold for broadcast JOIN (default: 100MB)
- `max_partitions`: Maximum partitions for shuffle JOIN (default: 256)

## Future Enhancements

- gRPC service implementation for inter-node communication
- Arrow Flight integration for efficient data transfer
- Query result caching
- Automatic shard rebalancing
- Co-located JOIN optimization
- Predicate and projection pushdown

## License

MIT OR Apache-2.0

