//! InfluxDB 3.0 Distributed Query Framework
//!
//! This crate implements a complete distributed query execution framework
//! based on GreptimeDB's architecture, providing:
//!
//! - **Distributed Query Planning**: Convert logical plans into distributed physical plans
//! - **Query Optimization**: Predicate pushdown, projection pushdown, limit pushdown
//! - **Region-based Partitioning**: Data is partitioned into regions across nodes
//! - **Parallel Execution**: Execute sub-queries in parallel across datanodes
//! - **Result Merging**: Merge and aggregate results from multiple nodes
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Frontend (Coordinator)                    │
//! │  ┌────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐ │
//! │  │ Parser │→ │ Analyzer │→ │ Planner  │→ │   Executor   │ │
//! │  └────────┘  └──────────┘  └──────────┘  └──────────────┘ │
//! └──────────────────────────┬──────────────────────────────────┘
//!                            │ gRPC
//!              ┌─────────────┼─────────────┐
//!              │             │             │
//!     ┌────────▼──┐   ┌──────▼──────┐   ┌─▼────────┐
//!     │ Datanode  │   │  Datanode   │   │ Datanode │
//!     │ ┌───────┐ │   │  ┌───────┐  │   │ ┌──────┐ │
//!     │ │Region │ │   │  │Region │  │   │ │Region│ │
//!     │ └───────┘ │   │  └───────┘  │   │ └──────┘ │
//!     └───────────┘   └─────────────┘   └──────────┘
//! ```
//!
//! # Core Components
//!
//! - **dist_plan**: Distributed query planning and optimization
//! - **executor**: Query execution on datanodes and result merging
//! - **region_query**: Region-level query execution
//! - **optimizer**: Query optimization rules
//! - **meta**: Metadata service for cluster coordination
//!
//! # Query Flow
//!
//! 1. Client sends SQL query to Frontend
//! 2. Frontend parses SQL and generates logical plan (DataFusion)
//! 3. DistPlanner analyzes logical plan:
//!    - Identifies tables and regions to query
//!    - Performs region pruning based on predicates
//!    - Generates sub-plans for each datanode
//! 4. Frontend executes sub-queries in parallel
//! 5. Datanodes execute local queries and return results
//! 6. Frontend merges results (sort, aggregate, etc.)
//! 7. Returns final result to client

// Allow some lints during development
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

// Core modules
pub mod error;
pub mod types;

// Distributed query planning
pub mod dist_plan;

// Query optimization
pub mod optimizer;

// Query execution
pub mod executor;

// Region-level queries
pub mod region_query;

// Metadata management
pub mod meta;

// HTTP Table Provider for remote data access
pub mod http_table_provider;

// Re-exports
pub use error::{Error, Result};
pub use types::{NodeId, NodeStatus, PartitionId, RegionId, RegionStatus, SchemaRef, TableId};

