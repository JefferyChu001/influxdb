//! InfluxDB 3.0 Distributed Query and Write Framework
//!
//! This crate provides a distributed layer on top of the single-node InfluxDB 3.0 Core,
//! enabling distributed queries and writes across multiple nodes.
//!
//! # Architecture
//!
//! The distributed framework follows a three-tier architecture inspired by GreptimeDB
//! and DataFusion-Ballista:
//!
//! - **Frontend**: Receives client requests, generates distributed query plans, and
//!   coordinates query execution across Datanodes.
//! - **MetaServer**: Manages cluster metadata, including node registry, region distribution,
//!   and schema information.
//! - **Datanode**: Stores data shards (regions) and executes local query plans.
//!
//! # Data Exchange
//!
//! Data is exchanged between nodes using Apache Arrow Flight protocol, which provides
//! efficient, zero-copy data transfer.
//!
//! # Example
//!
//! ```ignore
//! use influxdb3_distributed::{
//!     frontend::Frontend,
//!     meta::MetaClient,
//!     config::ClusterConfig,
//! };
//!
//! let config = ClusterConfig::new()
//!     .with_meta_servers(vec!["meta1:9000", "meta2:9000"]);
//!
//! let meta_client = MetaClient::connect(&config).await?;
//! let frontend = Frontend::new(meta_client);
//!
//! let stream = frontend.query_sql("mydb", "SELECT * FROM cpu").await?;
//! ```

// Temporarily allow some lints while the crate is under development
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(clippy::derive_partial_eq_without_eq)]
#![allow(clippy::missing_const_for_fn)]
#![allow(clippy::redundant_field_names)]
#![allow(clippy::uninlined_format_args)]
#![allow(clippy::type_complexity)]
#![warn(
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::clone_on_ref_ptr
)]

pub mod common;
pub mod config;
pub mod datanode;
pub mod error;
pub mod frontend;
pub mod meta;
pub mod proto;

// Re-export commonly used types
pub use common::{NodeId, RegionId};
pub use config::{ClusterConfig, NodeMode};
pub use error::{DistributedError, Result};
