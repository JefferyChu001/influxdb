//! Datanode module for data storage and query execution.
//!
//! The Datanode is responsible for:
//! - Storing data regions
//! - Executing query plans locally
//! - Handling write requests for assigned regions

mod client;
mod executor;
mod server;

pub use client::DatanodeClient;
pub use executor::LocalExecutor;
pub use server::DatanodeServer;

use crate::common::{NodeId, RegionId, RegionInfo};
use crate::error::Result;
use crate::proto::{ExecutePlanRequest, GetRegionStatusResponse, WriteRequest, WriteResponse};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;

/// Trait for Datanode operations.
///
/// This defines the interface that a Datanode must implement for handling
/// queries and writes.
#[async_trait]
pub trait DatanodeApi: Send + Sync + std::fmt::Debug {
    /// Execute a query plan and return a stream of record batches.
    async fn execute_plan(&self, request: ExecutePlanRequest) -> Result<SendableRecordBatchStream>;

    /// Write data to a region.
    async fn write(&self, request: WriteRequest) -> Result<WriteResponse>;

    /// Get the status of a region.
    async fn get_region_status(&self, region_id: RegionId) -> Result<GetRegionStatusResponse>;

    /// Get all regions managed by this datanode.
    async fn get_managed_regions(&self) -> Result<Vec<RegionInfo>>;

    /// Get the node ID of this datanode.
    fn node_id(&self) -> NodeId;

    /// Check if a region is managed by this datanode.
    fn has_region(&self, region_id: RegionId) -> bool;
}
