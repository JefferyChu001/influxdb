//! Frontend module for distributed query coordination.
//!
//! The Frontend is responsible for:
//! - Receiving client requests (SQL, InfluxQL, writes)
//! - Planning distributed queries
//! - Coordinating execution across Datanodes
//! - Aggregating results

mod coordinator;
mod distributed_executor;
mod planner;
mod router;

pub use coordinator::QueryCoordinator;
pub use distributed_executor::DistributedQueryExecutor;
pub use planner::DistributedPlanner;
pub use router::WriteRouter;

use crate::common::{NodeId, NodeInfo, RegionId, RegionInfo};
use crate::error::Result;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use influxdb3_write::BufferedWriteRequest;

/// Trait for distributed query execution.
#[async_trait]
pub trait DistributedQueryApi: Send + Sync + std::fmt::Debug {
    /// Execute a distributed SQL query.
    async fn query_sql(
        &self,
        database: &str,
        query: &str,
    ) -> Result<SendableRecordBatchStream>;

    /// Execute a distributed InfluxQL query.
    async fn query_influxql(
        &self,
        database: &str,
        query: &str,
    ) -> Result<SendableRecordBatchStream>;
}

/// Trait for distributed write routing.
#[async_trait]
pub trait DistributedWriteApi: Send + Sync + std::fmt::Debug {
    /// Route and execute a write request.
    async fn write_lp(
        &self,
        database: &str,
        line_protocol: &str,
        precision: influxdb3_types::write::Precision,
    ) -> Result<BufferedWriteRequest>;
}
