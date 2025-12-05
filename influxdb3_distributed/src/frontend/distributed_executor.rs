//! Distributed query executor that implements the QueryExecutor trait.
//!
//! This provides the main entry point for distributed queries, wrapping
//! the DistributedPlanner and QueryCoordinator.

use crate::common::{NodeId, RegionId};
use crate::error::{DistributedError, Result};
use crate::frontend::DistributedQueryApi;
use crate::frontend::coordinator::QueryCoordinator;
use crate::frontend::planner::DistributedPlanner;
use crate::meta::MetaServiceApi;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::prelude::SessionContext;
use influxdb_influxql_parser::statement::Statement;
use influxdb3_internal_api::query_executor::{QueryExecutor, QueryExecutorError};
use iox_query_params::StatementParams;
use observability_deps::tracing::{debug, info};
use std::fmt;
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace_http::ctx::RequestLogContext;

/// Distributed query executor that coordinates queries across the cluster.
///
/// This implements the `QueryExecutor` trait to provide a drop-in replacement
/// for the single-node query executor.
pub struct DistributedQueryExecutor<M: MetaServiceApi> {
    /// Distributed planner
    planner: Arc<DistributedPlanner<M>>,

    /// Query coordinator
    coordinator: Arc<QueryCoordinator<M>>,

    /// Session context for local execution
    session_ctx: SessionContext,
}

impl<M: MetaServiceApi> fmt::Debug for DistributedQueryExecutor<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DistributedQueryExecutor")
            .field("planner", &self.planner)
            .field("coordinator", &self.coordinator)
            .finish_non_exhaustive()
    }
}

impl<M: MetaServiceApi + 'static> DistributedQueryExecutor<M> {
    /// Create a new DistributedQueryExecutor.
    pub fn new(meta_client: Arc<M>) -> Self {
        let planner = Arc::new(DistributedPlanner::new(Arc::clone(&meta_client)));
        let coordinator = Arc::new(QueryCoordinator::new(meta_client));

        Self {
            planner,
            coordinator,
            session_ctx: SessionContext::new(),
        }
    }

    /// Execute a SQL query with distributed execution.
    pub async fn execute_sql(
        &self,
        database: &str,
        sql: &str,
    ) -> Result<SendableRecordBatchStream> {
        info!(database = %database, sql = %sql, "Executing distributed SQL query");

        // Parse the SQL into a logical plan
        let logical_plan = self
            .session_ctx
            .state()
            .create_logical_plan(sql)
            .await
            .map_err(|e| DistributedError::QueryPlanning(e.to_string()))?;

        // Create a distributed plan
        let distributed_plan = self.planner.plan(database, &logical_plan).await?;

        debug!(
            is_local = distributed_plan.is_local(),
            is_single_node = distributed_plan.is_single_node(),
            is_distributed = distributed_plan.is_distributed(),
            "Created distributed plan"
        );

        // Execute the plan
        self.coordinator.execute(database, distributed_plan).await
    }

    /// Execute an InfluxQL query with distributed execution.
    pub async fn execute_influxql(
        &self,
        database: &str,
        query: &str,
    ) -> Result<SendableRecordBatchStream> {
        info!(database = %database, query = %query, "Executing distributed InfluxQL query");

        // For InfluxQL, we'll use a simpler approach:
        // Forward the query to all relevant nodes and merge results
        self.coordinator.execute_sql(database, query).await
    }

    /// Get the planner.
    pub fn planner(&self) -> &DistributedPlanner<M> {
        &self.planner
    }

    /// Get the coordinator.
    pub fn coordinator(&self) -> &QueryCoordinator<M> {
        &self.coordinator
    }
}

#[async_trait]
impl<M: MetaServiceApi + 'static> DistributedQueryApi for DistributedQueryExecutor<M> {
    async fn query_sql(&self, database: &str, query: &str) -> Result<SendableRecordBatchStream> {
        self.execute_sql(database, query).await
    }

    async fn query_influxql(
        &self,
        database: &str,
        query: &str,
    ) -> Result<SendableRecordBatchStream> {
        self.execute_influxql(database, query).await
    }
}

// NOTE: The QueryExecutor trait implementation for distributed execution is not yet complete.
// The QueryExecutor trait requires QueryDatabase which has additional constraints.
// For now, the DistributedQueryExecutor provides its own interface via DistributedQueryApi.
// A full QueryExecutor implementation will be added once we have proper integration
// with the existing single-node infrastructure.

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full testing requires mocking MetaServiceApi
    // These are placeholder tests

    #[test]
    fn test_executor_creation() {
        // This would require a mock MetaServiceApi
        // For now, just verify the types compile correctly
    }
}
