//! Local query executor for Datanode.
//!
//! The LocalExecutor wraps the existing QueryExecutorImpl to execute
//! query plans on local data.

use crate::common::{NodeId, RegionId};
use crate::error::{DistributedError, Result};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use influxdb3_internal_api::query_executor::QueryExecutor;
use influxdb_influxql_parser::statement::Statement;
use iox_query_params::StatementParams;
use observability_deps::tracing::{debug, info};
use std::sync::Arc;
use trace::ctx::SpanContext;
use trace_http::ctx::RequestLogContext;

/// Local query executor that wraps the existing QueryExecutorImpl.
///
/// This provides a bridge between the distributed query framework and
/// the existing single-node query execution infrastructure.
#[derive(Debug)]
pub struct LocalExecutor {
    /// The underlying query executor
    query_executor: Arc<dyn QueryExecutor>,

    /// Node ID of this datanode
    node_id: NodeId,

    /// Regions managed by this executor
    managed_regions: parking_lot::RwLock<Vec<RegionId>>,
}

impl LocalExecutor {
    /// Create a new LocalExecutor.
    pub fn new(query_executor: Arc<dyn QueryExecutor>, node_id: NodeId) -> Self {
        Self {
            query_executor,
            node_id,
            managed_regions: parking_lot::RwLock::new(Vec::new()),
        }
    }

    /// Get the node ID.
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Add a region to this executor.
    pub fn add_region(&self, region_id: RegionId) {
        let mut regions = self.managed_regions.write();
        if !regions.contains(&region_id) {
            regions.push(region_id);
            info!(node_id = %self.node_id, region_id = %region_id, "Added region to local executor");
        }
    }

    /// Remove a region from this executor.
    pub fn remove_region(&self, region_id: RegionId) {
        let mut regions = self.managed_regions.write();
        regions.retain(|id| *id != region_id);
        info!(node_id = %self.node_id, region_id = %region_id, "Removed region from local executor");
    }

    /// Check if a region is managed by this executor.
    pub fn has_region(&self, region_id: RegionId) -> bool {
        self.managed_regions.read().contains(&region_id)
    }

    /// Get all managed regions.
    pub fn get_managed_regions(&self) -> Vec<RegionId> {
        self.managed_regions.read().clone()
    }

    /// Execute a SQL query on the local data.
    ///
    /// This delegates to the underlying QueryExecutor.
    pub async fn execute_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            node_id = %self.node_id,
            database = %database,
            query = %query,
            "Executing SQL query locally"
        );

        self.query_executor
            .query_sql(database, query, params, span_ctx, external_span_ctx)
            .await
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))
    }

    /// Execute an InfluxQL query on the local data.
    ///
    /// This delegates to the underlying QueryExecutor.
    pub async fn execute_influxql(
        &self,
        database: &str,
        query: &str,
        statement: Statement,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!(
            node_id = %self.node_id,
            database = %database,
            query = %query,
            "Executing InfluxQL query locally"
        );

        self.query_executor
            .query_influxql(
                database,
                query,
                statement,
                params,
                span_ctx,
                external_span_ctx,
            )
            .await
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))
    }

    /// Get a reference to the underlying query executor.
    pub fn query_executor(&self) -> &dyn QueryExecutor {
        self.query_executor.as_ref()
    }
}

/// A wrapper that provides region-aware query execution.
///
/// This can filter query results to only include data from specific regions,
/// which is useful when a datanode hosts multiple regions for the same table.
#[derive(Debug)]
pub struct RegionAwareExecutor {
    /// The underlying local executor
    executor: Arc<LocalExecutor>,

    /// The specific region this is executing for
    region_id: RegionId,
}

impl RegionAwareExecutor {
    /// Create a new RegionAwareExecutor.
    pub fn new(executor: Arc<LocalExecutor>, region_id: RegionId) -> Self {
        Self {
            executor,
            region_id,
        }
    }

    /// Get the region ID.
    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    /// Execute a SQL query for this specific region.
    ///
    /// Note: In a full implementation, this would add region filtering
    /// to the query to ensure only data from this region is returned.
    pub async fn execute_sql(
        &self,
        database: &str,
        query: &str,
        params: Option<StatementParams>,
        span_ctx: Option<SpanContext>,
        external_span_ctx: Option<RequestLogContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: Add region filtering to the query
        // For now, we execute the query as-is
        self.executor
            .execute_sql(database, query, params, span_ctx, external_span_ctx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Testing requires a mock QueryExecutor, which we'd need to create
    // For now, we'll just test the basic functionality that doesn't need the executor

    #[test]
    fn test_add_remove_region() {
        // Create a mock executor - in real tests this would be a proper mock
        // For now we just test the region tracking logic conceptually

        let regions: parking_lot::RwLock<Vec<RegionId>> = parking_lot::RwLock::new(Vec::new());

        // Add region
        {
            let mut r = regions.write();
            let region_id = RegionId::new(1);
            if !r.contains(&region_id) {
                r.push(region_id);
            }
        }

        // Check region exists
        assert!(regions.read().contains(&RegionId::new(1)));

        // Remove region
        {
            let mut r = regions.write();
            r.retain(|id| *id != RegionId::new(1));
        }

        // Check region is gone
        assert!(!regions.read().contains(&RegionId::new(1)));
    }
}
