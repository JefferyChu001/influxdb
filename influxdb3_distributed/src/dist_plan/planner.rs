//! Distributed query planner
//!
//! Converts DataFusion logical plans into distributed physical plans

use crate::error::*;
use crate::meta::MetaServiceRef;
use crate::types::*;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Distributed query plan
///
/// Contains both the coordinator-side plan and remote execution plans
#[derive(Debug, Clone)]
pub struct DistributedPlan {
    /// The plan to execute on the coordinator node
    pub coordinator_plan: Arc<dyn ExecutionPlan>,
    /// Plans to execute on remote datanodes
    pub remote_plans: Vec<RemotePlan>,
}

/// Plan to execute on a remote datanode
#[derive(Debug, Clone)]
pub struct RemotePlan {
    /// Target node ID
    pub node_id: NodeId,
    /// Regions to query on this node
    pub regions: Vec<RegionId>,
    /// Physical plan to execute
    pub plan: Arc<dyn ExecutionPlan>,
}

/// Distributed query planner
pub struct DistributedPlanner {
    meta_service: MetaServiceRef,
}

impl DistributedPlanner {
    pub fn new(meta_service: MetaServiceRef) -> Self {
        Self { meta_service }
    }

    /// Convert a logical plan to a distributed physical plan
    ///
    /// This is the main entry point for distributed query planning
    pub async fn plan(&self, logical_plan: &LogicalPlan) -> Result<DistributedPlan> {
        // TODO: Implement in Phase 2
        Err(Error::not_implemented("Distributed planning"))
    }
}

