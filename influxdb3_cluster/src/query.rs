//! Distributed query execution
//!
//! This module handles distributed query planning and execution.

pub mod distributed_planner;
pub mod federated_query;
pub mod join;

pub use federated_query::FederatedQueryExecutor;
pub use federated_query::StandaloneFederatedExecutor;

