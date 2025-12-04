//! Distributed query execution components
//!
//! This module provides distributed query capabilities by extending the
//! existing iox_query infrastructure with remote table providers and
//! execution plans that can fetch data from remote nodes.

pub mod table_provider;
pub mod scan_exec;
pub mod multi_node_scan_exec;
pub mod expr_converter;
pub mod statistics;
pub mod join_optimizer;
pub mod physical_join_optimizer;

pub use table_provider::DistributedTableProvider;
pub use scan_exec::RemoteTableScanExec;
pub use multi_node_scan_exec::MultiNodeScanExec;
pub use expr_converter::ExprToSqlConverter;
pub use statistics::RemoteTableStatistics;
pub use join_optimizer::DistributedJoinOptimizer;
pub use physical_join_optimizer::PhysicalJoinOptimizer;

