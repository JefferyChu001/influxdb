//! Distributed query execution components
//!
//! This module provides distributed query capabilities by extending the
//! existing iox_query infrastructure with remote table providers and
//! execution plans that can fetch data from remote nodes.

pub mod table_provider;
pub mod scan_exec;
pub mod expr_converter;
pub mod statistics;

pub use table_provider::DistributedTableProvider;
pub use scan_exec::RemoteTableScanExec;
pub use expr_converter::ExprToSqlConverter;
pub use statistics::RemoteTableStatistics;

