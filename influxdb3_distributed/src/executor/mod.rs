//! Query execution on datanodes and result merging
//!
//! This module implements the execution layer for distributed queries,
//! including remote execution and result streaming.

pub mod remote_client;
pub mod remote_exec;

pub use remote_client::{QueryStatus, RemoteQueryClient};
pub use remote_exec::RemoteExec;

