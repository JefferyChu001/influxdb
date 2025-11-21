//! Error types for the cluster module

use crate::types::{NodeId, ShardId};
use snafu::Snafu;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Node not found: {}", node_id))]
    NodeNotFound { node_id: NodeId },

    #[snafu(display("Shard not found: {}", shard_id))]
    ShardNotFound { shard_id: ShardId },

    #[snafu(display("No leader found for shard: {}", shard_id))]
    NoLeaderFound { shard_id: ShardId },

    #[snafu(display("Insufficient nodes: required {}, available {}", required, available))]
    InsufficientNodes { required: usize, available: usize },

    #[snafu(display("Quorum not reached: required {}, achieved {}", required, achieved))]
    QuorumNotReached { required: usize, achieved: usize },

    #[snafu(display("Invalid node configuration: {}", message))]
    InvalidNodeConfig { message: String },

    #[snafu(display("Invalid shard configuration: {}", message))]
    InvalidShardConfig { message: String },

    #[snafu(display("Metadata store error: {}", source))]
    MetaStoreError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Raft error: {}", source))]
    RaftError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("RPC error: {}", source))]
    RpcError { source: tonic::Status },

    #[snafu(display("Serialization error: {}", source))]
    SerializationError { source: serde_json::Error },

    #[snafu(display("IO error: {}", source))]
    IoError { source: std::io::Error },

    #[snafu(display("Column not found: {}", column_name))]
    ColumnNotFound { column_name: String },

    #[snafu(display("Unsupported JOIN key type: {:?}", data_type))]
    UnsupportedJoinKeyType { data_type: arrow::datatypes::DataType },

    #[snafu(display("Query execution error: {}", message))]
    QueryExecutionError { message: String },

    #[snafu(display("Write error: {}", message))]
    WriteError { message: String },

    #[snafu(display("Timeout error: {}", message))]
    TimeoutError { message: String },

    #[snafu(display("Internal error: {}", message))]
    InternalError { message: String },
}

impl From<tonic::Status> for Error {
    fn from(status: tonic::Status) -> Self {
        Error::RpcError { source: status }
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::SerializationError { source: err }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError { source: err }
    }
}

