//! Error types for the distributed framework.

use std::fmt;

use arrow_flight::error::FlightError;
use datafusion::error::DataFusionError;
use tonic::Status;

/// Result type alias for distributed operations.
pub type Result<T, E = DistributedError> = std::result::Result<T, E>;

/// Errors that can occur in the distributed framework.
#[derive(Debug, thiserror::Error)]
pub enum DistributedError {
    /// Error from the metadata service
    #[error("MetaServer error: {0}")]
    MetaError(String),

    /// Error when a node is not found
    #[error("Node not found: {node_id}")]
    NodeNotFound { node_id: u64 },

    /// Error when a region is not found
    #[error("Region not found: {region_id}")]
    RegionNotFound { region_id: u64 },

    /// Error when a database is not found
    #[error("Database not found: {database}")]
    DatabaseNotFound { database: String },

    /// Error when a table is not found
    #[error("Table not found: {database}.{table}")]
    TableNotFound { database: String, table: String },

    /// Error from network communication
    #[error("Network error: {0}")]
    NetworkError(String),

    /// Error from gRPC transport
    #[error("gRPC transport error: {0}")]
    GrpcTransport(#[from] tonic::transport::Error),

    /// Error from gRPC status
    #[error("gRPC status error: {0}")]
    GrpcStatus(String),

    /// Error from Arrow Flight
    #[error("Arrow Flight error: {0}")]
    FlightError(String),

    /// Error from query execution
    #[error("Query execution error: {0}")]
    QueryExecution(String),

    /// Error from DataFusion
    #[error("DataFusion error: {0}")]
    DataFusion(#[from] DataFusionError),

    /// Error from query planning
    #[error("Query planning error: {0}")]
    QueryPlanning(String),

    /// Error when plan serialization fails
    #[error("Plan serialization error: {0}")]
    PlanSerialization(String),

    /// Error when plan deserialization fails
    #[error("Plan deserialization error: {0}")]
    PlanDeserialization(String),

    /// Error from write operations
    #[error("Write error: {0}")]
    WriteError(String),

    /// Error when the cluster is not initialized
    #[error("Cluster not initialized")]
    ClusterNotInitialized,

    /// Error when the node is not ready
    #[error("Node not ready: {node_id}")]
    NodeNotReady { node_id: u64 },

    /// Error from configuration
    #[error("Configuration error: {0}")]
    ConfigError(String),

    /// Error when an operation times out
    #[error("Operation timed out: {0}")]
    Timeout(String),

    /// Error when a node is already registered
    #[error("Node already registered: {node_id}")]
    NodeAlreadyRegistered { node_id: u64 },

    /// Error when a region is already assigned
    #[error("Region already assigned: {region_id} to node {node_id}")]
    RegionAlreadyAssigned { region_id: u64, node_id: u64 },

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),

    /// Error from the catalog
    #[error("Catalog error: {0}")]
    CatalogError(String),

    /// Error from serialization/deserialization
    #[error("Serialization error: {0}")]
    SerializationError(String),

    /// Connection pool exhausted
    #[error("Connection pool exhausted for node {node_id}")]
    ConnectionPoolExhausted { node_id: u64 },

    /// Invalid request
    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    /// Unsupported operation
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
}

impl DistributedError {
    /// Create a new meta error
    pub fn meta<S: Into<String>>(msg: S) -> Self {
        Self::MetaError(msg.into())
    }

    /// Create a new network error
    pub fn network<S: Into<String>>(msg: S) -> Self {
        Self::NetworkError(msg.into())
    }

    /// Create a new query execution error
    pub fn query<S: Into<String>>(msg: S) -> Self {
        Self::QueryExecution(msg.into())
    }

    /// Create a new write error
    pub fn write<S: Into<String>>(msg: S) -> Self {
        Self::WriteError(msg.into())
    }

    /// Create a new internal error
    pub fn internal<S: Into<String>>(msg: S) -> Self {
        Self::Internal(msg.into())
    }

    /// Check if this error is retriable
    pub fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::NetworkError(_)
                | Self::Timeout(_)
                | Self::NodeNotReady { .. }
                | Self::ConnectionPoolExhausted { .. }
                | Self::GrpcTransport(_)
        )
    }

    /// Check if this error indicates the node is unavailable
    pub fn is_node_unavailable(&self) -> bool {
        matches!(
            self,
            Self::NodeNotFound { .. }
                | Self::NodeNotReady { .. }
                | Self::NetworkError(_)
                | Self::GrpcTransport(_)
        )
    }
}

impl From<FlightError> for DistributedError {
    fn from(err: FlightError) -> Self {
        Self::FlightError(err.to_string())
    }
}

impl From<DistributedError> for Status {
    fn from(err: DistributedError) -> Self {
        match &err {
            DistributedError::NodeNotFound { .. }
            | DistributedError::RegionNotFound { .. }
            | DistributedError::DatabaseNotFound { .. }
            | DistributedError::TableNotFound { .. } => Status::not_found(err.to_string()),

            DistributedError::InvalidRequest(_) => Status::invalid_argument(err.to_string()),

            DistributedError::ClusterNotInitialized | DistributedError::NodeNotReady { .. } => {
                Status::unavailable(err.to_string())
            }

            DistributedError::Timeout(_) => Status::deadline_exceeded(err.to_string()),

            DistributedError::UnsupportedOperation(_) => Status::unimplemented(err.to_string()),

            DistributedError::NodeAlreadyRegistered { .. }
            | DistributedError::RegionAlreadyAssigned { .. } => {
                Status::already_exists(err.to_string())
            }

            _ => Status::internal(err.to_string()),
        }
    }
}

/// Extension trait for adding context to errors
pub trait ErrorContext<T> {
    /// Add context to an error
    fn context<S: Into<String>>(self, msg: S) -> Result<T>;
}

impl<T, E: std::error::Error> ErrorContext<T> for std::result::Result<T, E> {
    fn context<S: Into<String>>(self, msg: S) -> Result<T> {
        self.map_err(|e| DistributedError::Internal(format!("{}: {}", msg.into(), e)))
    }
}
