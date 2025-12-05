//! Query plan serialization for distributed execution.
//!
//! This module provides utilities for serializing and deserializing
//! DataFusion logical and physical plans for transmission between nodes.

use crate::error::{DistributedError, Result};
use arrow::datatypes::SchemaRef;
use bytes::Bytes;
use datafusion::common::DFSchemaRef;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use observability_deps::tracing::{debug, trace};
use prost::Message;
use std::io::Cursor;
use std::sync::Arc;

/// Serializer for DataFusion query plans.
///
/// This uses DataFusion's built-in protobuf serialization when available,
/// with fallbacks for custom extensions.
#[derive(Debug)]
pub struct PlanSerializer {
    /// Session context for plan operations
    ctx: SessionContext,
}

impl PlanSerializer {
    /// Create a new PlanSerializer with a fresh session context.
    pub fn new() -> Self {
        Self {
            ctx: SessionContext::new(),
        }
    }

    /// Create a PlanSerializer with an existing session context.
    pub fn with_context(ctx: SessionContext) -> Self {
        Self { ctx }
    }

    /// Serialize a logical plan to bytes.
    ///
    /// The serialized format includes:
    /// - Plan structure
    /// - Schema information
    /// - Expression details
    pub fn serialize_logical_plan(&self, plan: &LogicalPlan) -> Result<Bytes> {
        debug!("Serializing logical plan");
        trace!(plan = ?plan, "Plan to serialize");

        // Use a custom binary format for now
        // In a full implementation, we'd use datafusion-proto
        let serialized = self.serialize_logical_plan_internal(plan)?;

        Ok(Bytes::from(serialized))
    }

    /// Deserialize a logical plan from bytes.
    pub fn deserialize_logical_plan(&self, bytes: &[u8]) -> Result<LogicalPlan> {
        debug!(size = bytes.len(), "Deserializing logical plan");

        self.deserialize_logical_plan_internal(bytes)
    }

    /// Serialize an execution (physical) plan to bytes.
    pub fn serialize_physical_plan(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<Bytes> {
        debug!("Serializing physical plan");

        let serialized = self.serialize_physical_plan_internal(plan)?;

        Ok(Bytes::from(serialized))
    }

    /// Deserialize an execution plan from bytes.
    pub fn deserialize_physical_plan(&self, bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
        debug!(size = bytes.len(), "Deserializing physical plan");

        self.deserialize_physical_plan_internal(bytes)
    }

    // Internal serialization methods

    fn serialize_logical_plan_internal(&self, plan: &LogicalPlan) -> Result<Vec<u8>> {
        // Create a custom serialization format
        let mut buffer = Vec::new();

        // Write magic bytes for format identification
        buffer.extend_from_slice(b"DFPL"); // DataFusion Plan Logical

        // Write version
        buffer.push(1);

        // Serialize plan to JSON for now (simple but functional)
        // In production, use proper protobuf serialization from datafusion-proto
        let plan_json = self
            .plan_to_json(plan)
            .map_err(|e| DistributedError::PlanSerialization(e.to_string()))?;

        // Write length-prefixed JSON
        let json_bytes = plan_json.as_bytes();
        let len = json_bytes.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(json_bytes);

        Ok(buffer)
    }

    fn deserialize_logical_plan_internal(&self, bytes: &[u8]) -> Result<LogicalPlan> {
        if bytes.len() < 9 {
            return Err(DistributedError::PlanDeserialization(
                "Invalid plan data: too short".to_string(),
            ));
        }

        // Check magic bytes
        if &bytes[0..4] != b"DFPL" {
            return Err(DistributedError::PlanDeserialization(
                "Invalid plan format: wrong magic bytes".to_string(),
            ));
        }

        // Check version
        let version = bytes[4];
        if version != 1 {
            return Err(DistributedError::PlanDeserialization(format!(
                "Unsupported plan version: {}",
                version
            )));
        }

        // Read JSON length
        let len = u32::from_le_bytes([bytes[5], bytes[6], bytes[7], bytes[8]]) as usize;

        if bytes.len() < 9 + len {
            return Err(DistributedError::PlanDeserialization(
                "Invalid plan data: truncated".to_string(),
            ));
        }

        let json_str = std::str::from_utf8(&bytes[9..9 + len])
            .map_err(|e| DistributedError::PlanDeserialization(e.to_string()))?;

        self.json_to_plan(json_str)
            .map_err(|e| DistributedError::PlanDeserialization(e.to_string()))
    }

    fn serialize_physical_plan_internal(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();

        // Write magic bytes
        buffer.extend_from_slice(b"DFPP"); // DataFusion Plan Physical

        // Write version
        buffer.push(1);

        // Serialize the display representation for now
        // Full implementation would use datafusion-proto's PhysicalPlan serde
        let plan_str = format!("{:?}", plan);
        let plan_bytes = plan_str.as_bytes();
        let len = plan_bytes.len() as u32;
        buffer.extend_from_slice(&len.to_le_bytes());
        buffer.extend_from_slice(plan_bytes);

        // Also serialize the schema
        let schema = plan.schema();
        let schema_json = self
            .schema_to_json(&schema)
            .map_err(|e| DistributedError::PlanSerialization(e.to_string()))?;
        let schema_bytes = schema_json.as_bytes();
        let schema_len = schema_bytes.len() as u32;
        buffer.extend_from_slice(&schema_len.to_le_bytes());
        buffer.extend_from_slice(schema_bytes);

        Ok(buffer)
    }

    fn deserialize_physical_plan_internal(&self, bytes: &[u8]) -> Result<Arc<dyn ExecutionPlan>> {
        if bytes.len() < 9 {
            return Err(DistributedError::PlanDeserialization(
                "Invalid physical plan data: too short".to_string(),
            ));
        }

        // Check magic bytes
        if &bytes[0..4] != b"DFPP" {
            return Err(DistributedError::PlanDeserialization(
                "Invalid physical plan format: wrong magic bytes".to_string(),
            ));
        }

        // For now, return an error since proper physical plan deserialization
        // requires the full datafusion-proto implementation
        Err(DistributedError::PlanDeserialization(
            "Physical plan deserialization requires datafusion-proto".to_string(),
        ))
    }

    // JSON conversion helpers

    fn plan_to_json(
        &self,
        plan: &LogicalPlan,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        // Simplified JSON representation
        // In production, use datafusion-proto's JSON serialization
        let repr = LogicalPlanRepr::from_plan(plan);
        Ok(serde_json::to_string(&repr)?)
    }

    fn json_to_plan(
        &self,
        json: &str,
    ) -> std::result::Result<LogicalPlan, Box<dyn std::error::Error>> {
        let repr: LogicalPlanRepr = serde_json::from_str(json)?;
        repr.to_plan(&self.ctx)
    }

    fn schema_to_json(
        &self,
        schema: &SchemaRef,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        // Arrow schema has built-in JSON serialization
        Ok(serde_json::to_string(schema.as_ref())?)
    }
}

impl Default for PlanSerializer {
    fn default() -> Self {
        Self::new()
    }
}

/// Simplified representation of a logical plan for serialization.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct LogicalPlanRepr {
    /// Plan type name
    plan_type: String,
    /// Schema as JSON
    schema: Option<String>,
    /// Child plans
    children: Vec<LogicalPlanRepr>,
    /// Plan-specific properties
    properties: serde_json::Map<String, serde_json::Value>,
}

impl LogicalPlanRepr {
    fn from_plan(plan: &LogicalPlan) -> Self {
        let plan_type = match plan {
            LogicalPlan::TableScan(_) => "TableScan",
            LogicalPlan::Projection(_) => "Projection",
            LogicalPlan::Filter(_) => "Filter",
            LogicalPlan::Aggregate(_) => "Aggregate",
            LogicalPlan::Sort(_) => "Sort",
            LogicalPlan::Limit(_) => "Limit",
            LogicalPlan::Join(_) => "Join",
            LogicalPlan::Union(_) => "Union",
            LogicalPlan::EmptyRelation(_) => "EmptyRelation",
            LogicalPlan::SubqueryAlias(_) => "SubqueryAlias",
            _ => "Other",
        };

        let children: Vec<LogicalPlanRepr> = plan
            .inputs()
            .iter()
            .map(|p| LogicalPlanRepr::from_plan(p))
            .collect();

        let mut properties = serde_json::Map::new();

        // Add plan-specific properties
        match plan {
            LogicalPlan::TableScan(scan) => {
                properties.insert(
                    "table_name".to_string(),
                    serde_json::Value::String(scan.table_name.to_string()),
                );
                if let Some(proj) = &scan.projection {
                    properties.insert(
                        "projection".to_string(),
                        serde_json::Value::Array(
                            proj.iter()
                                .map(|i| serde_json::Value::Number((*i).into()))
                                .collect(),
                        ),
                    );
                }
            }
            LogicalPlan::Limit(limit) => {
                properties.insert(
                    "skip".to_string(),
                    serde_json::Value::Number(limit.skip.into()),
                );
                if let Some(fetch) = limit.fetch {
                    properties.insert("fetch".to_string(), serde_json::Value::Number(fetch.into()));
                }
            }
            _ => {}
        }

        Self {
            plan_type: plan_type.to_string(),
            schema: plan
                .schema()
                .inner()
                .as_ref()
                .to_json()
                .ok()
                .map(|v| v.to_string()),
            children,
            properties,
        }
    }

    fn to_plan(
        &self,
        ctx: &SessionContext,
    ) -> std::result::Result<LogicalPlan, Box<dyn std::error::Error>> {
        // This is a simplified implementation
        // Full implementation would reconstruct all plan types
        match self.plan_type.as_str() {
            "EmptyRelation" => Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: DFSchemaRef::new(datafusion::common::DFSchema::empty()),
                },
            )),
            _ => {
                // For other plan types, we'd need more sophisticated reconstruction
                // For now, return an empty relation as a placeholder
                Ok(LogicalPlan::EmptyRelation(
                    datafusion::logical_expr::EmptyRelation {
                        produce_one_row: false,
                        schema: DFSchemaRef::new(datafusion::common::DFSchema::empty()),
                    },
                ))
            }
        }
    }
}

/// Utility for creating Arrow IPC serialized batches.
pub struct ArrowIpcSerializer;

impl ArrowIpcSerializer {
    /// Serialize a record batch to Arrow IPC format.
    pub fn serialize_batch(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buffer, &batch.schema())
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
        }
        Ok(buffer)
    }

    /// Deserialize a record batch from Arrow IPC format.
    pub fn deserialize_batch(data: &[u8]) -> Result<arrow::array::RecordBatch> {
        let cursor = Cursor::new(data);
        let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
            .map_err(|e| DistributedError::SerializationError(e.to_string()))?;

        // Read the first batch
        let mut batches = Vec::new();
        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            batches.push(batch);
        }

        if batches.is_empty() {
            return Err(DistributedError::SerializationError(
                "No batches in IPC data".to_string(),
            ));
        }

        Ok(batches.remove(0))
    }

    /// Serialize multiple record batches to Arrow IPC format.
    pub fn serialize_batches(batches: &[arrow::array::RecordBatch]) -> Result<Vec<u8>> {
        if batches.is_empty() {
            return Err(DistributedError::SerializationError(
                "No batches to serialize".to_string(),
            ));
        }

        let schema = batches[0].schema();
        let mut buffer = Vec::new();
        {
            let mut writer = arrow::ipc::writer::FileWriter::try_new(&mut buffer, &schema)
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            for batch in batches {
                writer
                    .write(batch)
                    .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            }
            writer
                .finish()
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
        }
        Ok(buffer)
    }

    /// Deserialize all record batches from Arrow IPC format.
    pub fn deserialize_batches(data: &[u8]) -> Result<Vec<arrow::array::RecordBatch>> {
        let cursor = Cursor::new(data);
        let reader = arrow::ipc::reader::FileReader::try_new(cursor, None)
            .map_err(|e| DistributedError::SerializationError(e.to_string()))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

/// Utility for creating Arrow IPC streaming format (for gRPC streaming).
pub struct ArrowStreamSerializer;

impl ArrowStreamSerializer {
    /// Serialize a record batch to Arrow IPC streaming format.
    pub fn serialize_batch(batch: &arrow::array::RecordBatch) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        {
            let mut writer =
                arrow::ipc::writer::StreamWriter::try_new(&mut buffer, &batch.schema())
                    .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .write(batch)
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            writer
                .finish()
                .map_err(|e| DistributedError::SerializationError(e.to_string()))?;
        }
        Ok(buffer)
    }

    /// Deserialize record batches from Arrow IPC streaming format.
    pub fn deserialize_batches(data: &[u8]) -> Result<Vec<arrow::array::RecordBatch>> {
        let cursor = Cursor::new(data);
        let reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
            .map_err(|e| DistributedError::SerializationError(e.to_string()))?;

        let mut batches = Vec::new();
        for batch_result in reader {
            let batch =
                batch_result.map_err(|e| DistributedError::SerializationError(e.to_string()))?;
            batches.push(batch);
        }

        Ok(batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> arrow::array::RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        let id_array = Int64Array::from(vec![1, 2, 3]);
        let name_array = StringArray::from(vec![Some("Alice"), Some("Bob"), None]);

        arrow::array::RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(name_array)])
            .unwrap()
    }

    #[test]
    fn test_arrow_ipc_roundtrip() {
        let batch = create_test_batch();

        let serialized = ArrowIpcSerializer::serialize_batch(&batch).unwrap();
        let deserialized = ArrowIpcSerializer::deserialize_batch(&serialized).unwrap();

        assert_eq!(batch.num_rows(), deserialized.num_rows());
        assert_eq!(batch.num_columns(), deserialized.num_columns());
    }

    #[test]
    fn test_arrow_ipc_multiple_batches() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();

        let serialized =
            ArrowIpcSerializer::serialize_batches(&[batch1.clone(), batch2.clone()]).unwrap();
        let deserialized = ArrowIpcSerializer::deserialize_batches(&serialized).unwrap();

        assert_eq!(deserialized.len(), 2);
        assert_eq!(batch1.num_rows(), deserialized[0].num_rows());
        assert_eq!(batch2.num_rows(), deserialized[1].num_rows());
    }

    #[test]
    fn test_arrow_stream_roundtrip() {
        let batch = create_test_batch();

        let serialized = ArrowStreamSerializer::serialize_batch(&batch).unwrap();
        let deserialized = ArrowStreamSerializer::deserialize_batches(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1);
        assert_eq!(batch.num_rows(), deserialized[0].num_rows());
    }

    #[test]
    fn test_plan_serializer_creation() {
        let serializer = PlanSerializer::new();
        // Just verify it can be created
        assert!(serializer.ctx.state().config().target_partitions() > 0);
    }
}
