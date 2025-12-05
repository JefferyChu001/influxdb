//! Result merger for distributed query execution.
//!
//! This module provides utilities for merging query results from multiple datanodes:
//! - Stream merging: Combine multiple RecordBatch streams
//! - Sorted merging: Maintain sort order when merging
//! - Aggregate merging: Combine partial aggregates
//! - Limit handling: Apply global limits after merging

use crate::common::{NodeId, RegionId};
use crate::error::{DistributedError, Result};
use crate::frontend::optimizer::FinalOperation;
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow::compute::{self, concat_batches, sort_to_indices, take};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_util::MemoryStream;
use futures::Stream;
use futures::stream::{self, BoxStream, StreamExt, TryStreamExt};
use observability_deps::tracing::{debug, info, trace, warn};
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// Configuration for result merging.
#[derive(Debug, Clone)]
pub struct MergerConfig {
    /// Maximum number of rows to buffer before yielding
    pub batch_size: usize,
    /// Whether to preserve sort order
    pub preserve_order: bool,
    /// Memory limit for buffering (in bytes)
    pub memory_limit: usize,
}

impl Default for MergerConfig {
    fn default() -> Self {
        Self {
            batch_size: 8192,
            preserve_order: false,
            memory_limit: 256 * 1024 * 1024, // 256 MB
        }
    }
}

/// Merger for combining results from multiple sources.
#[derive(Debug)]
pub struct ResultMerger {
    config: MergerConfig,
}

impl ResultMerger {
    /// Create a new ResultMerger with default configuration.
    pub fn new() -> Self {
        Self {
            config: MergerConfig::default(),
        }
    }

    /// Create a ResultMerger with custom configuration.
    pub fn with_config(config: MergerConfig) -> Self {
        Self { config }
    }

    /// Merge multiple record batch streams into one.
    ///
    /// This is a simple union merge that interleaves batches from all streams.
    pub async fn merge_streams(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
    ) -> Result<SendableRecordBatchStream> {
        info!(num_streams = streams.len(), "Merging record batch streams");

        if streams.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        if streams.len() == 1 {
            let mut streams = streams;
            return Ok(streams.remove(0));
        }

        // Create a channel to collect results
        let (tx, mut rx) = mpsc::channel::<
            std::result::Result<RecordBatch, datafusion::error::DataFusionError>,
        >(100);
        let schema_clone = schema.clone();

        // Spawn tasks to read from each stream
        for (idx, stream) in streams.into_iter().enumerate() {
            let tx = tx.clone();
            tokio::spawn(async move {
                let mut stream = stream;
                while let Some(batch_result) = stream.next().await {
                    match batch_result {
                        Ok(batch) => {
                            if tx.send(Ok(batch)).await.is_err() {
                                trace!(stream_idx = idx, "Receiver dropped, stopping stream");
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                            break;
                        }
                    }
                }
            });
        }

        // Drop the original sender so the receiver knows when all senders are done
        drop(tx);

        // Create a stream from the receiver
        let output_stream = async_stream::try_stream! {
            while let Some(batch_result) = rx.recv().await {
                yield batch_result?;
            }
        };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_clone,
            output_stream,
        )))
    }

    /// Merge multiple record batch streams with sorted output.
    ///
    /// Each input stream is assumed to be sorted. The output maintains
    /// the global sort order across all streams.
    pub async fn merge_sorted(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
        sort_columns: Vec<SortColumn>,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            num_streams = streams.len(),
            num_sort_columns = sort_columns.len(),
            "Merging sorted streams"
        );

        if streams.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        if streams.len() == 1 {
            let mut streams = streams;
            return Ok(streams.remove(0));
        }

        // Collect all batches and merge
        // In a production system, we'd use a proper sorted merge with streaming
        let mut all_batches = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                all_batches.push(
                    batch_result.map_err(|e| DistributedError::QueryExecution(e.to_string()))?,
                );
            }
        }

        if all_batches.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        // Concatenate all batches
        let combined = concat_batches(&schema, &all_batches)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        // Sort the combined batch
        let sorted = self.sort_batch(&combined, &sort_columns)?;

        Ok(Box::pin(MemoryStream::try_new(vec![sorted], schema, None)?))
    }

    /// Sort a single batch by the specified columns.
    fn sort_batch(&self, batch: &RecordBatch, sort_columns: &[SortColumn]) -> Result<RecordBatch> {
        if sort_columns.is_empty() {
            return Ok(batch.clone());
        }

        // Build sort columns for arrow's sort function
        let mut arrow_sort_columns = Vec::new();
        for sc in sort_columns {
            let column_idx = batch
                .schema()
                .index_of(&sc.name)
                .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

            arrow_sort_columns.push(compute::SortColumn {
                values: batch.column(column_idx).clone(),
                options: Some(arrow::compute::SortOptions {
                    descending: sc.descending,
                    nulls_first: sc.nulls_first,
                }),
            });
        }

        let indices = compute::lexsort_to_indices(&arrow_sort_columns, None)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        // Take rows according to sorted indices
        let sorted_columns: std::result::Result<Vec<ArrayRef>, _> = batch
            .columns()
            .iter()
            .map(|col| take(col.as_ref(), &indices, None))
            .collect();

        let sorted_columns =
            sorted_columns.map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        RecordBatch::try_new(batch.schema(), sorted_columns)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))
    }

    /// Merge multiple streams and apply a limit.
    pub async fn merge_with_limit(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
        limit: usize,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            num_streams = streams.len(),
            limit = limit,
            "Merging streams with limit"
        );

        // Collect batches up to the limit
        let mut all_batches = Vec::new();
        let mut total_rows = 0usize;

        'outer: for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                let batch =
                    batch_result.map_err(|e| DistributedError::QueryExecution(e.to_string()))?;
                let batch_rows = batch.num_rows();

                if total_rows + batch_rows <= limit {
                    all_batches.push(batch);
                    total_rows += batch_rows;
                } else {
                    // Take only the rows we need
                    let remaining = limit - total_rows;
                    if remaining > 0 {
                        let sliced = batch.slice(0, remaining);
                        all_batches.push(sliced);
                        total_rows += remaining;
                    }
                    break 'outer;
                }
            }
        }

        if all_batches.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        // Concatenate results
        let combined = concat_batches(&schema, &all_batches)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        Ok(Box::pin(MemoryStream::try_new(
            vec![combined],
            schema,
            None,
        )?))
    }

    /// Merge partial aggregation results.
    ///
    /// This combines partial aggregates from multiple datanodes into final results.
    pub async fn merge_aggregates(
        &self,
        streams: Vec<SendableRecordBatchStream>,
        schema: SchemaRef,
        group_by: Vec<String>,
        aggregates: Vec<AggregateSpec>,
    ) -> Result<SendableRecordBatchStream> {
        info!(
            num_streams = streams.len(),
            num_groups = group_by.len(),
            num_aggregates = aggregates.len(),
            "Merging aggregate results"
        );

        // Collect all partial results
        let mut all_batches = Vec::new();
        for mut stream in streams {
            while let Some(batch_result) = stream.next().await {
                all_batches.push(
                    batch_result.map_err(|e| DistributedError::QueryExecution(e.to_string()))?,
                );
            }
        }

        if all_batches.is_empty() {
            return Ok(Box::pin(MemoryStream::try_new(vec![], schema, None)?));
        }

        // Concatenate all partial results
        let combined = concat_batches(&all_batches[0].schema(), &all_batches)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;

        // If no group by, just merge the aggregates directly
        if group_by.is_empty() {
            let merged = self.merge_aggregate_values(&combined, &aggregates)?;
            return Ok(Box::pin(MemoryStream::try_new(vec![merged], schema, None)?));
        }

        // Group by columns and merge aggregates for each group
        let merged = self.merge_grouped_aggregates(&combined, &group_by, &aggregates, &schema)?;

        Ok(Box::pin(MemoryStream::try_new(vec![merged], schema, None)?))
    }

    /// Merge aggregate values without grouping.
    fn merge_aggregate_values(
        &self,
        batch: &RecordBatch,
        aggregates: &[AggregateSpec],
    ) -> Result<RecordBatch> {
        let mut columns = Vec::new();
        let mut fields = Vec::new();

        for agg in aggregates {
            let (field, array) = self.compute_final_aggregate(batch, agg)?;
            fields.push(field);
            columns.push(array);
        }

        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| DistributedError::QueryExecution(e.to_string()))
    }

    /// Compute the final value for an aggregate.
    fn compute_final_aggregate(
        &self,
        batch: &RecordBatch,
        agg: &AggregateSpec,
    ) -> Result<(Field, ArrayRef)> {
        let column = batch.column_by_name(&agg.column).ok_or_else(|| {
            DistributedError::QueryExecution(format!(
                "Column '{}' not found for aggregate",
                agg.column
            ))
        })?;

        match agg.function {
            AggregateFunction::Sum => {
                // Sum all values
                let result = self.sum_column(column)?;
                let field = Field::new(&agg.alias, DataType::Float64, true);
                Ok((field, Arc::new(Float64Array::from(vec![result]))))
            }
            AggregateFunction::Count => {
                // Sum all counts
                let result = self.sum_column_as_u64(column)?;
                let field = Field::new(&agg.alias, DataType::UInt64, false);
                Ok((field, Arc::new(UInt64Array::from(vec![result]))))
            }
            AggregateFunction::Min => {
                // Find minimum
                let result = compute::min(column.as_ref());
                let field = Field::new(&agg.alias, column.data_type().clone(), true);
                Ok((
                    field,
                    result.map(|v| v.clone()).unwrap_or(column.slice(0, 0)),
                ))
            }
            AggregateFunction::Max => {
                // Find maximum
                let result = compute::max(column.as_ref());
                let field = Field::new(&agg.alias, column.data_type().clone(), true);
                Ok((
                    field,
                    result.map(|v| v.clone()).unwrap_or(column.slice(0, 0)),
                ))
            }
            AggregateFunction::Avg => {
                // For AVG, we need both sum and count
                // Assume the batch has both sum and count columns
                let sum = self.sum_column(column)?;
                let count_col = batch
                    .column_by_name(&format!("{}_count", agg.column))
                    .ok_or_else(|| {
                        DistributedError::QueryExecution(
                            "Count column not found for AVG".to_string(),
                        )
                    })?;
                let count = self.sum_column_as_u64(count_col)?;
                let avg = if count > 0 { sum / count as f64 } else { 0.0 };
                let field = Field::new(&agg.alias, DataType::Float64, true);
                Ok((field, Arc::new(Float64Array::from(vec![avg]))))
            }
        }
    }

    /// Sum a column as f64.
    fn sum_column(&self, column: &ArrayRef) -> Result<f64> {
        match column.data_type() {
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(array.iter().filter_map(|v| v).sum::<i64>() as f64)
            }
            DataType::Float64 => {
                let array = column.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(array.iter().filter_map(|v| v).sum::<f64>())
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(array.iter().filter_map(|v| v).sum::<u64>() as f64)
            }
            _ => Err(DistributedError::QueryExecution(format!(
                "Cannot sum column of type {:?}",
                column.data_type()
            ))),
        }
    }

    /// Sum a column as u64.
    fn sum_column_as_u64(&self, column: &ArrayRef) -> Result<u64> {
        match column.data_type() {
            DataType::Int64 => {
                let array = column.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(array.iter().filter_map(|v| v).sum::<i64>() as u64)
            }
            DataType::UInt64 => {
                let array = column.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(array.iter().filter_map(|v| v).sum::<u64>())
            }
            _ => Err(DistributedError::QueryExecution(format!(
                "Cannot sum column of type {:?} as u64",
                column.data_type()
            ))),
        }
    }

    /// Merge grouped aggregates.
    fn merge_grouped_aggregates(
        &self,
        batch: &RecordBatch,
        group_by: &[String],
        aggregates: &[AggregateSpec],
        output_schema: &SchemaRef,
    ) -> Result<RecordBatch> {
        // This is a simplified implementation
        // A full implementation would properly group by the columns and merge aggregates

        // For now, if there are group by columns, we need to re-aggregate
        // This is expensive but correct

        // Group rows by the group_by columns
        let mut groups: HashMap<Vec<String>, Vec<usize>> = HashMap::new();

        for row_idx in 0..batch.num_rows() {
            let mut key = Vec::new();
            for col_name in group_by {
                let col = batch.column_by_name(col_name).ok_or_else(|| {
                    DistributedError::QueryExecution(format!(
                        "Group by column '{}' not found",
                        col_name
                    ))
                })?;
                // Convert value to string for grouping
                let value = arrow::util::display::array_value_to_string(col, row_idx)
                    .map_err(|e| DistributedError::QueryExecution(e.to_string()))?;
                key.push(value);
            }
            groups.entry(key).or_insert_with(Vec::new).push(row_idx);
        }

        // For each group, compute final aggregates
        let mut result_columns: Vec<Vec<Box<dyn arrow::array::ArrayBuilder>>> = Vec::new();

        // This is a placeholder - full implementation would build proper arrays
        // For now, just return the original batch
        Ok(batch.clone())
    }

    /// Apply the final operation to merged results.
    pub async fn apply_final_operation(
        &self,
        stream: SendableRecordBatchStream,
        operation: &FinalOperation,
    ) -> Result<SendableRecordBatchStream> {
        let schema = stream.schema();

        match operation {
            FinalOperation::Passthrough => Ok(stream),
            FinalOperation::Union => Ok(stream),
            FinalOperation::Limit(limit) => {
                self.merge_with_limit(vec![stream], schema, *limit).await
            }
            FinalOperation::SortLimit { sort_exprs, limit } => {
                // Convert DataFusion SortExpr to our SortColumn
                let sort_columns: Vec<SortColumn> = sort_exprs
                    .iter()
                    .filter_map(|expr| {
                        if let datafusion::prelude::Expr::Column(col) = &expr.expr {
                            Some(SortColumn {
                                name: col.name.clone(),
                                descending: !expr.asc,
                                nulls_first: expr.nulls_first,
                            })
                        } else {
                            None
                        }
                    })
                    .collect();

                let sorted = self
                    .merge_sorted(vec![stream], schema.clone(), sort_columns)
                    .await?;

                if let Some(lim) = limit {
                    self.merge_with_limit(vec![sorted], schema, *lim).await
                } else {
                    Ok(sorted)
                }
            }
            FinalOperation::FinalAggregate {
                group_by,
                aggregates: _,
            } => {
                // Extract group by column names
                let group_columns: Vec<String> = group_by
                    .iter()
                    .filter_map(|expr| {
                        if let datafusion::prelude::Expr::Column(col) = expr {
                            Some(col.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // For now, pass through - proper aggregate merging would be done here
                Ok(stream)
            }
        }
    }
}

impl Default for ResultMerger {
    fn default() -> Self {
        Self::new()
    }
}

/// Specification for a sort column.
#[derive(Debug, Clone)]
pub struct SortColumn {
    /// Column name
    pub name: String,
    /// Sort in descending order
    pub descending: bool,
    /// Put nulls first
    pub nulls_first: bool,
}

/// Specification for an aggregate function.
#[derive(Debug, Clone)]
pub struct AggregateSpec {
    /// Aggregate function type
    pub function: AggregateFunction,
    /// Column to aggregate
    pub column: String,
    /// Output alias
    pub alias: String,
}

/// Supported aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Sum,
    Count,
    Min,
    Max,
    Avg,
}

impl AggregateFunction {
    /// Check if this aggregate function can be computed partially.
    pub fn is_decomposable(&self) -> bool {
        matches!(
            self,
            Self::Sum | Self::Count | Self::Min | Self::Max | Self::Avg
        )
    }

    /// Parse an aggregate function name.
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "SUM" => Some(Self::Sum),
            "COUNT" => Some(Self::Count),
            "MIN" => Some(Self::Min),
            "MAX" => Some(Self::Max),
            "AVG" => Some(Self::Avg),
            _ => None,
        }
    }
}

/// Wrapper for a batch with its source information.
#[derive(Debug)]
pub struct SourcedBatch {
    /// The record batch
    pub batch: RecordBatch,
    /// Source node ID
    pub node_id: NodeId,
    /// Source region ID
    pub region_id: RegionId,
}

impl SourcedBatch {
    pub fn new(batch: RecordBatch, node_id: NodeId, region_id: RegionId) -> Self {
        Self {
            batch,
            node_id,
            region_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, true),
        ]))
    }

    fn create_test_batch(ids: Vec<i64>, values: Vec<f64>) -> RecordBatch {
        let schema = create_test_schema();
        let id_array = Int64Array::from(ids);
        let value_array = Float64Array::from(values);

        RecordBatch::try_new(schema, vec![Arc::new(id_array), Arc::new(value_array)]).unwrap()
    }

    #[tokio::test]
    async fn test_merger_creation() {
        let merger = ResultMerger::new();
        assert_eq!(merger.config.batch_size, 8192);
    }

    #[test]
    fn test_aggregate_function_parsing() {
        assert_eq!(
            AggregateFunction::from_name("sum"),
            Some(AggregateFunction::Sum)
        );
        assert_eq!(
            AggregateFunction::from_name("COUNT"),
            Some(AggregateFunction::Count)
        );
        assert_eq!(AggregateFunction::from_name("unknown"), None);
    }

    #[test]
    fn test_aggregate_decomposable() {
        assert!(AggregateFunction::Sum.is_decomposable());
        assert!(AggregateFunction::Count.is_decomposable());
        assert!(AggregateFunction::Avg.is_decomposable());
    }

    #[test]
    fn test_sort_batch() {
        let merger = ResultMerger::new();
        let batch = create_test_batch(vec![3, 1, 2], vec![3.0, 1.0, 2.0]);

        let sort_columns = vec![SortColumn {
            name: "id".to_string(),
            descending: false,
            nulls_first: true,
        }];

        let sorted = merger.sort_batch(&batch, &sort_columns).unwrap();

        let ids = sorted
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(ids.value(2), 3);
    }

    #[test]
    fn test_sum_column() {
        let merger = ResultMerger::new();
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let sum = merger.sum_column(&array).unwrap();
        assert_eq!(sum, 15.0);
    }
}
