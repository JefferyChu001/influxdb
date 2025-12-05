//! HTTP Table Provider for InfluxDB
//!
//! 通过 HTTP API 从 InfluxDB 节点读取数据的 TableProvider 实现

use std::any::Any;
use std::fmt;
use std::io::Cursor;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use arrow_json;
use datafusion::catalog::Session;
use datafusion::datasource::TableType;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{stream, StreamExt};
use reqwest;
use serde_json::Value;

/// HTTP 执行计划 - 从远程 InfluxDB 节点通过 HTTP API 获取数据
#[derive(Debug, Clone)]
pub struct HttpScanExec {
    table_name: String,
    schema: SchemaRef,
    database: String,
    endpoint: String,
    projection: Option<Vec<usize>>,
    filters: Vec<String>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl HttpScanExec {
    pub fn new(
        table_name: String,
        schema: SchemaRef,
        database: String,
        endpoint: String,
        projection: Option<Vec<usize>>,
        filters: Vec<String>,
        limit: Option<usize>,
    ) -> Self {
        let eq_properties = EquivalenceProperties::new(schema.clone());
        let partitioning = Partitioning::UnknownPartitioning(1);
        let properties = PlanProperties::new(
            eq_properties,
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Final,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            schema,
            database,
            endpoint,
            projection,
            filters,
            limit,
            properties,
        }
    }

    fn build_sql(&self) -> String {
        // 根据 projection 构建列列表
        let columns = if let Some(ref proj) = self.projection {
            proj.iter()
                .map(|&i| self.schema.field(i).name().as_str())
                .collect::<Vec<_>>()
                .join(", ")
        } else {
            "*".to_string()
        };

        let mut sql = format!("SELECT {} FROM {}", columns, self.table_name);

        // 添加 WHERE 子句（filter pushdown）
        if !self.filters.is_empty() {
            sql.push_str(&format!(" WHERE {}", self.filters.join(" AND ")));
        }

        // 添加 LIMIT 子句（limit pushdown）
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        sql
    }

    async fn fetch_data(&self) -> DataFusionResult<Vec<RecordBatch>> {
        let sql = self.build_sql();
        println!("  📡 查询 {}: {}", self.endpoint, sql);

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/query_sql", self.endpoint);

        let payload = serde_json::json!({
            "db": self.database,
            "query": sql
        });

        let response = client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(DataFusionError::External(
                format!("HTTP {} - {}", status, error_text).into(),
            ));
        }

        let json_text = response
            .text()
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        println!("    ✓ 获取响应数据");

        // 使用 arrow_json 解析（需要根据 projection 调整 schema）
        let output_schema = if let Some(ref proj) = self.projection {
            let fields: Vec<_> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(arrow::datatypes::Schema::new(fields))
        } else {
            self.schema.clone()
        };

        self.json_to_record_batch(&json_text, output_schema)
    }

    fn json_to_record_batch(
        &self,
        json_text: &str,
        schema: SchemaRef,
    ) -> DataFusionResult<Vec<RecordBatch>> {
        if json_text.trim().is_empty() || json_text.trim() == "[]" {
            println!("    ⚠️  响应为空");
            return Ok(vec![]);
        }

        // InfluxDB 返回 JSON 数组格式: [{"col1": val1}, {"col2": val2}]
        // arrow_json 需要 NDJSON 格式: {"col1": val1}\n{"col2": val2}

        // 1. 解析 JSON 数组
        let json_array: Vec<serde_json::Value> = serde_json::from_str(json_text)
            .map_err(|e| {
                DataFusionError::External(format!("Failed to parse JSON array: {}", e).into())
            })?;

        if json_array.is_empty() {
            println!("    ⚠️  JSON 数组为空");
            return Ok(vec![]);
        }

        // 2. 转换为 NDJSON 格式（每行一个 JSON 对象）
        let ndjson = json_array
            .iter()
            .map(|obj| serde_json::to_string(obj).unwrap())
            .collect::<Vec<_>>()
            .join("\n");

        // 3. 使用 arrow_json::ReaderBuilder 解析 NDJSON
        let cursor = Cursor::new(ndjson.as_bytes());

        match arrow_json::ReaderBuilder::new(schema.clone()).build(cursor) {
            Ok(mut reader) => {
                let mut batches = Vec::new();
                while let Some(batch_result) = reader.next() {
                    match batch_result {
                        Ok(batch) => {
                            println!("    ✓ 解析 {} 行数据", batch.num_rows());
                            batches.push(batch);
                        }
                        Err(e) => {
                            println!("    ✗ 解析 batch 失败: {}", e);
                            return Err(DataFusionError::External(Box::new(e)));
                        }
                    }
                }
                Ok(batches)
            }
            Err(e) => {
                println!("    ✗ 创建 JSON Reader 失败: {}", e);
                println!("    NDJSON 示例: {}", &ndjson[..ndjson.len().min(200)]);
                Err(DataFusionError::External(Box::new(e)))
            }
        }
    }
}

impl ExecutionPlan for HttpScanExec {
    fn name(&self) -> &str {
        "HttpScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let exec = self.clone();
        let schema = self.schema.clone();

        let stream = stream::once(async move { exec.fetch_data().await })
            .flat_map(|result| {
                stream::iter(match result {
                    Ok(batches) => batches.into_iter().map(Ok).collect(),
                    Err(e) => vec![Err(e)],
                })
            });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

impl DisplayAs for HttpScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "HttpScanExec: table={}, endpoint={}",
            self.table_name, self.endpoint
        )
    }
}

/// HTTP TableProvider - 使用 HttpScanExec 作为物理计划
#[derive(Debug)]
pub struct HttpTableProvider {
    name: String,
    schema: SchemaRef,
    database: String,
    endpoint: String,
}

impl HttpTableProvider {
    pub fn new(name: String, schema: SchemaRef, database: String, endpoint: String) -> Self {
        Self {
            name,
            schema,
            database,
            endpoint,
        }
    }
}

#[async_trait::async_trait]
impl datafusion::catalog::TableProvider for HttpTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        // 实现下推优化

        // 1. Projection pushdown - 只查询需要的列
        let projection_opt = projection.cloned();

        // 2. Filter pushdown - 将 WHERE 条件转换为 SQL
        let filter_strs = self.convert_filters_to_sql(filters);

        // 3. Limit pushdown - 传递 LIMIT 子句
        let exec = HttpScanExec::new(
            self.name.clone(),
            self.schema.clone(),
            self.database.clone(),
            self.endpoint.clone(),
            projection_opt,
            filter_strs,
            limit,
        );

        Ok(Arc::new(exec))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DataFusionResult<Vec<TableProviderFilterPushDown>> {
        // 支持精确的 filter pushdown
        Ok(vec![TableProviderFilterPushDown::Exact; filters.len()])
    }
}

impl HttpTableProvider {
    /// 将 DataFusion 的 Expr 转换为 SQL WHERE 子句
    fn convert_filters_to_sql(&self, filters: &[Expr]) -> Vec<String> {
        filters
            .iter()
            .filter_map(|expr| self.expr_to_sql(expr))
            .collect()
    }

    /// 递归地将 Expr 转换为 SQL 字符串
    fn expr_to_sql(&self, expr: &Expr) -> Option<String> {
        use datafusion::logical_expr::Operator;

        match expr {
            // 二元表达式: column op value
            Expr::BinaryExpr(binary_expr) => {
                let left = self.expr_to_sql(&binary_expr.left)?;
                let right = self.expr_to_sql(&binary_expr.right)?;
                let op = match binary_expr.op {
                    Operator::Eq => "=",
                    Operator::NotEq => "!=",
                    Operator::Lt => "<",
                    Operator::LtEq => "<=",
                    Operator::Gt => ">",
                    Operator::GtEq => ">=",
                    Operator::And => "AND",
                    Operator::Or => "OR",
                    _ => return None,
                };
                Some(format!("{} {} {}", left, op, right))
            }
            // 列引用
            Expr::Column(col) => Some(col.name.clone()),
            // 字面量
            Expr::Literal(scalar, _) => {
                use datafusion_common::ScalarValue;
                match scalar {
                    ScalarValue::Int64(Some(v)) => Some(v.to_string()),
                    ScalarValue::Float64(Some(v)) => Some(v.to_string()),
                    ScalarValue::Utf8(Some(v)) => Some(format!("'{}'", v)),
                    ScalarValue::Boolean(Some(v)) => Some(v.to_string()),
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

