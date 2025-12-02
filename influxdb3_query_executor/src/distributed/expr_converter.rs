//! Convert DataFusion expressions to SQL for remote execution
//!
//! This module provides utilities to convert DataFusion's logical expressions
//! into SQL WHERE clauses that can be sent to remote nodes for predicate pushdown.

use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use datafusion::scalar::ScalarValue;

/// Converter for DataFusion Expr to SQL
#[derive(Debug, Copy, Clone)]
pub struct ExprToSqlConverter;

impl ExprToSqlConverter {
    /// Convert a DataFusion Expr to a SQL WHERE clause
    pub fn expr_to_sql(expr: &Expr) -> Result<String, String> {
        match expr {
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
                Self::binary_expr_to_sql(left, op, right)
            }
            Expr::Column(col) => Ok(col.name.clone()),
            Expr::Literal(scalar, _) => Self::scalar_to_sql(scalar),
            Expr::IsNull(expr) => {
                let inner = Self::expr_to_sql(expr)?;
                Ok(format!("{} IS NULL", inner))
            }
            Expr::IsNotNull(expr) => {
                let inner = Self::expr_to_sql(expr)?;
                Ok(format!("{} IS NOT NULL", inner))
            }
            Expr::Not(expr) => {
                let inner = Self::expr_to_sql(expr)?;
                Ok(format!("NOT ({})", inner))
            }
            Expr::Between(between) => {
                let expr = Self::expr_to_sql(&between.expr)?;
                let low = Self::expr_to_sql(&between.low)?;
                let high = Self::expr_to_sql(&between.high)?;
                if between.negated {
                    Ok(format!("{} NOT BETWEEN {} AND {}", expr, low, high))
                } else {
                    Ok(format!("{} BETWEEN {} AND {}", expr, low, high))
                }
            }
            Expr::Like(like) => {
                let expr = Self::expr_to_sql(&like.expr)?;
                let pattern = Self::expr_to_sql(&like.pattern)?;
                if like.negated {
                    Ok(format!("{} NOT LIKE {}", expr, pattern))
                } else {
                    Ok(format!("{} LIKE {}", expr, pattern))
                }
            }
            Expr::InList(in_list) => {
                let expr = Self::expr_to_sql(&in_list.expr)?;
                let list_items: Result<Vec<String>, String> = in_list
                    .list
                    .iter()
                    .map(|e| Self::expr_to_sql(e))
                    .collect();
                let list_str = list_items?.join(", ");
                if in_list.negated {
                    Ok(format!("{} NOT IN ({})", expr, list_str))
                } else {
                    Ok(format!("{} IN ({})", expr, list_str))
                }
            }
            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    /// Convert a binary expression to SQL
    fn binary_expr_to_sql(left: &Expr, op: &Operator, right: &Expr) -> Result<String, String> {
        let left_sql = Self::expr_to_sql(left)?;
        let right_sql = Self::expr_to_sql(right)?;
        let op_sql = Self::operator_to_sql(op)?;
        Ok(format!("({} {} {})", left_sql, op_sql, right_sql))
    }

    /// Convert an operator to SQL
    fn operator_to_sql(op: &Operator) -> Result<String, String> {
        match op {
            Operator::Eq => Ok("=".to_string()),
            Operator::NotEq => Ok("!=".to_string()),
            Operator::Lt => Ok("<".to_string()),
            Operator::LtEq => Ok("<=".to_string()),
            Operator::Gt => Ok(">".to_string()),
            Operator::GtEq => Ok(">=".to_string()),
            Operator::And => Ok("AND".to_string()),
            Operator::Or => Ok("OR".to_string()),
            Operator::Plus => Ok("+".to_string()),
            Operator::Minus => Ok("-".to_string()),
            Operator::Multiply => Ok("*".to_string()),
            Operator::Divide => Ok("/".to_string()),
            Operator::Modulo => Ok("%".to_string()),
            _ => Err(format!("Unsupported operator: {:?}", op)),
        }
    }

    /// Convert a ScalarValue to SQL literal
    fn scalar_to_sql(scalar: &ScalarValue) -> Result<String, String> {
        match scalar {
            ScalarValue::Int8(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int16(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int32(Some(v)) => Ok(v.to_string()),
            ScalarValue::Int64(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt8(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt16(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt32(Some(v)) => Ok(v.to_string()),
            ScalarValue::UInt64(Some(v)) => Ok(v.to_string()),
            ScalarValue::Float32(Some(v)) => Ok(v.to_string()),
            ScalarValue::Float64(Some(v)) => Ok(v.to_string()),
            ScalarValue::Boolean(Some(v)) => Ok(v.to_string()),
            ScalarValue::Utf8(Some(v)) | ScalarValue::LargeUtf8(Some(v)) => {
                // Escape single quotes by doubling them
                let escaped = v.replace('\'', "''");
                Ok(format!("'{}'", escaped))
            }
            ScalarValue::TimestampNanosecond(Some(v), _) => {
                // Convert nanoseconds to timestamp string
                // Format as ISO 8601
                let secs = v / 1_000_000_000;
                let nanos = (v % 1_000_000_000) as u32;
                if let Some(datetime) = chrono::DateTime::from_timestamp(secs, nanos) {
                    Ok(format!("'{}'", datetime.to_rfc3339()))
                } else {
                    Ok(v.to_string())
                }
            }
            ScalarValue::TimestampMicrosecond(Some(v), _) => {
                let secs = v / 1_000_000;
                let micros = (v % 1_000_000) as u32;
                let nanos = micros * 1000;
                if let Some(datetime) = chrono::DateTime::from_timestamp(secs, nanos) {
                    Ok(format!("'{}'", datetime.to_rfc3339()))
                } else {
                    Ok(v.to_string())
                }
            }
            ScalarValue::TimestampMillisecond(Some(v), _) => {
                let secs = v / 1000;
                let millis = (v % 1000) as u32;
                let nanos = millis * 1_000_000;
                if let Some(datetime) = chrono::DateTime::from_timestamp(secs, nanos) {
                    Ok(format!("'{}'", datetime.to_rfc3339()))
                } else {
                    Ok(v.to_string())
                }
            }
            ScalarValue::Null => Ok("NULL".to_string()),
            _ => Err(format!("Unsupported scalar type: {:?}", scalar)),
        }
    }

    /// Convert a list of filter expressions to a WHERE clause
    pub fn filters_to_where_clause(filters: &[Expr]) -> Result<String, String> {
        if filters.is_empty() {
            return Ok(String::new());
        }

        let conditions: Result<Vec<String>, String> = filters
            .iter()
            .map(|f| Self::expr_to_sql(f))
            .collect();

        Ok(conditions?.join(" AND "))
    }
}

