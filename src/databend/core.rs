// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{collections::BTreeMap, fmt::Display};

use chrono::{TimeZone, Utc};
use databend_driver::{Client, NumberValue, Row, Value};
use url::Url;

use crate::{
    error::AppError,
    logql::{LineFilter, LineFilterOp, LogqlExpr, MetricExpr, RangeFunction, VectorAggregationOp},
};

use super::{flat::FlatSchema, loki::LokiSchema};

pub async fn execute_query(client: &Client, sql: &str) -> Result<Vec<Row>, AppError> {
    let conn = client.get_conn().await?;
    conn.set_session("timezone", "UTC")?;
    let rows = conn.query_all(sql).await?;
    Ok(rows)
}

#[derive(Clone)]
pub struct TableRef {
    pub database: String,
    pub table: String,
}

impl TableRef {
    pub fn fq_name(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.table)
        )
    }
}

#[derive(Clone, Copy, Debug)]
pub enum SchemaType {
    Loki,
    Flat,
}

impl Display for SchemaType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaType::Loki => write!(f, "loki"),
            SchemaType::Flat => write!(f, "flat"),
        }
    }
}

#[derive(Clone, Default)]
pub struct SchemaConfig {
    pub timestamp_column: Option<String>,
    pub line_column: Option<String>,
    pub labels_column: Option<String>,
}

#[derive(Clone)]
pub enum SchemaAdapter {
    Loki(LokiSchema),
    Flat(FlatSchema),
}

impl SchemaAdapter {
    pub fn build_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &QueryBounds,
    ) -> Result<String, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => schema.build_query(table, expr, bounds),
            SchemaAdapter::Flat(schema) => schema.build_query(table, expr, bounds),
        }
    }

    pub fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => schema.parse_row(row),
            SchemaAdapter::Flat(schema) => schema.parse_row(row),
        }
    }

    pub fn build_metric_query(
        &self,
        table: &TableRef,
        expr: &MetricExpr,
        bounds: &MetricQueryBounds,
    ) -> Result<MetricQueryPlan, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => schema.build_metric_query(table, expr, bounds),
            SchemaAdapter::Flat(schema) => schema.build_metric_query(table, expr, bounds),
        }
    }

    pub fn parse_metric_rows(
        &self,
        rows: Vec<Row>,
        plan: &MetricQueryPlan,
    ) -> Result<Vec<MetricSample>, AppError> {
        parse_metric_rows(rows, plan)
    }

    pub fn build_metric_range_query(
        &self,
        table: &TableRef,
        expr: &MetricExpr,
        bounds: &MetricRangeQueryBounds,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => schema.build_metric_range_query(table, expr, bounds),
            SchemaAdapter::Flat(schema) => schema.build_metric_range_query(table, expr, bounds),
        }
    }

    pub fn parse_metric_matrix_rows(
        &self,
        rows: Vec<Row>,
        plan: &MetricRangeQueryPlan,
    ) -> Result<Vec<MetricMatrixSample>, AppError> {
        parse_metric_matrix_rows(rows, plan)
    }

    pub async fn list_labels(
        &self,
        client: &Client,
        table: &TableRef,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => schema.list_labels(client, table, bounds).await,
            SchemaAdapter::Flat(schema) => Ok(schema.list_labels()),
        }
    }

    pub async fn list_label_values(
        &self,
        client: &Client,
        table: &TableRef,
        label: &str,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        match self {
            SchemaAdapter::Loki(schema) => {
                schema.list_label_values(client, table, label, bounds).await
            }
            SchemaAdapter::Flat(schema) => {
                schema.list_label_values(client, table, label, bounds).await
            }
        }
    }
}

pub struct QueryBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: u64,
    pub order: SqlOrder,
}

pub struct MetricQueryBounds {
    pub start_ns: i64,
    pub end_ns: i64,
}

pub struct MetricRangeQueryBounds {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
    pub window_ns: i64,
}

#[derive(Clone, Copy, Default)]
pub struct LabelQueryBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
}

#[derive(Clone, Copy)]
pub enum SqlOrder {
    Asc,
    Desc,
}

impl SqlOrder {
    pub fn sql(self) -> &'static str {
        match self {
            SqlOrder::Asc => "ASC",
            SqlOrder::Desc => "DESC",
        }
    }
}

#[derive(Clone)]
pub struct LogEntry {
    pub timestamp_ns: i128,
    pub labels: BTreeMap<String, String>,
    pub line: String,
}

#[derive(Clone)]
pub struct MetricSample {
    pub labels: BTreeMap<String, String>,
    pub value: f64,
}

#[derive(Clone)]
pub struct MetricMatrixSample {
    pub labels: BTreeMap<String, String>,
    pub timestamp_ns: i64,
    pub value: f64,
}

#[derive(Clone)]
pub struct MetricQueryPlan {
    pub sql: String,
    pub labels: MetricLabelsPlan,
}

#[derive(Clone)]
pub struct MetricRangeQueryPlan {
    pub sql: String,
    pub labels: MetricLabelsPlan,
}

#[derive(Clone)]
pub enum MetricLabelsPlan {
    LokiFull,
    Columns(Vec<String>),
}

#[derive(Clone)]
pub(crate) struct TableColumn {
    pub(crate) name: String,
    pub(crate) data_type: String,
}

pub fn resolve_table_ref(dsn: &str, table: &str) -> Result<TableRef, AppError> {
    let url =
        Url::parse(dsn).map_err(|err| AppError::Config(format!("invalid DSN {dsn}: {err}")))?;
    let default_db = url.path().trim_start_matches('/').to_string();
    let (database, table_name) = if let Some((db, tbl)) = table.split_once('.') {
        (db.to_string(), tbl.to_string())
    } else if !default_db.is_empty() {
        (default_db, table.to_string())
    } else {
        return Err(AppError::Config(
            "table must include database (db.table) or DSN must specify default database".into(),
        ));
    };
    if database.is_empty() || table_name.is_empty() {
        return Err(AppError::Config(
            "database/table names cannot be empty".into(),
        ));
    }
    Ok(TableRef {
        database,
        table: table_name,
    })
}

pub async fn load_schema(
    client: &Client,
    table: &TableRef,
    schema_type: SchemaType,
    config: &SchemaConfig,
) -> Result<SchemaAdapter, AppError> {
    let columns = fetch_columns(client, table).await?;
    match schema_type {
        SchemaType::Loki => LokiSchema::from_columns(columns, config).map(SchemaAdapter::Loki),
        SchemaType::Flat => FlatSchema::from_columns(columns, config).map(SchemaAdapter::Flat),
    }
}

async fn fetch_columns(client: &Client, table: &TableRef) -> Result<Vec<TableColumn>, AppError> {
    let query = format!(
        "SELECT name, data_type FROM system.columns WHERE database = '{db}' AND table = '{tbl}' ORDER BY name",
        db = escape_sql(&table.database),
        tbl = escape_sql(&table.table)
    );
    let rows = execute_query(client, &query).await?;
    let mut columns = Vec::new();
    for row in rows {
        let values = row.values();
        if values.len() < 2 {
            continue;
        }
        let name = values[0].to_string();
        let data_type = values[1].to_string();
        columns.push(TableColumn { name, data_type });
    }
    Ok(columns)
}

pub(crate) fn matches_named_column(
    desired_lower: &Option<String>,
    candidate_lower: &str,
    default: &str,
) -> bool {
    if let Some(target) = desired_lower {
        candidate_lower == target
    } else {
        candidate_lower == default
    }
}

pub(crate) fn matches_line_column(desired_lower: &Option<String>, candidate_lower: &str) -> bool {
    if let Some(target) = desired_lower {
        candidate_lower == target
    } else {
        is_line_candidate(candidate_lower)
    }
}

pub(crate) fn is_line_candidate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "request" | "line" | "message" | "msg" | "log" | "payload" | "body" | "text"
    )
}

pub(crate) fn missing_required_column(
    default: &str,
    override_name: Option<&str>,
    context: &str,
) -> AppError {
    match override_name {
        Some(name) => AppError::Config(format!("{context} column `{name}` not found in table")),
        None => AppError::Config(format!("{context} requires `{default}` column")),
    }
}

pub(crate) fn ensure_timestamp_column(column: &TableColumn) -> Result<(), AppError> {
    if is_timestamp_type(&column.data_type) {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "column `{}` must be TIMESTAMP, found {}",
            column.name, column.data_type
        )))
    }
}

pub(crate) fn ensure_line_column(column: &TableColumn) -> Result<(), AppError> {
    if is_string_type(&column.data_type) {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "column `{}` must be STRING/VARCHAR, found {}",
            column.name, column.data_type
        )))
    }
}

pub(crate) fn ensure_labels_column(column: &TableColumn) -> Result<(), AppError> {
    if is_variant_type(&column.data_type) {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "column `{}` must be VARIANT or MAP, found {}",
            column.name, column.data_type
        )))
    }
}

fn is_timestamp_type(data_type: &str) -> bool {
    let lower = data_type.trim().to_ascii_lowercase();
    lower.starts_with("timestamp")
}

fn is_string_type(data_type: &str) -> bool {
    let lower = data_type.trim().to_ascii_lowercase();
    lower.contains("string") || lower.contains("varchar") || lower == "text"
}

fn is_variant_type(data_type: &str) -> bool {
    let lower = data_type.trim().to_ascii_lowercase();
    lower.contains("variant") || lower.starts_with("map(")
}

pub(crate) fn is_numeric_type(data_type: &str) -> bool {
    let lower = data_type.trim().to_ascii_lowercase();
    lower.contains("int")
        || lower.contains("decimal")
        || lower.contains("number")
        || lower.contains("float")
        || lower.contains("double")
        || lower.contains("real")
}

pub(crate) fn line_filter_clause(line_col: String, filter: &LineFilter) -> String {
    match filter.op {
        LineFilterOp::Contains => like_contains_clause(line_col, &filter.value, false),
        LineFilterOp::NotContains => like_contains_clause(line_col, &filter.value, true),
        LineFilterOp::Regex => {
            let value = escape(&filter.value);
            format!("match({line_col}, '{value}')")
        }
        LineFilterOp::NotRegex => {
            let value = escape(&filter.value);
            format!("NOT match({line_col}, '{value}')")
        }
    }
}

fn like_contains_clause(line_col: String, value: &str, negate: bool) -> String {
    let pattern = escape_like_pattern(value);
    let op = if negate { "NOT LIKE" } else { "LIKE" };
    format!("{line_col} {op} '%{pattern}%' ESCAPE '\\\\'")
}

fn escape_like_pattern(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '%' | '_' | '\\' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            '\'' => escaped.push_str("''"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

pub(crate) fn timestamp_literal(ns: i64) -> Result<String, AppError> {
    let secs = ns.div_euclid(1_000_000_000);
    let nanos = ns.rem_euclid(1_000_000_000) as u32;
    let datetime = Utc
        .timestamp_opt(secs, nanos)
        .single()
        .ok_or_else(|| AppError::BadRequest("timestamp is out of range".into()))?;
    Ok(format!(
        "TIMESTAMP '{}'",
        datetime.format("%Y-%m-%d %H:%M:%S%.f")
    ))
}

pub(crate) fn value_to_timestamp(value: &Value) -> Result<i128, AppError> {
    match value {
        Value::Timestamp(zoned) | Value::TimestampTz(zoned) => {
            Ok(zoned.timestamp().as_nanosecond())
        }
        _ => Err(AppError::Internal(
            "timestamp column has unexpected type".into(),
        )),
    }
}

pub(crate) fn parse_labels_value(value: &Value) -> Result<BTreeMap<String, String>, AppError> {
    match value {
        Value::Variant(raw) | Value::String(raw) => parse_labels_json(raw),
        Value::Map(pairs) => {
            let mut map = BTreeMap::new();
            for (key, val) in pairs {
                let key = match key {
                    Value::String(text) => text.clone(),
                    _ => return Err(AppError::Internal("labels map key must be string".into())),
                };
                let value = match val {
                    Value::String(text) => text.clone(),
                    _ => return Err(AppError::Internal("labels map value must be string".into())),
                };
                map.insert(key, value);
            }
            Ok(map)
        }
        _ => Err(AppError::Internal(
            "labels column must be VARIANT or MAP".into(),
        )),
    }
}

pub fn parse_labels_json(raw: &str) -> Result<BTreeMap<String, String>, AppError> {
    let map: serde_json::Map<String, serde_json::Value> = serde_json::from_str(raw)
        .map_err(|err| AppError::Internal(format!("labels column is not valid JSON: {err}")))?;
    let mut labels = BTreeMap::new();
    for (key, value) in map {
        let value = value
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| AppError::Internal("label values must be strings".into()))?;
        labels.insert(key, value);
    }
    Ok(labels)
}

pub(crate) fn escape(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

pub(crate) fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}

pub(crate) fn format_float_literal(value: f64) -> String {
    let mut text = format!("{value:.9}");
    if text.contains('.') {
        while text.ends_with('0') {
            text.pop();
        }
        if text.ends_with('.') {
            text.pop();
        }
    }
    if text.is_empty() { "0".into() } else { text }
}

pub(crate) fn aggregate_value_select(op: VectorAggregationOp, value_ident: &str) -> String {
    match op {
        VectorAggregationOp::Sum => format!("SUM({value_ident}) AS value"),
        VectorAggregationOp::Avg => format!("AVG({value_ident}) AS value"),
        VectorAggregationOp::Min => format!("MIN({value_ident}) AS value"),
        VectorAggregationOp::Max => format!("MAX({value_ident}) AS value"),
        VectorAggregationOp::Count => "COUNT(*) AS value".to_string(),
    }
}

pub(crate) fn range_bucket_value_expression(
    function: RangeFunction,
    window_ns: i64,
    ts_ident: &str,
) -> String {
    match function {
        RangeFunction::CountOverTime => format!("COUNT({ts_ident})"),
        RangeFunction::Rate => {
            let seconds = window_ns as f64 / 1_000_000_000_f64;
            let literal = format_float_literal(seconds);
            format!("COUNT({ts_ident}) / {literal}")
        }
    }
}

pub(crate) fn metric_bucket_cte(bounds: &MetricRangeQueryBounds) -> Result<String, AppError> {
    let step_us = bounds
        .step_ns
        .checked_div(1_000)
        .ok_or_else(|| AppError::BadRequest("metric step must be at least 1 microsecond".into()))?;
    if step_us == 0 {
        return Err(AppError::BadRequest(
            "metric step must be at least 1 microsecond".into(),
        ));
    }
    Ok(format!(
        "SELECT generate_series AS bucket_start FROM generate_series({start}, {end}, {step})",
        start = timestamp_literal(bounds.start_ns)?,
        end = timestamp_literal(bounds.end_ns)?,
        step = step_us
    ))
}

pub(crate) fn timestamp_offset_expr(base_expr: &str, offset_ns: i64) -> String {
    let mut expr = base_expr.to_string();
    let seconds = offset_ns / 1_000_000_000;
    let micros = (offset_ns % 1_000_000_000) / 1_000;
    if seconds != 0 {
        expr = format!("date_add('second', {seconds}, {expr})");
    }
    if micros != 0 {
        expr = format!("date_add('microsecond', {micros}, {expr})");
    }
    expr
}

fn timestamp_value_to_i64(value: &Value) -> Result<i64, AppError> {
    let ns = value_to_timestamp(value)?;
    i64::try_from(ns)
        .map_err(|_| AppError::Internal("timestamp column is outside supported range".into()))
}

fn parse_metric_rows(
    rows: Vec<Row>,
    plan: &MetricQueryPlan,
) -> Result<Vec<MetricSample>, AppError> {
    let mut samples = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.values();
        if values.is_empty() {
            continue;
        }
        let value_cell = values
            .last()
            .ok_or_else(|| AppError::Internal("metric row is missing value column".into()))?;
        let value = metric_value(value_cell)?;
        let labels = match &plan.labels {
            MetricLabelsPlan::LokiFull => {
                if values.len() < 2 {
                    return Err(AppError::Internal(
                        "metric row is missing labels column".into(),
                    ));
                }
                parse_labels_value(&values[0])?
            }
            MetricLabelsPlan::Columns(names) => {
                if values.len() < names.len() + 1 {
                    return Err(AppError::Internal(
                        "metric row returned fewer columns than expected".into(),
                    ));
                }
                let mut labels = BTreeMap::new();
                for (idx, key) in names.iter().enumerate() {
                    if let Some(text) = metric_label_string(&values[idx]) {
                        labels.insert(key.clone(), text);
                    }
                }
                labels
            }
        };
        samples.push(MetricSample { labels, value });
    }
    Ok(samples)
}

fn parse_metric_matrix_rows(
    rows: Vec<Row>,
    plan: &MetricRangeQueryPlan,
) -> Result<Vec<MetricMatrixSample>, AppError> {
    let mut samples = Vec::with_capacity(rows.len());
    for row in rows {
        let values = row.values();
        if values.len() < 2 {
            continue;
        }
        let bucket_ns = timestamp_value_to_i64(&values[0])?;
        let value_cell = values
            .last()
            .ok_or_else(|| AppError::Internal("metric row is missing value column".into()))?;
        let value = metric_value(value_cell)?;
        let labels = match &plan.labels {
            MetricLabelsPlan::LokiFull => {
                if values.len() < 3 {
                    return Err(AppError::Internal(
                        "metric row is missing labels column".into(),
                    ));
                }
                let label_value = &values[1];
                if matches!(label_value, Value::Null) {
                    continue;
                }
                parse_labels_value(label_value)?
            }
            MetricLabelsPlan::Columns(names) => {
                if values.len() < names.len() + 2 {
                    return Err(AppError::Internal(
                        "metric row returned fewer columns than expected".into(),
                    ));
                }
                let mut labels = BTreeMap::new();
                for (idx, key) in names.iter().enumerate() {
                    if let Some(text) = metric_label_string(&values[idx + 1]) {
                        labels.insert(key.clone(), text);
                    }
                }
                labels
            }
        };
        samples.push(MetricMatrixSample {
            labels,
            timestamp_ns: bucket_ns,
            value,
        });
    }
    Ok(samples)
}

fn metric_label_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(text) => Some(text.clone()),
        Value::Variant(text) => Some(text.clone()),
        Value::Boolean(flag) => Some(flag.to_string()),
        Value::Number(num) => Some(num.to_string()),
        other => {
            let text = other.to_string();
            (!text.is_empty()).then_some(text)
        }
    }
}

fn metric_value(value: &Value) -> Result<f64, AppError> {
    match value {
        Value::Number(num) => metric_number_to_f64(num),
        Value::String(text) => text
            .parse::<f64>()
            .map_err(|_| AppError::Internal("metric value is not numeric".into())),
        other => Err(AppError::Internal(format!(
            "metric value must be numeric, found {}",
            other
        ))),
    }
}

fn metric_number_to_f64(num: &NumberValue) -> Result<f64, AppError> {
    let value = match num {
        NumberValue::Int8(v) => *v as f64,
        NumberValue::Int16(v) => *v as f64,
        NumberValue::Int32(v) => *v as f64,
        NumberValue::Int64(v) => *v as f64,
        NumberValue::UInt8(v) => *v as f64,
        NumberValue::UInt16(v) => *v as f64,
        NumberValue::UInt32(v) => *v as f64,
        NumberValue::UInt64(v) => *v as f64,
        NumberValue::Float32(v) => *v as f64,
        NumberValue::Float64(v) => *v,
        NumberValue::Decimal64(v, size) => {
            let scale = size.scale as i32;
            let divisor = 10_f64.powi(scale);
            *v as f64 / divisor
        }
        NumberValue::Decimal128(v, size) => {
            let scale = size.scale as i32;
            let divisor = 10_f64.powi(scale);
            *v as f64 / divisor
        }
        NumberValue::Decimal256(_, _) => num
            .to_string()
            .parse()
            .map_err(|_| AppError::Internal("metric decimal value overflowed".into()))?,
    };
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::{LineFilter, LineFilterOp};

    #[test]
    fn contains_uses_like_with_escaping() {
        let filter = LineFilter {
            op: LineFilterOp::Contains,
            value: "foo%_\\'bar".into(),
        };
        let clause = line_filter_clause("line_col".into(), &filter);
        assert_eq!(clause, "line_col LIKE '%foo\\%\\_\\\\''bar%' ESCAPE '\\\\'");
    }

    #[test]
    fn not_contains_negates_like() {
        let filter = LineFilter {
            op: LineFilterOp::NotContains,
            value: "needle".into(),
        };
        let clause = line_filter_clause("line_col".into(), &filter);
        assert_eq!(clause, "line_col NOT LIKE '%needle%' ESCAPE '\\\\'");
    }

    #[test]
    fn regex_filters_keep_match_calls() {
        let filter = LineFilter {
            op: LineFilterOp::Regex,
            value: "foo.*".into(),
        };
        let clause = line_filter_clause("line_col".into(), &filter);
        assert_eq!(clause, "match(line_col, 'foo.*')");
    }
}
