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
use databend_driver::{Client, Row, Value};
use url::Url;

use crate::{
    error::AppError,
    logql::{LineFilter, LineFilterOp, LogqlExpr},
};

use super::{flat::FlatSchema, loki::LokiSchema};

pub async fn execute_query(client: &Client, sql: &str) -> Result<Vec<Row>, AppError> {
    let conn = client.get_conn().await?;
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
    let value = escape(&filter.value);
    match filter.op {
        LineFilterOp::Contains => format!("position('{value}' in {line_col}) > 0"),
        LineFilterOp::NotContains => format!("position('{value}' in {line_col}) = 0"),
        LineFilterOp::Regex => format!("match({line_col}, '{value}')"),
        LineFilterOp::NotRegex => format!("NOT match({line_col}, '{value}')"),
    }
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
