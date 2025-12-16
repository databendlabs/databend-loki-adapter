use std::{collections::BTreeMap, fmt::Display};

use chrono::{TimeZone, Utc};
use databend_driver::{Client, Row, Value};
use url::Url;

use crate::{
    error::AppError,
    logql::{LabelMatcher, LabelOp, LineFilter, LineFilterOp, LogqlExpr},
};

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
}

#[derive(Clone)]
pub struct LokiSchema {
    timestamp_col: String,
    labels_col: String,
    line_col: String,
}

impl LokiSchema {
    fn build_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &QueryBounds,
    ) -> Result<String, AppError> {
        let mut clauses = Vec::new();
        if let Some(start) = bounds.start_ns {
            clauses.push(format!(
                "{} >= {}",
                quote_ident(&self.timestamp_col),
                timestamp_literal(start)?
            ));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!(
                "{} <= {}",
                quote_ident(&self.timestamp_col),
                timestamp_literal(end)?
            ));
        }
        clauses.extend(
            expr.selectors
                .iter()
                .map(|m| label_clause_loki(m, quote_ident(&self.labels_col))),
        );
        clauses.extend(
            expr.filters
                .iter()
                .map(|f| line_filter_clause(quote_ident(&self.line_col), f)),
        );

        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };

        Ok(format!(
            "SELECT {ts}, {labels}, {line} FROM {table} WHERE {where} ORDER BY {ts} {order} LIMIT {limit}",
            ts = quote_ident(&self.timestamp_col),
            labels = quote_ident(&self.labels_col),
            line = quote_ident(&self.line_col),
            table = table.fq_name(),
            where = where_clause,
            order = bounds.order.sql(),
            limit = bounds.limit
        ))
    }

    fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        if row.values().len() < 3 {
            return Err(AppError::Internal(
                "Loki schema query must return timestamp, labels, line".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&row.values()[0])?;
        let labels = parse_labels_value(&row.values()[1])?;
        let line = value_to_string(&row.values()[2]);
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }
}

#[derive(Clone)]
pub struct FlatSchema {
    timestamp_col: String,
    line_col: String,
    label_cols: Vec<String>,
}

impl FlatSchema {
    fn build_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &QueryBounds,
    ) -> Result<String, AppError> {
        let ts_col = quote_ident(&self.timestamp_col);
        let line_col = quote_ident(&self.line_col);

        let mut clauses = Vec::new();
        if let Some(start) = bounds.start_ns {
            clauses.push(format!("{} >= {}", ts_col, timestamp_literal(start)?));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!("{} <= {}", ts_col, timestamp_literal(end)?));
        }

        for matcher in &expr.selectors {
            clauses.push(label_clause_flat(matcher, &self.label_cols)?);
        }
        clauses.extend(
            expr.filters
                .iter()
                .map(|f| line_filter_clause(line_col.clone(), f)),
        );

        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };

        let mut selected = Vec::with_capacity(self.label_cols.len() + 2);
        selected.push(format!("{} AS ts_col", ts_col));
        selected.push(format!("{} AS line_col", line_col));
        for col in &self.label_cols {
            selected.push(quote_ident(col));
        }

        Ok(format!(
            "SELECT {cols} FROM {table} WHERE {where} ORDER BY {ts} {order} LIMIT {limit}",
            cols = selected.join(", "),
            table = table.fq_name(),
            where = where_clause,
            ts = ts_col,
            order = bounds.order.sql(),
            limit = bounds.limit
        ))
    }

    fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        let values = row.values();
        if values.len() < self.label_cols.len() + 2 {
            return Err(AppError::Internal(
                "flat schema query must return timestamp, line, labels".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&values[0])?;
        let line = value_to_string(&values[1]);
        let mut labels = BTreeMap::new();
        for (idx, key) in self.label_cols.iter().enumerate() {
            let value = value_to_string(&values[idx + 2]);
            labels.insert(key.clone(), value);
        }
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }
}

pub struct QueryBounds {
    pub start_ns: Option<i64>,
    pub end_ns: Option<i64>,
    pub limit: u64,
    pub order: SqlOrder,
}

#[derive(Clone, Copy)]
pub enum SqlOrder {
    Asc,
    Desc,
}

impl SqlOrder {
    fn sql(self) -> &'static str {
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
struct TableColumn {
    name: String,
    data_type: String,
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

impl LokiSchema {
    fn from_columns(columns: Vec<TableColumn>, config: &SchemaConfig) -> Result<Self, AppError> {
        if columns.is_empty() {
            return Err(AppError::Config("loki schema table has no columns".into()));
        }

        let timestamp_override = config.timestamp_column.as_deref();
        let desired_timestamp = timestamp_override.map(|value| value.to_lowercase());
        let labels_override = config.labels_column.as_deref();
        let desired_labels = labels_override.map(|value| value.to_lowercase());
        let line_override = config.line_column.as_deref();
        let desired_line = line_override.map(|value| value.to_lowercase());

        let mut timestamp = None;
        let mut labels = None;
        let mut line = None;
        for column in columns {
            let lower = column.name.to_lowercase();
            if timestamp.is_none() && matches_named_column(&desired_timestamp, &lower, "timestamp")
            {
                ensure_timestamp_column(&column)?;
                timestamp = Some(column.name.clone());
                continue;
            }
            if labels.is_none() && matches_named_column(&desired_labels, &lower, "labels") {
                ensure_labels_column(&column)?;
                labels = Some(column.name.clone());
                continue;
            }
            if line.is_none() && matches_line_column(&desired_line, &lower) {
                ensure_line_column(&column)?;
                line = Some(column.name.clone());
            }
        }

        let timestamp_col = timestamp.ok_or_else(|| {
            missing_required_column("timestamp", timestamp_override, "loki schema")
        })?;
        let labels_col = labels
            .ok_or_else(|| missing_required_column("labels", labels_override, "loki schema"))?;
        let line_col = line.ok_or_else(|| match line_override {
            Some(name) => AppError::Config(format!("line column `{name}` not found in table")),
            None => AppError::Config(
                "loki schema requires a `line` column (or use --line-column to override)".into(),
            ),
        })?;

        Ok(Self {
            timestamp_col,
            labels_col,
            line_col,
        })
    }
}

impl FlatSchema {
    fn from_columns(columns: Vec<TableColumn>, config: &SchemaConfig) -> Result<Self, AppError> {
        if columns.is_empty() {
            return Err(AppError::Config("flat schema table has no columns".into()));
        }

        let timestamp_override = config.timestamp_column.as_deref();
        let desired_timestamp = timestamp_override.map(|value| value.to_lowercase());
        let line_override = config.line_column.as_deref();
        let desired_line = line_override.map(|value| value.to_lowercase());

        let mut timestamp_col: Option<TableColumn> = None;
        let mut explicit_line: Option<TableColumn> = None;
        let mut label_cols: Vec<TableColumn> = Vec::new();

        for column in columns {
            let lower = column.name.to_lowercase();
            if timestamp_col.is_none()
                && matches_named_column(&desired_timestamp, &lower, "timestamp")
            {
                ensure_timestamp_column(&column)?;
                timestamp_col = Some(column);
                continue;
            }

            if desired_line
                .as_ref()
                .map_or(false, |target| lower == *target)
                && explicit_line.is_none()
            {
                ensure_line_column(&column)?;
                explicit_line = Some(column);
                continue;
            }

            label_cols.push(column);
        }

        let timestamp = timestamp_col.ok_or_else(|| {
            missing_required_column("timestamp", timestamp_override, "flat schema")
        })?;

        let line = if let Some(line) = explicit_line {
            line
        } else if let Some(name) = line_override {
            return Err(AppError::Config(format!(
                "line column `{name}` not found in table"
            )));
        } else {
            let idx = label_cols
                .iter()
                .position(|col| is_line_candidate(&col.name));
            let Some(idx) = idx else {
                return Err(AppError::Config(
                    "flat schema requires --line-column or a column named request/line/message/msg"
                        .into(),
                ));
            };
            let column = label_cols.remove(idx);
            ensure_line_column(&column)?;
            column
        };

        let label_cols = label_cols
            .into_iter()
            .filter(|col| col.name != line.name)
            .map(|col| col.name)
            .collect();

        Ok(Self {
            timestamp_col: timestamp.name,
            line_col: line.name,
            label_cols,
        })
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
        let name = value_to_string(&values[0]);
        let data_type = value_to_string(&values[1]);
        columns.push(TableColumn { name, data_type });
    }
    Ok(columns)
}

fn matches_named_column(
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

fn matches_line_column(desired_lower: &Option<String>, candidate_lower: &str) -> bool {
    if let Some(target) = desired_lower {
        candidate_lower == target
    } else {
        is_line_candidate(candidate_lower)
    }
}

fn is_line_candidate(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "request" | "line" | "message" | "msg" | "log" | "payload" | "body" | "text"
    )
}

fn missing_required_column(default: &str, override_name: Option<&str>, context: &str) -> AppError {
    match override_name {
        Some(name) => AppError::Config(format!("{context} column `{name}` not found in table")),
        None => AppError::Config(format!("{context} requires `{default}` column")),
    }
}

fn ensure_timestamp_column(column: &TableColumn) -> Result<(), AppError> {
    if is_timestamp_type(&column.data_type) {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "column `{}` must be TIMESTAMP, found {}",
            column.name, column.data_type
        )))
    }
}

fn ensure_line_column(column: &TableColumn) -> Result<(), AppError> {
    if is_string_type(&column.data_type) {
        Ok(())
    } else {
        Err(AppError::Config(format!(
            "column `{}` must be STRING/VARCHAR, found {}",
            column.name, column.data_type
        )))
    }
}

fn ensure_labels_column(column: &TableColumn) -> Result<(), AppError> {
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

fn label_clause_loki(matcher: &LabelMatcher, column: String) -> String {
    let value = escape(&matcher.value);
    match matcher.op {
        LabelOp::Eq => format!("{column}['{key}'] = '{value}'", key = matcher.key),
        LabelOp::NotEq => format!("{column}['{key}'] != '{value}'", key = matcher.key),
        LabelOp::RegexEq => format!("match({column}['{key}'], '{value}')", key = matcher.key),
        LabelOp::RegexNotEq => {
            format!("NOT match({column}['{key}'], '{value}')", key = matcher.key)
        }
    }
}

fn label_clause_flat(matcher: &LabelMatcher, columns: &[String]) -> Result<String, AppError> {
    let column = columns
        .iter()
        .find(|col| col.eq_ignore_ascii_case(&matcher.key))
        .ok_or_else(|| {
            AppError::BadRequest(format!(
                "label `{}` is not a valid column in flat schema",
                matcher.key
            ))
        })?;
    let column = quote_ident(column);
    let value = escape(&matcher.value);
    Ok(match matcher.op {
        LabelOp::Eq => format!("{column} = '{value}'"),
        LabelOp::NotEq => format!("{column} != '{value}'"),
        LabelOp::RegexEq => format!("match({column}, '{value}')"),
        LabelOp::RegexNotEq => format!("NOT match({column}, '{value}')"),
    })
}

fn line_filter_clause(line_col: String, filter: &LineFilter) -> String {
    let value = escape(&filter.value);
    match filter.op {
        LineFilterOp::Contains => format!("position('{value}' in {line_col}) > 0"),
        LineFilterOp::NotContains => format!("position('{value}' in {line_col}) = 0"),
        LineFilterOp::Regex => format!("match({line_col}, '{value}')"),
        LineFilterOp::NotRegex => format!("NOT match({line_col}, '{value}')"),
    }
}

fn timestamp_literal(ns: i64) -> Result<String, AppError> {
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

fn value_to_timestamp(value: &Value) -> Result<i128, AppError> {
    match value {
        Value::Timestamp(zoned) | Value::TimestampTz(zoned) => {
            Ok(zoned.timestamp().as_nanosecond())
        }
        _ => Err(AppError::Internal(
            "timestamp column has unexpected type".into(),
        )),
    }
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => "".into(),
        Value::Boolean(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Variant(v) => v.clone(),
        Value::Number(n) => format!("{:?}", n),
        Value::Date(v) => v.to_string(),
        Value::Timestamp(v) | Value::TimestampTz(v) => v.timestamp().to_string(),
        Value::Binary(bin) => String::from_utf8_lossy(bin).to_string(),
        other => format!("{other:?}"),
    }
}

fn parse_labels_value(value: &Value) -> Result<BTreeMap<String, String>, AppError> {
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

fn escape(value: &str) -> String {
    value.replace('\'', "''")
}

fn escape_sql(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(ident: &str) -> String {
    format!("`{}`", ident.replace('`', "``"))
}
