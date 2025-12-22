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

use databend_driver::{Client, Row};

use crate::{
    error::AppError,
    logql::{
        GroupModifier, LabelMatcher, LabelOp, LogqlExpr, MetricExpr, RangeFunction,
        VectorAggregation,
    },
};

use super::core::{
    LabelQueryBounds, LogEntry, MetricLabelsPlan, MetricQueryBounds, MetricQueryPlan,
    MetricRangeQueryBounds, MetricRangeQueryPlan, QueryBounds, SchemaConfig, TableColumn, TableRef,
    aggregate_value_select, ensure_labels_column, ensure_line_column, ensure_timestamp_column,
    escape, execute_query, format_float_literal, line_filter_clause, matches_line_column,
    matches_named_column, metric_bucket_cte, missing_required_column, parse_labels_value,
    quote_ident, range_bucket_value_expression, timestamp_literal, timestamp_offset_expr,
    value_to_timestamp,
};

#[derive(Clone)]
pub struct LokiSchema {
    pub(crate) timestamp_col: String,
    pub(crate) labels_col: String,
    pub(crate) line_col: String,
}

impl LokiSchema {
    pub(crate) fn build_query(
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

    pub(crate) fn build_metric_query(
        &self,
        table: &TableRef,
        expr: &MetricExpr,
        bounds: &MetricQueryBounds,
    ) -> Result<MetricQueryPlan, AppError> {
        let drop_labels = expr
            .range
            .selector
            .pipeline
            .metric_drop_labels()
            .map_err(AppError::BadRequest)?;
        let mut clauses = Vec::new();
        let ts = quote_ident(&self.timestamp_col);
        clauses.push(format!("{ts} >= {}", timestamp_literal(bounds.start_ns)?));
        clauses.push(format!("{ts} <= {}", timestamp_literal(bounds.end_ns)?));
        clauses.extend(
            expr.range
                .selector
                .selectors
                .iter()
                .map(|m| label_clause_loki(m, quote_ident(&self.labels_col))),
        );
        clauses.extend(
            expr.range
                .selector
                .filters
                .iter()
                .map(|f| line_filter_clause(quote_ident(&self.line_col), f)),
        );
        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        let value_expr =
            range_value_expression(expr.range.function, expr.range.duration.as_nanoseconds());
        let mut plan = match &expr.aggregation {
            None => self.metric_streams_sql(table, &where_clause, &value_expr),
            Some(aggregation) => {
                self.metric_aggregation_sql(table, &where_clause, &value_expr, aggregation)
            }
        }?;
        plan.drop_labels = drop_labels;
        Ok(plan)
    }

    pub(crate) fn build_metric_range_query(
        &self,
        table: &TableRef,
        expr: &MetricExpr,
        bounds: &MetricRangeQueryBounds,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        let drop_labels = expr
            .range
            .selector
            .pipeline
            .metric_drop_labels()
            .map_err(AppError::BadRequest)?;
        let buckets_source = metric_bucket_cte(bounds)?;
        let buckets_table = format!("({}) AS buckets", buckets_source);
        let ts_col = format!("source.{}", quote_ident(&self.timestamp_col));
        let labels_col = format!("source.{}", quote_ident(&self.labels_col));
        let line_col = format!("source.{}", quote_ident(&self.line_col));
        let bucket_window_start = timestamp_offset_expr("bucket_start", -bounds.window_ns);
        let mut join_conditions = vec![
            format!("{ts_col} >= {bucket_window_start}"),
            format!("{ts_col} <= bucket_start"),
        ];
        join_conditions.extend(
            expr.range
                .selector
                .selectors
                .iter()
                .map(|m| label_clause_loki(m, labels_col.clone())),
        );
        join_conditions.extend(
            expr.range
                .selector
                .filters
                .iter()
                .map(|f| line_filter_clause(line_col.clone(), f)),
        );
        let join_clause = join_conditions.join(" AND ");
        let value_expr =
            range_bucket_value_expression(expr.range.function, bounds.window_ns, &ts_col);
        let mut plan = match &expr.aggregation {
            None => self.metric_range_stream_sql(
                table,
                &buckets_table,
                &labels_col,
                &value_expr,
                &join_clause,
            ),
            Some(aggregation) => self.metric_range_aggregation_sql(
                table,
                &buckets_table,
                &labels_col,
                &value_expr,
                &join_clause,
                aggregation,
            ),
        }?;
        plan.drop_labels = drop_labels;
        Ok(plan)
    }

    fn metric_streams_sql(
        &self,
        table: &TableRef,
        where_clause: &str,
        value_expr: &str,
    ) -> Result<MetricQueryPlan, AppError> {
        let labels = quote_ident(&self.labels_col);
        let sql = format!(
            "SELECT {labels} AS labels, {value_expr} AS value FROM {table} \
             WHERE {where} GROUP BY {labels}",
            table = table.fq_name(),
            where = where_clause
        );
        Ok(MetricQueryPlan {
            sql,
            labels: MetricLabelsPlan::LokiFull,
            drop_labels: Vec::new(),
        })
    }

    fn metric_aggregation_sql(
        &self,
        table: &TableRef,
        where_clause: &str,
        value_expr: &str,
        aggregation: &VectorAggregation,
    ) -> Result<MetricQueryPlan, AppError> {
        if let Some(GroupModifier::Without(labels)) = &aggregation.grouping {
            return Err(AppError::BadRequest(format!(
                "metric queries do not support `without` grouping ({labels:?})"
            )));
        }
        let label_column = quote_ident(&self.labels_col);
        let grouping_labels = match &aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let group_columns = build_loki_group_columns(&grouping_labels, &label_column);

        let mut select_parts: Vec<String> = group_columns
            .iter()
            .map(|column| format!("{} AS {}", column.expr, column.alias))
            .collect();
        select_parts.push(format!("{value_expr} AS value"));
        let mut group_parts = vec![label_column.clone()];
        group_parts.extend(group_columns.iter().map(|column| column.expr.clone()));
        let inner_sql = format!(
            "SELECT {select_clause} FROM {table} WHERE {where} GROUP BY {group_by}",
            select_clause = select_parts.join(", "),
            table = table.fq_name(),
            where = where_clause,
            group_by = group_parts.join(", ")
        );

        let alias_list: Vec<String> = group_columns
            .iter()
            .map(|column| column.alias.clone())
            .collect();
        let alias_clause = alias_list.join(", ");
        let aggregate = aggregate_value_select(aggregation.op, "value");
        let outer_select = if alias_list.is_empty() {
            aggregate.clone()
        } else {
            format!("{alias_clause}, {aggregate}")
        };
        let outer_group = if alias_list.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {alias_clause}")
        };
        let sql = format!(
            "WITH stream_data AS ({inner}) SELECT {outer_select} FROM stream_data{outer_group}",
            inner = inner_sql,
            outer_select = outer_select,
            outer_group = outer_group
        );

        let labels = if grouping_labels.is_empty() {
            MetricLabelsPlan::Columns(Vec::new())
        } else {
            MetricLabelsPlan::Columns(grouping_labels)
        };
        Ok(MetricQueryPlan {
            sql,
            labels,
            drop_labels: Vec::new(),
        })
    }

    fn metric_range_stream_sql(
        &self,
        table: &TableRef,
        buckets_table: &str,
        labels_column: &str,
        value_expr: &str,
        join_clause: &str,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        let sql = format!(
            "SELECT bucket_start AS bucket, {labels_column} AS labels, {value_expr} AS value \
             FROM {buckets} LEFT JOIN {table} AS source ON {join} \
             GROUP BY bucket_start, {labels_column} ORDER BY bucket",
            buckets = buckets_table,
            table = table.fq_name(),
            join = join_clause
        );
        Ok(MetricRangeQueryPlan {
            sql,
            labels: MetricLabelsPlan::LokiFull,
            drop_labels: Vec::new(),
        })
    }

    fn metric_range_aggregation_sql(
        &self,
        table: &TableRef,
        buckets_table: &str,
        labels_column: &str,
        value_expr: &str,
        join_clause: &str,
        aggregation: &VectorAggregation,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        if let Some(GroupModifier::Without(labels)) = &aggregation.grouping {
            return Err(AppError::BadRequest(format!(
                "metric queries do not support `without` grouping ({labels:?})"
            )));
        }
        let grouping_labels = match &aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let group_columns = build_loki_group_columns(&grouping_labels, labels_column);
        let mut select_parts = vec!["bucket_start AS bucket".to_string()];
        select_parts.extend(
            group_columns
                .iter()
                .map(|column| format!("{} AS {}", column.expr, column.alias)),
        );
        select_parts.push(format!("{value_expr} AS value"));
        let mut group_parts = vec!["bucket_start".to_string()];
        group_parts.extend(group_columns.iter().map(|column| column.expr.clone()));
        let inner_sql = format!(
            "SELECT {select_clause} FROM {buckets} LEFT JOIN {table} AS source ON {join} GROUP BY {group_by}",
            select_clause = select_parts.join(", "),
            buckets = buckets_table,
            table = table.fq_name(),
            join = join_clause,
            group_by = group_parts.join(", ")
        );
        let alias_list: Vec<String> = group_columns
            .iter()
            .map(|column| column.alias.clone())
            .collect();
        let alias_clause = alias_list.join(", ");
        let aggregate = aggregate_value_select(aggregation.op, "value");
        let select_prefix = if alias_list.is_empty() {
            format!("bucket, {aggregate}")
        } else {
            format!("bucket, {alias_clause}, {aggregate}")
        };
        let group_suffix = if alias_list.is_empty() {
            " GROUP BY bucket".to_string()
        } else {
            format!(" GROUP BY bucket, {alias_clause}")
        };
        let sql = format!(
            "SELECT {select_prefix} FROM ({inner}) AS stream_data{group_suffix} ORDER BY bucket",
            inner = inner_sql,
            select_prefix = select_prefix,
            group_suffix = group_suffix
        );
        let labels = if grouping_labels.is_empty() {
            MetricLabelsPlan::Columns(Vec::new())
        } else {
            MetricLabelsPlan::Columns(grouping_labels)
        };
        Ok(MetricRangeQueryPlan {
            sql,
            labels,
            drop_labels: Vec::new(),
        })
    }

    pub(crate) fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        if row.values().len() < 3 {
            return Err(AppError::Internal(
                "Loki schema query must return timestamp, labels, line".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&row.values()[0])?;
        let labels = parse_labels_value(&row.values()[1])?;
        let line = row.values()[2].to_string();
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }

    pub(crate) fn from_columns(
        columns: Vec<TableColumn>,
        config: &SchemaConfig,
    ) -> Result<Self, AppError> {
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

    pub(crate) async fn list_labels(
        &self,
        client: &Client,
        table: &TableRef,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
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
        let mut where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        where_clause.push_str(" AND f.value IS NOT NULL");
        let sql = format!("SELECT DISTINCT f.value AS label \
                FROM {table}, LATERAL FLATTEN(input => map_keys({labels})) AS f \
                WHERE {where} \
                ORDER BY label",
            table = table.fq_name(),
            labels = quote_ident(&self.labels_col),
            where = where_clause,
        );
        let rows = execute_query(client, &sql).await?;
        let mut labels = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = row.values().first() {
                let label = value.to_string();
                if !label.is_empty() {
                    labels.push(label);
                }
            }
        }
        Ok(labels)
    }

    pub(crate) async fn list_label_values(
        &self,
        client: &Client,
        table: &TableRef,
        label: &str,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
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
        let labels = quote_ident(&self.labels_col);
        let escaped_label = escape(label);
        let target = format!("{labels}['{escaped_label}']");
        clauses.push(format!("{target} IS NOT NULL"));
        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        let sql = format!(
            "SELECT DISTINCT {target} AS value FROM {table} WHERE {where} ORDER BY value",
            table = table.fq_name(),
            where = where_clause
        );
        let rows = execute_query(client, &sql).await?;
        let mut values = Vec::with_capacity(rows.len());
        for row in rows {
            if let Some(value) = row.values().first() {
                let text = value.to_string();
                if !text.is_empty() {
                    values.push(text);
                }
            }
        }
        Ok(values)
    }
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

struct LokiGroupColumn {
    expr: String,
    alias: String,
}

fn build_loki_group_columns(labels: &[String], column: &str) -> Vec<LokiGroupColumn> {
    labels
        .iter()
        .enumerate()
        .map(|(idx, label)| {
            let escaped = escape(label);
            LokiGroupColumn {
                expr: format!("{column}['{escaped}']"),
                alias: format!("group_{idx}"),
            }
        })
        .collect()
}

fn range_value_expression(function: RangeFunction, duration_ns: i64) -> String {
    match function {
        RangeFunction::CountOverTime => "COUNT(*)".to_string(),
        RangeFunction::Rate => {
            let seconds = duration_ns as f64 / 1_000_000_000_f64;
            let literal = format_float_literal(seconds);
            format!("COUNT(*) / {literal}")
        }
    }
}
