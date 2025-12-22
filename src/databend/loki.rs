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
        match &expr.aggregation {
            None => self.metric_streams_sql(table, &where_clause, &value_expr, &drop_labels),
            Some(aggregation) => self.metric_aggregation_sql(
                table,
                &where_clause,
                &value_expr,
                aggregation,
                &drop_labels,
            ),
        }
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
        let stream_ts_col = format!("stream_source.{}", quote_ident(&self.timestamp_col));
        let stream_labels_col = format!("stream_source.{}", quote_ident(&self.labels_col));
        let stream_line_col = format!("stream_source.{}", quote_ident(&self.line_col));
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
        let stream_window_start_ns = bounds.start_ns.saturating_sub(bounds.window_ns);
        let stream_start_literal = timestamp_literal(stream_window_start_ns)?;
        let stream_end_literal = timestamp_literal(bounds.end_ns)?;
        let mut stream_conditions = vec![
            format!("{stream_ts_col} >= {stream_start_literal}"),
            format!("{stream_ts_col} <= {stream_end_literal}"),
        ];
        stream_conditions.extend(
            expr.range
                .selector
                .selectors
                .iter()
                .map(|m| label_clause_loki(m, stream_labels_col.clone())),
        );
        stream_conditions.extend(
            expr.range
                .selector
                .filters
                .iter()
                .map(|f| line_filter_clause(stream_line_col.clone(), f)),
        );
        let stream_where_clause = stream_conditions.join(" AND ");
        let value_expr =
            range_bucket_value_expression(expr.range.function, bounds.window_ns, &ts_col);
        match &expr.aggregation {
            None => self.metric_range_stream_sql(&MetricRangeStreamContext {
                table,
                buckets_table: &buckets_table,
                labels_column: &labels_col,
                stream_labels_column: &stream_labels_col,
                value_expr: &value_expr,
                join_clause: &join_clause,
                stream_where_clause: &stream_where_clause,
                drop_labels: &drop_labels,
            }),
            Some(aggregation) => {
                self.metric_range_aggregation_sql(&MetricRangeAggregationContext {
                    table,
                    buckets_table: &buckets_table,
                    labels_column: &labels_col,
                    value_expr: &value_expr,
                    join_clause: &join_clause,
                    aggregation,
                    drop_labels: &drop_labels,
                })
            }
        }
    }

    fn metric_streams_sql(
        &self,
        table: &TableRef,
        where_clause: &str,
        value_expr: &str,
        drop_labels: &[String],
    ) -> Result<MetricQueryPlan, AppError> {
        let labels_column = quote_ident(&self.labels_col);
        let labels_expr = drop_labels_expr(&labels_column, drop_labels);
        let sql = format!(
            "SELECT {labels_expr} AS labels, {value_expr} AS value FROM {table} \
             WHERE {where} GROUP BY {labels_expr}",
            table = table.fq_name(),
            where = where_clause,
            labels_expr = labels_expr
        );
        Ok(MetricQueryPlan {
            sql,
            labels: MetricLabelsPlan::LokiFull,
        })
    }

    fn metric_aggregation_sql(
        &self,
        table: &TableRef,
        where_clause: &str,
        value_expr: &str,
        aggregation: &VectorAggregation,
        drop_labels: &[String],
    ) -> Result<MetricQueryPlan, AppError> {
        if let Some(GroupModifier::Without(labels)) = &aggregation.grouping {
            return Err(AppError::BadRequest(format!(
                "metric queries do not support `without` grouping ({labels:?})"
            )));
        }
        let label_column = quote_ident(&self.labels_col);
        let grouped_labels_expr = drop_labels_expr(&label_column, drop_labels);
        let grouping_labels = match &aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let group_columns = build_loki_group_columns(&grouping_labels, &grouped_labels_expr);

        let mut select_parts: Vec<String> = group_columns
            .iter()
            .map(|column| format!("{} AS {}", column.expr, column.alias))
            .collect();
        select_parts.push(format!("{value_expr} AS value"));
        let mut group_parts = vec![grouped_labels_expr.clone()];
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
        Ok(MetricQueryPlan { sql, labels })
    }

    fn metric_range_stream_sql(
        &self,
        ctx: &MetricRangeStreamContext<'_>,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        let labels_expr = drop_labels_expr(ctx.labels_column, ctx.drop_labels);
        let stream_labels_expr = drop_labels_expr(ctx.stream_labels_column, ctx.drop_labels);
        let stream_cte = format!(
            "SELECT DISTINCT {stream_labels_expr} AS labels FROM {table} AS stream_source WHERE {where}",
            table = ctx.table.fq_name(),
            where = ctx.stream_where_clause,
        );
        let labels_match_clause = format!("{labels_expr} = stream_labels.labels");
        let sql = format!(
            "WITH stream_labels AS ({stream_cte}) \
             SELECT bucket_start AS bucket, stream_labels.labels AS labels, {value_expr} AS value \
             FROM {buckets} CROSS JOIN stream_labels \
             LEFT JOIN {table} AS source ON {join} AND {labels_match} \
             GROUP BY bucket_start, stream_labels.labels \
             ORDER BY bucket, stream_labels.labels",
            stream_cte = stream_cte,
            value_expr = ctx.value_expr,
            buckets = ctx.buckets_table,
            table = ctx.table.fq_name(),
            join = ctx.join_clause,
            labels_match = labels_match_clause
        );
        Ok(MetricRangeQueryPlan {
            sql,
            labels: MetricLabelsPlan::LokiFull,
        })
    }

    fn metric_range_aggregation_sql(
        &self,
        ctx: &MetricRangeAggregationContext<'_>,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        if let Some(GroupModifier::Without(labels)) = &ctx.aggregation.grouping {
            return Err(AppError::BadRequest(format!(
                "metric queries do not support `without` grouping ({labels:?})"
            )));
        }
        let grouping_labels = match &ctx.aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let grouped_labels_expr = drop_labels_expr(ctx.labels_column, ctx.drop_labels);
        let group_columns = build_loki_group_columns(&grouping_labels, &grouped_labels_expr);
        let mut select_parts = vec!["bucket_start AS bucket".to_string()];
        select_parts.extend(
            group_columns
                .iter()
                .map(|column| format!("{} AS {}", column.expr, column.alias)),
        );
        select_parts.push(format!("{} AS value", ctx.value_expr));
        let mut group_parts = vec!["bucket_start".to_string(), grouped_labels_expr.clone()];
        group_parts.extend(group_columns.iter().map(|column| column.expr.clone()));
        let inner_sql = format!(
            "SELECT {select_clause} FROM {buckets} LEFT JOIN {table} AS source ON {join} GROUP BY {group_by}",
            select_clause = select_parts.join(", "),
            buckets = ctx.buckets_table,
            table = ctx.table.fq_name(),
            join = ctx.join_clause,
            group_by = group_parts.join(", ")
        );
        let alias_list: Vec<String> = group_columns
            .iter()
            .map(|column| column.alias.clone())
            .collect();
        let alias_clause = alias_list.join(", ");
        let aggregate = aggregate_value_select(ctx.aggregation.op, "value");
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
        Ok(MetricRangeQueryPlan { sql, labels })
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

struct MetricRangeStreamContext<'a> {
    table: &'a TableRef,
    buckets_table: &'a str,
    labels_column: &'a str,
    stream_labels_column: &'a str,
    value_expr: &'a str,
    join_clause: &'a str,
    stream_where_clause: &'a str,
    drop_labels: &'a [String],
}

struct MetricRangeAggregationContext<'a> {
    table: &'a TableRef,
    buckets_table: &'a str,
    labels_column: &'a str,
    value_expr: &'a str,
    join_clause: &'a str,
    aggregation: &'a VectorAggregation,
    drop_labels: &'a [String],
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
    let base = format!("({column})");
    labels
        .iter()
        .enumerate()
        .map(|(idx, label)| {
            let escaped = escape(label);
            LokiGroupColumn {
                expr: format!("{base}['{escaped}']"),
                alias: format!("group_{idx}"),
            }
        })
        .collect()
}

fn drop_labels_expr(column: &str, drop_labels: &[String]) -> String {
    if drop_labels.is_empty() {
        return column.to_string();
    }
    let mut expr = column.to_string();
    for label in drop_labels {
        let escaped = escape(label);
        expr = format!("OBJECT_DELETE({expr}, '{escaped}')");
    }
    expr
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
