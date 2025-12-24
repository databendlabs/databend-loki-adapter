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

use std::collections::BTreeMap;

use databend_driver::{Client, Row};
use log::info;

use crate::{
    error::AppError,
    logql::{
        GroupModifier, LabelMatcher, LabelOp, LogqlExpr, MetricExpr, RangeFunction,
        VectorAggregation,
    },
};

use super::core::{
    LabelQueryBounds, LogEntry, MetricLabelsPlan, MetricQueryBounds, MetricQueryPlan,
    MetricRangeQueryBounds, MetricRangeQueryPlan, QueryBounds, SchemaConfig, StatsQueryBounds,
    StatsQueryPlan, TableColumn, TableRef, aggregate_value_select, ensure_line_column,
    ensure_timestamp_column, escape, execute_query, format_float_literal, is_line_candidate,
    is_numeric_type, line_filter_clause, matches_named_column, metric_bucket_cte,
    missing_required_column, quote_ident, range_bucket_value_expression, timestamp_literal,
    timestamp_offset_expr, value_to_timestamp,
};

#[derive(Clone)]
pub struct FlatSchema {
    pub(crate) timestamp_col: String,
    pub(crate) line_col: String,
    pub(crate) label_cols: Vec<FlatLabelColumn>,
}

#[derive(Clone, Debug)]
pub(crate) struct FlatLabelColumn {
    pub(crate) name: String,
    pub(crate) is_numeric: bool,
}

impl From<TableColumn> for FlatLabelColumn {
    fn from(column: TableColumn) -> Self {
        Self {
            is_numeric: is_numeric_type(&column.data_type),
            name: column.name,
        }
    }
}

impl FlatSchema {
    pub(crate) fn build_query(
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
            clauses.push(label_clause_flat(matcher, &self.label_cols, None)?);
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
        let line_source = expr
            .pipeline
            .pushdown_line_field()
            .and_then(|field| self.resolve_field_column(field))
            .unwrap_or_else(|| line_col.clone());
        selected.push(format!("{line_source} AS line_col"));
        for col in &self.label_cols {
            selected.push(quote_ident(&col.name));
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
        let ts_col = quote_ident(&self.timestamp_col);
        clauses.push(format!(
            "{ts_col} >= {}",
            timestamp_literal(bounds.start_ns)?
        ));
        clauses.push(format!("{ts_col} <= {}", timestamp_literal(bounds.end_ns)?));
        for matcher in &expr.range.selector.selectors {
            clauses.push(label_clause_flat(matcher, &self.label_cols, None)?);
        }
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
            None => self.metric_stream_sql(table, &where_clause, &value_expr, &drop_labels),
            Some(aggregation) => {
                self.metric_group_sql(table, &where_clause, &value_expr, aggregation, &drop_labels)
            }
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
        let line_col = format!("source.{}", quote_ident(&self.line_col));
        let stream_ts_col = format!("stream_source.{}", quote_ident(&self.timestamp_col));
        let stream_line_col = format!("stream_source.{}", quote_ident(&self.line_col));
        let bucket_window_start = timestamp_offset_expr("bucket_start", -bounds.window_ns);
        let mut join_conditions = vec![
            format!("{ts_col} >= {bucket_window_start}"),
            format!("{ts_col} <= bucket_start"),
        ];
        for matcher in &expr.range.selector.selectors {
            join_conditions.push(label_clause_flat(
                matcher,
                &self.label_cols,
                Some("source"),
            )?);
        }
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
        for matcher in &expr.range.selector.selectors {
            stream_conditions.push(label_clause_flat(
                matcher,
                &self.label_cols,
                Some("stream_source"),
            )?);
        }
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
            None => self.metric_range_stream_sql(
                table,
                &buckets_table,
                &join_clause,
                &value_expr,
                &drop_labels,
                &stream_where_clause,
            ),
            Some(aggregation) => self.metric_range_group_sql(
                table,
                &buckets_table,
                &join_clause,
                &value_expr,
                aggregation,
                &drop_labels,
            ),
        }
    }

    pub(crate) fn build_index_stats_query(
        &self,
        table: &TableRef,
        expr: &LogqlExpr,
        bounds: &StatsQueryBounds,
    ) -> Result<StatsQueryPlan, AppError> {
        let ts_col = quote_ident(&self.timestamp_col);
        let mut clauses = vec![
            format!("{ts_col} >= {}", timestamp_literal(bounds.start_ns)?),
            format!("{ts_col} <= {}", timestamp_literal(bounds.end_ns)?),
        ];
        for matcher in &expr.selectors {
            clauses.push(label_clause_flat(matcher, &self.label_cols, None)?);
        }
        clauses.extend(
            expr.filters
                .iter()
                .map(|f| line_filter_clause(quote_ident(&self.line_col), f)),
        );
        let where_clause = clauses.join(" AND ");
        let stream_expr = self.stream_identity_expr(None);
        let line_col = quote_ident(&self.line_col);
        let sql = format!(
            "SELECT \
                COUNT(DISTINCT {stream}) AS streams, \
                COUNT(DISTINCT {stream}) AS chunks, \
                COUNT(*) AS entries, \
                COALESCE(SUM(length({line_col})), 0) AS bytes \
             FROM {table} \
             WHERE {where_clause}",
            stream = stream_expr,
            table = table.fq_name()
        );
        Ok(StatsQueryPlan { sql })
    }

    fn stream_identity_expr(&self, alias: Option<&str>) -> String {
        if self.label_cols.is_empty() {
            return "''".to_string();
        }
        let qualifier = alias.map(|name| format!("{name}.")).unwrap_or_default();
        let mut args = Vec::with_capacity(self.label_cols.len() + 1);
        args.push("'|'".to_string());
        for column in &self.label_cols {
            let qualified = format!("{qualifier}{}", quote_ident(&column.name));
            args.push(format!("COALESCE(CAST({qualified} AS STRING), '')"));
        }
        format!("concat_ws({})", args.join(", "))
    }

    fn metric_stream_sql(
        &self,
        table: &TableRef,
        where_clause: &str,
        value_expr: &str,
        drop_labels: &[String],
    ) -> Result<MetricQueryPlan, AppError> {
        let retained: Vec<&FlatLabelColumn> = self
            .label_cols
            .iter()
            .filter(|col| !is_dropped_label(&col.name, drop_labels))
            .collect();
        let mut select_parts: Vec<String> =
            retained.iter().map(|col| quote_ident(&col.name)).collect();
        select_parts.push(format!("{value_expr} AS value"));
        let group_by_clause = if retained.is_empty() {
            String::new()
        } else {
            format!(
                " GROUP BY {}",
                retained
                    .iter()
                    .map(|col| quote_ident(&col.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let sql = format!(
            "SELECT {select_clause} FROM {table} WHERE {where}{group_by}",
            select_clause = select_parts.join(", "),
            table = table.fq_name(),
            where = where_clause,
            group_by = group_by_clause
        );
        Ok(MetricQueryPlan {
            sql,
            labels: MetricLabelsPlan::Columns(
                retained.iter().map(|col| col.name.clone()).collect(),
            ),
        })
    }

    fn metric_group_sql(
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
        let group_labels = match &aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let group_columns = self.resolve_group_columns(&group_labels, drop_labels)?;
        let mut select_parts: Vec<String> = group_columns
            .iter()
            .map(|column| format!("{} AS {}", column.expression(None), column.alias))
            .collect();
        select_parts.push(format!("{value_expr} AS value"));
        let retained: Vec<&FlatLabelColumn> = self
            .label_cols
            .iter()
            .filter(|col| !is_dropped_label(&col.name, drop_labels))
            .collect();
        let group_by_clause = if retained.is_empty() {
            String::new()
        } else {
            format!(
                " GROUP BY {}",
                retained
                    .iter()
                    .map(|col| quote_ident(&col.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        let inner_sql = format!(
            "SELECT {select_clause} FROM {table} WHERE {where}{group_by}",
            select_clause = select_parts.join(", "),
            table = table.fq_name(),
            where = where_clause,
            group_by = group_by_clause
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
        let labels = if group_labels.is_empty() {
            MetricLabelsPlan::Columns(Vec::new())
        } else {
            MetricLabelsPlan::Columns(group_labels)
        };
        Ok(MetricQueryPlan { sql, labels })
    }

    fn metric_range_stream_sql(
        &self,
        table: &TableRef,
        buckets_table: &str,
        join_clause: &str,
        value_expr: &str,
        drop_labels: &[String],
        stream_where_clause: &str,
    ) -> Result<MetricRangeQueryPlan, AppError> {
        let retained: Vec<&FlatLabelColumn> = self
            .label_cols
            .iter()
            .filter(|col| !is_dropped_label(&col.name, drop_labels))
            .collect();
        let stream_projection = if retained.is_empty() {
            "1 AS stream_exists".to_string()
        } else {
            retained
                .iter()
                .map(|col| qualified_column(Some("stream_source"), &col.name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let stream_cte = format!(
            "SELECT DISTINCT {projection} FROM {table} AS stream_source WHERE {where}",
            projection = stream_projection,
            table = table.fq_name(),
            where = stream_where_clause,
        );
        let mut select_parts = vec!["bucket_start AS bucket".to_string()];
        let mut group_parts = vec!["bucket_start".to_string()];
        for col in &retained {
            let qualified = qualified_column(Some("stream_labels"), &col.name);
            select_parts.push(qualified.clone());
            group_parts.push(qualified);
        }
        select_parts.push(format!("{value_expr} AS value"));
        let label_match_clause = if retained.is_empty() {
            "1=1".to_string()
        } else {
            retained
                .iter()
                .map(|col| {
                    format!(
                        "{source} = {stream}",
                        source = qualified_column(Some("source"), &col.name),
                        stream = qualified_column(Some("stream_labels"), &col.name),
                    )
                })
                .collect::<Vec<_>>()
                .join(" AND ")
        };
        let order_clause = if retained.is_empty() {
            "bucket".to_string()
        } else {
            let mut parts = vec!["bucket".to_string()];
            parts.extend(
                retained
                    .iter()
                    .map(|col| qualified_column(Some("stream_labels"), &col.name)),
            );
            parts.join(", ")
        };
        let sql = format!(
            "WITH stream_labels AS ({stream_cte}) \
             SELECT {select_clause} FROM {buckets} CROSS JOIN stream_labels \
             LEFT JOIN {table} AS source ON {join} AND {labels_match} \
             GROUP BY {group_by} ORDER BY {order_clause}",
            stream_cte = stream_cte,
            select_clause = select_parts.join(", "),
            buckets = buckets_table,
            table = table.fq_name(),
            join = join_clause,
            labels_match = label_match_clause,
            group_by = group_parts.join(", "),
            order_clause = order_clause
        );
        Ok(MetricRangeQueryPlan {
            sql,
            labels: MetricLabelsPlan::Columns(
                retained.iter().map(|col| col.name.clone()).collect(),
            ),
        })
    }

    fn metric_range_group_sql(
        &self,
        table: &TableRef,
        buckets_table: &str,
        join_clause: &str,
        value_expr: &str,
        aggregation: &VectorAggregation,
        drop_labels: &[String],
    ) -> Result<MetricRangeQueryPlan, AppError> {
        if let Some(GroupModifier::Without(labels)) = &aggregation.grouping {
            return Err(AppError::BadRequest(format!(
                "metric queries do not support `without` grouping ({labels:?})"
            )));
        }
        let group_labels = match &aggregation.grouping {
            Some(GroupModifier::By(labels)) if !labels.is_empty() => labels.clone(),
            _ => Vec::new(),
        };
        let group_columns = self.resolve_group_columns(&group_labels, drop_labels)?;
        let mut select_parts = vec!["bucket_start AS bucket".to_string()];
        select_parts.extend(
            group_columns
                .iter()
                .map(|column| format!("{} AS {}", column.expression(Some("source")), column.alias)),
        );
        select_parts.push(format!("{value_expr} AS value"));
        let mut group_parts = vec!["bucket_start".to_string()];
        group_columns.iter().for_each(|column| {
            if let Some(expr) = column.group_expression(Some("source")) {
                group_parts.push(expr);
            }
        });
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
        let labels = if group_labels.is_empty() {
            MetricLabelsPlan::Columns(Vec::new())
        } else {
            MetricLabelsPlan::Columns(group_labels)
        };
        Ok(MetricRangeQueryPlan { sql, labels })
    }

    fn resolve_group_columns(
        &self,
        labels: &[String],
        drop_labels: &[String],
    ) -> Result<Vec<FlatGroupColumn>, AppError> {
        let mut columns = Vec::with_capacity(labels.len());
        for (idx, label) in labels.iter().enumerate() {
            let column = if is_dropped_label(label, drop_labels) {
                None
            } else {
                find_label_column(&self.label_cols, label).map(|col| col.name.clone())
            };
            columns.push(FlatGroupColumn {
                column,
                alias: format!("group_{idx}"),
            });
        }
        Ok(columns)
    }

    pub(crate) fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        let values = row.values();
        if values.len() < self.label_cols.len() + 2 {
            return Err(AppError::Internal(
                "flat schema query must return timestamp, line, labels".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&values[0])?;
        let mut labels = BTreeMap::new();
        labels.insert(self.timestamp_col.clone(), values[0].to_string());
        let line = values[1].to_string();
        for (idx, column) in self.label_cols.iter().enumerate() {
            let value = values[idx + 2].to_string();
            labels.insert(column.name.clone(), value);
        }
        labels.insert(self.line_col.clone(), line.clone());
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }

    pub(crate) fn list_labels(&self) -> Vec<String> {
        let mut labels: Vec<_> = self.label_cols.iter().map(|col| col.name.clone()).collect();
        labels.push(self.timestamp_col.clone());
        labels.push(self.line_col.clone());
        labels.sort();
        labels.dedup();
        labels
    }

    pub(crate) async fn list_label_values(
        &self,
        client: &Client,
        table: &TableRef,
        label: &str,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        if label == self.timestamp_col || label == self.line_col {
            return Ok(Vec::new());
        }
        let column = find_label_column(&self.label_cols, label).ok_or_else(|| {
            AppError::BadRequest(format!("label `{label}` is not available in flat schema"))
        })?;
        let label_name = column.name.clone();
        let mut clauses = Vec::new();
        let ts_col = quote_ident(&self.timestamp_col);
        if let Some(start) = bounds.start_ns {
            clauses.push(format!("{ts_col} >= {}", timestamp_literal(start)?));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!("{ts_col} <= {}", timestamp_literal(end)?));
        }
        let label_col = quote_ident(&label_name);
        clauses.push(format!("{label_col} IS NOT NULL"));
        let where_clause = if clauses.is_empty() {
            "1=1".to_string()
        } else {
            clauses.join(" AND ")
        };
        let sql = format!(
            "SELECT DISTINCT {label_col} FROM {table} WHERE {where} ORDER BY {label_col}",
            label_col = label_col,
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

    pub(crate) fn from_columns(
        columns: Vec<TableColumn>,
        config: &SchemaConfig,
    ) -> Result<Self, AppError> {
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

            if desired_line.as_ref().is_some_and(|target| lower == *target)
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

        let label_cols: Vec<FlatLabelColumn> = label_cols
            .into_iter()
            .filter(|col| col.name != line.name)
            .map(FlatLabelColumn::from)
            .collect();

        let label_names: Vec<String> = label_cols.iter().map(|col| col.name.clone()).collect();

        info!(
            "flat schema resolved: timestamp=`{}`, line=`{}`, labels={:?}",
            timestamp.name, line.name, label_names
        );

        Ok(Self {
            timestamp_col: timestamp.name,
            line_col: line.name,
            label_cols,
        })
    }
}

impl FlatSchema {
    fn resolve_field_column(&self, field: &str) -> Option<String> {
        if field.eq_ignore_ascii_case(&self.timestamp_col) {
            return Some(quote_ident(&self.timestamp_col));
        }
        if field.eq_ignore_ascii_case(&self.line_col) {
            return Some(quote_ident(&self.line_col));
        }
        for column in &self.label_cols {
            if field.eq_ignore_ascii_case(&column.name) {
                return Some(quote_ident(&column.name));
            }
        }
        None
    }
}

fn label_clause_flat(
    matcher: &LabelMatcher,
    columns: &[FlatLabelColumn],
    table_alias: Option<&str>,
) -> Result<String, AppError> {
    let column = find_label_column(columns, &matcher.key).ok_or_else(|| {
        AppError::BadRequest(format!(
            "label `{}` is not a valid column in flat schema",
            matcher.key
        ))
    })?;
    if column.is_numeric {
        numeric_label_clause(column, matcher, table_alias)
    } else {
        let column = qualified_column(table_alias, &column.name);
        let value = escape(&matcher.value);
        Ok(match matcher.op {
            LabelOp::Eq => format!("{column} = '{value}'"),
            LabelOp::NotEq => format!("{column} != '{value}'"),
            LabelOp::RegexEq => format!("match({column}, '{value}')"),
            LabelOp::RegexNotEq => format!("NOT match({column}, '{value}')"),
        })
    }
}

fn numeric_label_clause(
    column: &FlatLabelColumn,
    matcher: &LabelMatcher,
    table_alias: Option<&str>,
) -> Result<String, AppError> {
    let column_ident = qualified_column(table_alias, &column.name);
    match matcher.op {
        LabelOp::Eq => Ok(format!(
            "{column_ident} = {}",
            numeric_literal(&matcher.value, &matcher.key)?
        )),
        LabelOp::NotEq => Ok(format!(
            "{column_ident} != {}",
            numeric_literal(&matcher.value, &matcher.key)?
        )),
        LabelOp::RegexEq | LabelOp::RegexNotEq => {
            let values = parse_numeric_regex_values(&matcher.value)
                .ok_or_else(|| AppError::BadRequest(format!(
                    "label `{}` only supports regex selectors composed of numeric alternatives (e.g., \"(200|202)\")",
                    matcher.key
                )))?;
            let operator = match matcher.op {
                LabelOp::RegexEq => "IN",
                LabelOp::RegexNotEq => "NOT IN",
                _ => unreachable!(),
            };
            Ok(format!("{column_ident} {operator} ({})", values.join(", ")))
        }
    }
}

fn numeric_literal(raw: &str, label: &str) -> Result<String, AppError> {
    parse_numeric_literal(raw).ok_or_else(|| {
        AppError::BadRequest(format!(
            "label `{label}` expects a numeric literal, found `{raw}`"
        ))
    })
}

fn parse_numeric_literal(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    let mut chars = trimmed.chars().peekable();
    if matches!(chars.peek(), Some('+') | Some('-')) {
        chars.next();
        chars.peek()?;
    }
    let mut seen_digit = false;
    let mut seen_dot = false;
    for ch in chars {
        if ch.is_ascii_digit() {
            seen_digit = true;
            continue;
        }
        if ch == '.' && !seen_dot {
            seen_dot = true;
            continue;
        }
        return None;
    }
    if !seen_digit {
        return None;
    }
    Some(trimmed.to_string())
}

fn parse_numeric_regex_values(pattern: &str) -> Option<Vec<String>> {
    let mut text = pattern.trim();
    if text.is_empty() {
        return None;
    }
    if text.starts_with('^') && text.ends_with('$') && text.len() > 2 {
        text = &text[1..text.len() - 1];
    }
    text = trim_enclosing_parens(text);
    if text.is_empty() {
        return None;
    }
    let mut values = Vec::new();
    for part in text.split('|') {
        let candidate = part.trim();
        if candidate.is_empty() {
            return None;
        }
        let literal = parse_numeric_literal(candidate)?;
        values.push(literal);
    }
    Some(values)
}

fn trim_enclosing_parens(value: &str) -> &str {
    let mut text = value;
    while text.starts_with('(') && text.ends_with(')') && text.len() > 2 {
        text = &text[1..text.len() - 1];
    }
    text
}

fn find_label_column<'a>(
    columns: &'a [FlatLabelColumn],
    target: &str,
) -> Option<&'a FlatLabelColumn> {
    columns
        .iter()
        .find(|col| col.name.eq_ignore_ascii_case(target))
}

fn is_dropped_label(target: &str, drop_labels: &[String]) -> bool {
    drop_labels
        .iter()
        .any(|label| label.eq_ignore_ascii_case(target))
}

fn qualified_column(alias: Option<&str>, column: &str) -> String {
    match alias {
        Some(table) => format!("{table}.{}", quote_ident(column)),
        None => quote_ident(column),
    }
}

struct FlatGroupColumn {
    column: Option<String>,
    alias: String,
}

impl FlatGroupColumn {
    fn expression(&self, table_alias: Option<&str>) -> String {
        match &self.column {
            Some(name) => qualified_column(table_alias, name),
            None => "CAST(NULL AS VARCHAR)".to_string(),
        }
    }

    fn group_expression(&self, table_alias: Option<&str>) -> Option<String> {
        self.column
            .as_ref()
            .map(|name| qualified_column(table_alias, name))
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logql::{DurationValue, Pipeline, RangeExpr, VectorAggregationOp};

    fn matcher(key: &str, op: LabelOp, value: &str) -> LabelMatcher {
        LabelMatcher {
            key: key.to_string(),
            op,
            value: value.to_string(),
        }
    }

    fn columns() -> Vec<FlatLabelColumn> {
        vec![
            FlatLabelColumn {
                name: "host".to_string(),
                is_numeric: false,
            },
            FlatLabelColumn {
                name: "status".to_string(),
                is_numeric: true,
            },
        ]
    }

    #[test]
    fn regex_on_string_column_uses_match() {
        let clause = label_clause_flat(
            &matcher("host", LabelOp::RegexEq, "api|edge"),
            &columns(),
            None,
        )
        .unwrap();
        assert_eq!(clause, "match(`host`, 'api|edge')");
    }

    #[test]
    fn numeric_regex_translates_to_in() {
        let clause = label_clause_flat(
            &matcher("status", LabelOp::RegexEq, "(200|202)"),
            &columns(),
            None,
        )
        .unwrap();
        assert_eq!(clause, "`status` IN (200, 202)");
    }

    #[test]
    fn numeric_regex_not_translates_to_not_in() {
        let clause = label_clause_flat(
            &matcher("status", LabelOp::RegexNotEq, "(200|202)"),
            &columns(),
            None,
        )
        .unwrap();
        assert_eq!(clause, "`status` NOT IN (200, 202)");
    }

    #[test]
    fn invalid_numeric_regex_returns_error() {
        let err = label_clause_flat(
            &matcher("status", LabelOp::RegexEq, "[23]+"),
            &columns(),
            None,
        )
        .unwrap_err();
        match err {
            AppError::BadRequest(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn metric_group_allows_missing_label() {
        let schema = FlatSchema {
            timestamp_col: "timestamp".to_string(),
            line_col: "line".to_string(),
            label_cols: vec![FlatLabelColumn {
                name: "host".to_string(),
                is_numeric: false,
            }],
        };
        let expr = MetricExpr {
            range: RangeExpr {
                function: RangeFunction::CountOverTime,
                selector: LogqlExpr {
                    selectors: Vec::new(),
                    filters: Vec::new(),
                    pipeline: Pipeline::default(),
                },
                duration: DurationValue::new(1_000_000_000).unwrap(),
            },
            aggregation: Some(VectorAggregation {
                op: VectorAggregationOp::Sum,
                grouping: Some(GroupModifier::By(vec!["level".to_string()])),
            }),
        };
        let table = TableRef {
            database: "db".to_string(),
            table: "tbl".to_string(),
        };
        let plan = schema
            .build_metric_query(
                &table,
                &expr,
                &MetricQueryBounds {
                    start_ns: 0,
                    end_ns: 1_000_000_000,
                },
            )
            .unwrap();
        assert!(plan.sql.contains("CAST(NULL AS VARCHAR) AS group_0"));
    }
}
