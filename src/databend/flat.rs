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
    logql::{LabelMatcher, LabelOp, LogqlExpr},
};

use super::core::{
    LabelQueryBounds, LogEntry, QueryBounds, SchemaConfig, TableColumn, TableRef,
    ensure_line_column, ensure_timestamp_column, escape, execute_query, is_line_candidate,
    is_numeric_type, line_filter_clause, matches_named_column, missing_required_column,
    quote_ident, timestamp_literal, value_to_timestamp,
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

    pub(crate) fn parse_row(&self, row: &Row) -> Result<LogEntry, AppError> {
        let values = row.values();
        if values.len() < self.label_cols.len() + 2 {
            return Err(AppError::Internal(
                "flat schema query must return timestamp, line, labels".into(),
            ));
        }
        let timestamp_ns = value_to_timestamp(&values[0])?;
        let line = values[1].to_string();
        let mut labels = BTreeMap::new();
        for (idx, column) in self.label_cols.iter().enumerate() {
            let value = values[idx + 2].to_string();
            labels.insert(column.name.clone(), value);
        }
        Ok(LogEntry {
            timestamp_ns,
            labels,
            line,
        })
    }

    pub(crate) fn list_labels(&self) -> Vec<String> {
        let mut labels: Vec<_> = self.label_cols.iter().map(|col| col.name.clone()).collect();
        labels.sort();
        labels
    }

    pub(crate) async fn list_label_values(
        &self,
        client: &Client,
        table: &TableRef,
        label: &str,
        bounds: &LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        let column = find_label_column(&self.label_cols, label).ok_or_else(|| {
            AppError::BadRequest(format!("label `{label}` is not available in flat schema"))
        })?;
        let mut clauses = Vec::new();
        let ts_col = quote_ident(&self.timestamp_col);
        if let Some(start) = bounds.start_ns {
            clauses.push(format!("{ts_col} >= {}", timestamp_literal(start)?));
        }
        if let Some(end) = bounds.end_ns {
            clauses.push(format!("{ts_col} <= {}", timestamp_literal(end)?));
        }
        let label_col = quote_ident(&column.name);
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

fn label_clause_flat(
    matcher: &LabelMatcher,
    columns: &[FlatLabelColumn],
) -> Result<String, AppError> {
    let column = find_label_column(columns, &matcher.key).ok_or_else(|| {
        AppError::BadRequest(format!(
            "label `{}` is not a valid column in flat schema",
            matcher.key
        ))
    })?;
    if column.is_numeric {
        numeric_label_clause(column, matcher)
    } else {
        let column = quote_ident(&column.name);
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
) -> Result<String, AppError> {
    let column_ident = quote_ident(&column.name);
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
            Ok(format!(
                "{column_ident} {operator} ({})",
                values.join(", ")
            ))
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
        if chars.peek().is_none() {
            return None;
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
        )
        .unwrap();
        assert_eq!(clause, "match(`host`, 'api|edge')");
    }

    #[test]
    fn numeric_regex_translates_to_in() {
        let clause = label_clause_flat(
            &matcher("status", LabelOp::RegexEq, "(200|202)"),
            &columns(),
        )
        .unwrap();
        assert_eq!(clause, "`status` IN (200, 202)");
    }

    #[test]
    fn numeric_regex_not_translates_to_not_in() {
        let clause = label_clause_flat(
            &matcher("status", LabelOp::RegexNotEq, "(200|202)"),
            &columns(),
        )
        .unwrap();
        assert_eq!(clause, "`status` NOT IN (200, 202)");
    }

    #[test]
    fn invalid_numeric_regex_returns_error() {
        let err = label_clause_flat(
            &matcher("status", LabelOp::RegexEq, "[23]+" ),
            &columns(),
        )
        .unwrap_err();
        match err {
            AppError::BadRequest(_) => {}
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
