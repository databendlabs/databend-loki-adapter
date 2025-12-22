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

use axum::{
    Json, Router,
    body::Body,
    extract::{Path, Query, State},
    http::Request,
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use chrono::Utc;
use serde::Deserialize;
use std::time::Instant;

use crate::{
    databend::{
        LabelQueryBounds, MetricQueryBounds, MetricRangeQueryBounds, QueryBounds, SqlOrder,
        execute_query,
    },
    error::AppError,
    logql::DurationValue,
};

use super::{
    responses::{LabelsResponse, LokiResponse, metric_matrix, metric_vector, rows_to_streams},
    state::{AppState, DEFAULT_LOOKBACK_NS},
};

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/loki/api/v1/labels", get(label_names))
        .route("/loki/api/v1/label/{label}/values", get(label_values))
        .route("/loki/api/v1/query", get(instant_query))
        .route("/loki/api/v1/query_range", get(range_query))
        .with_state(state)
        .layer(middleware::from_fn(log_requests))
}

#[derive(Debug, Deserialize)]
struct InstantQueryParams {
    query: String,
    limit: Option<u64>,
    time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RangeQueryParams {
    query: String,
    limit: Option<u64>,
    start: Option<i64>,
    end: Option<i64>,
    step: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LabelsQueryParams {
    start: Option<i64>,
    end: Option<i64>,
}

async fn instant_query(
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> Result<Json<LokiResponse>, AppError> {
    let target_ns = params.time.unwrap_or_else(current_time_ns);
    log::debug!(
        "instant query received: query=`{}` limit={:?} time_ns={}",
        params.query,
        params.limit,
        target_ns
    );
    if let Some(response) = try_constant_vector(&params.query, target_ns) {
        return Ok(Json(response));
    }
    if let Some(metric) = state.parse_metric(&params.query)? {
        let duration_ns = metric.range.duration.as_nanoseconds();
        let start_ns = target_ns.saturating_sub(duration_ns);
        let plan = state.schema().build_metric_query(
            state.table(),
            &metric,
            &MetricQueryBounds {
                start_ns,
                end_ns: target_ns,
            },
        )?;
        log::debug!("instant metric SQL: {}", plan.sql);
        let rows = execute_query(state.client(), &plan.sql).await?;
        let samples = state.schema().parse_metric_rows(rows, &plan)?;
        return Ok(Json(metric_vector(target_ns, samples)));
    }
    let expr = state.parse(&params.query)?;
    let start_ns = target_ns.saturating_sub(DEFAULT_LOOKBACK_NS);
    let limit = state.clamp_limit(params.limit);

    let sql = state.schema().build_query(
        state.table(),
        &expr,
        &QueryBounds {
            start_ns: Some(start_ns),
            end_ns: Some(target_ns),
            limit,
            order: SqlOrder::Desc,
        },
    )?;

    log::debug!(
        "instant query SQL (start_ns={}, end_ns={}, limit={}): {}",
        start_ns,
        target_ns,
        limit,
        sql
    );
    let rows = execute_query(state.client(), &sql).await?;
    let streams = rows_to_streams(state.schema(), rows, &expr.pipeline)?;
    Ok(Json(LokiResponse::success(streams)))
}

async fn range_query(
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> Result<Json<LokiResponse>, AppError> {
    log::debug!(
        "range query received: query=`{}` limit={:?} start={:?} end={:?} step={:?}",
        params.query,
        params.limit,
        params.start,
        params.end,
        params.step
    );
    let start = params
        .start
        .ok_or_else(|| AppError::BadRequest("start is required".into()))?;
    let end = params
        .end
        .ok_or_else(|| AppError::BadRequest("end is required".into()))?;

    if start >= end {
        return Err(AppError::BadRequest(
            "start must be smaller than end".into(),
        ));
    }

    if let Some(metric) = state.parse_metric(&params.query)? {
        let step_raw = params
            .step
            .as_deref()
            .ok_or_else(|| AppError::BadRequest("step is required for metric queries".into()))?;
        let step_duration = DurationValue::parse_literal(step_raw).map_err(|err| {
            AppError::BadRequest(format!("invalid step duration `{step_raw}`: {err}"))
        })?;
        let step_ns = step_duration.as_nanoseconds();
        let window_ns = metric.range.duration.as_nanoseconds();
        if step_ns != window_ns {
            return Err(AppError::BadRequest(
                "metric range queries require step to match the range selector duration".into(),
            ));
        }
        let plan = state.schema().build_metric_range_query(
            state.table(),
            &metric,
            &MetricRangeQueryBounds {
                start_ns: start,
                end_ns: end,
                step_ns,
                window_ns,
            },
        )?;
        log::debug!("range metric SQL: {}", plan.sql);
        let rows = execute_query(state.client(), &plan.sql).await?;
        let samples = state.schema().parse_metric_matrix_rows(rows, &plan)?;
        return Ok(Json(metric_matrix(samples)));
    }

    let expr = state.parse(&params.query)?;

    let limit = state.clamp_limit(params.limit);
    let sql = state.schema().build_query(
        state.table(),
        &expr,
        &QueryBounds {
            start_ns: Some(start),
            end_ns: Some(end),
            limit,
            order: SqlOrder::Asc,
        },
    )?;

    log::debug!(
        "range query SQL (start_ns={}, end_ns={}, limit={}): {}",
        start,
        end,
        limit,
        sql
    );
    let rows = execute_query(state.client(), &sql).await?;
    let streams = rows_to_streams(state.schema(), rows, &expr.pipeline)?;
    Ok(Json(LokiResponse::success(streams)))
}

async fn label_names(
    State(state): State<AppState>,
    Query(params): Query<LabelsQueryParams>,
) -> Result<Json<LabelsResponse>, AppError> {
    let now = current_time_ns();
    let end = params.end.unwrap_or(now);
    let start = params
        .start
        .unwrap_or_else(|| end.saturating_sub(DEFAULT_LOOKBACK_NS));
    if start >= end {
        return Err(AppError::BadRequest(
            "start must be smaller than end".into(),
        ));
    }
    let bounds = LabelQueryBounds {
        start_ns: Some(start),
        end_ns: Some(end),
    };
    let mut labels = state.list_labels(bounds).await?;
    labels.sort();
    labels.dedup();
    Ok(Json(LabelsResponse::success(labels)))
}

async fn label_values(
    State(state): State<AppState>,
    Path(label): Path<String>,
    Query(params): Query<LabelsQueryParams>,
) -> Result<Json<LabelsResponse>, AppError> {
    let now = current_time_ns();
    let end = params.end.unwrap_or(now);
    let start = params
        .start
        .unwrap_or_else(|| end.saturating_sub(DEFAULT_LOOKBACK_NS));
    if start >= end {
        return Err(AppError::BadRequest(
            "start must be smaller than end".into(),
        ));
    }
    let bounds = LabelQueryBounds {
        start_ns: Some(start),
        end_ns: Some(end),
    };
    let mut values = state.list_label_values(&label, bounds).await?;
    values.sort();
    values.dedup();
    Ok(Json(LabelsResponse::success(values)))
}

fn current_time_ns() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .unwrap_or_else(|| now.timestamp_micros() * 1_000)
}

fn try_constant_vector(query: &str, timestamp_ns: i64) -> Option<LokiResponse> {
    parse_constant_vector_expr(query)
        .map(|value| LokiResponse::vector_constant(timestamp_ns, value))
}

fn parse_constant_vector_expr(input: &str) -> Option<f64> {
    let mut total = 0f64;
    let mut has_term = false;
    for segment in input.split('+') {
        let value = parse_vector_term(segment.trim())?;
        total += value;
        has_term = true;
    }
    has_term.then_some(total)
}

fn parse_vector_term(segment: &str) -> Option<f64> {
    const PREFIX: &str = "vector(";
    let segment = segment.trim();
    if !segment.starts_with(PREFIX) || !segment.ends_with(')') {
        return None;
    }
    let inner = &segment[PREFIX.len()..segment.len() - 1];
    let value = inner.trim().parse::<f64>().ok()?;
    Some(value)
}

async fn log_requests(req: Request<Body>, next: Next) -> Response {
    let method = req.method().clone();
    let uri = req.uri().clone();
    let start = Instant::now();
    let response = next.run(req).await;
    let status = response.status();
    let elapsed = start.elapsed();
    log::info!(
        "method={} path={} status={} duration_ms={:.3}",
        method,
        uri.path(),
        status.as_u16(),
        elapsed.as_secs_f64() * 1000.0
    );
    response
}

#[cfg(test)]
mod tests {
    use super::parse_constant_vector_expr;

    #[test]
    fn parse_constant_vector_sum() {
        assert_eq!(
            parse_constant_vector_expr("vector(1)+vector(1)").unwrap(),
            2.0
        );
        assert_eq!(
            parse_constant_vector_expr(" vector(0.5) + vector(0.5) ").unwrap(),
            1.0
        );
        assert!(parse_constant_vector_expr("{app=\"loki\"}").is_none());
        assert!(parse_constant_vector_expr("vector(foo)").is_none());
        assert!(parse_constant_vector_expr("vector(1)+sum").is_none());
    }
}
