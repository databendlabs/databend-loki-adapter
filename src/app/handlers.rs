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
    extract::{
        Path, Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::Request,
    middleware::{self, Next},
    response::Response,
    routing::get,
};
use chrono::Utc;
use serde::Deserialize;
use std::{collections::HashSet, time::Instant};
use tokio::time::{Duration, sleep};

use crate::{
    databend::{
        LabelQueryBounds, MetricQueryBounds, MetricRangeQueryBounds, QueryBounds, SqlOrder,
        execute_query,
    },
    error::AppError,
    logql::{DurationValue, LogqlExpr},
};

use super::{
    responses::{
        LabelsResponse, LokiResponse, ProcessedEntry, collect_processed_entries,
        entries_to_streams, metric_matrix, metric_vector, rows_to_streams, tail_chunk,
    },
    state::{AppState, DEFAULT_LOOKBACK_NS},
};

const DEFAULT_TAIL_LIMIT: u64 = 100;
const DEFAULT_TAIL_LOOKBACK_NS: i64 = 60 * 60 * 1_000_000_000;
const MAX_TAIL_DELAY_SECONDS: u64 = 5;
const TAIL_IDLE_SLEEP_MS: u64 = 200;

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/loki/api/v1/labels", get(label_names))
        .route("/loki/api/v1/label/{label}/values", get(label_values))
        .route("/loki/api/v1/query", get(instant_query))
        .route("/loki/api/v1/query_range", get(range_query))
        .route("/loki/api/v1/tail", get(tail_logs))
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

#[derive(Debug, Deserialize)]
struct TailQueryParams {
    query: String,
    limit: Option<u64>,
    start: Option<i64>,
    delay_for: Option<u64>,
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
        let step_duration = parse_step_duration(step_raw)?;
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

async fn tail_logs(
    State(state): State<AppState>,
    Query(params): Query<TailQueryParams>,
    ws: WebSocketUpgrade,
) -> Result<Response, AppError> {
    let request = TailRequest::new(&state, params)?;
    let stream_state = state.clone();
    Ok(ws.on_upgrade(move |socket| async move {
        stream_tail(socket, stream_state, request).await;
    }))
}

fn parse_step_duration(step_raw: &str) -> Result<DurationValue, AppError> {
    match DurationValue::parse_literal(step_raw) {
        Ok(value) => Ok(value),
        Err(literal_err) => match parse_numeric_step_seconds(step_raw) {
            Ok(value) => Ok(value),
            Err(numeric_err) => Err(AppError::BadRequest(format!(
                "invalid step duration `{step_raw}`: {literal_err}; {numeric_err}"
            ))),
        },
    }
}

fn parse_numeric_step_seconds(step_raw: &str) -> Result<DurationValue, String> {
    let trimmed = step_raw.trim();
    if trimmed.is_empty() {
        return Err("numeric seconds cannot be empty".into());
    }
    let seconds: f64 = trimmed
        .parse()
        .map_err(|err| format!("failed to parse numeric seconds: {err}"))?;
    if !seconds.is_finite() {
        return Err("numeric seconds must be finite".into());
    }
    if seconds <= 0.0 {
        return Err("numeric seconds must be positive".into());
    }
    let nanos = seconds * 1_000_000_000.0;
    if nanos <= 0.0 {
        return Err("numeric seconds are too small".into());
    }
    if nanos > i64::MAX as f64 {
        return Err("numeric seconds exceed supported range".into());
    }
    let nanos = nanos.round() as i64;
    DurationValue::new(nanos)
        .map_err(|err| format!("failed to convert numeric seconds to duration: {err}"))
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

async fn stream_tail(mut socket: WebSocket, state: AppState, request: TailRequest) {
    if let Err(err) = tail_loop(&mut socket, state, request).await {
        log::warn!("tail stream closed: {}", err);
        let _ = socket.send(Message::Close(None)).await;
    }
}

async fn tail_loop(
    socket: &mut WebSocket,
    state: AppState,
    request: TailRequest,
) -> Result<(), AppError> {
    let mut cursor = TailCursor::new(request.start_ns);
    loop {
        let now = current_time_ns();
        let target_end_ns = now.saturating_sub(request.delay_ns);
        if target_end_ns <= cursor.next_start() {
            sleep(Duration::from_millis(TAIL_IDLE_SLEEP_MS)).await;
            continue;
        }
        let sql = state.schema().build_query(
            state.table(),
            &request.expr,
            &QueryBounds {
                start_ns: Some(cursor.next_start()),
                end_ns: Some(target_end_ns),
                limit: request.limit,
                order: SqlOrder::Asc,
            },
        )?;
        let rows = execute_query(state.client(), &sql).await?;
        let entries = collect_processed_entries(state.schema(), rows, &request.expr.pipeline)?;
        let filtered = filter_tail_entries(&mut cursor, entries)?;
        if filtered.is_empty() {
            sleep(Duration::from_millis(TAIL_IDLE_SLEEP_MS)).await;
            continue;
        }
        let streams = entries_to_streams(filtered)?;
        let payload = serde_json::to_string(&tail_chunk(streams))
            .map_err(|err| AppError::Internal(format!("failed to encode tail payload: {err}")))?;
        socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|err| AppError::Internal(format!("failed to send tail payload: {err}")))?;
    }
}

fn filter_tail_entries(
    cursor: &mut TailCursor,
    entries: Vec<ProcessedEntry>,
) -> Result<Vec<ProcessedEntry>, AppError> {
    let mut accepted = Vec::new();
    for entry in entries {
        let ts = i64::try_from(entry.timestamp_ns)
            .map_err(|_| AppError::Internal("tail timestamp is outside supported range".into()))?;
        if ts < cursor.next_start() {
            continue;
        }
        let fingerprint = entry_fingerprint(ts, &entry)?;
        if cursor.accept(ts, fingerprint) {
            accepted.push(entry);
        }
    }
    Ok(accepted)
}

fn entry_fingerprint(ts: i64, entry: &ProcessedEntry) -> Result<String, AppError> {
    let labels = serde_json::to_string(&entry.labels)
        .map_err(|err| AppError::Internal(format!("failed to encode labels: {err}")))?;
    Ok(format!("{ts}:{labels}:{}", entry.line))
}

struct TailCursor {
    next_start_ns: i64,
    last_timestamp_ns: Option<i64>,
    last_fingerprints: HashSet<String>,
}

impl TailCursor {
    fn new(start_ns: i64) -> Self {
        Self {
            next_start_ns: start_ns,
            last_timestamp_ns: None,
            last_fingerprints: HashSet::new(),
        }
    }

    fn next_start(&self) -> i64 {
        self.next_start_ns
    }

    fn accept(&mut self, ts: i64, fingerprint: String) -> bool {
        match self.last_timestamp_ns {
            Some(last) if ts < last => return false,
            Some(last) if ts > last => {
                self.last_timestamp_ns = Some(ts);
                self.last_fingerprints.clear();
            }
            None => {
                self.last_timestamp_ns = Some(ts);
                self.last_fingerprints.clear();
            }
            _ => {}
        }
        if !self.last_fingerprints.insert(fingerprint) {
            return false;
        }
        if ts > self.next_start_ns {
            self.next_start_ns = ts;
        }
        true
    }
}

struct TailRequest {
    expr: LogqlExpr,
    limit: u64,
    start_ns: i64,
    delay_ns: i64,
}

impl TailRequest {
    fn new(state: &AppState, params: TailQueryParams) -> Result<Self, AppError> {
        let limit = params.limit.unwrap_or(DEFAULT_TAIL_LIMIT).max(1);
        let limit = state.clamp_limit(Some(limit));
        let delay_for = params.delay_for.unwrap_or(0);
        if delay_for > MAX_TAIL_DELAY_SECONDS {
            return Err(AppError::BadRequest(format!(
                "delay_for cannot exceed {} seconds",
                MAX_TAIL_DELAY_SECONDS
            )));
        }
        let delay_ns = (delay_for as i64) * 1_000_000_000;
        let start_ns = params
            .start
            .unwrap_or_else(|| current_time_ns().saturating_sub(DEFAULT_TAIL_LOOKBACK_NS));
        let expr = state.parse(&params.query)?;
        Ok(Self {
            expr,
            limit,
            start_ns,
            delay_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{ProcessedEntry, TailCursor, filter_tail_entries, parse_constant_vector_expr};
    use std::collections::BTreeMap;

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

    #[test]
    fn tail_cursor_keeps_latest_timestamp() {
        let mut cursor = TailCursor::new(0);
        let mut entry = ProcessedEntry {
            timestamp_ns: 10,
            labels: BTreeMap::new(),
            line: "line1".into(),
        };
        assert_eq!(
            filter_tail_entries(&mut cursor, vec![entry.clone()])
                .unwrap()
                .len(),
            1
        );
        entry.line = "line2".into();
        entry.timestamp_ns = 10;
        assert_eq!(
            filter_tail_entries(&mut cursor, vec![entry.clone()])
                .unwrap()
                .len(),
            1
        );
        entry.line = "line1".into();
        assert_eq!(
            filter_tail_entries(&mut cursor, vec![entry]).unwrap().len(),
            0
        );
    }

    #[test]
    fn tail_cursor_skips_older_entries() {
        let mut cursor = TailCursor::new(50);
        let entry = ProcessedEntry {
            timestamp_ns: 40,
            labels: BTreeMap::new(),
            line: "old".into(),
        };
        assert!(
            filter_tail_entries(&mut cursor, vec![entry])
                .unwrap()
                .is_empty()
        );
    }
}
