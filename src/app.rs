use std::collections::BTreeMap;

use axum::{
    Json, Router,
    extract::{Query, State},
    routing::get,
};
use chrono::Utc;
use databend_driver::Client;
use log::{info, warn};
use serde::{Deserialize, Serialize};
use serde_json;

use crate::{
    databend::{QueryBounds, SchemaConfig, SqlOrder},
    databend::{SchemaAdapter, SchemaType, TableRef, execute_query, resolve_table_ref},
    error::AppError,
    logql::LogqlParser,
};

const DEFAULT_LIMIT: u64 = 500;
const MAX_LIMIT: u64 = 5_000;
const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

#[derive(Clone)]
pub struct AppState {
    client: Client,
    table: TableRef,
    parser: LogqlParser,
    schema: SchemaAdapter,
}

impl AppState {
    pub async fn bootstrap(config: AppConfig) -> Result<Self, AppError> {
        let AppConfig {
            dsn,
            table,
            schema_type,
            schema_config,
        } = config;
        info!("resolving table reference for `{table}`");
        let table = resolve_table_ref(&dsn, &table)?;
        info!("table resolved to {}", table.fq_name());
        let client = Client::new(dsn.clone());
        verify_connection(&client).await?;
        info!(
            "loading {} schema metadata for {}",
            schema_type,
            table.fq_name()
        );
        let schema =
            crate::databend::load_schema(&client, &table, schema_type, &schema_config).await?;
        info!(
            "{} schema ready; using table {}",
            schema_type,
            table.fq_name()
        );
        Ok(Self {
            client,
            table,
            parser: LogqlParser::default(),
            schema,
        })
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn table(&self) -> &TableRef {
        &self.table
    }

    pub fn schema(&self) -> &SchemaAdapter {
        &self.schema
    }

    fn clamp_limit(&self, requested: Option<u64>) -> u64 {
        requested
            .and_then(|value| (value > 0).then_some(value))
            .map(|value| value.min(MAX_LIMIT))
            .unwrap_or(DEFAULT_LIMIT)
    }
}

pub struct AppConfig {
    pub dsn: String,
    pub table: String,
    pub schema_type: SchemaType,
    pub schema_config: SchemaConfig,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/loki/api/v1/query", get(instant_query))
        .route("/loki/api/v1/query_range", get(range_query))
        .with_state(state)
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
    step: Option<u64>,
}

async fn instant_query(
    State(state): State<AppState>,
    Query(params): Query<InstantQueryParams>,
) -> Result<Json<LokiResponse>, AppError> {
    let expr = state.parser.parse(&params.query)?;
    let target_ns = params.time.unwrap_or_else(current_time_ns);
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

    let rows = execute_query(state.client(), &sql).await?;
    let streams = rows_to_streams(state.schema(), rows)?;
    Ok(Json(LokiResponse::success(streams)))
}

async fn range_query(
    State(state): State<AppState>,
    Query(params): Query<RangeQueryParams>,
) -> Result<Json<LokiResponse>, AppError> {
    let expr = state.parser.parse(&params.query)?;
    let _ = params.step;
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

    let rows = execute_query(state.client(), &sql).await?;
    let streams = rows_to_streams(state.schema(), rows)?;
    Ok(Json(LokiResponse::success(streams)))
}

fn current_time_ns() -> i64 {
    let now = Utc::now();
    now.timestamp_nanos_opt()
        .and_then(|ns| i64::try_from(ns).ok())
        .unwrap_or_else(|| now.timestamp_micros() * 1_000)
}

fn rows_to_streams(
    schema: &SchemaAdapter,
    rows: Vec<databend_driver::Row>,
) -> Result<Vec<LokiStream>, AppError> {
    let mut buckets: BTreeMap<String, StreamBucket> = BTreeMap::new();
    for row in rows {
        let entry = schema.parse_row(&row)?;
        let key = serde_json::to_string(&entry.labels)
            .map_err(|err| AppError::Internal(format!("failed to encode labels: {err}")))?;
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| StreamBucket::new(entry.labels.clone()));
        bucket.values.push((entry.timestamp_ns, entry.line.clone()));
    }

    let mut result = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_values() {
        result.push(bucket.into_stream());
    }
    Ok(result)
}

struct StreamBucket {
    labels: BTreeMap<String, String>,
    values: Vec<(i128, String)>,
}

impl StreamBucket {
    fn new(labels: BTreeMap<String, String>) -> Self {
        Self {
            labels,
            values: Vec::new(),
        }
    }

    fn into_stream(mut self) -> LokiStream {
        self.values.sort_by_key(|(ts, _)| *ts);
        let values = self
            .values
            .into_iter()
            .map(|(ts, line)| [ts.to_string(), line])
            .collect();
        LokiStream {
            stream: self.labels,
            values,
        }
    }
}

#[derive(Serialize)]
struct LokiResponse {
    status: &'static str,
    data: LokiData,
}

impl LokiResponse {
    fn success(streams: Vec<LokiStream>) -> Self {
        Self {
            status: "success",
            data: LokiData {
                result_type: "streams",
                result: streams,
            },
        }
    }
}

#[derive(Serialize)]
struct LokiData {
    #[serde(rename = "resultType")]
    result_type: &'static str,
    result: Vec<LokiStream>,
}

#[derive(Serialize)]
struct LokiStream {
    stream: BTreeMap<String, String>,
    values: Vec<[String; 2]>,
}

async fn verify_connection(client: &Client) -> Result<(), AppError> {
    let conn = client.get_conn().await?;
    let info = conn.info().await;
    info!("connected to Databend {}:{}", info.host, info.port);
    let version = match conn.version().await {
        Ok(version) => version,
        Err(err) => {
            warn!("server version unavailable: {err}");
            return Ok(());
        }
    };
    info!("server version {version}");
    let _ = conn.close().await;
    Ok(())
}
