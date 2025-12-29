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

use axum::http::HeaderMap;
use databend_driver::Client;
use log::{info, warn};
use serde::Deserialize;

use crate::{
    databend::{
        FlatLabelDefinition, FlatSchemaDefinition, LabelQueryBounds, LokiSchemaDefinition,
        SchemaAdapter, SchemaConfig, SchemaDefinition, SchemaType, TableRef, load_schema,
        resolve_table_ref, schema_from_definition,
    },
    error::AppError,
    logql::{LogqlExpr, LogqlParser, MetricExpr},
};

pub(crate) const DEFAULT_LIMIT: u64 = 500;
pub(crate) const MAX_LIMIT: u64 = 5_000;
pub(crate) const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;
pub const PROXY_DSN_HEADER: &str = "x-databend-dsn";
pub const PROXY_SCHEMA_HEADER: &str = "x-databend-schema";

#[derive(Clone)]
pub struct AppState {
    parser: LogqlParser,
    runtime: RuntimeState,
    max_metric_buckets: i64,
}

impl AppState {
    pub async fn bootstrap(config: AppConfig) -> Result<Self, AppError> {
        let AppConfig {
            runtime,
            max_metric_buckets,
        } = config;
        let runtime = match runtime {
            RuntimeConfig::Standalone(standalone) => {
                let StandaloneConfig {
                    dsn,
                    table,
                    schema_type,
                    schema_config,
                } = standalone;
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
                let schema = load_schema(&client, &table, schema_type, &schema_config).await?;
                info!(
                    "{} schema ready; using table {}",
                    schema_type,
                    table.fq_name()
                );
                RuntimeState::Standalone(StandaloneState {
                    client,
                    table,
                    schema,
                })
            }
            RuntimeConfig::Proxy => {
                info!("adapter starting in proxy mode");
                RuntimeState::Proxy(ProxyState)
            }
        };
        Ok(Self {
            parser: LogqlParser,
            runtime,
            max_metric_buckets: i64::from(max_metric_buckets.max(1)),
        })
    }

    pub fn max_metric_buckets(&self) -> i64 {
        self.max_metric_buckets
    }

    pub fn parse(&self, query: &str) -> Result<LogqlExpr, AppError> {
        self.parser.parse(query).map_err(AppError::from)
    }

    pub fn parse_metric(&self, query: &str) -> Result<Option<MetricExpr>, AppError> {
        self.parser.parse_metric(query).map_err(AppError::from)
    }

    pub fn clamp_limit(&self, requested: Option<u64>) -> u64 {
        requested
            .and_then(|value| (value > 0).then_some(value))
            .map(|value| value.min(MAX_LIMIT))
            .unwrap_or(DEFAULT_LIMIT)
    }

    pub fn resolve_request(&self, headers: &HeaderMap) -> Result<RequestContext, AppError> {
        match &self.runtime {
            RuntimeState::Standalone(runtime) => Ok(runtime.as_context()),
            RuntimeState::Proxy(runtime) => runtime.resolve(headers),
        }
    }
}

#[derive(Clone)]
enum RuntimeState {
    Standalone(StandaloneState),
    Proxy(ProxyState),
}

#[derive(Clone)]
struct StandaloneState {
    client: Client,
    table: TableRef,
    schema: SchemaAdapter,
}

impl StandaloneState {
    fn as_context(&self) -> RequestContext {
        RequestContext {
            client: self.client.clone(),
            table: self.table.clone(),
            schema: self.schema.clone(),
        }
    }
}

#[derive(Clone)]
struct ProxyState;

impl ProxyState {
    fn resolve(&self, headers: &HeaderMap) -> Result<RequestContext, AppError> {
        let dsn_header = parse_header(headers, PROXY_DSN_HEADER)?;
        let schema_header = parse_header(headers, PROXY_SCHEMA_HEADER)?;
        let schema_payload: ProxySchemaPayload =
            serde_json::from_str(schema_header).map_err(|err| {
                AppError::BadRequest(format!(
                    "invalid {PROXY_SCHEMA_HEADER} header payload: {err}"
                ))
            })?;
        if schema_payload.table.trim().is_empty() {
            return Err(AppError::BadRequest(
                "schema header requires a non-empty `table` field".into(),
            ));
        }
        let table = resolve_table_ref(dsn_header, schema_payload.table.trim())?;
        let definition = schema_payload.into_definition()?;
        let schema = schema_from_definition(definition);
        Ok(RequestContext {
            client: Client::new(dsn_header.to_string()),
            table,
            schema,
        })
    }
}

fn parse_header<'a>(headers: &'a HeaderMap, name: &str) -> Result<&'a str, AppError> {
    let value = headers
        .get(name)
        .ok_or_else(|| AppError::BadRequest(format!("missing `{name}` header")))?;
    value
        .to_str()
        .map_err(|_| AppError::BadRequest(format!("header `{name}` must be valid UTF-8")))
}

#[derive(Clone)]
pub struct RequestContext {
    client: Client,
    table: TableRef,
    schema: SchemaAdapter,
}

impl RequestContext {
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn table(&self) -> &TableRef {
        &self.table
    }

    pub fn schema(&self) -> &SchemaAdapter {
        &self.schema
    }

    pub async fn list_labels(&self, bounds: LabelQueryBounds) -> Result<Vec<String>, AppError> {
        self.schema
            .list_labels(self.client(), self.table(), &bounds)
            .await
    }

    pub async fn list_label_values(
        &self,
        label: &str,
        bounds: LabelQueryBounds,
    ) -> Result<Vec<String>, AppError> {
        self.schema
            .list_label_values(self.client(), self.table(), label, &bounds)
            .await
    }
}

pub struct AppConfig {
    pub runtime: RuntimeConfig,
    pub max_metric_buckets: u32,
}

pub enum RuntimeConfig {
    Standalone(StandaloneConfig),
    Proxy,
}

pub struct StandaloneConfig {
    pub dsn: String,
    pub table: String,
    pub schema_type: SchemaType,
    pub schema_config: SchemaConfig,
}

#[derive(Debug, Deserialize)]
struct ProxySchemaPayload {
    table: String,
    schema_type: ProxySchemaType,
    timestamp_column: String,
    line_column: String,
    #[serde(default)]
    labels_column: Option<String>,
    #[serde(default)]
    label_columns: Vec<ProxyLabelColumn>,
}

impl ProxySchemaPayload {
    fn into_definition(self) -> Result<SchemaDefinition, AppError> {
        let timestamp_column = normalize_field(self.timestamp_column, "timestamp_column")?;
        let line_column = normalize_field(self.line_column, "line_column")?;
        match self.schema_type {
            ProxySchemaType::Loki => {
                let labels_column = self.labels_column.ok_or_else(|| {
                    AppError::BadRequest("loki schema header requires `labels_column` field".into())
                })?;
                let labels_column = normalize_field(labels_column, "labels_column")?;
                Ok(SchemaDefinition::Loki(LokiSchemaDefinition {
                    timestamp_column,
                    labels_column,
                    line_column,
                }))
            }
            ProxySchemaType::Flat => {
                if self.label_columns.is_empty() {
                    return Err(AppError::BadRequest(
                        "flat schema header requires at least one entry in `label_columns`".into(),
                    ));
                }
                let columns = self
                    .label_columns
                    .into_iter()
                    .map(|column| {
                        let name = normalize_field(column.name, "label_columns.name")?;
                        Ok(FlatLabelDefinition {
                            name,
                            numeric: column.numeric,
                        })
                    })
                    .collect::<Result<Vec<_>, AppError>>()?;
                Ok(SchemaDefinition::Flat(FlatSchemaDefinition {
                    timestamp_column,
                    line_column,
                    label_columns: columns,
                }))
            }
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "lowercase")]
enum ProxySchemaType {
    Loki,
    Flat,
}

#[derive(Debug, Deserialize)]
struct ProxyLabelColumn {
    name: String,
    #[serde(default)]
    numeric: bool,
}

fn normalize_field(value: String, field: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        Err(AppError::BadRequest(format!(
            "schema header `{field}` must be non-empty"
        )))
    } else {
        Ok(trimmed.to_string())
    }
}

async fn verify_connection(client: &Client) -> Result<(), AppError> {
    let conn = client.get_conn().await?;
    conn.set_session("timezone", "UTC")?;
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proxy_schema_payload_builds_loki_definition() {
        let payload = ProxySchemaPayload {
            table: "logs".into(),
            schema_type: ProxySchemaType::Loki,
            timestamp_column: "ts".into(),
            line_column: "line".into(),
            labels_column: Some("labels".into()),
            label_columns: Vec::new(),
        };
        match payload.into_definition().unwrap() {
            SchemaDefinition::Loki(def) => {
                assert_eq!(def.timestamp_column, "ts");
                assert_eq!(def.labels_column, "labels");
                assert_eq!(def.line_column, "line");
            }
            _ => panic!("expected loki definition"),
        }
    }

    #[test]
    fn proxy_schema_payload_requires_flat_label_columns() {
        let payload = ProxySchemaPayload {
            table: "logs".into(),
            schema_type: ProxySchemaType::Flat,
            timestamp_column: "ts".into(),
            line_column: "line".into(),
            labels_column: None,
            label_columns: Vec::new(),
        };
        assert!(payload.into_definition().is_err());
    }
}
