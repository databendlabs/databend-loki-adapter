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

use databend_driver::Client;
use log::{info, warn};

use crate::{
    databend::{
        LabelQueryBounds, SchemaAdapter, SchemaConfig, SchemaType, TableRef, load_schema,
        resolve_table_ref,
    },
    error::AppError,
    logql::{LogqlExpr, LogqlParser},
};

pub(crate) const DEFAULT_LIMIT: u64 = 500;
pub(crate) const MAX_LIMIT: u64 = 5_000;
pub(crate) const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

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
        let schema = load_schema(&client, &table, schema_type, &schema_config).await?;
        info!(
            "{} schema ready; using table {}",
            schema_type,
            table.fq_name()
        );
        Ok(Self {
            client,
            table,
            parser: LogqlParser,
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

    pub fn parse(&self, query: &str) -> Result<LogqlExpr, AppError> {
        self.parser.parse(query).map_err(AppError::from)
    }

    pub fn clamp_limit(&self, requested: Option<u64>) -> u64 {
        requested
            .and_then(|value| (value > 0).then_some(value))
            .map(|value| value.min(MAX_LIMIT))
            .unwrap_or(DEFAULT_LIMIT)
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
    pub dsn: String,
    pub table: String,
    pub schema_type: SchemaType,
    pub schema_config: SchemaConfig,
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
