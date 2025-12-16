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

use std::{fmt::Display, net::SocketAddr};

use app::{AppConfig, AppState, router};
use clap::{Parser, ValueEnum};
use databend::{SchemaConfig, SchemaType};
use error::AppError;
use log::{LevelFilter, info};

mod app;
mod databend;
mod error;
mod logql;

#[derive(Debug, Parser)]
#[command(author, version, about, disable_help_subcommand = true)]
struct Args {
    /// Databend DSN string, e.g. databend://user:pass@host:port/db
    #[arg(long = "dsn", env = "DATABEND_DSN")]
    dsn: String,
    /// Target table to query; can be plain name or db.table
    #[arg(long, env = "LOGS_TABLE", default_value = "logs")]
    table: String,
    /// HTTP bind address for the adapter server
    #[arg(long = "bind", env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
    /// Schema interpretation for the table (loki for labels as VARIANT or flat for wide table)
    #[arg(
        long = "schema-type",
        env = "SCHEMA_TYPE",
        value_enum,
        default_value = "loki"
    )]
    schema_type: SchemaTypeArg,
    /// Override the column used as the log timestamp
    #[arg(long = "timestamp-column", env = "TIMESTAMP_COLUMN")]
    timestamp_column: Option<String>,
    /// Override the column used as the log line/payload
    #[arg(long = "line-column", env = "LINE_COLUMN")]
    line_column: Option<String>,
    /// Override the column storing labels (loki schema only)
    #[arg(long = "labels-column", env = "LABELS_COLUMN")]
    labels_column: Option<String>,
}

#[derive(Copy, Clone, Debug, ValueEnum)]
enum SchemaTypeArg {
    Loki,
    Flat,
}

impl From<SchemaTypeArg> for SchemaType {
    fn from(value: SchemaTypeArg) -> Self {
        match value {
            SchemaTypeArg::Loki => SchemaType::Loki,
            SchemaTypeArg::Flat => SchemaType::Flat,
        }
    }
}

impl Display for SchemaTypeArg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaTypeArg::Loki => write!(f, "loki"),
            SchemaTypeArg::Flat => write!(f, "flat"),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    init_logging();
    let args = Args::parse();
    info!(
        "starting databend-loki-adapter (table={}, schema_type={}, bind={})",
        args.table, args.schema_type, args.bind
    );
    let config = AppConfig {
        dsn: args.dsn.clone(),
        table: args.table.clone(),
        schema_type: args.schema_type.into(),
        schema_config: SchemaConfig {
            timestamp_column: args.timestamp_column.clone(),
            line_column: args.line_column.clone(),
            labels_column: args.labels_column.clone(),
        },
    };
    info!("bootstrapping application state");
    let state = AppState::bootstrap(config).await?;
    info!("router initialized, preparing HTTP server");
    let app = router(state);

    info!("binding TCP listener on {}", args.bind);
    let listener = tokio::net::TcpListener::bind(args.bind)
        .await
        .map_err(|err| AppError::Internal(format!("failed to bind listener: {err}")))?;
    info!("databend-loki-adapter listening on {}", args.bind);
    info!("starting HTTP server");
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Internal(format!("server error: {err}")))?;
    Ok(())
}

fn init_logging() {
    if std::env::var_os("RUST_LOG").is_some() {
        env_logger::Builder::from_default_env().init();
    } else {
        env_logger::Builder::new()
            .filter_level(LevelFilter::Warn)
            .filter_module("databend_loki_adapter", LevelFilter::Info)
            .init();
    }
}
