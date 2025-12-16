use std::net::SocketAddr;

use app::{AppConfig, AppState, router};
use clap::{Parser, ValueEnum};
use databend::{SchemaConfig, SchemaType};
use error::AppError;

mod app;
mod databend;
mod error;
mod logql;

#[derive(Debug, Parser)]
#[command(author, version, about, disable_help_subcommand = true)]
struct Cli {
    #[arg(long = "dsn", env = "DATABEND_DSN")]
    dsn: String,
    #[arg(long, env = "LOGS_TABLE", default_value = "logs")]
    table: String,
    #[arg(long = "bind", env = "BIND_ADDR", default_value = "0.0.0.0:8080")]
    bind: SocketAddr,
    #[arg(
        long = "schema-type",
        env = "SCHEMA_TYPE",
        value_enum,
        default_value = "loki"
    )]
    schema_type: SchemaTypeArg,
    #[arg(long = "timestamp-column", env = "TIMESTAMP_COLUMN")]
    timestamp_column: Option<String>,
    #[arg(long = "line-column", env = "LINE_COLUMN")]
    line_column: Option<String>,
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

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    if let Err(err) = run(cli).await {
        eprintln!("server failed: {err}");
    }
}

async fn run(cli: Cli) -> Result<(), AppError> {
    let state = AppState::bootstrap(AppConfig {
        dsn: cli.dsn.clone(),
        table: cli.table.clone(),
        schema_type: cli.schema_type.into(),
        schema_config: SchemaConfig {
            timestamp_column: cli.timestamp_column.clone(),
            line_column: cli.line_column.clone(),
            labels_column: cli.labels_column.clone(),
        },
    })
    .await?;
    let app = router(state);

    let listener = tokio::net::TcpListener::bind(cli.bind)
        .await
        .map_err(|err| AppError::Internal(format!("failed to bind listener: {err}")))?;
    println!("databend-loki-adapter listening on {}", cli.bind);
    axum::serve(listener, app)
        .await
        .map_err(|err| AppError::Internal(format!("server error: {err}")))?;
    Ok(())
}
