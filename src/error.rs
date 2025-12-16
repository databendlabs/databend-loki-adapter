use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use databend_driver::Error as DatabendError;
use serde::Serialize;
use thiserror::Error;

use crate::logql::LogqlError;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("{0}")]
    Config(String),
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Databend(#[from] DatabendError),
    #[error(transparent)]
    Logql(#[from] LogqlError),
    #[error("{0}")]
    Internal(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let message = self.to_string();
        let (status, error_type) = match self {
            Self::Config(_) | Self::BadRequest(_) | Self::Logql(_) => {
                (StatusCode::BAD_REQUEST, "bad_data")
            }
            Self::Databend(_) => (StatusCode::BAD_GATEWAY, "databend_error"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = LokiErrorResponse {
            status: "error",
            error_type,
            error: message,
        };
        (status, Json(body)).into_response()
    }
}

#[derive(Serialize)]
struct LokiErrorResponse<'a> {
    status: &'a str,
    #[serde(rename = "errorType")]
    error_type: &'a str,
    error: String,
}
