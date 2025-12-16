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
