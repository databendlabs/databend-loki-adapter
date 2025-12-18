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

use databend_driver::Row;
use serde::Serialize;

use crate::{databend::SchemaAdapter, error::AppError, logql::Pipeline};

pub(crate) fn rows_to_streams(
    schema: &SchemaAdapter,
    rows: Vec<Row>,
    pipeline: &Pipeline,
) -> Result<Vec<LokiStream>, AppError> {
    let mut buckets: BTreeMap<String, StreamBucket> = BTreeMap::new();
    for row in rows {
        let entry = schema.parse_row(&row)?;
        let processed = pipeline.process(&entry.labels, &entry.line);
        let key = serde_json::to_string(&processed.labels)
            .map_err(|err| AppError::Internal(format!("failed to encode labels: {err}")))?;
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| StreamBucket::new(processed.labels.clone()));
        bucket.values.push((entry.timestamp_ns, processed.line));
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
pub(crate) struct LokiResponse {
    status: &'static str,
    data: LokiData,
}

impl LokiResponse {
    pub(crate) fn success(streams: Vec<LokiStream>) -> Self {
        Self::streams(streams)
    }

    pub(crate) fn streams(streams: Vec<LokiStream>) -> Self {
        Self {
            status: "success",
            data: LokiData {
                result: LokiResult::Streams { result: streams },
            },
        }
    }

    pub(crate) fn vector_constant(timestamp_ns: i64, value: f64) -> Self {
        Self::vector(vec![LokiVectorSample::constant(timestamp_ns, value)])
    }

    pub(crate) fn vector(samples: Vec<LokiVectorSample>) -> Self {
        Self {
            status: "success",
            data: LokiData {
                result: LokiResult::Vector { result: samples },
            },
        }
    }
}

#[derive(Serialize)]
struct LokiData {
    #[serde(flatten)]
    result: LokiResult,
}

#[derive(Serialize)]
#[serde(tag = "resultType")]
enum LokiResult {
    #[serde(rename = "streams")]
    Streams { result: Vec<LokiStream> },
    #[serde(rename = "vector")]
    Vector { result: Vec<LokiVectorSample> },
}

#[derive(Serialize)]
pub(crate) struct LokiStream {
    stream: BTreeMap<String, String>,
    values: Vec<[String; 2]>,
}

#[derive(Serialize)]
pub(crate) struct LokiVectorSample {
    metric: BTreeMap<String, String>,
    value: VectorValue,
}

impl LokiVectorSample {
    pub(crate) fn constant(timestamp_ns: i64, value: f64) -> Self {
        Self {
            metric: BTreeMap::new(),
            value: VectorValue::new(timestamp_ns, value),
        }
    }
}

struct VectorValue {
    timestamp: f64,
    value: String,
}

impl VectorValue {
    fn new(timestamp_ns: i64, value: f64) -> Self {
        Self {
            timestamp: timestamp_ns as f64 / 1_000_000_000_f64,
            value: format_value(value),
        }
    }
}

impl Serialize for VectorValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&self.timestamp)?;
        seq.serialize_element(&self.value)?;
        seq.end()
    }
}

fn format_value(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{:.0}", value)
    } else {
        value.to_string()
    }
}

#[derive(Serialize)]
pub(crate) struct LabelsResponse {
    status: &'static str,
    data: Vec<String>,
}

impl LabelsResponse {
    pub(crate) fn success(labels: Vec<String>) -> Self {
        Self {
            status: "success",
            data: labels,
        }
    }
}
