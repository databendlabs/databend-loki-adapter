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

use crate::{
    databend::{MetricMatrixSample, MetricSample, SchemaAdapter},
    error::AppError,
    logql::Pipeline,
};

pub(crate) fn rows_to_streams(
    schema: &SchemaAdapter,
    rows: Vec<Row>,
    pipeline: &Pipeline,
) -> Result<Vec<LokiStream>, AppError> {
    let entries = collect_processed_entries(schema, rows, pipeline)?;
    entries_to_streams(entries)
}

pub(crate) fn collect_processed_entries(
    schema: &SchemaAdapter,
    rows: Vec<Row>,
    pipeline: &Pipeline,
) -> Result<Vec<ProcessedEntry>, AppError> {
    let mut entries = Vec::with_capacity(rows.len());
    for row in rows {
        let entry = schema.parse_row(&row)?;
        let processed = pipeline.process(&entry.labels, &entry.line);
        entries.push(ProcessedEntry {
            timestamp_ns: entry.timestamp_ns,
            labels: processed.labels,
            line: processed.line,
        });
    }
    Ok(entries)
}

pub(crate) fn entries_to_streams(
    entries: Vec<ProcessedEntry>,
) -> Result<Vec<LokiStream>, AppError> {
    let mut buckets: BTreeMap<String, StreamBucket> = BTreeMap::new();
    for entry in entries {
        let key = serde_json::to_string(&entry.labels)
            .map_err(|err| AppError::Internal(format!("failed to encode labels: {err}")))?;
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| StreamBucket::new(entry.labels.clone()));
        bucket.values.push((entry.timestamp_ns, entry.line));
    }
    let mut result = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_values() {
        result.push(bucket.into_stream());
    }
    Ok(result)
}

#[derive(Clone)]
pub(crate) struct ProcessedEntry {
    pub timestamp_ns: i128,
    pub labels: BTreeMap<String, String>,
    pub line: String,
}

pub(crate) fn metric_vector(timestamp_ns: i64, samples: Vec<MetricSample>) -> LokiResponse {
    let mut vectors = Vec::with_capacity(samples.len());
    for sample in samples {
        vectors.push(LokiVectorSample::new(
            sample.labels,
            timestamp_ns,
            sample.value,
        ));
    }
    LokiResponse::vector(vectors)
}

pub(crate) fn metric_matrix(samples: Vec<MetricMatrixSample>) -> LokiResponse {
    let mut buckets: BTreeMap<String, MatrixBucket> = BTreeMap::new();
    for sample in samples {
        let key = serde_json::to_string(&sample.labels).unwrap_or_else(|_| "{}".to_string());
        let bucket = buckets
            .entry(key)
            .or_insert_with(|| MatrixBucket::new(sample.labels.clone()));
        bucket.values.push((sample.timestamp_ns, sample.value));
    }
    let mut series = Vec::with_capacity(buckets.len());
    for bucket in buckets.into_values() {
        series.push(bucket.into_series());
    }
    LokiResponse::matrix(series)
}

pub(crate) fn tail_chunk(streams: Vec<LokiStream>) -> TailChunk {
    TailChunk {
        streams,
        dropped_entries: Vec::new(),
    }
}

#[derive(Serialize)]
pub(crate) struct TailChunk {
    pub(crate) streams: Vec<LokiStream>,
    #[serde(rename = "dropped_entries")]
    pub(crate) dropped_entries: Vec<DroppedEntry>,
}

#[derive(Serialize)]
pub(crate) struct DroppedEntry {
    pub(crate) labels: BTreeMap<String, String>,
    pub(crate) timestamp: String,
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

struct MatrixBucket {
    labels: BTreeMap<String, String>,
    values: Vec<(i64, f64)>,
}

impl MatrixBucket {
    fn new(labels: BTreeMap<String, String>) -> Self {
        Self {
            labels,
            values: Vec::new(),
        }
    }

    fn into_series(mut self) -> LokiMatrixSeries {
        self.values.sort_by_key(|(ts, _)| *ts);
        let values = self
            .values
            .into_iter()
            .map(|(ts, value)| VectorValue::new(ts, value))
            .collect();
        LokiMatrixSeries {
            metric: self.labels,
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

    pub(crate) fn matrix(series: Vec<LokiMatrixSeries>) -> Self {
        Self {
            status: "success",
            data: LokiData {
                result: LokiResult::Matrix { result: series },
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
    #[serde(rename = "matrix")]
    Matrix { result: Vec<LokiMatrixSeries> },
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

    pub(crate) fn new(metric: BTreeMap<String, String>, timestamp_ns: i64, value: f64) -> Self {
        Self {
            metric,
            value: VectorValue::new(timestamp_ns, value),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct LokiMatrixSeries {
    metric: BTreeMap<String, String>,
    values: Vec<VectorValue>,
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
