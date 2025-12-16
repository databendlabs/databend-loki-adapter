# Databend Loki Adapter

Databend Loki Adapter exposes a minimal Loki-compatible HTTP API. It parses LogQL queries from Grafana, converts them to Databend SQL, runs the statements, and returns Loki-formatted JSON responses.

## Getting Started

```bash
export DATABEND_DSN="databend://user:pass@host:port/default"
databend-loki-adapter --table nginx_logs --schema-type flat
```

The adapter listens on `--bind` (default `0.0.0.0:3100`) and exposes a minimal subset of the Loki HTTP surface area.

## Configuration

| Flag                 | Env                | Default                 | Description                                                       |
| -------------------- | ------------------ | ----------------------- | ----------------------------------------------------------------- |
| `--dsn`              | `DATABEND_DSN`     | _required_              | Databend DSN with credentials and optional default database.      |
| `--table`            | `LOGS_TABLE`       | `logs`                  | Target table. Use `db.table` or rely on the DSN default database. |
| `--bind`             | `BIND_ADDR`        | `0.0.0.0:3100`          | HTTP bind address.                                                |
| `--schema-type`      | `SCHEMA_TYPE`      | `loki`                  | `loki` (labels as VARIANT) or `flat` (wide table).                |
| `--timestamp-column` | `TIMESTAMP_COLUMN` | auto-detect             | Override the timestamp column name.                               |
| `--line-column`      | `LINE_COLUMN`      | auto-detect             | Override the log line column name.                                |
| `--labels-column`    | `LABELS_COLUMN`    | auto-detect (loki only) | Override the labels column name.                                  |

## Schema Support

The adapter inspects the table via `system.columns` during startup and then maps the physical layout into Loki's timestamp/line/label model. Two schema styles are supported. The SQL snippets below are reference starting points rather than strict requirements -- feel free to rename columns, tweak indexes, or add computed fields as long as the final table exposes the required timestamp/line/label information. Use the CLI overrides (`--timestamp-column`, `--line-column`, `--labels-column`) if your column names differ.

### Loki schema

Use this schema when you already store labels as a serialized structure (VARIANT/MAP) alongside the log body. The adapter expects a timestamp column, a VARIANT/MAP column containing a JSON object of labels, and a string column for the log line or payload. Additional helper columns (hashes, shards, etc.) are ignored.

Recommended layout (adjust column names, clustering keys, and indexes to match your workload):

```sql
CREATE TABLE logs (
  `timestamp` TIMESTAMP NOT NULL,
  `labels` VARIANT NOT NULL,
  `line` STRING NOT NULL,
  `stream_hash` UInt64 NOT NULL AS (city64withseed(labels, 0)) STORED
) CLUSTER BY (to_start_of_hour(timestamp), stream_hash);

CREATE INVERTED INDEX logs_line_idx ON logs(line);
```

- `timestamp`: log event timestamp.
- `labels`: VARIANT/MAP storing serialized Loki labels.
- `line`: raw log line.
- `stream_hash`: computed hash of the label set; useful for clustering or fast equality filters on a stream.
- `CREATE INVERTED INDEX`: defined separately as required by Databend's inverted-index syntax.

Extra optimizations (optional but recommended, mix and match as needed):

```sql
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_stream(stream_hash);
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_labels_app (labels['app']);
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_labels_host (labels['host']);
ALTER TABLE logs ADD MINMAX INDEX logs_timestamp_idx(timestamp);
```

### Flat schema

Use this schema when logs arrive in a wide table where each attribute is already a separate column. The adapter chooses the timestamp column, picks one string column for the log line (either auto-detected or provided via `--line-column`), and treats every other column as a label. The examples below illustrate common shapes; substitute your own column names and indexes.

```sql
CREATE TABLE nginx_logs (
  `agent` STRING,
  `client` STRING,
  `host` STRING,
  `path` STRING,
  `protocol` STRING,
  `refer` STRING,
  `request` STRING,
  `size` INT,
  `status` INT,
  `timestamp` TIMESTAMP NOT NULL
) CLUSTER BY (to_start_of_hour(timestamp), host, status);
```

```sql
CREATE TABLE kubernetes_logs (
  `message` STRING,
  `log_time` TIMESTAMP NOT NULL,
  `pod_name` STRING,
  `pod_namespace` STRING,
  `cluster_name` STRING
) CLUSTER BY (to_start_of_hour(log_time), cluster_name, pod_namespace, pod_name);

CREATE INVERTED INDEX k8s_message_idx ON kubernetes_logs(message);
```

Guidelines:

- If the table does not have an obvious log-line column, pass `--line-column` (e.g., `--line-column request` for `nginx_logs`, or `--line-column message` for `kubernetes_logs`). The column may be nullable; the adapter will emit empty strings when needed.
- Every other column automatically becomes a LogQL label. These columns hold the actual metadata you want to query (`client`, `host`, `status`, `pod_name`, `pod_namespace`, `cluster_name`, etc.). Use Databend's SQL to rename or cast fields if you need canonical label names.
- Add bloom filter or inverted indexes as appropriate for your query workload. For example:

  ```sql
  CREATE INVERTED INDEX nginx_request_idx ON nginx_logs(request);
  CREATE INVERTED INDEX k8s_message_idx ON kubernetes_logs(message);
  ALTER TABLE nginx_logs ADD BLOOM FILTER INDEX nginx_host_idx(host);
  ALTER TABLE nginx_logs ADD BLOOM FILTER INDEX nginx_status_idx(status);
  ALTER TABLE kubernetes_logs ADD BLOOM FILTER INDEX k8s_pod_idx(pod_name);
  ALTER TABLE kubernetes_logs ADD MINMAX INDEX k8s_time_idx(log_time);
  ```

## Metadata lookup

The adapter validates table shape with:

```sql
SELECT name, data_type
FROM system.columns
WHERE database = '<database>'
  AND table = '<table>'
ORDER BY name;
```

Ensure the table matches one of the schemas above (including indexes) so Grafana can issue LogQL queries directly against Databend through this adapter.

## HTTP API

All endpoints return Loki-compatible JSON responses and reuse the same error shape that Loki expects (`status:error`, `errorType`, `error`). Grafana can therefore talk to the adapter using the stock Loki data source without any proxy layers or plugins.

| Endpoint                                | Description                                                                                                                                                                                                         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /loki/api/v1/query`                | Instant query. Supports the same LogQL used by Grafana's Explore panel. An optional `time` parameter (nanoseconds) defaults to "now", and the adapter automatically looks back 5 minutes when computing SQL bounds. |
| `GET /loki/api/v1/query_range`          | Range query. Requires `start`/`end` nanoseconds and accepts `limit`/`step`. The `step` parameter is parsed but ignored because the adapter streams raw log lines rather than resampled metrics.                     |
| `GET /loki/api/v1/labels`               | Lists known label keys for the selected schema. Optional `start`/`end` parameters (nanoseconds) fence the search window; unspecified values default to the last 5 minutes, matching Grafana's Explore defaults.     |
| `GET /loki/api/v1/label/{label}/values` | Lists distinct values for a specific label key using the same optional `start`/`end` bounds as `/labels`. Works for both `loki` and `flat` schemas and automatically filters out empty strings.                     |

`/query` and `/query_range` share the same LogQL parser and SQL builder. Instant queries fall back to `DEFAULT_LOOKBACK_NS` (5 minutes) when no explicit window is supplied, while range queries honor the caller's `start`/`end` bounds. `/labels` and `/label/{label}/values` delegate to schema-aware metadata lookups: the loki schema uses `map_keys`/`labels['key']` expressions, whereas the flat schema issues `SELECT DISTINCT` on the physical column and returns values in sorted order.

## Logging

By default the adapter configures `env_logger` with `databend_loki_adapter` at `info` level and every other module at `warn`. This keeps the startup flow visible without flooding the console with dependency logs. To override the levels, set `RUST_LOG` just like any other `env_logger` application, e.g.:

```bash
export RUST_LOG=databend_loki_adapter=debug,databend_driver=info
```

## Testing

Run the Rust test suite with `cargo nextest run`.
