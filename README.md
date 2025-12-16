# Databend Loki Adapter

Databend Loki Adapter exposes a minimal Loki-compatible HTTP API. It parses LogQL queries from Grafana, converts them to Databend SQL, runs the statements, and returns Loki-formatted JSON responses.

## Getting Started

```bash
databend-loki-adapter \
  --dsn "databend://user:pass@host:port/default" \
  --table logs \
  --schema-type loki
```

The adapter listens on `--bind` (default `0.0.0.0:8080`) and serves `/loki/api/v1/query` and `/loki/api/v1/query_range`.

## Logging

By default the adapter configures `env_logger` with `databend_loki_adapter` at `info` level and every other module at `warn`. This keeps the startup flow visible without flooding the console with dependency logs. To override the levels, set `RUST_LOG` just like any other `env_logger` application, e.g.:

```bash
RUST_LOG=databend_loki_adapter=debug,databend_driver=info databend-loki-adapter \
  --dsn "databend://user:pass@host:port/default" \
  --table logs
```

## Configuration

| Flag                 | Env                | Default                 | Description                                                       |
| -------------------- | ------------------ | ----------------------- | ----------------------------------------------------------------- |
| `--dsn`              | `DATABEND_DSN`     | _required_              | Databend DSN with credentials and optional default database.      |
| `--table`            | `LOGS_TABLE`       | `logs`                  | Target table. Use `db.table` or rely on the DSN default database. |
| `--bind`             | `BIND_ADDR`        | `0.0.0.0:8080`          | HTTP bind address.                                                |
| `--schema-type`      | `SCHEMA_TYPE`      | `loki`                  | `loki` (labels as VARIANT) or `flat` (wide table).                |
| `--timestamp-column` | `TIMESTAMP_COLUMN` | auto-detect             | Override the timestamp column name.                               |
| `--line-column`      | `LINE_COLUMN`      | auto-detect             | Override the log line column name.                                |
| `--labels-column`    | `LABELS_COLUMN`    | auto-detect (loki only) | Override the labels column name.                                  |

## Schema Support

The adapter inspects the table via `system.columns` during startup. Pick one of the schemas below and adjust names if needed using CLI overrides.

### Loki schema

Recommended layout:

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
- `CREATE INVERTED INDEX`: defined separately as required by Databend’s inverted-index syntax.

Extra optimizations (optional but recommended):

```sql
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_stream(stream_hash);
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_labels_app (labels['app']);
ALTER TABLE logs ADD BLOOM FILTER INDEX idx_labels_host (labels['host']);
ALTER TABLE logs ADD MINMAX INDEX logs_timestamp_idx(timestamp);
```

### Flat schema

Each column becomes a label except for the timestamp and line columns. The adapter automatically maps remaining columns into LogQL labels.

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
- Every other column automatically becomes a LogQL label. These columns hold the actual metadata you want to query (`client`, `host`, `status`, `pod_name`, `pod_namespace`, `cluster_name`, etc.).
- Add bloom filter or inverted indexes using additional statements. Examples:

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
SELECT column_name, data_type
FROM system.columns
WHERE database = '<database>'
  AND table = '<table>'
ORDER BY ordinal_position;
```

Ensure the table matches one of the schemas above (including indexes) so Grafana can issue LogQL queries directly against Databend through this adapter.
