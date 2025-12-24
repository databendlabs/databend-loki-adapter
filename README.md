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
| `--max-metric-buckets` | `MAX_METRIC_BUCKETS` | `240`                 | Maximum bucket count per metric range query before clamping `step`. |

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

  ```sql
  CREATE INVERTED INDEX nginx_request_idx ON nginx_logs(request);
  CREATE INVERTED INDEX k8s_message_idx ON kubernetes_logs(message);
  ```

## Index guidance

Databend only requires manual management for inverted indexes. See the official docs for [inverted indexes](https://docs.databend.com/sql/sql-commands/ddl/inverted-index/) and the dedicated [`CREATE INVERTED INDEX`](https://docs.databend.com/sql/sql-commands/ddl/inverted-index/create-inverted-index) and [`REFRESH INVERTED INDEX`](https://docs.databend.com/sql/sql-commands/ddl/inverted-index/refresh-inverted-index) statements. Bloom filter style pruning for MAP/VARIANT columns is built in, so you do not need to create standalone bloom filter or minmax indexes. Remember to refresh a newly created inverted index so historical data becomes searchable, e.g.:

```sql
REFRESH INVERTED INDEX logs_line_idx ON logs;
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

All endpoints return Loki-compatible JSON responses and reuse the same error shape that Loki expects (`status:error`, `errorType`, `error`). Grafana can therefore talk to the adapter using the stock Loki data source without any proxy layers or plugins. Refer to the upstream [Loki HTTP API reference](https://grafana.com/docs/loki/latest/reference/loki-http-api/) for the detailed contract of each endpoint.

| Endpoint                                | Description                                                                                                                                                                                                         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GET /loki/api/v1/query`                | Instant query. Supports the same LogQL used by Grafana's Explore panel. An optional `time` parameter (nanoseconds) defaults to "now", and the adapter automatically looks back 5 minutes when computing SQL bounds. |
| `GET /loki/api/v1/query_range`          | Range query. Accepts `start`/`end` (default past hour), `since` (relative duration), `limit`, `interval`, `step`, and `direction`. Log queries stream raw lines (`interval` down-samples entries, `direction` controls scan order); metric queries return Loki matrix results and require a `step` value (the adapter may clamp it to keep bucket counts bounded, default cap 240 buckets).          |
| `GET /loki/api/v1/labels`               | Lists known label keys for the selected schema. Optional `start`/`end` parameters (nanoseconds) fence the search window; unspecified values default to the last 5 minutes, matching Grafana's Explore defaults.     |
| `GET /loki/api/v1/label/{label}/values` | Lists distinct values for a specific label key using the same optional `start`/`end` bounds as `/labels`. Works for both `loki` and `flat` schemas and automatically filters out empty strings.                     |
| `GET /loki/api/v1/index/stats`          | Returns approximate `streams`, `chunks`, `entries`, and `bytes` counters for a selector over a `[start, end]` window. `chunks` are estimated via unique stream keys because Databend does not store Loki chunks.    |
| `GET /loki/api/v1/tail`                 | WebSocket tail endpoint that streams live logs for a LogQL query; compatible with Grafana Explore and `logcli --tail`.                                                                                            |

`/query` and `/query_range` share the same LogQL parser and SQL builder. Instant queries fall back to `DEFAULT_LOOKBACK_NS` (5 minutes) when no explicit window is supplied, while range queries default to `[now - 1h, now]` and also honor Loki's `since` helper to derive `start`. `/loki/api/v1/query_range` log queries fully implement Loki's `direction` (`forward`/`backward`) and `interval` parameters: the adapter scans in the requested direction, emits entries in that order, and down-samples each stream so successive log lines are at least `interval` apart starting from `start`. `/labels` and `/label/{label}/values` delegate to schema-aware metadata lookups: the loki schema uses `map_keys`/`labels['key']` expressions, whereas the flat schema issues `SELECT DISTINCT` on the physical column and returns values in sorted order.

### Tail streaming

`/loki/api/v1/tail` upgrades to a WebSocket connection and sends frames that match Loki's native shape (`{"streams":[...],"dropped_entries":[]}`). Supported query parameters:

- `query`: required LogQL selector.
- `limit`: max number of entries per batch (default 100, still subject to the global `MAX_LIMIT`).
- `start`: initial cursor in nanoseconds, defaults to "one hour ago".
- `delay_for`: optional delay (seconds) that lets slow writers catch up; defaults to `0` and cannot exceed `5`.

The adapter keeps a cursor and duplicate fingerprints so new rows are streamed in chronological order without repeats. Grafana Explore, `logcli --tail`, or any WebSocket client can connect directly.

### Metric queries

The adapter currently supports a narrow LogQL metric surface area:

- Range functions: `count_over_time` and `rate`. The latter reports per-second values (`COUNT / window_seconds`).
- Optional outer aggregations: `sum`, `avg`, `min`, `max`, `count`, each with `by (...)`. `without` or other modifiers return `errorType:bad_data`.
- Pipelines: only `drop` stages are honored (labels are removed after aggregation to match Loki semantics). Any other stage still results in `errorType:bad_data`.
- `/loki/api/v1/query_range` metric calls must provide `step`. When the requested `(end - start) / step` would exceed the configured bucket cap (default 240, tweak via `--max-metric-buckets`), the adapter automatically increases the effective step to keep the SQL result size manageable; the adapter never fans out multiple queries or aggregates in memory.
- `/loki/api/v1/query` metric calls reuse the same expressions but evaluate them over `[time - range, time]`.

Both schema adapters (loki VARIANT labels and flat wide tables) translate the metric expression into one SQL statement that joins generated buckets with the raw rows via `generate_series`, so all aggregation happens inside Databend. Non-metric queries continue to stream raw logs.

## LogQL template functions

`line_format` and `label_format` now ship with a lightweight template engine that supports field interpolation (`{{ .message }}`) plus the full set of [Grafana Loki template string functions](https://grafana.com/docs/loki/latest/query/template_functions/). Supported functions are listed below:

| Function | Status | Notes |
| --- | --- | --- |
| `__line__`, `__timestamp__`, `now` | ✅ | Expose the raw line, the row timestamp, and the adapter host's current time. |
| `date`, `toDate`, `toDateInZone` | ✅ | Go-style datetime formatting and parsing (supports IANA zones). |
| `duration`, `duration_seconds` | ✅ | Parse Go duration strings into seconds (positive/negative). |
| `unixEpoch`, `unixEpochMillis`, `unixEpochNanos`, `unixToTime` | ✅ | Unix timestamp helpers. |
| `alignLeft`, `alignRight` | ✅ | Align field contents to a fixed width. |
| `b64enc`, `b64dec` | ✅ | Base64 encode/decode a field or literal. |
| `bytes` | ✅ | Parses human-readable byte strings (e.g. `2 KB` → `2000`). |
| `default` | ✅ | Provides a fallback when a field is empty or missing. |
| `fromJson` | ⚠️ | Validates and normalizes JSON strings (advanced loops like `range` remain unsupported). |
| `indent`, `nindent` | ✅ | Indent multi-line strings. |
| `lower`, `upper`, `title` | ✅ | Case conversion helpers. |
| `repeat` | ✅ | String repetition helper. |
| `printf` | ✅ | Supports `%s`, `%d`, `%f`, width/precision flags. |
| `replace`, `substr`, `trunc` | ✅ | String replacement, slicing, and truncation. |
| `trim`, `trimAll`, `trimPrefix`, `trimSuffix` | ✅ | String trimming helpers. |
| `urlencode`, `urldecode` | ✅ | URL encoding/decoding. |
| `contains`, `eq`, `hasPrefix`, `hasSuffix` | ✅ | Logical helpers for comparisons. |
| `int`, `float64` | ✅ | Cast values to integers/floats. |
| `add`, `addf`, `sub`, `subf`, `mul`, `mulf`, `div`, `divf`, `mod` | ✅ | Integer and floating-point arithmetic. |
| `ceil`, `floor`, `round` | ✅ | Floating-point rounding helpers. |
| `max`, `min`, `maxf`, `minf` | ✅ | Extremum helpers for integers/floats. |
| `count` | ✅ | Count regex matches (`{{ __line__ | count "foo" }}`). |
| `regexReplaceAll`, `regexReplaceAllLiteral` | ✅ | Regex replacement helpers (literal and capture-aware). |

`fromJson` currently only validates and re-serializes JSON strings because the template engine has no looping constructs yet. For advanced constructs (e.g., `range`), preprocess data upstream or continue to rely on Grafana/Loki-native features until control flow support arrives.

## Logging

By default the adapter configures `env_logger` with `databend_loki_adapter` at `info` level and every other module at `warn`. This keeps the startup flow visible without flooding the console with dependency logs. To override the levels, set `RUST_LOG` just like any other `env_logger` application, e.g.:

```bash
export RUST_LOG=databend_loki_adapter=debug,databend_driver=info
```

## Testing

Run the Rust test suite with `cargo nextest run`.
