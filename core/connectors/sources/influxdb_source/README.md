# InfluxDB Source Connector

The InfluxDB source connector polls Flux query results from InfluxDB 2.x (`/api/v2/query`) and produces messages to Iggy topics.

## Features

- Poll-based ingestion from Flux queries
- Persistent cursor tracking (`cursor_field`, default `_time`)
- Retry logic for transient query failures
- Optional payload extraction from a single column
- Supports output schemas: `json`, `text`, `raw`

## Configuration

```toml
[plugin_config]
url = "http://localhost:8086"
org = "iggy"
token = "replace-with-token"
query = '''
from(bucket: "events")
  |> range(start: -1h)
  |> filter(fn: (r) => r._measurement == "iggy_messages")
  |> filter(fn: (r) => r._time > time(v: "$cursor"))
  |> sort(columns: ["_time"])
  |> limit(n: $limit)
'''
poll_interval = "5s"
batch_size = 500
cursor_field = "_time"
initial_offset = "1970-01-01T00:00:00Z"
include_metadata = true
payload_format = "json"
max_retries = 3
retry_delay = "1s"
timeout = "10s"
```

## Configuration Options

| Option | Type | Default | Description |
| ------ | ---- | ------- | ----------- |
| `url` | string | required | InfluxDB base URL |
| `org` | string | required | InfluxDB organization |
| `token` | string | required | InfluxDB API token |
| `query` | string | required | Flux query to execute |
| `poll_interval` | string | `5s` | Poll interval |
| `batch_size` | u32 | `500` | Value injected into `$limit` placeholder |
| `cursor_field` | string | `_time` | Field used for stateful cursor |
| `initial_offset` | string | `1970-01-01T00:00:00Z` | Initial cursor value |
| `payload_column` | string | none | Column extracted directly as payload |
| `payload_format` | string | `json` | Payload mode for `payload_column`: `json`, `text`, `raw` |
| `include_metadata` | bool | `true` | Include full row details in JSON wrapper |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Max retry attempts for transient failures |
| `retry_delay` | string | `1s` | Base retry delay |
| `timeout` | string | `10s` | HTTP timeout |

## Query Placeholders

You can use placeholders in the Flux query:

- `$cursor` → last processed cursor value (from connector state)
- `$limit` → configured `batch_size`

## Output Modes

- Without `payload_column`, connector emits JSON wrapper payloads with parsed row values (`schema = json`).
- With `payload_column`:
  - `payload_format = "json"` → `schema = json`
  - `payload_format = "text"` → `schema = text`
  - `payload_format = "raw"` → `schema = raw` (base64-decoded when possible, otherwise raw UTF-8 bytes)
