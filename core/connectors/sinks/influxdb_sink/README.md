# InfluxDB Sink Connector

The InfluxDB sink connector consumes messages from Iggy topics and stores them in InfluxDB 2.x using line protocol (`/api/v2/write`).

## Features

- Batch writes to InfluxDB buckets
- Retry logic for transient HTTP failures
- Optional metadata projection into tags/fields
- Multiple payload encodings (`json`, `text`, `base64`)
- Configurable timestamp precision (`ns`, `us`, `ms`, `s`)

## Configuration

```toml
[plugin_config]
url = "http://localhost:8086"
org = "iggy"
bucket = "events"
token = "replace-with-token"
measurement = "iggy_messages"
precision = "ms"
batch_size = 500
include_metadata = true
include_checksum = true
include_origin_timestamp = true
include_stream_tag = true
include_topic_tag = true
include_partition_tag = true
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
| `bucket` | string | required | Target bucket |
| `token` | string | required | InfluxDB API token |
| `measurement` | string | `iggy_messages` | Measurement name |
| `precision` | string | `ms` | Timestamp precision: `ns`, `us`, `ms`, `s` |
| `batch_size` | u32 | `500` | Messages per write batch |
| `include_metadata` | bool | `true` | Include stream/topic/partition metadata |
| `include_checksum` | bool | `true` | Include message checksum field |
| `include_origin_timestamp` | bool | `true` | Include origin timestamp field |
| `include_stream_tag` | bool | `true` | Store stream name as tag |
| `include_topic_tag` | bool | `true` | Store topic name as tag |
| `include_partition_tag` | bool | `true` | Store partition id as tag |
| `payload_format` | string | `json` | Payload storage mode: `json`, `text`, `base64` |
| `verbose_logging` | bool | `false` | Log at info level instead of debug |
| `max_retries` | u32 | `3` | Max retry attempts for transient failures |
| `retry_delay` | string | `1s` | Base retry delay |
| `timeout` | string | `10s` | HTTP timeout |

## Notes

- `payload_format = "json"` requires valid JSON payload bytes.
- `payload_format = "text"` requires valid UTF-8 payload bytes.
- `payload_format = "base64"` always works and preserves raw bytes.
