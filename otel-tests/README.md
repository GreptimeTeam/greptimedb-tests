# OpenTelemetry Integration Tests (Node.js)

Tests GreptimeDB OTLP protocol compatibility using Node.js SDK.

> **Note:** Java, Go, and Python OTel tests have been merged into their respective
> language test directories (`java-tests`, `go-tests`, `python-tests`).

## SDK Version

| Language | SDK Version | Status |
|----------|-------------|--------|
| Node.js | 1.8.0 | ✅ |

## Test Signals

Tests three OpenTelemetry signals:

- **Metrics**: Counter, Gauge, Histogram
- **Traces**: Parent-child spans
- **Logs**: Multiple severity levels

## Architecture

```
otel-tests/
├── run_tests.sh      # Test runner
└── nodejs/           # Node.js SDK tests
```

## Run Tests

```bash
./run_tests.sh
```

Uses isolated database: `otel_tests_nodejs`

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_HOST` | 127.0.0.1 | GreptimeDB HTTP host |
| `HTTP_PORT` | 4000 | GreptimeDB HTTP port |
| `MYSQL_HOST` | 127.0.0.1 | MySQL protocol host |
| `MYSQL_PORT` | 4002 | MySQL protocol port |
| `GREPTIME_USERNAME` | (empty) | Authentication username |
| `GREPTIME_PASSWORD` | (empty) | Authentication password |

## Key Implementation Details

- OTLP HTTP protocol with protobuf encoding
- Traces require header: `x-greptime-pipeline-name: greptime_trace_v1`
- Metrics use Prometheus naming: `_total`, `_bytes`, `_milliseconds_count`
- SQL verification via MySQL protocol
