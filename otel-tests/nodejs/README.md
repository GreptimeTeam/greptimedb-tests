# OpenTelemetry Node.js Tests

Tests GreptimeDB OTLP protocol compatibility using OpenTelemetry JavaScript SDK.

## Requirements

- Node.js 18+

## Test Coverage

- **Metrics**: Counter, Gauge (UpDownCounter), Histogram
- **Traces**: Parent-child spans with attributes
- **Logs**: INFO/WARN/ERROR severity levels

## Run Tests

```bash
./run_tests.sh
```

## Dependencies

- @opentelemetry/api ^1.8.0
- OTLP Proto Exporters
- mysql2 (for verification)
