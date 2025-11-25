# Python Integration Tests for GreptimeDB

Integration tests for GreptimeDB using MySQL/PostgreSQL drivers and OpenTelemetry SDK.

## Prerequisites

- Python 3.8+
- GreptimeDB running on default ports (MySQL: 4002, PostgreSQL: 4003, HTTP: 4000)

## Quick Start

```bash
# Start GreptimeDB first
cargo run --bin greptime -- standalone start

# Run all tests
./run_tests.sh
```

## Test Coverage

Tests are executed against both MySQL and PostgreSQL drivers using pytest parameterization.

### test_crud_operations
- CREATE TABLE with all data types (INTEGER, DOUBLE, FLOAT, STRING, TIMESTAMP, DATE, BINARY, BOOLEAN)
- INSERT using SQL literals and parameterized queries
- SELECT and verify data
- UPDATE by overwriting with same primary key and timestamp
- DELETE (DROP TABLE)

### test_timezone_insert_and_select
- INSERT from different timezones (UTC, Asia/Shanghai, America/New_York)
- Verify timestamps stored in UTC
- Verify timezone-aware WHERE clause interpretation
- **Note**: Only runs for MySQL driver

### test_batch_insert
- Batch insert 5 rows using `executemany()`
- Verify batch execution results
- Query and validate all inserted rows

### OpenTelemetry Tests (test_otel.py)

**test_counter/gauge/histogram**: Export metrics via OTLP HTTP

**test_traces**: Export spans, verify in `opentelemetry_traces` table

**test_logs**: Export logs, verify in `opentelemetry_logs` table

**test_all_signals_together**: Combined test of all three signals

## Environment Variables

- `DB_NAME` - Database name (default: `public`)
- `GREPTIME_USERNAME` - Username for authentication (default: empty)
- `GREPTIME_PASSWORD` - Password for authentication (default: empty)
- `MYSQL_HOST` / `MYSQL_PORT` - MySQL connection (default: `127.0.0.1:4002`)
- `POSTGRES_HOST` / `POSTGRES_PORT` - PostgreSQL connection (default: `127.0.0.1:4003`)

## Manual Execution

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run all tests
pytest tests/ -v

# Run specific test
pytest tests/test_greptimedb_driver.py::test_crud_operations -v

# Run with specific driver
pytest tests/test_greptimedb_driver.py::test_crud_operations[mysql] -v
```

## Dependencies

- `pytest>=7.4.0` - Testing framework
- `mysql-connector-python>=8.0.33` - MySQL driver
- `psycopg2-binary>=2.9.9` - PostgreSQL driver
- `opentelemetry-sdk>=1.24.0` - OpenTelemetry SDK
- `opentelemetry-exporter-otlp-proto-http>=1.24.0` - OTLP HTTP exporter

## License

Copyright 2023 Greptime Team. Licensed under Apache License 2.0.
