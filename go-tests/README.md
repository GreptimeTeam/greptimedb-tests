# Go Integration Tests for GreptimeDB

Integration tests for GreptimeDB using Go MySQL driver and gRPC ingester client.

## Prerequisites

- Go 1.24+
- GreptimeDB running on default ports (MySQL: 4002, gRPC: 4001, HTTP: 4000)

## Quick Start

```bash
# Start GreptimeDB first
cargo run --bin greptime -- standalone start

# Run all tests
./run_tests.sh
```

## Test Coverage

Tests validate both MySQL JDBC protocol and gRPC ingester protocol.

### MySQL Driver Tests (driver_test.go)

**TestCrudOperations:**
- CREATE TABLE with all data types (INTEGER, DOUBLE, FLOAT, STRING, TIMESTAMP, DATE, BINARY, BOOLEAN)
- INSERT using SQL literals and prepared statements
- SELECT and verify data
- UPDATE by overwriting with same primary key and timestamp
- DELETE (DROP TABLE)

**TestTimezoneInsertAndSelect:**
- INSERT from different timezones (UTC, Asia/Shanghai, America/New_York)
- Verify timestamps stored in UTC
- Verify timezone-aware WHERE clause interpretation

**TestBatchInsert:**
- Batch insert 10,000 rows using prepared statements
- Verify batch execution results
- Query and validate inserted rows

### gRPC Ingester Tests (ingester_test.go)

**TestBasicWriteAndQuery:**
- Write data using gRPC ingester
- Query via MySQL to verify data persistence
- Validate all data types

**TestTableOperations:**
- Create table via gRPC
- Insert data via gRPC
- Query and verify via MySQL

**TestBatchWrite:**
- Write 5 CPU metric series with multiple data points
- Verify data persistence via MySQL query

**TestTimestampPrecision:**
- Validate millisecond, microsecond, and nanosecond precision
- Verify precision preservation in storage and retrieval

**TestStreamWrite:**
- Stream 100 rows using gRPC streaming
- Verify all rows persisted correctly

## Supported Data Types

- INTEGER, DOUBLE, FLOAT
- STRING (Unicode support)
- TIMESTAMP (millisecond, microsecond, nanosecond precision)
- DATE
- BINARY
- BOOLEAN

## Environment Variables

- `DB_NAME` - Database name (default: `go_tests`)
- `GREPTIME_USERNAME` - Username for authentication (default: empty)
- `GREPTIME_PASSWORD` - Password for authentication (default: empty)
- `MYSQL_HOST` / `MYSQL_PORT` - MySQL connection (default: `127.0.0.1:4002`)
- `GRPC_HOST` / `GRPC_PORT` - gRPC connection (default: `127.0.0.1:4001`)
- `HTTP_PORT` - HTTP health check port (default: `4000`)

## Manual Execution

```bash
# Install dependencies
go mod download

# Run all tests
go test -v .

# Run specific test
go test -v -run TestCrudOperations

# Run with coverage
go test -v -cover .
```

## Code Formatting

```bash
# Check formatting
gofmt -l .

# Auto-format
gofmt -w .

# Run linter
golangci-lint run --timeout=5m

# Auto-fix lint issues
golangci-lint run --fix
```

## Dependencies

- `github.com/go-sql-driver/mysql` - MySQL driver
- `github.com/GreptimeTeam/greptimedb-ingester-go` - gRPC ingester client
- `github.com/stretchr/testify` - Testing utilities

## License

Apache License 2.0
