# GreptimeDB Integration Tests

Multi-language integration tests for GreptimeDB, validating compatibility with different client drivers and protocols.

## Project Structure

```
greptimedb-tests/
├── run_tests.sh           # Master test runner
├── create_database.py     # Database creation (mysql-connector-python)
├── java-tests/            # Java tests (MySQL JDBC + PostgreSQL JDBC + gRPC Ingester + OTLP)
├── python-tests/          # Python tests (mysql-connector + psycopg2 + OTLP)
├── go-tests/              # Go tests (MySQL + PostgreSQL + gRPC Ingester + OTLP)
├── otel-tests/            # OpenTelemetry OTLP protocol tests (Node.js)
└── .github/workflows/     # CI workflows
```

## Quick Start

### Prerequisites

- GreptimeDB running on default ports (MySQL: 4002, PostgreSQL: 4003, gRPC: 4001)
- Python 3.8+ with `mysql-connector-python` (for database creation)
- Java 11+ and Maven 3.6+ (for Java tests)
- Go 1.24+ (for Go tests)
- Node.js 18+ (for OpenTelemetry tests)

```bash
pip install mysql-connector-python
```

### Start GreptimeDB

**Without authentication:**
```bash
cargo run --bin greptime -- standalone start
```

**With authentication (matches CI):**
```bash
cargo run --bin greptime -- standalone start \
  --user-provider=static_user_provider:cmd:greptime_user=greptime_pwd

export GREPTIME_USERNAME=greptime_user
export GREPTIME_PASSWORD=greptime_pwd
```

### Run Tests

**All test suites:**
```bash
./run_tests.sh
```

**Individual suite:**
```bash
cd java-tests && ./run_tests.sh
cd python-tests && ./run_tests.sh
cd go-tests && ./run_tests.sh
cd otel-tests && ./run_tests.sh
```

## Test Suites

### Java Tests (`java-tests/`)
- **Protocols**: MySQL JDBC, PostgreSQL JDBC, gRPC Ingester, OTLP
- **Tests**: CRUD operations, timezone handling, batch inserts, gRPC ingester operations, OpenTelemetry signals
- **Coverage**: All GreptimeDB data types
- **Docs**: [java-tests/README.md](java-tests/README.md)

### Python Tests (`python-tests/`)
- **Drivers**: mysql-connector-python, psycopg2
- **Protocols**: MySQL, PostgreSQL, OTLP
- **Tests**: CRUD operations, timezone handling, batch inserts, OpenTelemetry signals
- **Framework**: pytest with parameterized tests
- **Docs**: [python-tests/README.md](python-tests/README.md)

### Go Tests (`go-tests/`)
- **Drivers**: MySQL (go-sql-driver/mysql), PostgreSQL (lib/pq)
- **Protocols**: MySQL, PostgreSQL, gRPC Ingester, OTLP
- **Tests**: CRUD operations, timezone handling, batch inserts, gRPC ingester operations, OpenTelemetry signals
- **Coverage**: All GreptimeDB data types
- **Docs**: [go-tests/README.md](go-tests/README.md)

### OpenTelemetry Tests (`otel-tests/`)
- **Language**: Node.js
- **Protocol**: OTLP HTTP (protobuf)
- **Signals**: Metrics (Counter/Gauge/Histogram), Traces, Logs
- **Verification**: SQL queries via MySQL protocol
- **Docs**: [otel-tests/README.md](otel-tests/README.md)

## Environment Variables

**Authentication:**
- `GREPTIME_USERNAME` / `GREPTIME_PASSWORD` - Credentials (default: empty)

**Database:**
- `DB_NAME` - Database name (default: auto-derived)
- `MYSQL_HOST` / `MYSQL_PORT` - MySQL connection (default: `127.0.0.1:4002`)
- `POSTGRES_HOST` / `POSTGRES_PORT` - PostgreSQL connection (default: `127.0.0.1:4003`)

**Optional:**
- `MYSQL_URL` / `POSTGRES_URL` - Complete connection URLs

## Architecture

**Database Isolation:**
Each test suite uses a separate database named after its directory:
- `java-tests/` → `java_tests`
- `python-tests/` → `python_tests`
- `go-tests/` → `go_tests`
- `otel-tests/` → `otel_tests_nodejs`

**Test Discovery:**
Root `run_tests.sh` automatically discovers and executes all test suites (directories with `run_tests.sh` or `run.sh`).

**Database Creation:**
`create_database.py` creates databases before running each suite. Note: Must connect to `public` database first (GreptimeDB requirement).

## CI Integration

### GitHub Actions
- **Format Check**: Java (Spotless), Python (black + flake8), Go (gofmt)
- **Integration Tests**: Runs all suites with authentication in Docker

### External CI
```bash
# In GreptimeDB main repo CI:
export GREPTIME_USERNAME=user
export GREPTIME_PASSWORD=pass
./run_tests.sh
```

## Adding Test Suites

1. Create directory: `mkdir new-tests && cd new-tests`
2. Create `run_tests.sh` that uses `$DB_NAME` env var
3. Make executable: `chmod +x run_tests.sh`
4. Test: `./run_tests.sh` (root runner auto-discovers)

See existing test suites for reference.

## License

Apache License 2.0
