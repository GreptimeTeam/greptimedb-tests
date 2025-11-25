# GreptimeDB Tests

Multi-language integration tests for GreptimeDB. This repository contains test suites for various programming languages and SDKs to validate GreptimeDB's compatibility with different client drivers and protocols.

## Project Structure

```
greptimedb-tests/
├── run_tests.sh              # Master test runner (runs all test suites)
├── java-tests/               # Java integration tests (JDBC + gRPC)
│   ├── run_tests.sh
│   └── src/test/java/
└── [future test suites]      # Additional test suites for other languages
```

## Quick Start

### Prerequisites
- GreptimeDB instance running on localhost (default ports)
- Python 3 with mysql-connector-python (for database creation)
- Test suite specific dependencies (see individual test suite READMEs)

**Install mysql-connector-python:**
```bash
pip install mysql-connector-python
# or
pip3 install mysql-connector-python
```

### Start GreptimeDB

**Option 1: Using Cargo (from source)**
```bash
cargo run --bin greptime -- standalone start
```

**Option 2: Using Docker**
```bash
mkdir -p greptimedb_data
docker run -d \
  -p 127.0.0.1:4000-4003:4000-4003 \
  -v "$(pwd)/greptimedb_data:/greptimedb_data" \
  --name greptime --rm \
  greptime/greptimedb:v1.0.0-beta.1 standalone start \
  --http-addr 0.0.0.0:4000 \
  --rpc-addr 0.0.0.0:4001 \
  --mysql-addr 0.0.0.0:4002 \
  --postgres-addr 0.0.0.0:4003

# Wait for startup
sleep 10

# Check health
curl http://localhost:4000/health

# Stop when done
docker stop greptime
```

### Run All Tests
```bash
./run_tests.sh
```

This will automatically discover and run all test suites in the repository.

### Run Individual Test Suite
```bash
cd java-tests
./run_tests.sh
```

## Test Suites

### Java Tests
- **Location**: `java-tests/`
- **Description**: JDBC integration tests (MySQL and PostgreSQL protocols). gRPC tests planned.
- **Requirements**: Java 11+, Maven 3.6+
- **Documentation**: See [java-tests/README.md](java-tests/README.md)

## Architecture

### Database Isolation
Each test suite uses a separate database named after its directory:
- `java-tests/` → database `java_tests`

The root `run_tests.sh` creates the database before running each test suite.

### Test Discovery
The root test runner automatically discovers test suites by looking for:
- Subdirectories containing `run_tests.sh` or `run.sh`
- Executes each suite in sequence
- Reports aggregated results

## Environment Variables

**Common:**
- `DB_NAME` - Database name (default: derived from directory name)
- `GREPTIME_USERNAME` / `GREPTIME_PASSWORD` - Authentication credentials (default: empty)
- `MYSQL_HOST` / `MYSQL_PORT` - MySQL connection (default: `127.0.0.1:4002`)
- `POSTGRES_HOST` / `POSTGRES_PORT` - PostgreSQL connection (default: `127.0.0.1:4003`)

**Optional overrides:**
- `MYSQL_URL` / `POSTGRES_URL` - Complete JDBC URLs (override host/port/db)

## GitHub Actions CI

### Code Format Check
- Validates Java code formatting with Spotless + Google Java Format
- Runs `mvn spotless:check` on Java test files

### Integration Tests
- Starts GreptimeDB in Docker with authentication (`greptime_user`/`greptime_pwd`)
- Runs all test suites via `./run_tests.sh`
- Uploads logs on failure
- Caches Maven dependencies for faster builds

## External CI Integration

Can be used in GreptimeDB main repository CI:
1. Build and start GreptimeDB
2. Run `./run_tests.sh` from this repo
3. Collect logs on failure

## Adding New Test Suites

To add a test suite for a new language or SDK:

1. **Create a subdirectory** with a descriptive name:
   ```bash
   mkdir python-sdk-tests
   cd python-sdk-tests
   ```

2. **Create a `run_tests.sh`** script that:
   - Uses `DB_NAME` env var (set by root `run_tests.sh`)
   - Returns exit code 0 on success, non-zero on failure
   - Assumes database already exists

3. **Make it executable**:
   ```bash
   chmod +x run_tests.sh
   ```

4. **Test it**:
   ```bash
   cd ..
   ./run_tests.sh  # Root runner will discover it automatically
   ```

For more details, see [CLAUDE.md](CLAUDE.md).

## License

Apache License 2.0
