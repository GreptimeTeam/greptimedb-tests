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
- Test suite specific dependencies (see individual test suite READMEs)

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

Test suites support flexible configuration through environment variables.

**Common variables:**
- `DB_NAME` - Database name (default: derived from directory name)

**Java tests specific:**
- `MYSQL_URL` - MySQL JDBC URL (default: `jdbc:mysql://localhost:4002/{DB_NAME}`)
- `POSTGRES_URL` - PostgreSQL JDBC URL (default: `jdbc:postgresql://localhost:4003/{DB_NAME}`)
- `MYSQL_PORT` - MySQL protocol port (default: `4002`)
- `POSTGRES_PORT` - PostgreSQL protocol port (default: `4003`)
- `HTTP_PORT` - GreptimeDB HTTP API port (default: `4000`)

## GitHub Actions CI

This repository includes two GitHub Actions workflows:

### 1. Code Format Check (`.github/workflows/format-check.yml`)
- **Trigger**: Pull requests and pushes to main/master affecting Java files
- **Purpose**: Validates code formatting using Spotless + Google Java Format
- **Action**: Runs `mvn spotless:check` on Java tests
- **Requirement**: All Java code must follow Google Java Format style

### 2. Integration Tests (`.github/workflows/test.yml`)
- **Trigger**: Pull requests and pushes to main/master
- **Purpose**: Run full integration test suite
- **Steps**:
  1. Start GreptimeDB using Docker (`greptime/greptimedb:v1.0.0-beta.1`)
  2. Wait for GreptimeDB health check to pass (up to 120 seconds)
  3. Execute `./run_tests.sh` to run all test suites
  4. Collect container logs on failure
  5. Upload test logs on failure
  6. Clean up container resources
- **Ports**: HTTP (4000), gRPC (4001), MySQL (4002), PostgreSQL (4003)
- **Data**: Persisted to `greptimedb_data/` directory

Both workflows use JDK 17 and cache Maven dependencies for faster builds.

## CI Integration (External)

This repository is also designed to be checked out during CI runs in the main GreptimeDB repository:

1. Checkout GreptimeDB main repository
2. Checkout this test repository (greptimedb-tests)
3. Build GreptimeDB binary
4. Start GreptimeDB in standalone mode
5. Run `./run_tests.sh` from this repository
6. Stop GreptimeDB and upload logs on failure

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
