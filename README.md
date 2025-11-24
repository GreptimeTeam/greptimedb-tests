# GreptimeDB Tests

Multi-language integration tests for GreptimeDB. This repository contains test suites for various programming languages and SDKs to validate GreptimeDB's compatibility with different client drivers and protocols.

## Project Structure

```
greptimedb-tests/
├── run_tests.sh              # Master test runner (runs all test suites)
├── java-jdbc-tests/          # Java JDBC integration tests
│   ├── run_tests.sh
│   └── src/test/java/
└── [future test suites]      # Additional test suites for other languages
```

## Quick Start

### Prerequisites
- GreptimeDB instance running on localhost (default ports)
- Test suite specific dependencies (see individual test suite READMEs)

### Start GreptimeDB
```bash
cargo run --bin greptime -- standalone start
```

### Run All Tests
```bash
./run_tests.sh
```

This will automatically discover and run all test suites in the repository.

### Run Individual Test Suite
```bash
cd java-jdbc-tests
./run_tests.sh
```

## Test Suites

### Java JDBC Tests
- **Location**: `java-jdbc-tests/`
- **Description**: JDBC integration tests for MySQL and PostgreSQL protocols
- **Requirements**: Java 11+, Maven 3.6+
- **Documentation**: See [java-jdbc-tests/README.md](java-jdbc-tests/README.md)

## Architecture

### Database Isolation
Each test suite uses a separate database named after its directory:
- `java-jdbc-tests/` → database `java_jdbc_tests`

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

**Java JDBC specific:**
- `MYSQL_URL` - MySQL JDBC URL (default: `jdbc:mysql://localhost:4002/{DB_NAME}`)
- `POSTGRES_URL` - PostgreSQL JDBC URL (default: `jdbc:postgresql://localhost:4003/{DB_NAME}`)
- `MYSQL_PORT` - MySQL protocol port (default: `4002`)
- `POSTGRES_PORT` - PostgreSQL protocol port (default: `4003`)
- `HTTP_PORT` - GreptimeDB HTTP API port (default: `4000`)

## CI Integration

This repository is designed to be checked out during CI runs in the main GreptimeDB repository:

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
