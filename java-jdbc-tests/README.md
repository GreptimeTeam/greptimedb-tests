# GreptimeDB JDBC Integration Tests

This directory contains integration tests for GreptimeDB using both MySQL and PostgreSQL JDBC drivers.

> **Note**: This test suite is maintained in the [GreptimeTeam/greptimedb-tests](https://github.com/GreptimeTeam/greptimedb-tests) repository and is automatically checked out during CI runs.

## Overview

The test suite validates comprehensive CRUD operations (Create, Read, Update, Delete) using a single table containing all basic data types supported by GreptimeDB:

- **INTEGER** - 32-bit signed integers
- **DOUBLE** - 64-bit floating-point numbers
- **FLOAT** - 32-bit floating-point numbers
- **STRING** - Variable-length text (including Unicode and emoji)
- **TIMESTAMP** - Date and time with microsecond precision
- **DATE** - Calendar dates
- **BINARY** - Binary data (byte arrays)
- **BOOLEAN** - True/false values

The test suite includes two main test methods:

### 1. Comprehensive Data Types Test
Tests two insertion methods:
- **SQL Literal INSERT** - Direct SQL with values in the query
- **PreparedStatement INSERT** - Parameterized queries (recommended for production)

### 2. Timezone Handling Test
Tests timezone behavior across different timezone settings:
- **UTC** - Coordinated Universal Time
- **Asia/Shanghai** - UTC+8
- **America/New_York** - UTC-5 (EST) / UTC-4 (EDT)

## Prerequisites

- Java 11 or later
- Maven 3.6 or later
- GreptimeDB binary (built or available in `target/debug/greptime`)

## Running Tests Locally

### Prerequisites
Before running the tests, you need to start GreptimeDB:

```bash
# From the project root
cargo run --bin greptime -- standalone start
```

### Running the tests

Once GreptimeDB is running, execute the test script:

```bash
# From tests-integration/java-jdbc-tests directory
./run_tests.sh
```

The script will:
1. Verify GreptimeDB is running
2. Check Java and Maven are installed
3. Run all JDBC tests with both MySQL and PostgreSQL drivers

### Alternative: Direct Maven execution

```bash
cd tests-integration/java-jdbc-tests
mvn clean test
```

## Environment Variables

The following environment variables can be used to customize the test execution:

- `MYSQL_URL` - JDBC URL for MySQL connection (default: `jdbc:mysql://localhost:4002/public`)
- `POSTGRES_URL` - JDBC URL for PostgreSQL connection (default: `jdbc:postgresql://localhost:4003/public`)
- `MYSQL_PORT` - MySQL protocol port (default: `4002`)
- `POSTGRES_PORT` - PostgreSQL protocol port (default: `4003`)
- `HTTP_PORT` - HTTP API port for health checks (default: `4000`)

Example:
```bash
MYSQL_URL="jdbc:mysql://localhost:14002/public" \
POSTGRES_URL="jdbc:postgresql://localhost:14003/public" \
HTTP_PORT=14000 \
./run_tests.sh
```

## Test Structure

The tests are organized in `src/test/java/com/greptime/jdbc/GreptimeDBBasicTypesTest.java`:

### Test 1: Comprehensive Data Types (`testAllTypesComprehensive`)

This test performs the following operations on a single table containing all data types:

1. **CREATE TABLE** - Creates a table with all supported data types
2. **INSERT (SQL Literal)** - Inserts a row using direct SQL with literal values
3. **INSERT (PreparedStatement)** - Inserts a second row using parameterized query
4. **SELECT (Read)** - Queries and verifies both inserted rows
5. **UPDATE** - Updates a row by inserting with a later timestamp
6. **SELECT with WHERE** - Tests conditional queries with PreparedStatement
7. **DELETE** - Drops the table to clean up

### Test 2: Timezone Handling (`testTimezone`)

This test validates timezone behavior and demonstrates how GreptimeDB handles timestamps across different timezones:

1. **CREATE TABLE (UTC)** - Creates a table with UTC timezone connection
2. **INSERT with UTC** - Inserts timestamp using UTC timezone
3. **SELECT with UTC** - Retrieves and verifies the timestamp
4. **SELECT with Asia/Shanghai** - Reconnects with different timezone and queries same data
5. **INSERT with Asia/Shanghai** - Inserts timestamp interpreted in Shanghai timezone
6. **INSERT with SQL Literal (America/New_York)** - Tests literal timestamp with NY timezone
7. **Compare All Events** - Queries all events with UTC to compare epoch milliseconds
8. **Cleanup** - Drops the test table

#### Timezone Configuration

The test demonstrates two methods to set timezone:

**For MySQL JDBC:**
```
jdbc:mysql://localhost:4002/public?connectionTimeZone=Asia/Shanghai&forceConnectionTimeZoneToSession=true
```

**For PostgreSQL JDBC:**
```
jdbc:postgresql://localhost:4003/public?TimeZone=Asia/Shanghai
```

#### Key Insights

- Timezone affects how string literals are interpreted during INSERT
- The underlying timestamp storage is timezone-independent (epoch milliseconds)
- Same epoch time appears differently when retrieved with different timezone settings
- All three test events represent approximately the same moment, inserted from different timezones

### Key Features

- **Parameterized**: Each test runs twice - once with MySQL driver, once with PostgreSQL driver
- **Comprehensive**: Single table tests all data types in realistic scenarios
- **Production-ready**: Demonstrates both literal and prepared statement approaches
- **Unicode Support**: Tests Chinese characters and emoji in strings
- **Binary Data**: Tests binary data insertion and retrieval
- **Timezone Aware**: Tests timezone handling as documented in GreptimeDB user guide
- **Detailed Output**: Prints progress and verification messages for debugging

## CI Integration

The tests are integrated into the GreptimeDB main repository's GitHub Actions CI pipeline through `.github/workflows/java-jdbc-tests.yml`.

### CI Workflow

The workflow performs the following steps:

1. **Checkout repositories**:
   - Checks out the GreptimeDB main repository to `greptimedb/`
   - Checks out the GreptimeTeam/greptimedb-tests repository to `greptimedb-tests/`

2. **Build GreptimeDB**:
   - Sets up Rust toolchain
   - Builds GreptimeDB binary from the main repository

3. **Setup test environment**:
   - Sets up Java 11 and Maven
   - Starts GreptimeDB in standalone mode
   - Waits for GreptimeDB to be ready (health check)

4. **Run tests**:
   - Executes the Java JDBC test suite from `greptimedb-tests/java-jdbc-tests/`
   - Tests run against the locally built GreptimeDB instance

5. **Cleanup**:
   - Stops GreptimeDB
   - Uploads test results and logs on failure

### Trigger Conditions

The workflow is triggered on:
- Pull requests that modify server, SQL, or datatype code in the GreptimeDB repository
- Pushes to the main branch
- Manual dispatch via GitHub Actions UI

## Troubleshooting

### GreptimeDB is not running

If you see the error "GreptimeDB is not running", start it first:
```bash
cargo run --bin greptime -- standalone start
```

### Connection refused errors

Ensure GreptimeDB is running and listening on the expected ports:
```bash
# Check if ports are listening
lsof -i :4002  # MySQL
lsof -i :4003  # PostgreSQL
lsof -i :4000  # HTTP

# Or check the health endpoint
curl http://localhost:4000/health
```

### Test failures

Check the test output in the terminal. For more detailed logs, you can enable DEBUG logging:
```bash
mvn -X clean test
```

### Maven dependency issues

Clear Maven cache and retry:
```bash
mvn dependency:purge-local-repository
mvn clean test
```

## Adding New Tests

To add new test cases or data types:

1. **Add new data type to existing test**:
   - Update the `createTable()` method to add the new column
   - Update the INSERT statements (both literal and prepared)
   - Add verification in the SELECT assertions

2. **Add a new test method**:
   - Add a new `@ParameterizedTest` method in `GreptimeDBBasicTypesTest.java`
   - Use `@ValueSource(strings = {"mysql", "postgresql"})` to test both drivers
   - Follow the existing pattern: CREATE → INSERT (both methods) → SELECT → UPDATE → DROP
   - Ensure proper cleanup in case of test failures

3. **Test edge cases**:
   - Add test methods for NULL values, special characters, boundary values, etc.
   - Consider testing concurrent operations, transactions, or batch inserts

## License

Licensed under the Apache License, Version 2.0. See the LICENSE file for details.
