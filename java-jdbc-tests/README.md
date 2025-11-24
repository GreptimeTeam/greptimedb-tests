# Java JDBC Tests

Integration tests for GreptimeDB using MySQL and PostgreSQL JDBC drivers.

## Supported Data Types

- INTEGER, DOUBLE, FLOAT
- STRING (Unicode and emoji)
- TIMESTAMP, DATE
- BINARY, BOOLEAN

## Running Tests

### Prerequisites
- Java 11+
- Maven 3.6+
- GreptimeDB running on localhost

### Quick Start
```bash
# Start GreptimeDB
cargo run --bin greptime -- standalone start

# Run tests
./run_tests.sh
```

### Direct Maven Execution
```bash
mvn clean test
```

## Environment Variables

Configure connection details:

```bash
# Option 1: Complete URLs
export MYSQL_URL="jdbc:mysql://localhost:4002/java_jdbc_tests"
export POSTGRES_URL="jdbc:postgresql://localhost:4003/java_jdbc_tests"

# Option 2: Components (URLs built automatically)
export DB_NAME="java_jdbc_tests"
export MYSQL_PORT="4002"
export POSTGRES_PORT="4003"
```

## Test Structure

**testCrudOperations**: Comprehensive CRUD test covering all data types
- CREATE TABLE
- INSERT (SQL literal and PreparedStatement)
- SELECT and verify
- UPDATE (insert with later timestamp)
- DELETE (drop table)

Each test runs twice - once for MySQL driver, once for PostgreSQL driver.

## Troubleshooting

**GreptimeDB not running:**
```bash
cargo run --bin greptime -- standalone start
```

**Connection refused:**
```bash
curl http://localhost:4000/health
lsof -i :4002  # MySQL
lsof -i :4003  # PostgreSQL
```

**Debug mode:**
```bash
mvn -X clean test
```

## License

Apache License 2.0
