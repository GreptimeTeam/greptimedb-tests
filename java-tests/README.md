# Java Tests

Integration tests for GreptimeDB using JDBC (MySQL and PostgreSQL drivers). gRPC tests planned.

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

## Code Formatting

This project uses **Spotless** with **Google Java Format** for consistent code style.

### Format Code
```bash
# Auto-format all Java files
mvn spotless:apply
```

### Check Formatting
```bash
# Check if code is properly formatted (runs automatically in verify phase)
mvn spotless:check

# Or run during build
mvn clean verify
```

### IDE Integration

**IntelliJ IDEA:**
1. Install "google-java-format" plugin
2. Enable: Settings → google-java-format Settings → Enable

**VS Code:**
1. Install "Language Support for Java" extension
2. Install "google-java-format" formatter

### Pre-commit Hook (Optional)
```bash
# Add to .git/hooks/pre-commit
#!/bin/bash
mvn spotless:apply
git add -u
```

## Environment Variables

Configure connection details:

```bash
# Option 1: Complete URLs
export MYSQL_URL="jdbc:mysql://localhost:4002/java_tests"
export POSTGRES_URL="jdbc:postgresql://localhost:4003/java_tests"

# Option 2: Components (URLs built automatically)
export DB_NAME="java_tests"
export MYSQL_PORT="4002"
export POSTGRES_PORT="4003"
```

## Test Structure

**GreptimeDBJdbcTest** - JDBC integration tests

**testCrudOperations**: Comprehensive CRUD test covering all data types
- CREATE TABLE, INSERT (literal + PreparedStatement), SELECT, UPDATE, DELETE

**testTimezoneInsertAndSelect**: Timezone handling validation
- Inserts from different timezones, verifies interpretation and display behavior

**testBatchInsert**: Batch operations using PreparedStatement
- Batch insert 5 rows with addBatch() + executeBatch()

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
