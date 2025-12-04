// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.greptime.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for GreptimeDB JDBC drivers (MySQL and PostgreSQL). Tests various JDBC
 * operations including CRUD, timezone handling, and batch inserts.
 */
public class GreptimeDBJdbcTest {

  private static final Logger log = LoggerFactory.getLogger(GreptimeDBJdbcTest.class);
  private Connection conn;
  private String driver;
  private HikariDataSource dataSource;

  @AfterEach
  void tearDown() throws SQLException {
    if (conn != null && !conn.isClosed()) {
      conn.close();
    }
    if (dataSource != null && !dataSource.isClosed()) {
      dataSource.close();
    }
  }

  private void connect(String driverType) throws SQLException {
    connect(driverType, null);
  }

  private void connect(String driverType, String timezone) throws SQLException {
    connect(driverType, timezone, false);
  }

  private void connectWithPool(String driverType, String timezone) throws SQLException {
    connect(driverType, timezone, true);
  }

  private void connect(String driverType, String timezone, boolean usePool) throws SQLException {
    this.driver = driverType;
    String url = buildUrl(driverType, timezone);
    String username = getEnv("GREPTIME_USERNAME", "");
    String password = getEnv("GREPTIME_PASSWORD", "");

    if (usePool) {
      log.info("Connecting to {} with HikariCP pool, URL: {}", driverType, url);
      conn = createHikariConnection(driverType, url, username, password);
    } else {
      log.info("Connecting to {} with direct connection, URL: {}", driverType, url);
      conn = DriverManager.getConnection(url, username, password);
    }

    if (driverType.equals("postgresql") && timezone != null) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute("SET TIME ZONE = '" + timezone + "'");
      }
    }

    assertNotNull(conn);
    assertFalse(conn.isClosed());
  }

  private Connection createHikariConnection(String driverType, String url, String username, String password) throws SQLException {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(username);
    config.setPassword(password);
    config.setMaximumPoolSize(10);
    config.setMinimumIdle(2);
    config.setConnectionTimeout(30000);
    config.setIdleTimeout(600000);
    config.setMaxLifetime(1800000);
    config.setLeakDetectionThreshold(60000);

    // Set driver-specific properties
    if ("mysql".equals(driverType)) {
      config.addDataSourceProperty("cachePrepStmts", "true");
      config.addDataSourceProperty("prepStmtCacheSize", "250");
      config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
      config.addDataSourceProperty("useServerPrepStmts", "true");
    } else if ("postgresql".equals(driverType)) {
      config.addDataSourceProperty("prepareThreshold", "3");
      config.addDataSourceProperty("preparedStatementCacheQueries", "256");
    }

    dataSource = new HikariDataSource(config);
    return dataSource.getConnection();
  }

  private String buildUrl(String driverType, String timezone) {
    String baseUrl = getBaseUrl(driverType);
    if (timezone == null) {
      return baseUrl;
    }

    String separator = baseUrl.contains("?") ? "&" : "?";
    if ("mysql".equals(driverType)) {
      return baseUrl
          + separator
          + "connectionTimeZone="
          + timezone
          + "&forceConnectionTimeZoneToSession=true";
    } else {
      return baseUrl + separator + "TimeZone=" + timezone;
    }
  }

  private String getBaseUrl(String driverType) {
    if ("mysql".equals(driverType)) {
      String url = System.getenv("MYSQL_URL");
      if (url != null && !url.trim().isEmpty()) {
        return url;
      }
      String db = getEnv("DB_NAME", "public");
      String host = getEnv("MYSQL_HOST", "127.0.0.1");
      String port = getEnv("MYSQL_PORT", "4002");
      return "jdbc:mysql://" + host + ":" + port + "/" + db;
    } else if ("postgresql".equals(driverType)) {
      String url = System.getenv("POSTGRES_URL");
      if (url != null && !url.trim().isEmpty()) {
        return url;
      }
      String db = getEnv("DB_NAME", "public");
      String host = getEnv("POSTGRES_HOST", "127.0.0.1");
      String port = getEnv("POSTGRES_PORT", "4003");
      return "jdbc:postgresql://" + host + ":" + port + "/" + db;
    }
    throw new IllegalArgumentException("Unknown driver: " + driverType);
  }

  private String getEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
  }

  private void execute(Statement stmt, String sql) throws SQLException {
    if ("postgresql".equals(driver)) {
      stmt.execute(sql);
    } else {
      stmt.executeUpdate(sql);
    }
  }

  private void execute(PreparedStatement ps) throws SQLException {
    ps.executeUpdate();
  }

  private String tableName() {
    return "test_all_types_" + driver;
  }

  private void dropTable(String table) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + table);
    }
  }

  /**
   * Formats a Timestamp to UTC time string in "yyyy-MM-dd HH:mm:ss.SSS" format. This is used for
   * consistent timestamp verification in timezone tests.
   *
   * @param ts the Timestamp to format
   * @return formatted UTC time string
   */
  private String formatTimestampAsUtc(Timestamp ts) {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
    return sdf.format(ts);
  }

  private void createTable(String table, String schema) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (" + schema + ")");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"mysql", "postgresql"})
  void testCrudOperations(String driverType) throws SQLException {
    log.info("Starting CRUD test for driver: {}", driverType);
    connect(driverType);
    String table = tableName();

    try {
      log.info("[{}] Dropping table if exists: {}", driver, table);
      dropTable(table);

      log.info("[{}] Creating table: {}", driver, table);
      createTable(
          table,
          "ts TIMESTAMP TIME INDEX, "
              + "row_id STRING PRIMARY KEY, "
              + "int_col INTEGER, "
              + "double_col DOUBLE, "
              + "float_col FLOAT, "
              + "string_col STRING, "
              + "date_col DATE, "
              + "timestamp_col TIMESTAMP, "
              + "bool_col BOOLEAN, "
              + "binary_col BINARY");

      // INSERT with SQL literal
      log.info("[{}] Inserting row with SQL literal", driver);
      String literalInsert =
          String.format(
              "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) "
                  + "VALUES ('2024-11-24 10:00:00', 'row1', 42, 3.14159, 2.718, 'Hello GreptimeDB! 你好🚀', '2024-01-15', '2024-11-24 10:00:00', true, X'0102030405FFFE')",
              table);
      try (Statement stmt = conn.createStatement()) {
        execute(stmt, literalInsert);
      }

      // INSERT with PreparedStatement
      log.info("[{}] Inserting row with PreparedStatement", driver);
      String preparedInsert =
          String.format(
              "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) "
                  + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
              table);

      Timestamp ts = Timestamp.valueOf("2024-11-24 11:00:00");
      Date date = Date.valueOf("2024-06-20");
      byte[] binary = new byte[] {0x0A, 0x0B, 0x0C, (byte) 0xAB, (byte) 0xCD};

      try (PreparedStatement ps = conn.prepareStatement(preparedInsert)) {
        ps.setTimestamp(1, ts);
        ps.setString(2, "row2");
        ps.setInt(3, 999);
        ps.setDouble(4, 123.456);
        ps.setFloat(5, 78.9f);
        ps.setString(6, "PreparedStatement test 测试");
        ps.setDate(7, date);
        ps.setTimestamp(8, ts);
        ps.setBoolean(9, false);
        ps.setBytes(10, binary);
        execute(ps);
      }

      // SELECT and verify
      log.info("[{}] Selecting and verifying inserted rows", driver);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY row_id")) {

        assertTrue(rs.next());
        assertEquals("row1", rs.getString("row_id"));
        assertEquals(42, rs.getInt("int_col"));
        assertEquals(3.14159, rs.getDouble("double_col"), 0.00001);
        assertEquals(2.718f, rs.getFloat("float_col"), 0.001f);
        assertEquals("Hello GreptimeDB! 你好🚀", rs.getString("string_col"));
        assertEquals("2024-01-15", rs.getDate("date_col").toString());
        assertTrue(rs.getBoolean("bool_col"));
        assertArrayEquals(
            new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF, (byte) 0xFE},
            rs.getBytes("binary_col"));

        assertTrue(rs.next());
        assertEquals("row2", rs.getString("row_id"));
        assertEquals(999, rs.getInt("int_col"));
        assertEquals(123.456, rs.getDouble("double_col"), 0.001);
        assertEquals(78.9f, rs.getFloat("float_col"), 0.01f);
        assertEquals("PreparedStatement test 测试", rs.getString("string_col"));
        assertEquals("2024-06-20", rs.getDate("date_col").toString());
        assertFalse(rs.getBoolean("bool_col"));
        assertArrayEquals(binary, rs.getBytes("binary_col"));

        assertFalse(rs.next());
      }

      // UPDATE by inserting with SAME primary key + SAME time index
      // This should overwrite the first row (row1 at 10:00:00)
      log.info("[{}] Updating row by inserting with same primary key + time index", driver);
      Timestamp sameTs = Timestamp.valueOf("2024-11-24 10:00:00");
      try (PreparedStatement ps = conn.prepareStatement(preparedInsert)) {
        ps.setTimestamp(1, sameTs); // Same time index as row1
        ps.setString(2, "row1"); // Same primary key
        ps.setInt(3, 100); // Updated value
        ps.setDouble(4, 9.999); // Updated value
        ps.setFloat(5, 1.234f);
        ps.setString(6, "Updated value");
        ps.setDate(7, Date.valueOf("2024-12-25"));
        ps.setTimestamp(8, sameTs);
        ps.setBoolean(9, false);
        ps.setBytes(10, new byte[] {0x11, 0x22});
        execute(ps);
      }

      // Verify the row was overwritten (not added)
      log.info("[{}] Verifying row was overwritten (not added)", driver);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY row_id, ts")) {

        // Should only have 2 rows total (row1 was overwritten, not added)
        assertTrue(rs.next());
        assertEquals("row1", rs.getString("row_id"));
        assertEquals(100, rs.getInt("int_col"), "row1 should be updated");
        assertEquals(9.999, rs.getDouble("double_col"), 0.001);
        assertEquals("Updated value", rs.getString("string_col"));

        assertTrue(rs.next());
        assertEquals("row2", rs.getString("row_id"));
        assertEquals(999, rs.getInt("int_col"));

        assertFalse(rs.next(), "Should have exactly 2 rows (row1 overwritten, not added)");
      }

      // SELECT with WHERE
      log.info("[{}] Testing SELECT with WHERE clause", driver);
      try (PreparedStatement ps =
          conn.prepareStatement("SELECT * FROM " + table + " WHERE int_col > ? ORDER BY row_id")) {
        ps.setInt(1, 50);
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) count++;
          assertTrue(count > 0);
        }
      }

      // DELETE
      log.info("[{}] Testing DELETE with WHERE clause", driver);
      try (PreparedStatement ps =
          conn.prepareStatement("DELETE FROM " + table + " WHERE int_col > ?")) {
        ps.setInt(1, 50);
        var removes = ps.executeUpdate();
        assertTrue(removes > 0);
      }

      log.info("[{}] Dropping table", driver);
      dropTable(table);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + table + "'")) {
        assertFalse(rs.next());
      }

      log.info("[{}] CRUD test completed successfully", driver);

    } catch (SQLException e) {
      log.error("[{}] Test failed: {}", driver, e.getMessage(), e);
      throw e;
    }
  }

  /**
   * Test timezone behavior for INSERT and SELECT operations. Validates that timestamp strings are
   * interpreted using client timezone.
   */
  @ParameterizedTest
  @ValueSource(strings = {"mysql"})
  void testTimezoneInsertAndSelect(String driverType) throws SQLException {
    log.info("Starting timezone test for driver: {}", driverType);
    String table = "test_timezone_" + driverType;

    try {
      // Part 1: INSERT from different timezones
      connect(driverType, "UTC");
      dropTable(table);
      createTable(
          table, "ts TIMESTAMP TIME INDEX, row_id STRING PRIMARY KEY, timezone_used STRING");

      // Insert from UTC
      try (Statement stmt = conn.createStatement()) {
        execute(
            stmt,
            String.format(
                "INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'utc_row', 'UTC')",
                table));
      }
      conn.close();

      // Insert from Asia/Shanghai (UTC+8)
      connect(driverType, "Asia/Shanghai");
      try (Statement stmt = conn.createStatement()) {
        execute(
            stmt,
            String.format(
                "INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'shanghai_row', 'Asia/Shanghai')",
                table));
      }
      conn.close();

      // Insert from America/New_York (UTC-5)
      connect(driverType, "America/New_York");
      try (Statement stmt = conn.createStatement()) {
        execute(
            stmt,
            String.format(
                "INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'newyork_row', 'America/New_York')",
                table));
      }
      conn.close();

      // Part 2: Verify timestamps stored in UTC (connection timezone affects INSERT interpretation)
      connect(driverType, "UTC");
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT row_id, ts, timezone_used FROM " + table + " ORDER BY row_id")) {

        // New York: 2024-01-01 12:00:00 (local) -> 2024-01-01 17:00:00 (UTC)
        assertTrue(rs.next());
        assertEquals("newyork_row", rs.getString("row_id"));
        assertEquals("America/New_York", rs.getString("timezone_used"));
        String nyTs = formatTimestampAsUtc(rs.getTimestamp("ts"));
        assertTrue(
            nyTs.startsWith("2024-01-01 17:00:00"),
            "Expected UTC 17:00:00, got: " + rs.getTimestamp("ts"));

        // Shanghai: 2024-01-01 12:00:00 (local) -> 2024-01-01 04:00:00 (UTC)
        assertTrue(rs.next());
        assertEquals("shanghai_row", rs.getString("row_id"));
        assertEquals("Asia/Shanghai", rs.getString("timezone_used"));
        String shanghaiTs = formatTimestampAsUtc(rs.getTimestamp("ts"));
        assertTrue(
            shanghaiTs.startsWith("2024-01-01 04:00:00"),
            "Expected UTC 04:00:00, got: " + shanghaiTs);

        // UTC: 2024-01-01 12:00:00 (local) -> 2024-01-01 12:00:00 (UTC)
        assertTrue(rs.next());
        assertEquals("utc_row", rs.getString("row_id"));
        assertEquals("UTC", rs.getString("timezone_used"));
        String utcTs = formatTimestampAsUtc(rs.getTimestamp("ts"));
        assertTrue(utcTs.startsWith("2024-01-01 12:00:00"), "Expected UTC 12:00:00, got: " + utcTs);

        assertFalse(rs.next());
      }
      conn.close();

      // Part 3: Verify formatTimestampAsUtc always returns UTC regardless of connection timezone
      connect(driverType, "Asia/Shanghai");
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery("SELECT ts FROM " + table + " WHERE row_id = 'utc_row'")) {
        assertTrue(rs.next());
        String tsInShanghai = formatTimestampAsUtc(rs.getTimestamp("ts"));
        assertTrue(
            tsInShanghai.startsWith("2024-01-01 12:00:00"),
            "Expected UTC 12:00:00, got: " + tsInShanghai);
      }
      conn.close();

      connect(driverType, "America/New_York");
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery("SELECT ts FROM " + table + " WHERE row_id = 'utc_row'")) {
        assertTrue(rs.next());
        String tsInNY = formatTimestampAsUtc(rs.getTimestamp("ts"));
        assertTrue(
            tsInNY.startsWith("2024-01-01 12:00:00"), "Expected UTC 12:00:00, got: " + tsInNY);
      }
      conn.close();

      // Part 4: Verify WHERE clause interprets timestamp literals using connection timezone
      connect(driverType, "Asia/Shanghai");
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT row_id FROM " + table + " WHERE ts = '2024-01-01 12:00:00'")) {
        assertTrue(rs.next());
        assertEquals("shanghai_row", rs.getString("row_id"));
        assertFalse(rs.next());
      }
      conn.close();

      connect(driverType, "UTC");
      try (Statement stmt = conn.createStatement();
          ResultSet rs =
              stmt.executeQuery(
                  "SELECT row_id FROM " + table + " WHERE ts = '2024-01-01 12:00:00'")) {
        assertTrue(rs.next());
        assertEquals("utc_row", rs.getString("row_id"));
        assertFalse(rs.next());
      }

      dropTable(table);
      log.info("[{}] Timezone test completed successfully", driverType);

    } catch (SQLException e) {
      log.error("[{}] Timezone test failed: {}", driverType, e.getMessage(), e);
      try {
        if (conn != null && !conn.isClosed()) {
          dropTable(table);
        }
      } catch (SQLException cleanupEx) {
        log.warn("[{}] Cleanup failed: {}", driverType, cleanupEx.getMessage());
      }
      throw e;
    }
  }

  /**
   * Test batch insert using PreparedStatement.addBatch() and executeBatch(). Validates that batch
   * operations work correctly for both MySQL and PostgreSQL drivers.
   */
  @ParameterizedTest
  @ValueSource(strings = {"mysql", "postgresql"})
  void testBatchInsert(String driverType) throws SQLException {
    log.info("Starting batch insert test for driver: {}", driverType);
    connect(driverType);
    String table = "test_batch_insert_" + driverType;

    try {
      log.info("[{}] Dropping table if exists: {}", driver, table);
      dropTable(table);

      log.info("[{}] Creating table: {}", driver, table);
      createTable(
          table,
          "ts TIMESTAMP TIME INDEX, "
              + "row_id STRING PRIMARY KEY, "
              + "int_col INTEGER, "
              + "double_col DOUBLE, "
              + "string_col STRING, "
              + "bool_col BOOLEAN");

      // Prepare batch insert with 5 rows
      int batchSize = 5;
      log.info("[{}] Preparing batch insert with {} rows", driver, batchSize);

      String insertSql =
          String.format(
              "INSERT INTO %s (ts, row_id, int_col, double_col, string_col, bool_col) VALUES (?, ?, ?, ?, ?, ?)",
              table);

      try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
        // Add 5 rows to the batch
        for (int i = 1; i <= batchSize; i++) {
          Timestamp ts = Timestamp.valueOf(String.format("2024-11-24 %02d:00:00", 10 + i));
          ps.setTimestamp(1, ts);
          ps.setString(2, "batch_row_" + i);
          ps.setInt(3, i * 100);
          ps.setDouble(4, i * 1.5);
          ps.setString(5, "Batch test row " + i + " 测试");
          ps.setBoolean(6, i % 2 == 0);

          ps.addBatch();
          log.info("[{}] Added row {} to batch: batch_row_{}, ts={}", driver, i, i, ts);
        }

        // Execute batch
        log.info("[{}] Executing batch insert", driver);
        int[] results = ps.executeBatch();

        // Verify batch execution results
        log.info("[{}] Batch execution completed, results length: {}", driver, results.length);
        assertEquals(batchSize, results.length, "Batch should return " + batchSize + " results");
      }

      // Verify all rows were inserted correctly
      log.info("[{}] Verifying inserted rows", driver);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY row_id")) {

        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String rowId = rs.getString("row_id");
          int intCol = rs.getInt("int_col");
          double doubleCol = rs.getDouble("double_col");
          String stringCol = rs.getString("string_col");
          boolean boolCol = rs.getBoolean("bool_col");

          log.info(
              "[{}] Row {}: {} | int={}, double={}, string={}, bool={}",
              driver,
              rowCount,
              rowId,
              intCol,
              doubleCol,
              stringCol,
              boolCol);

          // Verify data matches what we inserted
          assertEquals("batch_row_" + rowCount, rowId);
          assertEquals(rowCount * 100, intCol);
          assertEquals(rowCount * 1.5, doubleCol, 0.01);
          assertEquals("Batch test row " + rowCount + " 测试", stringCol);
          assertEquals(rowCount % 2 == 0, boolCol);
        }

        assertEquals(batchSize, rowCount, "Should have inserted exactly " + batchSize + " rows");
        log.info("[{}] ✓ Verified all {} rows were inserted correctly", driver, batchSize);
      }

      // Cleanup
      log.info("[{}] Dropping table", driver);
      dropTable(table);

      log.info("[{}] Batch insert test completed successfully", driver);

    } catch (SQLException e) {
      log.error("[{}] Batch insert test failed: {}", driver, e.getMessage(), e);
      // Cleanup on failure
      try {
        dropTable(table);
      } catch (SQLException cleanupEx) {
        log.warn("[{}] Cleanup failed: {}", driver, cleanupEx.getMessage());
      }
      throw e;
    }
  }

  /**
   * Test HikariCP connection pooling functionality. Validates that connection pooling works
   * correctly for both MySQL and PostgreSQL drivers with proper resource management.
   */
  @ParameterizedTest
  @ValueSource(strings = {"mysql", "postgresql"})
  void testHikariConnectionPooling(String driverType) throws SQLException {
    log.info("Starting HikariCP connection pool test for driver: {}", driverType);
    connectWithPool(driverType, null);
    String table = "test_hikaricp_" + driverType;

    try {
      log.info("[{}] Dropping table if exists: {}", driver, table);
      dropTable(table);

      log.info("[{}] Creating table: {}", driver, table);
      createTable(
          table,
          "ts TIMESTAMP TIME INDEX, "
              + "row_id STRING PRIMARY KEY, "
              + "int_col INTEGER, "
              + "double_col DOUBLE, "
              + "string_col STRING, "
              + "bool_col BOOLEAN");

      // Test multiple connections from the pool
      log.info("[{}] Testing multiple connections from pool", driver);
      for (int i = 1; i <= 5; i++) {
        try (Connection pooledConn = dataSource.getConnection();
            Statement stmt = pooledConn.createStatement()) {

          // Insert test data
          String insertSql = String.format(
              "INSERT INTO %s (ts, row_id, int_col, double_col, string_col, bool_col) "
                  + "VALUES ('2024-11-24 10:%02d:00', 'pool_row_%d', %d, %f, 'Pool test %d', %s)",
              table, i, i, i * 10, i * 1.5, i, i % 2 == 0);
          execute(stmt, insertSql);

          log.info("[{}] Inserted row {} using pooled connection", driver, i);
        }
      }

      // Verify all data was inserted correctly
      log.info("[{}] Verifying all inserted data", driver);
      try (Statement stmt = conn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY row_id")) {

        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String rowId = rs.getString("row_id");
          int intCol = rs.getInt("int_col");
          double doubleCol = rs.getDouble("double_col");
          String stringCol = rs.getString("string_col");
          boolean boolCol = rs.getBoolean("bool_col");

          log.info(
              "[{}] Pool Row {}: {} | int={}, double={}, string={}, bool={}",
              driver,
              rowCount,
              rowId,
              intCol,
              doubleCol,
              stringCol,
              boolCol);

          // Verify data matches what we inserted
          assertEquals("pool_row_" + rowCount, rowId);
          assertEquals(rowCount * 10, intCol);
          assertEquals(rowCount * 1.5, doubleCol, 0.01);
          assertEquals("Pool test " + rowCount, stringCol);
          assertEquals(rowCount % 2 == 0, boolCol);
        }

        assertEquals(5, rowCount, "Should have inserted exactly 5 rows using pooled connections");
        log.info("[{}] ✓ Verified all {} rows were inserted correctly using HikariCP", driver, rowCount);
      }

      // Test pool statistics
      log.info("[{}] Pool statistics - Active: {}, Idle: {}, Total: {}, Waiting: {}",
          driver,
          dataSource.getHikariPoolMXBean().getActiveConnections(),
          dataSource.getHikariPoolMXBean().getIdleConnections(),
          dataSource.getHikariPoolMXBean().getTotalConnections(),
          dataSource.getHikariPoolMXBean().getThreadsAwaitingConnection());

      // Cleanup
      log.info("[{}] Dropping table", driver);
      dropTable(table);

      log.info("[{}] HikariCP connection pool test completed successfully", driver);

    } catch (SQLException e) {
      log.error("[{}] HikariCP test failed: {}", driver, e.getMessage(), e);
      // Cleanup on failure
      try {
        dropTable(table);
      } catch (SQLException cleanupEx) {
        log.warn("[{}] Cleanup failed: {}", driver, cleanupEx.getMessage());
      }
      throw e;
    }
  }
}
