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

package com.greptime.ingester;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.greptime.*;
import io.greptime.models.*;
import io.greptime.options.GreptimeOptions;

/**
 * Integration tests for GreptimeDB Java Ingester SDK. Tests data insertion and updates using the
 * ingester, with MySQL JDBC verification.
 */
public class GreptimeDBIngesterTest {

  private static final Logger log = LoggerFactory.getLogger(GreptimeDBIngesterTest.class);
  private GreptimeDB greptimeDB;
  private Connection jdbcConn;

  @BeforeEach
  void setUp() throws Exception {
    // Initialize GreptimeDB client
    String database = getEnv("DB_NAME", "public");
    String grpcHost = getEnv("GRPC_HOST", "127.0.0.1");
    String grpcPort = getEnv("GRPC_PORT", "4001");
    String[] endpoints = {grpcHost + ":" + grpcPort};

    String username = getEnv("GREPTIME_USERNAME", "");
    String password = getEnv("GREPTIME_PASSWORD", "");

    GreptimeOptions.Builder optsBuilder = GreptimeOptions.newBuilder(endpoints, database);

    if (!username.isEmpty() && !password.isEmpty()) {
      AuthInfo authInfo = new AuthInfo(username, password);
      optsBuilder.authInfo(authInfo);
    }

    GreptimeOptions opts = optsBuilder.build();
    greptimeDB = GreptimeDB.create(opts);

    log.info("GreptimeDB client initialized: database={}, endpoints={}", database, endpoints[0]);
  }

  @AfterEach
  void tearDown() throws Exception {
    if (jdbcConn != null && !jdbcConn.isClosed()) {
      jdbcConn.close();
    }
    if (greptimeDB != null) {
      greptimeDB.shutdownGracefully();
    }
  }

  private void connectJdbc() throws SQLException {
    String url = buildJdbcUrl();
    String username = getEnv("GREPTIME_USERNAME", "");
    String password = getEnv("GREPTIME_PASSWORD", "");
    log.info("Connecting to MySQL JDBC with URL: {}", url);
    jdbcConn = DriverManager.getConnection(url, username, password);
    assertNotNull(jdbcConn);
    assertFalse(jdbcConn.isClosed());
  }

  private String buildJdbcUrl() {
    String url = System.getenv("MYSQL_URL");
    if (url != null && !url.trim().isEmpty()) {
      return url;
    }
    String db = getEnv("DB_NAME", "public");
    String host = getEnv("MYSQL_HOST", "127.0.0.1");
    String port = getEnv("MYSQL_PORT", "4002");
    return "jdbc:mysql://" + host + ":" + port + "/" + db;
  }

  private String getEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
  }

  private void dropTable(String table) throws SQLException {
    try (Statement stmt = jdbcConn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS " + table);
    }
  }

  /**
   * Tests insert and update operations using GreptimeDB Ingester SDK. Verifies the data using MySQL
   * JDBC queries.
   */
  @Test
  void testIngesterInsertAndUpdate() throws Exception {
    log.info("Starting ingester insert/update test");
    connectJdbc();
    String tableName = "test_ingester";

    try {
      log.info("Dropping table if exists: {}", tableName);
      dropTable(tableName);

      // Define table schema with various data types
      log.info("Creating table schema");
      TableSchema tableSchema =
          TableSchema.newBuilder(tableName)
              .addTag("row_id", DataType.String) // Primary key as tag
              .addTimestamp("ts", DataType.TimestampMillisecond) // Time index
              .addField("int_col", DataType.Int32)
              .addField("double_col", DataType.Float64)
              .addField("float_col", DataType.Float32)
              .addField("string_col", DataType.String)
              .addField("bool_col", DataType.Bool)
              .addField("binary_col", DataType.Binary)
              .build();

      // Create table and insert first row
      log.info("Inserting first row using Ingester SDK");
      Table table1 = Table.from(tableSchema);
      byte[] binaryData1 = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF, (byte) 0xFE};
      table1.addRow(
          "row1", // row_id (tag)
          1732435200000L, // ts: 2024-11-24 10:00:00 in milliseconds
          42, // int_col
          3.14159, // double_col
          2.718f, // float_col
          "Hello GreptimeDB! 你好🚀", // string_col
          true, // bool_col
          binaryData1 // binary_col
          );
      table1.complete();

      CompletableFuture<Result<WriteOk, Err>> future1 = greptimeDB.write(table1);
      Result<WriteOk, Err> result1 = future1.get();
      assertTrue(result1.isOk(), "First insert should succeed");
      log.info("First row inserted successfully");

      // Insert second row
      log.info("Inserting second row using Ingester SDK");
      Table table2 = Table.from(tableSchema);
      byte[] binaryData2 = new byte[] {0x0A, 0x0B, 0x0C, (byte) 0xAB, (byte) 0xCD};
      table2.addRow(
          "row2", // row_id (tag)
          1732438800000L, // ts: 2024-11-24 11:00:00 in milliseconds
          999, // int_col
          123.456, // double_col
          78.9f, // float_col
          "Ingester test 测试", // string_col
          false, // bool_col
          binaryData2 // binary_col
          );
      table2.complete();

      CompletableFuture<Result<WriteOk, Err>> future2 = greptimeDB.write(table2);
      Result<WriteOk, Err> result2 = future2.get();
      assertTrue(result2.isOk(), "Second insert should succeed");
      log.info("Second row inserted successfully");

      // Verify inserted data using JDBC
      log.info("Verifying inserted rows using JDBC");
      try (Statement stmt = jdbcConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY row_id")) {

        assertTrue(rs.next());
        assertEquals("row1", rs.getString("row_id"));
        assertEquals(42, rs.getInt("int_col"));
        assertEquals(3.14159, rs.getDouble("double_col"), 0.00001);
        assertEquals(2.718f, rs.getFloat("float_col"), 0.001f);
        assertEquals("Hello GreptimeDB! 你好🚀", rs.getString("string_col"));
        assertTrue(rs.getBoolean("bool_col"));
        assertArrayEquals(binaryData1, rs.getBytes("binary_col"));

        assertTrue(rs.next());
        assertEquals("row2", rs.getString("row_id"));
        assertEquals(999, rs.getInt("int_col"));
        assertEquals(123.456, rs.getDouble("double_col"), 0.001);
        assertEquals(78.9f, rs.getFloat("float_col"), 0.01f);
        assertEquals("Ingester test 测试", rs.getString("string_col"));
        assertFalse(rs.getBoolean("bool_col"));
        assertArrayEquals(binaryData2, rs.getBytes("binary_col"));

        assertFalse(rs.next());
      }
      log.info("✓ Verified 2 rows inserted correctly");

      // UPDATE by inserting with SAME tag (row_id) and SAME timestamp
      log.info("Updating row by inserting with same tag (row_id) and timestamp using Ingester SDK");
      Table table3 = Table.from(tableSchema);
      byte[] binaryData3 = new byte[] {0x11, 0x22};
      table3.addRow(
          "row1", // Same row_id (tag/primary key)
          1732435200000L, // Same ts: 2024-11-24 10:00:00 in milliseconds
          100, // Updated int_col
          9.999, // Updated double_col
          1.234f, // Updated float_col
          "Updated value", // Updated string_col
          false, // Updated bool_col
          binaryData3 // Updated binary_col
          );
      table3.complete();

      CompletableFuture<Result<WriteOk, Err>> future3 = greptimeDB.write(table3);
      Result<WriteOk, Err> result3 = future3.get();
      assertTrue(result3.isOk(), "Update (overwrite) should succeed");
      log.info("Row updated (overwritten) successfully");

      // Verify the row was overwritten (not added)
      log.info("Verifying row was overwritten (not added) using JDBC");
      try (Statement stmt = jdbcConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY row_id, ts")) {

        // Should only have 2 rows total (row1 was overwritten, not added)
        assertTrue(rs.next());
        assertEquals("row1", rs.getString("row_id"));
        assertEquals(100, rs.getInt("int_col"), "row1 should be updated");
        assertEquals(9.999, rs.getDouble("double_col"), 0.001);
        assertEquals("Updated value", rs.getString("string_col"));
        assertFalse(rs.getBoolean("bool_col"));
        assertArrayEquals(binaryData3, rs.getBytes("binary_col"));

        assertTrue(rs.next());
        assertEquals("row2", rs.getString("row_id"));
        assertEquals(999, rs.getInt("int_col"));

        assertFalse(rs.next(), "Should have exactly 2 rows (row1 overwritten, not added)");
      }
      log.info("✓ Verified row was overwritten correctly");

      // Query with WHERE clause
      log.info("Testing JDBC SELECT with WHERE clause");
      try (PreparedStatement ps =
          jdbcConn.prepareStatement(
              "SELECT * FROM " + tableName + " WHERE int_col > ? ORDER BY row_id")) {
        ps.setInt(1, 50);
        try (ResultSet rs = ps.executeQuery()) {
          int count = 0;
          while (rs.next()) {
            count++;
            log.info("Found row: {}, int_col={}", rs.getString("row_id"), rs.getInt("int_col"));
          }
          assertEquals(2, count, "Should find 2 rows with int_col > 50");
        }
      }
      log.info("✓ WHERE clause query verified");

      // Cleanup
      log.info("Dropping table");
      dropTable(tableName);
      try (Statement stmt = jdbcConn.createStatement();
          ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'")) {
        assertFalse(rs.next());
      }

      log.info("Ingester insert/update test completed successfully");

    } catch (Exception e) {
      log.error("Test failed: {}", e.getMessage(), e);
      try {
        dropTable(tableName);
      } catch (SQLException cleanupEx) {
        log.warn("Cleanup failed: {}", cleanupEx.getMessage());
      }
      throw e;
    }
  }

  /**
   * Tests batch write operations using GreptimeDB Ingester SDK. Inserts multiple rows in a single
   * table and verifies using MySQL JDBC queries.
   */
  @Test
  void testIngesterBatchWrite() throws Exception {
    log.info("Starting ingester batch write test");
    connectJdbc();
    String tableName = "test_batch_ingester";

    try {
      log.info("Dropping table if exists: {}", tableName);
      dropTable(tableName);

      // Define table schema
      log.info("Creating table schema for batch write");
      TableSchema tableSchema =
          TableSchema.newBuilder(tableName)
              .addTag("row_id", DataType.String) // Primary key as tag
              .addTimestamp("ts", DataType.TimestampMillisecond) // Time index
              .addField("int_col", DataType.Int32)
              .addField("double_col", DataType.Float64)
              .addField("string_col", DataType.String)
              .addField("bool_col", DataType.Bool)
              .build();

      // Create table and insert multiple rows in batch
      int batchSize = 5;
      log.info("Inserting {} rows in batch using Ingester SDK", batchSize);
      Table table = Table.from(tableSchema);

      // Add 5 rows to the table
      for (int i = 1; i <= batchSize; i++) {
        long timestamp = 1732435200000L + (i * 3600000L); // Base time + i hours
        table.addRow(
            "batch_row_" + i, // row_id (tag)
            timestamp, // ts
            i * 100, // int_col
            i * 1.5, // double_col
            "Batch test row " + i + " 测试", // string_col
            i % 2 == 0 // bool_col
            );
        log.info("Added row {} to batch: batch_row_{}, ts={}", i, i, timestamp);
      }
      table.complete();

      // Write batch
      log.info("Writing batch to GreptimeDB");
      CompletableFuture<Result<WriteOk, Err>> future = greptimeDB.write(table);
      Result<WriteOk, Err> result = future.get();
      assertTrue(result.isOk(), "Batch write should succeed");
      log.info("Batch write completed successfully");

      // Verify all rows were inserted correctly using JDBC
      log.info("Verifying inserted rows using JDBC");
      try (Statement stmt = jdbcConn.createStatement();
          ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " ORDER BY row_id")) {

        int rowCount = 0;
        while (rs.next()) {
          rowCount++;
          String rowId = rs.getString("row_id");
          int intCol = rs.getInt("int_col");
          double doubleCol = rs.getDouble("double_col");
          String stringCol = rs.getString("string_col");
          boolean boolCol = rs.getBoolean("bool_col");

          log.info(
              "Row {}: {} | int={}, double={}, string={}, bool={}",
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
        log.info("✓ Verified all {} rows were inserted correctly", batchSize);
      }

      // Cleanup
      log.info("Dropping table");
      dropTable(tableName);
      try (Statement stmt = jdbcConn.createStatement();
          ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + tableName + "'")) {
        assertFalse(rs.next());
      }

      log.info("Ingester batch write test completed successfully");

    } catch (Exception e) {
      log.error("Batch write test failed: {}", e.getMessage(), e);
      try {
        dropTable(tableName);
      } catch (SQLException cleanupEx) {
        log.warn("Cleanup failed: {}", cleanupEx.getMessage());
      }
      throw e;
    }
  }
}
