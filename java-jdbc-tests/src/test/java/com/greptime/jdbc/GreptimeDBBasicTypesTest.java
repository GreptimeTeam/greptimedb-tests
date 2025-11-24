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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for GreptimeDB JDBC drivers (MySQL and PostgreSQL).
 * Tests comprehensive CRUD operations with all basic data types in a single table.
 *
 * Note: The test database should be created by the run_tests.sh script before running these tests.
 * The database name is derived from the test suite directory name.
 */
public class GreptimeDBBasicTypesTest {

    private Connection connection;
    private String driverType;
    private static final String TABLE_NAME_PREFIX = "test_all_types_";

    @BeforeEach
    void setUp() {
        // Connection will be established in each parameterized test
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    private void setupConnection(String driver) throws SQLException {
        setupConnection(driver, null);
    }

    private void setupConnection(String driver, String timezone) throws SQLException {
        this.driverType = driver;
        String url;
        String user = "";
        String password = "";

        if ("mysql".equals(driver)) {
            String baseUrl;
            String mysqlUrl = System.getenv("MYSQL_URL");
            if (mysqlUrl != null && !mysqlUrl.trim().isEmpty()) {
                // Use explicitly provided URL
                baseUrl = mysqlUrl;
            } else {
                // Build URL from DB_NAME or use default
                String dbName = getEnvOrDefault("DB_NAME", "public");
                String host = getEnvOrDefault("MYSQL_HOST", "127.0.0.1");
                String port = getEnvOrDefault("MYSQL_PORT", "4002");
                baseUrl = "jdbc:mysql://" + host + ":" + port + "/" + dbName;
            }

            if (timezone != null) {
                // Add timezone parameters for MySQL
                String separator = baseUrl.contains("?") ? "&" : "?";
                url = baseUrl + separator + "connectionTimeZone=" + timezone +
                      "&forceConnectionTimeZoneToSession=true";
            } else {
                url = baseUrl;
            }
        } else if ("postgresql".equals(driver)) {
            String baseUrl;
            String postgresUrl = System.getenv("POSTGRES_URL");
            if (postgresUrl != null && !postgresUrl.trim().isEmpty()) {
                // Use explicitly provided URL
                baseUrl = postgresUrl;
            } else {
                // Build URL from DB_NAME or use default
                String dbName = getEnvOrDefault("DB_NAME", "public");
                String host = getEnvOrDefault("POSTGRES_HOST", "127.0.0.1");
                String port = getEnvOrDefault("POSTGRES_PORT", "4003");
                baseUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
            }

            if (timezone != null) {
                // Add timezone parameter for PostgreSQL
                String separator = baseUrl.contains("?") ? "&" : "?";
                url = baseUrl + separator + "TimeZone=" + timezone;
            } else {
                url = baseUrl;
            }
        } else {
            throw new IllegalArgumentException("Unknown driver: " + driver);
        }

        connection = DriverManager.getConnection(url, user, password);
        assertNotNull(connection, "Connection should not be null");
        assertFalse(connection.isClosed(), "Connection should be open");
    }

    /**
     * Helper method to get environment variable value or return default if null or empty.
     */
    private static String getEnvOrDefault(String envName, String defaultValue) {
        String value = System.getenv(envName);
        return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
    }

    /**
     * Execute DML statement with driver-specific handling.
     * PostgreSQL requires execute() to avoid "A result was returned when none was expected" error.
     * MySQL uses executeUpdate() to get row count.
     */
    private void executeDML(Statement stmt, String sql) throws SQLException {
        if ("postgresql".equals(driverType)) {
            stmt.execute(sql);
        } else {
            stmt.executeUpdate(sql);
        }
    }

    /**
     * Execute DML with PreparedStatement, driver-specific handling.
     */
    private void executeDML(PreparedStatement ps) throws SQLException {
        if ("postgresql".equals(driverType)) {
            ps.execute();
        } else {
            ps.executeUpdate();
        }
    }

    private String getTableName() {
        return TABLE_NAME_PREFIX + driverType;
    }

    private void createTable() throws SQLException {
        String createTableSQL = String.format(
            "CREATE TABLE IF NOT EXISTS %s (" +
            "ts TIMESTAMP TIME INDEX, " +
            "row_id STRING PRIMARY KEY, " +
            "int_col INTEGER, " +
            "double_col DOUBLE, " +
            "float_col FLOAT, " +
            "string_col STRING, " +
            "date_col DATE, " +
            "timestamp_col TIMESTAMP, " +
            "bool_col BOOLEAN, " +
            "binary_col BINARY)",
            getTableName()
        );

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("Table created: " + getTableName());
        }
    }

    private void dropTable() throws SQLException {
        String dropTableSQL = String.format("DROP TABLE IF EXISTS %s", getTableName());
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(dropTableSQL);
            System.out.println("Table dropped: " + getTableName());
        }
    }

    /**
     * Comprehensive CRUD test covering all basic data types.
     * Tests both SQL literal INSERT and PreparedStatement INSERT.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mysql", "postgresql"})
    void testAllTypesComprehensive(String driver) throws SQLException {
        setupConnection(driver);

        try {
            // Step 1: Create table with all types
            System.out.println("\n[" + driverType + "] ========== Step 1: CREATE TABLE ==========");
            createTable();

            // Step 2: INSERT with SQL literal
            System.out.println("\n[" + driverType + "] ========== Step 2: INSERT (SQL Literal) ==========");
            String literalInsert = String.format(
                "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) " +
                "VALUES ('2024-11-24 10:00:00', 'row1', 42, 3.14159, 2.718, 'Hello GreptimeDB! 你好🚀', '2024-01-15', '2024-11-24 10:00:00', true, X'0102030405FFFE')",
                getTableName()
            );

            try (Statement stmt = connection.createStatement()) {
                executeDML(stmt, literalInsert);
                System.out.println("Row inserted (literal)");
            }

            // Step 3: INSERT with PreparedStatement
            System.out.println("\n[" + driverType + "] ========== Step 3: INSERT (PreparedStatement) ==========");
            String preparedInsert = String.format(
                "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                getTableName()
            );

            Timestamp ts2 = Timestamp.valueOf("2024-11-24 11:00:00");
            Timestamp eventTime2 = Timestamp.valueOf("2024-11-24 11:00:00");
            Date date2 = Date.valueOf("2024-06-20");
            byte[] binary2 = new byte[]{0x0A, 0x0B, 0x0C, (byte) 0xAB, (byte) 0xCD};

            try (PreparedStatement ps = connection.prepareStatement(preparedInsert)) {
                ps.setTimestamp(1, ts2);
                ps.setString(2, "row2");
                ps.setInt(3, 999);
                ps.setDouble(4, 123.456);
                ps.setFloat(5, 78.9f);
                ps.setString(6, "PreparedStatement test 测试");
                ps.setDate(7, date2);
                ps.setTimestamp(8, eventTime2);
                ps.setBoolean(9, false);
                ps.setBytes(10, binary2);

                executeDML(ps);
                System.out.println("Row inserted (prepared)");
            }

            // Step 4: SELECT and verify both rows
            System.out.println("\n[" + driverType + "] ========== Step 4: SELECT (Read) ==========");
            String selectAll = String.format("SELECT * FROM %s ORDER BY row_id", getTableName());

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectAll)) {

                // Verify row1 (from literal insert)
                assertTrue(rs.next(), "Should have first row");
                assertEquals("row1", rs.getString("row_id"));
                assertEquals(42, rs.getInt("int_col"));
                assertEquals(3.14159, rs.getDouble("double_col"), 0.00001);
                assertEquals(2.718f, rs.getFloat("float_col"), 0.001f);
                assertEquals("Hello GreptimeDB! 你好🚀", rs.getString("string_col"));
                assertEquals("2024-01-15", rs.getDate("date_col").toString());
                assertTrue(rs.getBoolean("bool_col"));

                byte[] binary1Expected = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF, (byte) 0xFE};
                assertArrayEquals(binary1Expected, rs.getBytes("binary_col"));

                System.out.println("Row 1 verified: row_id=" + rs.getString("row_id") +
                                 ", int=" + rs.getInt("int_col") +
                                 ", double=" + rs.getDouble("double_col") +
                                 ", string=" + rs.getString("string_col"));

                // Verify row2 (from prepared insert)
                assertTrue(rs.next(), "Should have second row");
                assertEquals("row2", rs.getString("row_id"));
                assertEquals(999, rs.getInt("int_col"));
                assertEquals(123.456, rs.getDouble("double_col"), 0.001);
                assertEquals(78.9f, rs.getFloat("float_col"), 0.01f);
                assertEquals("PreparedStatement test 测试", rs.getString("string_col"));
                assertEquals("2024-06-20", rs.getDate("date_col").toString());
                assertFalse(rs.getBoolean("bool_col"));
                assertArrayEquals(binary2, rs.getBytes("binary_col"));

                System.out.println("Row 2 verified: row_id=" + rs.getString("row_id") +
                                 ", int=" + rs.getInt("int_col") +
                                 ", double=" + rs.getDouble("double_col") +
                                 ", string=" + rs.getString("string_col"));

                assertFalse(rs.next(), "Should have exactly two rows");
            }

            // Step 5: UPDATE (insert with later timestamp to update row1)
            System.out.println("\n[" + driverType + "] ========== Step 5: UPDATE ==========");
            String updateInsert = String.format(
                "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                getTableName()
            );

            Timestamp updateTs = Timestamp.valueOf("2024-11-24 12:00:00");
            try (PreparedStatement ps = connection.prepareStatement(updateInsert)) {
                ps.setTimestamp(1, updateTs);
                ps.setString(2, "row1");  // Same row_id to update
                ps.setInt(3, 100);        // Updated value
                ps.setDouble(4, 9.999);   // Updated value
                ps.setFloat(5, 1.234f);   // Updated value
                ps.setString(6, "Updated value");
                ps.setDate(7, Date.valueOf("2024-12-25"));
                ps.setTimestamp(8, updateTs);
                ps.setBoolean(9, false);  // Changed from true to false
                ps.setBytes(10, new byte[]{0x11, 0x22});

                executeDML(ps);
                System.out.println("Row updated");
            }

            // Verify update - get latest version of row1
            String selectUpdated = String.format(
                "SELECT * FROM %s WHERE row_id = 'row1' ORDER BY ts DESC LIMIT 1",
                getTableName()
            );

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectUpdated)) {
                assertTrue(rs.next(), "Should have updated row");
                assertEquals("row1", rs.getString("row_id"));
                assertEquals(100, rs.getInt("int_col"), "int_col should be updated");
                assertEquals(9.999, rs.getDouble("double_col"), 0.001, "double_col should be updated");
                assertEquals("Updated value", rs.getString("string_col"), "string_col should be updated");
                assertFalse(rs.getBoolean("bool_col"), "bool_col should be updated to false");

                System.out.println("Update verified: int_col=" + rs.getInt("int_col") +
                                 ", string_col=" + rs.getString("string_col"));
            }

            // Step 6: Test SELECT with WHERE conditions
            System.out.println("\n[" + driverType + "] ========== Step 6: SELECT with WHERE ==========");
            String selectWhere = String.format(
                "SELECT * FROM %s WHERE int_col > ? ORDER BY row_id",
                getTableName()
            );

            try (PreparedStatement ps = connection.prepareStatement(selectWhere)) {
                ps.setInt(1, 50);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        System.out.println("Found: row_id=" + rs.getString("row_id") + ", int_col=" + rs.getInt("int_col"));
                    }
                    assertTrue(count > 0, "Should find rows with int_col > 50");
                }
            }

            // Step 7: DELETE (drop table)
            System.out.println("\n[" + driverType + "] ========== Step 7: DELETE (Drop Table) ==========");
            dropTable();

            // Verify table is dropped
            String verifyDrop = String.format("SHOW TABLES LIKE '%s'", getTableName());
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(verifyDrop)) {
                assertFalse(rs.next(), "Table should be dropped");
                System.out.println("Table successfully dropped and verified");
            }

            System.out.println("\n[" + driverType + "] ========== All Tests Passed! ==========\n");

        } catch (SQLException e) {
            System.err.println("[" + driverType + "] Test failed: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * Tests timezone handling with different timezone settings.
     * Verifies that timezone affects how timestamps are interpreted and displayed.
     */
    @ParameterizedTest
    @ValueSource(strings = {"mysql", "postgresql"})
    void testTimezone(String driver) throws SQLException {
        String tableName = "test_timezone_" + driver;

        try {
            // Step 1: Create table with UTC timezone
            System.out.println("\n[" + driver + "] ========== Timezone Test Start ==========");
            System.out.println("\n[" + driver + "] ========== Step 1: CREATE TABLE (UTC) ==========");

            setupConnection(driver, "UTC");

            String createTable = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                "ts TIMESTAMP TIME INDEX, " +
                "row_id STRING PRIMARY KEY, " +
                "event_time TIMESTAMP, " +
                "description STRING)",
                tableName
            );

            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createTable);
                System.out.println("Table created: " + tableName);
            }

            // Step 2: Insert data with UTC timezone using PreparedStatement
            System.out.println("\n[" + driver + "] ========== Step 2: INSERT with UTC Timezone ==========");

            // Insert a known timestamp: 2024-11-24 10:00:00 UTC
            Timestamp utcTimestamp = Timestamp.valueOf("2024-11-24 10:00:00");

            String insert = String.format(
                "INSERT INTO %s (ts, row_id, event_time, description) VALUES (?, ?, ?, ?)",
                tableName
            );

            try (PreparedStatement ps = connection.prepareStatement(insert)) {
                ps.setTimestamp(1, utcTimestamp);
                ps.setString(2, "event1");
                ps.setTimestamp(3, utcTimestamp);
                ps.setString(4, "Inserted with UTC timezone");
                executeDML(ps);
                System.out.println("Inserted with UTC: " + utcTimestamp);
            }

            // Step 3: Query with UTC timezone and verify
            System.out.println("\n[" + driver + "] ========== Step 3: SELECT with UTC Timezone ==========");

            String select = String.format("SELECT * FROM %s WHERE row_id = 'event1'", tableName);
            Timestamp retrievedUtc;

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(select)) {
                assertTrue(rs.next(), "Should find the inserted row");
                retrievedUtc = rs.getTimestamp("event_time");
                System.out.println("Retrieved with UTC: " + retrievedUtc);
                System.out.println("Description: " + rs.getString("description"));

                // Verify timestamp matches
                assertEquals(utcTimestamp.getTime(), retrievedUtc.getTime(),
                    "UTC timestamp should match exactly");
            }

            // Close UTC connection
            connection.close();

            // Step 4: Reconnect with Asia/Shanghai timezone and query same data
            System.out.println("\n[" + driver + "] ========== Step 4: SELECT with Asia/Shanghai Timezone ==========");

            setupConnection(driver, "Asia/Shanghai");

            Timestamp retrievedShanghai;
            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(select)) {
                assertTrue(rs.next(), "Should find the inserted row");
                retrievedShanghai = rs.getTimestamp("event_time");
                System.out.println("Retrieved with Asia/Shanghai: " + retrievedShanghai);

                // The underlying timestamp should be the same (same epoch millis)
                assertEquals(retrievedUtc.getTime(), retrievedShanghai.getTime(),
                    "Epoch milliseconds should be the same regardless of timezone");

                // But the string representation might differ based on timezone
                System.out.println("UTC timestamp millis: " + retrievedUtc.getTime());
                System.out.println("Shanghai timestamp millis: " + retrievedShanghai.getTime());
            }

            connection.close();

            // Step 5: Insert with Asia/Shanghai timezone
            System.out.println("\n[" + driver + "] ========== Step 5: INSERT with Asia/Shanghai Timezone ==========");

            setupConnection(driver, "Asia/Shanghai");

            // When we say "2024-11-24 18:00:00" in Shanghai timezone,
            // it represents the same moment as "2024-11-24 10:00:00" UTC (Shanghai is UTC+8)
            Timestamp shanghaiTimestamp = Timestamp.valueOf("2024-11-24 18:00:00");

            try (PreparedStatement ps = connection.prepareStatement(insert)) {
                ps.setTimestamp(1, Timestamp.valueOf("2024-11-24 18:00:01"));
                ps.setString(2, "event2");
                ps.setTimestamp(3, shanghaiTimestamp);
                ps.setString(4, "Inserted with Asia/Shanghai timezone");
                executeDML(ps);
                System.out.println("Inserted with Asia/Shanghai: " + shanghaiTimestamp);
            }

            connection.close();

            // Step 6: Insert data using SQL literal with timezone
            System.out.println("\n[" + driver + "] ========== Step 6: INSERT with SQL Literal ==========");

            setupConnection(driver, "America/New_York");

            // Insert using literal timestamp - will be interpreted in America/New_York timezone
            String literalInsert = String.format(
                "INSERT INTO %s (ts, row_id, event_time, description) " +
                "VALUES ('2024-11-24 05:00:00', 'event3', '2024-11-24 05:00:00', 'Inserted with America/New_York timezone')",
                tableName
            );

            try (Statement stmt = connection.createStatement()) {
                executeDML(stmt, literalInsert);
                System.out.println("Inserted with SQL literal in America/New_York timezone");
            }

            connection.close();

            // Step 7: Query all events with UTC timezone to compare
            System.out.println("\n[" + driver + "] ========== Step 7: Compare All Events with UTC ==========");

            setupConnection(driver, "UTC");

            String selectAll = String.format(
                "SELECT row_id, event_time, description FROM %s ORDER BY row_id",
                tableName
            );

            try (Statement stmt = connection.createStatement();
                 ResultSet rs = stmt.executeQuery(selectAll)) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    String rowId = rs.getString("row_id");
                    Timestamp eventTime = rs.getTimestamp("event_time");
                    String description = rs.getString("description");

                    System.out.println(String.format(
                        "Event %s: %s (%s) - %s",
                        rowId, eventTime, eventTime.getTime(), description
                    ));
                }

                assertEquals(3, count, "Should have exactly 3 events");

                System.out.println("\nNote: All three events represent approximately the same moment in time,");
                System.out.println("but were inserted using different timezone contexts:");
                System.out.println("  - event1: 2024-11-24 10:00:00 UTC");
                System.out.println("  - event2: 2024-11-24 18:00:00 Asia/Shanghai (UTC+8)");
                System.out.println("  - event3: 2024-11-24 05:00:00 America/New_York (UTC-5)");
            }

            // Step 8: Cleanup
            System.out.println("\n[" + driver + "] ========== Step 8: Cleanup ==========");

            String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropTable);
                System.out.println("Table dropped: " + tableName);
            }

            System.out.println("\n[" + driver + "] ========== Timezone Test Passed! ==========\n");

        } catch (SQLException e) {
            System.err.println("[" + driver + "] Timezone test failed: " + e.getMessage());
            e.printStackTrace();

            // Cleanup on failure
            try {
                if (connection != null && !connection.isClosed()) {
                    String dropTable = String.format("DROP TABLE IF EXISTS %s", tableName);
                    try (Statement stmt = connection.createStatement()) {
                        stmt.execute(dropTable);
                    }
                }
            } catch (SQLException cleanupEx) {
                // Ignore cleanup errors
            }

            throw e;
        }
    }
}
