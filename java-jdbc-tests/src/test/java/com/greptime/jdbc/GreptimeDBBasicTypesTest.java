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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for GreptimeDB JDBC drivers (MySQL and PostgreSQL).
 * Validates CRUD operations across all supported data types.
 */
public class GreptimeDBBasicTypesTest {

    private Connection conn;
    private String driver;

    @AfterEach
    void tearDown() throws SQLException {
        if (conn != null && !conn.isClosed()) {
            conn.close();
        }
    }

    private void connect(String driverType) throws SQLException {
        connect(driverType, null);
    }

    private void connect(String driverType, String timezone) throws SQLException {
        this.driver = driverType;
        String url = buildUrl(driverType, timezone);
        conn = DriverManager.getConnection(url, "", "");
        assertNotNull(conn);
        assertFalse(conn.isClosed());
    }

    private String buildUrl(String driverType, String timezone) {
        String baseUrl = getBaseUrl(driverType);
        if (timezone == null) {
            return baseUrl;
        }

        String separator = baseUrl.contains("?") ? "&" : "?";
        if ("mysql".equals(driverType)) {
            return baseUrl + separator + "connectionTimeZone=" + timezone +
                   "&forceConnectionTimeZoneToSession=true";
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

    private void createTable(String table, String schema) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS " + table + " (" + schema + ")");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"mysql", "postgresql"})
    void testCrudOperations(String driverType) throws SQLException {
        connect(driverType);
        String table = tableName();

        try {
            dropTable(table);
            createTable(table,
                "ts TIMESTAMP TIME INDEX, " +
                "row_id STRING PRIMARY KEY, " +
                "int_col INTEGER, " +
                "double_col DOUBLE, " +
                "float_col FLOAT, " +
                "string_col STRING, " +
                "date_col DATE, " +
                "timestamp_col TIMESTAMP, " +
                "bool_col BOOLEAN, " +
                "binary_col BINARY"
            );

            // INSERT with SQL literal
            String literalInsert = String.format(
                "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) " +
                "VALUES ('2024-11-24 10:00:00', 'row1', 42, 3.14159, 2.718, 'Hello GreptimeDB! 你好🚀', '2024-01-15', '2024-11-24 10:00:00', true, X'0102030405FFFE')",
                table
            );
            try (Statement stmt = conn.createStatement()) {
                execute(stmt, literalInsert);
            }

            // INSERT with PreparedStatement
            String preparedInsert = String.format(
                "INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                table
            );

            Timestamp ts = Timestamp.valueOf("2024-11-24 11:00:00");
            Date date = Date.valueOf("2024-06-20");
            byte[] binary = new byte[]{0x0A, 0x0B, 0x0C, (byte) 0xAB, (byte) 0xCD};

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
                assertArrayEquals(new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF, (byte) 0xFE},
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

            // UPDATE (insert with later timestamp)
            Timestamp updateTs = Timestamp.valueOf("2024-11-24 12:00:00");
            try (PreparedStatement ps = conn.prepareStatement(preparedInsert)) {
                ps.setTimestamp(1, updateTs);
                ps.setString(2, "row1");
                ps.setInt(3, 100);
                ps.setDouble(4, 9.999);
                ps.setFloat(5, 1.234f);
                ps.setString(6, "Updated value");
                ps.setDate(7, Date.valueOf("2024-12-25"));
                ps.setTimestamp(8, updateTs);
                ps.setBoolean(9, false);
                ps.setBytes(10, new byte[]{0x11, 0x22});
                execute(ps);
            }

            // Verify update
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT * FROM " + table + " WHERE row_id = 'row1' ORDER BY ts DESC LIMIT 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt("int_col"));
                assertEquals(9.999, rs.getDouble("double_col"), 0.001);
                assertEquals("Updated value", rs.getString("string_col"));
                assertFalse(rs.getBoolean("bool_col"));
            }

            // SELECT with WHERE
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT * FROM " + table + " WHERE int_col > ? ORDER BY row_id")) {
                ps.setInt(1, 50);
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) count++;
                    assertTrue(count > 0);
                }
            }

            // DELETE
            dropTable(table);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SHOW TABLES LIKE '" + table + "'")) {
                assertFalse(rs.next());
            }

        } catch (SQLException e) {
            System.err.println("[" + driver + "] Test failed: " + e.getMessage());
            throw e;
        }
    }

}
