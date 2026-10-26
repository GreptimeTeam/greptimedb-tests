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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Integration tests for GreptimeDB PostgreSQL {@code COPY ... FROM STDIN} (the copy-in
 * sub-protocol) through the PostgreSQL JDBC driver's {@link CopyManager}.
 *
 * <p>GreptimeDB currently implements only {@code WITH (FORMAT csv)}; PostgreSQL's default
 * text/binary copy formats are rejected with guidance. See greptimedb#9297.
 */
public class GreptimeDBPostgresCopyInTest {

  private static final Logger log = LoggerFactory.getLogger(GreptimeDBPostgresCopyInTest.class);

  private Connection conn;

  @AfterEach
  void tearDown() throws SQLException {
    if (conn != null && !conn.isClosed()) {
      conn.close();
    }
  }

  private void connect() throws SQLException {
    String url = buildUrl();
    String username = getEnv("GREPTIME_USERNAME", "");
    String password = getEnv("GREPTIME_PASSWORD", "");
    log.info("Connecting to PostgreSQL with CopyManager, URL: {}", url);
    conn = DriverManager.getConnection(url, username, password);
    assertNotNull(conn);
    // pgjdbc sends the JVM default timezone as the TimeZone startup parameter,
    // which would make naive-timestamp semantics depend on the environment.
    // Pin the session timezone so assertions are deterministic.
    execute("SET TIME ZONE = 'UTC'");
  }

  private String buildUrl() {
    String url = System.getenv("POSTGRES_URL");
    if (url != null && !url.trim().isEmpty()) {
      return url;
    }
    String db = getEnv("DB_NAME", "public");
    String host = getEnv("POSTGRES_HOST", "127.0.0.1");
    String port = getEnv("POSTGRES_PORT", "4003");
    return "jdbc:postgresql://" + host + ":" + port + "/" + db;
  }

  private String getEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    return (value != null && !value.trim().isEmpty()) ? value : defaultValue;
  }

  private CopyManager copyManager() throws SQLException {
    return conn.unwrap(PGConnection.class).getCopyAPI();
  }

  private long copyIn(String sql, String data) throws SQLException, IOException {
    return copyManager().copyIn(sql, new StringReader(data));
  }

  private void execute(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  private int queryInt(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      int value = rs.getInt(1);
      assertTrue(!rs.next());
      return value;
    }
  }

  private boolean queryBoolean(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      boolean value = rs.getBoolean(1);
      assertTrue(!rs.next());
      return value;
    }
  }

  /** Reads a single BIGINT (e.g. `CAST(ts AS BIGINT)`), immune to the JVM default timezone. */
  private long queryLong(String sql) throws SQLException {
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      long value = rs.getLong(1);
      assertTrue(!rs.next());
      return value;
    }
  }

  private void dropTable(String table) throws SQLException {
    execute("DROP TABLE IF EXISTS " + table);
  }

  private static final String METRICS_SCHEMA =
      "host STRING, val DOUBLE, note STRING, ts TIMESTAMP, TIME INDEX(ts)";

  /** Basic CSV COPY: NULL from an unquoted empty field, quoted commas, escaped quotes. */
  @Test
  void testCopyInBasic() throws Exception {
    connect();
    String table = "test_copy_in_basic_jdbc";
    dropTable(table);
    execute("CREATE TABLE " + table + " (" + METRICS_SCHEMA + ")");

    String data =
        "host1,1.5,hello,2023-11-14 22:13:20\n"
            + "host2,,\"quoted, comma\",2023-11-14 22:13:21\n"
            + "host3,3.5,\"say \"\"hi\"\"\",2023-11-14 22:13:22\n";
    long rows = copyIn("COPY " + table + " FROM STDIN WITH (FORMAT csv)", data);
    assertEquals(3L, rows);

    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT host, val, note FROM " + table + " ORDER BY ts")) {
      assertTrue(rs.next());
      assertEquals("host1", rs.getString(1));
      assertEquals(1.5, rs.getDouble(2), 0.0001);
      assertEquals("hello", rs.getString(3));
      assertTrue(rs.next());
      // Unquoted empty field is NULL.
      assertEquals(0.0, rs.getDouble(2), 0.0);
      assertTrue(rs.wasNull());
      assertEquals("quoted, comma", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("say \"hi\"", rs.getString(3));
      assertTrue(!rs.next());
    }
    // The session timezone is pinned to UTC by connect(), so the naive
    // '2023-11-14 22:13:20' is stored as that UTC instant
    // (2023-11-14T22:13:20Z == 1700000000).
    assertEquals(
        1700000000000L,
        queryLong("SELECT CAST(ts AS BIGINT) FROM " + table + " WHERE host = 'host1'"));

    dropTable(table);
  }

  /** A column list may select and reorder a subset of columns; the rest are NULL. */
  @Test
  void testCopyInColumnList() throws Exception {
    connect();
    String table = "test_copy_in_columns_jdbc";
    dropTable(table);
    execute("CREATE TABLE " + table + " (" + METRICS_SCHEMA + ")");

    long rows =
        copyIn(
            "COPY " + table + " (note, ts, val) FROM STDIN WITH (FORMAT csv)",
            "from_column_list,2023-11-14 22:13:24,7.25\n");
    assertEquals(1L, rows);

    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT host, val, note FROM " + table)) {
      assertTrue(rs.next());
      assertNull(rs.getString(1)); // host not targeted: NULL
      assertEquals(7.25, rs.getDouble(2), 0.0001);
      assertEquals("from_column_list", rs.getString(3));
      assertTrue(!rs.next());
    }

    // A column list omitting the time index column is rejected.
    SQLException e =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY " + table + " (host) FROM STDIN WITH (FORMAT csv)", "host1\n"));
    assertTrue(e.getMessage().contains("time index"), e.getMessage());

    dropTable(table);
  }

  /** HEADER, a custom delimiter, CRLF separators and a quoted multi-line field. */
  @Test
  void testCopyInCsvOptions() throws Exception {
    connect();
    String table = "test_copy_in_options_jdbc";
    dropTable(table);
    execute("CREATE TABLE " + table + " (" + METRICS_SCHEMA + ")");

    String data =
        "host;val;note;ts\r\n"
            + "h1;1.5;\"line one\nline two\";2023-11-14 22:13:20\r\n"
            + "h2;2.5;plain;2023-11-14 22:13:21\r\n";
    // WITH omitted, matching PostgreSQL's grammar.
    long rows = copyIn("COPY " + table + " FROM STDIN (FORMAT csv, HEADER, DELIMITER ';')", data);
    assertEquals(2L, rows); // the header record is skipped

    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT host, val, note FROM " + table + " ORDER BY ts")) {
      assertTrue(rs.next());
      assertEquals("h1", rs.getString(1));
      assertEquals(1.5, rs.getDouble(2), 0.0001);
      assertEquals("line one\nline two", rs.getString(3));
      assertTrue(rs.next());
      assertEquals("h2", rs.getString(1));
      assertEquals("plain", rs.getString(3));
      assertTrue(!rs.next());
    }

    dropTable(table);
  }

  /** A stream larger than the server's 32768-row flush batch crosses batch boundaries. */
  @Test
  void testCopyInLargeBatch() throws Exception {
    connect();
    String table = "test_copy_in_large_jdbc";
    dropTable(table);
    execute("CREATE TABLE " + table + " (host STRING, seq BIGINT, ts TIMESTAMP, TIME INDEX(ts))");

    int total = 40000;
    DateTimeFormatter tsFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    LocalDateTime base = LocalDateTime.of(2024, 1, 1, 0, 0, 0);
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < total; i++) {
      // A unique millisecond timestamp per row: without primary keys a
      // repeated time index would upsert instead of appending.
      String ts = base.plusNanos(i * 1_000_000L).format(tsFormat);
      buf.append(String.format("host_%d,%d,%s%n", i, i, ts));
    }
    long rows =
        copyIn("COPY " + table + " (host, seq, ts) FROM STDIN WITH (FORMAT csv)", buf.toString());
    assertEquals((long) total, rows);
    assertEquals(total, queryInt("SELECT COUNT(*) FROM " + table));

    // Spot-check the first and last rows.
    assertEquals(0, queryInt("SELECT seq FROM " + table + " WHERE host = 'host_0'"));
    assertEquals(
        total - 1,
        queryInt("SELECT seq FROM " + table + " WHERE host = 'host_" + (total - 1) + "'"));

    dropTable(table);
  }

  /** COPY and INSERT agree on offset-free timestamps: both use the session timezone. */
  @Test
  void testCopyInSessionTimezoneParity() throws Exception {
    connect();
    String table = "test_copy_in_tz_jdbc";
    dropTable(table);
    execute(
        "CREATE TABLE " + table + " (case_id STRING PRIMARY KEY, ts TIMESTAMP, TIME INDEX(ts))");
    execute("SET TIME ZONE = 'Asia/Shanghai'");

    execute("INSERT INTO " + table + " (case_id, ts) VALUES ('insert', '2024-01-01 00:00:00')");
    long rows =
        copyIn(
            "COPY " + table + " (case_id, ts) FROM STDIN WITH (FORMAT csv)",
            "copy,2024-01-01 00:00:00\n");
    assertEquals(1L, rows);

    try (Statement stmt = conn.createStatement();
        ResultSet rs =
            stmt.executeQuery(
                "SELECT case_id, CAST(ts AS BIGINT) FROM " + table + " ORDER BY case_id")) {
      assertTrue(rs.next());
      assertEquals("copy", rs.getString(1));
      long copyEpoch = rs.getLong(2);
      assertTrue(rs.next());
      assertEquals("insert", rs.getString(1));
      long insertEpoch = rs.getLong(2);
      assertTrue(!rs.next());
      assertEquals(insertEpoch, copyEpoch, "COPY and INSERT must agree on naive timestamps");
      assertEquals(1704038400000L, copyEpoch); // the Asia/Shanghai interpretation
    }

    execute("SET TIME ZONE = 'UTC'");
    dropTable(table);
  }

  /** Statement-level failures, a clean table after bad data, and connection reuse. */
  @Test
  void testCopyInErrors() throws Exception {
    connect();
    String table = "test_copy_in_errors_jdbc";
    dropTable(table);
    execute("CREATE TABLE " + table + " (" + METRICS_SCHEMA + ")");
    String okRow = "host1,1.5,ok,2023-11-14 22:13:20\n";

    // COPY without an explicit format is rejected with guidance.
    SQLException e =
        assertThrows(SQLException.class, () -> copyIn("COPY " + table + " FROM STDIN", okRow));
    assertTrue(e.getMessage().contains("requires an explicit format"), e.getMessage());

    // PostgreSQL's text and binary formats are not implemented yet.
    for (String fmt : new String[] {"text", "txt", "binary"}) {
      SQLException fe =
          assertThrows(
              SQLException.class,
              () -> copyIn("COPY " + table + " FROM STDIN WITH (FORMAT " + fmt + ")", okRow));
      assertTrue(fe.getMessage().contains("not supported"), fe.getMessage());
    }

    // Unknown format.
    SQLException ue =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY " + table + " FROM STDIN WITH (FORMAT parquet)", okRow));
    assertTrue(ue.getMessage().contains("unknown COPY format"), ue.getMessage());

    // Unknown table.
    SQLException te =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY copy_in_does_not_exist FROM STDIN WITH (FORMAT csv)", okRow));
    assertTrue(te.getMessage().contains("does not exist"), te.getMessage());

    // Options the csv format does not understand are rejected.
    SQLException oe =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY " + table + " FROM STDIN WITH (FORMAT csv, PRECISION 'ns')", okRow));
    assertTrue(oe.getMessage().contains("not supported by format csv"), oe.getMessage());

    // A custom NULL string is not supported (an empty field is NULL).
    SQLException ne =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY " + table + " FROM STDIN WITH (FORMAT csv, NULL 'N')", okRow));
    assertTrue(ne.getMessage().contains("custom NULL string"), ne.getMessage());

    // COPY FROM STDIN must run alone.
    SQLException me =
        assertThrows(
            SQLException.class,
            () -> copyIn("COPY " + table + " FROM STDIN WITH (FORMAT csv); SELECT 1", okRow));
    assertTrue(me.getMessage().contains("alone"), me.getMessage());

    // Bad data aborts the whole copy and writes nothing.
    SQLException be =
        assertThrows(
            SQLException.class,
            () ->
                copyIn(
                    "COPY " + table + " FROM STDIN WITH (FORMAT csv)",
                    "host1,1.5,ok,2023-11-14 22:13:20\n"
                        + "host2,not_a_double,ok,2023-11-14 22:13:21\n"));
    assertTrue(be.getMessage().contains("failed to parse"), be.getMessage());
    assertEquals(0, queryInt("SELECT COUNT(*) FROM " + table));

    // The connection stays usable: a new COPY succeeds on it.
    long rows = copyIn("COPY " + table + " FROM STDIN WITH (FORMAT csv)", okRow);
    assertEquals(1L, rows);
    assertEquals(1, queryInt("SELECT COUNT(*) FROM " + table));

    dropTable(table);
  }

  /** COPY OUT (FORMAT csv) bytes import back unchanged: t/f booleans, offset timestamps. */
  @Test
  void testCopyOutCopyInRoundTrip() throws Exception {
    connect();
    String src = "test_copy_rt_src_jdbc";
    String dst = "test_copy_rt_dst_jdbc";
    for (String table : new String[] {src, dst}) {
      dropTable(table);
      execute(
          "CREATE TABLE " + table + " (host STRING, flag BOOLEAN, ts TIMESTAMP, TIME INDEX(ts))");
    }
    execute(
        "INSERT INTO "
            + src
            + " VALUES ('a', true, '2024-01-01 00:00:00'), ('b', false, '2024-07-01 12:30:00')");
    execute("SET TIME ZONE = 'Asia/Shanghai'");

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    long exported =
        copyManager()
            .copyOut(
                "COPY (SELECT host, flag, ts FROM "
                    + src
                    + " ORDER BY ts) TO STDOUT WITH (FORMAT csv)",
                out);
    assertEquals(2L, exported);
    String data = out.toString("UTF-8");
    log.info("COPY OUT produced: {}", data);
    assertTrue(data.contains("t") && data.contains("f"), data);
    assertTrue(data.contains("+08"), data); // timestamps rendered with the session-tz offset

    // The exact COPY OUT bytes import back unchanged.
    long rows = copyIn("COPY " + dst + " (host, flag, ts) FROM STDIN WITH (FORMAT csv)", data);
    assertEquals(2L, rows);

    assertEquals(
        0,
        queryInt(
            "SELECT COUNT(*) FROM "
                + src
                + " src JOIN "
                + dst
                + " dst ON src.host = dst.host "
                + "WHERE CAST(src.ts AS BIGINT) <> CAST(dst.ts AS BIGINT)"),
        "timestamps must round-trip exactly");
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT flag FROM " + dst + " ORDER BY ts")) {
      assertTrue(rs.next());
      assertTrue(rs.getBoolean(1));
      assertTrue(rs.next());
      assertTrue(!rs.getBoolean(1));
      assertTrue(!rs.next());
    }

    execute("SET TIME ZONE = 'UTC'");
    dropTable(src);
    dropTable(dst);
  }
}
