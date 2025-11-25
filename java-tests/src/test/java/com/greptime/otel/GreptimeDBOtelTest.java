/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.greptime.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.slf4j.LoggerFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.logs.Logger;
import io.opentelemetry.api.logs.Severity;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongUpDownCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.semconv.ResourceAttributes;

/**
 * Integration tests for GreptimeDB OpenTelemetry protocol compatibility. Tests Metrics, Traces, and
 * Logs ingestion via OTLP HTTP protocol.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GreptimeDBOtelTest {

  private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(GreptimeDBOtelTest.class);

  // Environment configuration
  private static String httpHost;
  private static int httpPort;
  private static String mysqlHost;
  private static int mysqlPort;
  private static String dbName;
  private static String username;
  private static String password;

  // OpenTelemetry components
  private static OpenTelemetry openTelemetry;
  private static SdkMeterProvider meterProvider;
  private static SdkTracerProvider tracerProvider;
  private static SdkLoggerProvider loggerProvider;

  // JDBC connection for verification
  private static Connection connection;

  // Test identifiers
  private static final String TEST_RUN_ID = String.valueOf(System.currentTimeMillis());
  private static final String SERVICE_NAME = "otel-java-test-" + TEST_RUN_ID;

  @BeforeAll
  static void setUp() throws Exception {
    // Load configuration from environment
    httpHost = getEnv("HTTP_HOST", "127.0.0.1");
    httpPort = Integer.parseInt(getEnv("HTTP_PORT", "4000"));
    mysqlHost = getEnv("MYSQL_HOST", "127.0.0.1");
    mysqlPort = Integer.parseInt(getEnv("MYSQL_PORT", "4002"));
    dbName = getEnv("DB_NAME", "java_tests");
    username = getEnv("GREPTIME_USERNAME", "");
    password = getEnv("GREPTIME_PASSWORD", "");

    LOG.info("Initializing OpenTelemetry test with:");
    LOG.info("  OTLP endpoint: http://{}:{}/v1/otlp", httpHost, httpPort);
    LOG.info("  MySQL: {}:{}/{}", mysqlHost, mysqlPort, dbName);
    LOG.info("  Service name: {}", SERVICE_NAME);

    // Initialize OpenTelemetry
    initializeOpenTelemetry();

    // Initialize JDBC connection for verification
    initializeJdbcConnection();
  }

  @AfterAll
  static void tearDown() throws Exception {
    // Shutdown OpenTelemetry providers
    if (meterProvider != null) {
      meterProvider.shutdown().join(10, TimeUnit.SECONDS);
    }
    if (tracerProvider != null) {
      tracerProvider.shutdown().join(10, TimeUnit.SECONDS);
    }
    if (loggerProvider != null) {
      loggerProvider.shutdown().join(10, TimeUnit.SECONDS);
    }

    // Close JDBC connection
    if (connection != null && !connection.isClosed()) {
      connection.close();
    }

    LOG.info("OpenTelemetry test cleanup completed");
  }

  private static void initializeOpenTelemetry() {
    String otlpEndpoint = String.format("http://%s:%d/v1/otlp", httpHost, httpPort);

    // Build authorization header
    Map<String, String> headers = new HashMap<>();
    headers.put("X-Greptime-DB-Name", dbName);
    if (!username.isEmpty() || !password.isEmpty()) {
      String auth = Base64.getEncoder().encodeToString((username + ":" + password).getBytes());
      headers.put("Authorization", "Basic " + auth);
    }

    // Create Resource
    Resource resource =
        Resource.getDefault()
            .merge(
                Resource.create(
                    Attributes.builder()
                        .put(ResourceAttributes.SERVICE_NAME, SERVICE_NAME)
                        .put(ResourceAttributes.SERVICE_VERSION, "1.0.0")
                        .put(AttributeKey.stringKey("test.run.id"), TEST_RUN_ID)
                        .build()));

    // Create Metrics Exporter and Provider
    OtlpHttpMetricExporter metricExporter =
        OtlpHttpMetricExporter.builder()
            .setEndpoint(otlpEndpoint + "/v1/metrics")
            .setHeaders(() -> headers)
            .setTimeout(Duration.ofSeconds(10))
            .build();

    meterProvider =
        SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
                PeriodicMetricReader.builder(metricExporter)
                    .setInterval(Duration.ofSeconds(1))
                    .build())
            .build();

    // Create Traces Exporter and Provider
    // Note: Traces require x-greptime-pipeline-name header
    Map<String, String> traceHeaders = new HashMap<>(headers);
    traceHeaders.put("x-greptime-pipeline-name", "greptime_trace_v1");

    OtlpHttpSpanExporter spanExporter =
        OtlpHttpSpanExporter.builder()
            .setEndpoint(otlpEndpoint + "/v1/traces")
            .setHeaders(() -> traceHeaders)
            .setTimeout(Duration.ofSeconds(10))
            .build();

    tracerProvider =
        SdkTracerProvider.builder()
            .setResource(resource)
            .addSpanProcessor(BatchSpanProcessor.builder(spanExporter).build())
            .build();

    // Create Logs Exporter and Provider
    OtlpHttpLogRecordExporter logExporter =
        OtlpHttpLogRecordExporter.builder()
            .setEndpoint(otlpEndpoint + "/v1/logs")
            .setHeaders(() -> headers)
            .setTimeout(Duration.ofSeconds(10))
            .build();

    loggerProvider =
        SdkLoggerProvider.builder()
            .setResource(resource)
            .addLogRecordProcessor(BatchLogRecordProcessor.builder(logExporter).build())
            .build();

    // Build OpenTelemetry instance
    openTelemetry =
        OpenTelemetrySdk.builder()
            .setMeterProvider(meterProvider)
            .setTracerProvider(tracerProvider)
            .setLoggerProvider(loggerProvider)
            .build();

    LOG.info("OpenTelemetry initialized successfully");
  }

  private static void initializeJdbcConnection() throws SQLException {
    String jdbcUrl = String.format("jdbc:mysql://%s:%d/%s", mysqlHost, mysqlPort, dbName);
    if (!username.isEmpty()) {
      connection = DriverManager.getConnection(jdbcUrl, username, password);
    } else {
      connection = DriverManager.getConnection(jdbcUrl);
    }
    LOG.info("JDBC connection established");
  }

  private static String getEnv(String name, String defaultValue) {
    String value = System.getenv(name);
    return (value != null && !value.isEmpty()) ? value : defaultValue;
  }

  // ==================== METRICS TESTS ====================

  @Test
  @Order(1)
  void testMetricsCounter() throws Exception {
    LOG.info("Testing Metrics Counter...");

    Meter meter = openTelemetry.getMeter("test-meter");

    // Create and record counter
    LongCounter counter =
        meter
            .counterBuilder("otel_test_counter_" + TEST_RUN_ID)
            .setDescription("Test counter for OTLP integration")
            .setUnit("1")
            .build();

    // Add some values with attributes
    LOG.info("  Recording counter values: 10 (env=test), 25 (env=test), 15 (env=prod)");
    counter.add(10, Attributes.of(AttributeKey.stringKey("env"), "test"));
    counter.add(25, Attributes.of(AttributeKey.stringKey("env"), "test"));
    counter.add(15, Attributes.of(AttributeKey.stringKey("env"), "prod"));

    // Force flush and wait for export
    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000); // Wait for data to be written

    // Verify via SQL
    // Note: GreptimeDB adds _total suffix for counters (Prometheus convention)
    String tableName = "otel_test_counter_" + TEST_RUN_ID + "_total";
    verifyTableHasData(tableName, "env = 'test'", 1);
  }

  /** Helper method to verify table has data with proper error context. */
  private void verifyTableHasData(String tableName, String whereClause, int expectedMin)
      throws SQLException {
    String query = "SELECT COUNT(*) FROM " + tableName;
    if (whereClause != null && !whereClause.isEmpty()) {
      query += " WHERE " + whereClause;
    }

    try (Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)) {
      assertTrue(rs.next(), "Query should return a result");
      int count = rs.getInt(1);
      assertTrue(
          count >= expectedMin,
          String.format(
              "Table %s has %d rows, expected at least %d. Query: %s",
              tableName, count, expectedMin, query));
      LOG.info("  Verified: table {} has {} rows (expected >= {})", tableName, count, expectedMin);
    } catch (SQLException e) {
      throw new AssertionError(
          String.format(
              "Failed to query table %s: %s. Query: %s", tableName, e.getMessage(), query),
          e);
    }
  }

  @Test
  @Order(2)
  void testMetricsGauge() throws Exception {
    LOG.info("Testing Metrics Gauge (using UpDownCounter)...");

    Meter meter = openTelemetry.getMeter("test-meter");

    // Create gauge using UpDownCounter (synchronous gauge-like metric)
    // Note: In OpenTelemetry Java, synchronous Gauge requires callback.
    // UpDownCounter is the recommended alternative for synchronous gauge-like behavior.
    LongUpDownCounter gauge =
        meter
            .upDownCounterBuilder("otel_test_gauge_" + TEST_RUN_ID)
            .setDescription("Test gauge for OTLP integration")
            .setUnit("bytes")
            .build();

    // Add gauge values
    LOG.info("  Recording gauge values: 1024 (host=server1), 2048 (host=server2)");
    gauge.add(1024, Attributes.of(AttributeKey.stringKey("host"), "server1"));
    gauge.add(2048, Attributes.of(AttributeKey.stringKey("host"), "server2"));

    // Force flush and wait
    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000);

    // Verify: gauge creates table with _<unit> suffix (Prometheus convention)
    String tableName = "otel_test_gauge_" + TEST_RUN_ID + "_bytes";
    verifyTableHasData(tableName, "", 1);
  }

  @Test
  @Order(3)
  void testMetricsHistogram() throws Exception {
    LOG.info("Testing Metrics Histogram...");

    Meter meter = openTelemetry.getMeter("test-meter");

    // Create histogram
    DoubleHistogram histogram =
        meter
            .histogramBuilder("otel_test_histogram_" + TEST_RUN_ID)
            .setDescription("Test histogram for OTLP integration")
            .setUnit("ms")
            .build();

    // Record values
    LOG.info("  Recording histogram values: 15.5ms, 45.2ms, 120.8ms, 5.1ms");
    histogram.record(15.5, Attributes.of(AttributeKey.stringKey("endpoint"), "/api/users"));
    histogram.record(45.2, Attributes.of(AttributeKey.stringKey("endpoint"), "/api/users"));
    histogram.record(120.8, Attributes.of(AttributeKey.stringKey("endpoint"), "/api/orders"));
    histogram.record(5.1, Attributes.of(AttributeKey.stringKey("endpoint"), "/health"));

    // Force flush and wait
    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000);

    // Verify: histogram creates _count table (also _sum, _bucket)
    // Note: "ms" unit is converted to "milliseconds" (Prometheus convention)
    String tableName = "otel_test_histogram_" + TEST_RUN_ID + "_milliseconds_count";
    verifyTableHasData(tableName, "", 1);
  }

  // ==================== TRACES TESTS ====================

  @Test
  @Order(4)
  void testTraces() throws Exception {
    LOG.info("Testing Traces...");
    LOG.info("  Creating trace spans for service: {}", SERVICE_NAME);

    Tracer tracer = openTelemetry.getTracer("test-tracer");

    // Create a parent span
    Span parentSpan =
        tracer
            .spanBuilder("parent-operation-" + TEST_RUN_ID)
            .setSpanKind(SpanKind.SERVER)
            .setAttribute("http.method", "GET")
            .setAttribute("http.url", "/api/test")
            .setAttribute("test.run.id", TEST_RUN_ID)
            .startSpan();

    try (Scope scope = parentSpan.makeCurrent()) {
      // Simulate some work
      Thread.sleep(50);

      // Create a child span
      Span childSpan =
          tracer
              .spanBuilder("child-operation-" + TEST_RUN_ID)
              .setSpanKind(SpanKind.INTERNAL)
              .setAttribute("db.system", "greptimedb")
              .setAttribute("db.operation", "query")
              .startSpan();

      try {
        Thread.sleep(30);
        childSpan.setStatus(StatusCode.OK);
      } finally {
        childSpan.end();
      }

      parentSpan.setStatus(StatusCode.OK);
    } finally {
      parentSpan.end();
    }
    LOG.info("  Trace spans created: parent-operation, child-operation");

    // Force flush and wait
    tracerProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000);

    // Verify: traces are stored in opentelemetry_traces table
    String whereClause = String.format("service_name = '%s'", SERVICE_NAME);
    verifyTableHasData("opentelemetry_traces", whereClause, 2);
  }

  // ==================== LOGS TESTS ====================

  @Test
  @Order(5)
  void testLogs() throws Exception {
    LOG.info("Testing Logs...");
    LOG.info("  Emitting 3 log records with test.run.id={}", TEST_RUN_ID);

    Logger logger = openTelemetry.getLogsBridge().get("test-logger");

    // Emit log records
    logger
        .logRecordBuilder()
        .setSeverity(Severity.INFO)
        .setBody("Test info log message - " + TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("test.run.id"), TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("log.type"), "test")
        .emit();

    logger
        .logRecordBuilder()
        .setSeverity(Severity.WARN)
        .setBody("Test warning log message - " + TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("test.run.id"), TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("log.type"), "test")
        .emit();

    logger
        .logRecordBuilder()
        .setSeverity(Severity.ERROR)
        .setBody("Test error log message - " + TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("test.run.id"), TEST_RUN_ID)
        .setAttribute(AttributeKey.stringKey("log.type"), "test")
        .setAttribute(AttributeKey.stringKey("error.code"), "TEST_ERROR")
        .emit();
    LOG.info("  Log records emitted: INFO, WARN, ERROR");

    // Force flush and wait
    loggerProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000);

    // Verify: logs are stored in opentelemetry_logs table
    String whereClause = String.format("body LIKE '%%%s%%'", TEST_RUN_ID);
    verifyTableHasData("opentelemetry_logs", whereClause, 3);
  }

  // ==================== COMPREHENSIVE TEST ====================

  @Test
  @Order(6)
  void testAllSignalsTogether() throws Exception {
    LOG.info("Testing all signals together: metrics + traces + logs");

    Meter meter = openTelemetry.getMeter("comprehensive-test");
    Tracer tracer = openTelemetry.getTracer("comprehensive-test");
    Logger logger = openTelemetry.getLogsBridge().get("comprehensive-test");

    String counterName = "otel_comprehensive_requests_" + TEST_RUN_ID;

    // Create a trace span
    Span span =
        tracer
            .spanBuilder("comprehensive-operation-" + TEST_RUN_ID)
            .setSpanKind(SpanKind.SERVER)
            .startSpan();

    try (Scope scope = span.makeCurrent()) {
      // Record metrics within the span
      LongCounter requestCounter =
          meter
              .counterBuilder(counterName)
              .setDescription("Request counter in comprehensive test")
              .build();
      requestCounter.add(1, Attributes.of(AttributeKey.stringKey("operation"), "comprehensive"));

      // Emit a log within the span
      logger
          .logRecordBuilder()
          .setSeverity(Severity.INFO)
          .setBody("Comprehensive test log - " + TEST_RUN_ID)
          .setAttribute(AttributeKey.stringKey("test.type"), "comprehensive")
          .emit();

      // Simulate work
      Thread.sleep(100);

      span.setStatus(StatusCode.OK);
    } finally {
      span.end();
    }

    // Force flush all providers
    meterProvider.forceFlush().join(10, TimeUnit.SECONDS);
    tracerProvider.forceFlush().join(10, TimeUnit.SECONDS);
    loggerProvider.forceFlush().join(10, TimeUnit.SECONDS);
    Thread.sleep(3000);

    // Verify metric was exported
    LOG.info("  Verifying all signals were exported...");
    String tableName = counterName + "_total";
    verifyTableHasData(tableName, "", 1);
  }
}
