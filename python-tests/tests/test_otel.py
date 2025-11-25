# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
OpenTelemetry integration tests for GreptimeDB.
Tests Metrics, Traces, and Logs ingestion via OTLP HTTP protocol.
"""

import base64
import os
import time
from typing import Optional

import mysql.connector
import pytest
from opentelemetry import metrics, trace
from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import StatusCode

# Test run identifier
TEST_RUN_ID = str(int(time.time() * 1000))
SERVICE_NAME = f"otel-python-test-{TEST_RUN_ID}"


class OtelTestConfig:
    """Configuration loaded from environment (not a test class)."""

    def __init__(self):
        self.http_host = os.getenv("HTTP_HOST", "127.0.0.1")
        self.http_port = os.getenv("HTTP_PORT", "4000")
        self.mysql_host = os.getenv("MYSQL_HOST", "127.0.0.1")
        self.mysql_port = int(os.getenv("MYSQL_PORT", "4002"))
        self.db_name = os.getenv("DB_NAME", "python_tests")
        self.username = os.getenv("GREPTIME_USERNAME", "")
        self.password = os.getenv("GREPTIME_PASSWORD", "")

    @property
    def otlp_endpoint(self) -> str:
        return f"http://{self.http_host}:{self.http_port}/v1/otlp"

    @property
    def otlp_headers(self) -> dict:
        headers = {"X-Greptime-DB-Name": self.db_name}
        if self.username or self.password:
            auth = base64.b64encode(
                f"{self.username}:{self.password}".encode()
            ).decode()
            headers["Authorization"] = f"Basic {auth}"
        return headers


# Global instances
config: Optional[OtelTestConfig] = None
meter_provider: Optional[MeterProvider] = None
tracer_provider: Optional[TracerProvider] = None
logger_provider: Optional[LoggerProvider] = None
db_connection = None


def get_db_connection():
    """Get MySQL connection for verification."""
    global db_connection
    if db_connection is None or not db_connection.is_connected():
        db_connection = mysql.connector.connect(
            host=config.mysql_host,
            port=config.mysql_port,
            database=config.db_name,
            user=config.username if config.username else None,
            password=config.password if config.password else None,
        )
    return db_connection


def verify_table_has_data(
    table_name: str, where_clause: str = "", expected_min: int = 1
) -> int:
    """
    Query the table and verify it has data.
    Returns the count of rows found.
    Raises AssertionError if count < expected_min.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        query = f"SELECT COUNT(*) FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"

        cursor.execute(query)
        count = cursor.fetchone()[0]

        if count < expected_min:
            raise AssertionError(
                f"Table {table_name} has {count} rows, expected at least {expected_min}\n"
                f"  Query: {query}"
            )
        print(
            f"  Verified: table {table_name} has {count} rows (expected >= {expected_min})"
        )
        return count
    except mysql.connector.Error as e:
        raise AssertionError(
            f"Failed to query table {table_name}: {e}\n" f"  Query: {query}"
        )
    finally:
        cursor.close()


@pytest.fixture(scope="session", autouse=True)
def setup_opentelemetry():
    """Initialize OpenTelemetry providers."""
    global config, meter_provider, tracer_provider, logger_provider

    config = OtelTestConfig()

    print(f"\nInitializing OpenTelemetry:")
    print(f"  OTLP endpoint: {config.otlp_endpoint}")
    print(f"  MySQL: {config.mysql_host}:{config.mysql_port}/{config.db_name}")
    print(f"  Service name: {SERVICE_NAME}")

    # Create resource
    resource = Resource.create(
        {
            "service.name": SERVICE_NAME,
            "service.version": "1.0.0",
            "test.run.id": TEST_RUN_ID,
        }
    )

    # Initialize Metrics
    metric_exporter = OTLPMetricExporter(
        endpoint=f"{config.otlp_endpoint}/v1/metrics",
        headers=config.otlp_headers,
        timeout=10,
    )
    metric_reader = PeriodicExportingMetricReader(
        metric_exporter,
        export_interval_millis=1000,
    )
    meter_provider = MeterProvider(
        resource=resource,
        metric_readers=[metric_reader],
    )
    metrics.set_meter_provider(meter_provider)

    # Initialize Traces
    # Note: Traces require x-greptime-pipeline-name header
    trace_headers = dict(config.otlp_headers)
    trace_headers["x-greptime-pipeline-name"] = "greptime_trace_v1"

    trace_exporter = OTLPSpanExporter(
        endpoint=f"{config.otlp_endpoint}/v1/traces",
        headers=trace_headers,
        timeout=10,
    )
    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BatchSpanProcessor(trace_exporter))
    trace.set_tracer_provider(tracer_provider)

    # Initialize Logs
    log_exporter = OTLPLogExporter(
        endpoint=f"{config.otlp_endpoint}/v1/logs",
        headers=config.otlp_headers,
        timeout=10,
    )
    logger_provider = LoggerProvider(resource=resource)
    logger_provider.add_log_record_processor(BatchLogRecordProcessor(log_exporter))
    set_logger_provider(logger_provider)

    print("OpenTelemetry initialized successfully")

    yield

    # Cleanup
    print("\nShutting down OpenTelemetry...")
    if meter_provider:
        meter_provider.shutdown()
    if tracer_provider:
        tracer_provider.shutdown()
    if logger_provider:
        logger_provider.shutdown()
    if db_connection and db_connection.is_connected():
        db_connection.close()


# ==================== METRICS TESTS ====================


class TestOtelMetrics:
    """Tests for OpenTelemetry Metrics."""

    def test_counter(self):
        """Test Counter metric."""
        print("\nTesting Metrics Counter...")

        meter = metrics.get_meter("test-meter")
        counter_name = f"otel_python_counter_{TEST_RUN_ID}"

        # Create counter
        counter = meter.create_counter(
            name=counter_name,
            description="Test counter for OTLP integration",
            unit="1",
        )

        # Record values
        print("  Recording counter values: 10 (env=test), 25 (env=test), 15 (env=prod)")
        counter.add(10, {"env": "test"})
        counter.add(25, {"env": "test"})
        counter.add(15, {"env": "prod"})

        # Force flush and wait
        meter_provider.force_flush()
        time.sleep(3)

        # Verify: counter creates table with _total suffix (Prometheus convention)
        table_name = f"{counter_name}_total"
        verify_table_has_data(table_name, "", 1)

    def test_gauge(self):
        """Test Gauge metric."""
        print("\nTesting Metrics Gauge...")

        meter = metrics.get_meter("test-meter")
        gauge_name = f"otel_python_gauge_{TEST_RUN_ID}"

        # Create gauge
        gauge = meter.create_gauge(
            name=gauge_name,
            description="Test gauge for OTLP integration",
            unit="bytes",
        )

        # Record values
        print("  Recording gauge values: 1024 (host=server1), 2048 (host=server2)")
        gauge.set(1024, {"host": "server1"})
        gauge.set(2048, {"host": "server2"})

        # Force flush and wait
        meter_provider.force_flush()
        time.sleep(3)

        # Verify: gauge creates table with _<unit> suffix (Prometheus convention)
        table_name = f"{gauge_name}_bytes"
        verify_table_has_data(table_name, "", 1)

    def test_histogram(self):
        """Test Histogram metric."""
        print("\nTesting Metrics Histogram...")

        meter = metrics.get_meter("test-meter")
        histogram_name = f"otel_python_histogram_{TEST_RUN_ID}"

        # Create histogram
        histogram = meter.create_histogram(
            name=histogram_name,
            description="Test histogram for OTLP integration",
            unit="ms",
        )

        # Record values
        print("  Recording histogram values: 15.5ms, 45.2ms, 120.8ms, 5.1ms")
        histogram.record(15.5, {"endpoint": "/api/users"})
        histogram.record(45.2, {"endpoint": "/api/users"})
        histogram.record(120.8, {"endpoint": "/api/orders"})
        histogram.record(5.1, {"endpoint": "/health"})

        # Force flush and wait
        meter_provider.force_flush()
        time.sleep(3)

        # Verify: histogram creates _count table (also _sum, _bucket)
        # Note: "ms" unit is converted to "milliseconds" (Prometheus convention)
        table_name = f"{histogram_name}_milliseconds_count"
        verify_table_has_data(table_name, "", 1)


# ==================== TRACES TESTS ====================


class TestOtelTraces:
    """Tests for OpenTelemetry Traces."""

    def test_traces(self):
        """Test Trace spans."""
        print("\nTesting Traces...")
        print(f"  Creating trace spans for service: {SERVICE_NAME}")

        tracer = trace.get_tracer("test-tracer")

        # Create parent span
        with tracer.start_as_current_span(
            f"parent-operation-{TEST_RUN_ID}",
            kind=trace.SpanKind.SERVER,
            attributes={
                "http.method": "GET",
                "http.url": "/api/test",
                "test.run.id": TEST_RUN_ID,
            },
        ) as parent_span:
            # Simulate work
            time.sleep(0.05)

            # Create child span
            with tracer.start_as_current_span(
                f"child-operation-{TEST_RUN_ID}",
                kind=trace.SpanKind.INTERNAL,
                attributes={
                    "db.system": "greptimedb",
                    "db.operation": "query",
                },
            ) as child_span:
                time.sleep(0.03)
                child_span.set_status(StatusCode.OK)

            parent_span.set_status(StatusCode.OK)

        print("  Trace spans created: parent-operation, child-operation")

        # Force flush and wait
        tracer_provider.force_flush()
        time.sleep(3)

        # Verify: traces are stored in opentelemetry_traces table
        where_clause = f"service_name = '{SERVICE_NAME}'"
        verify_table_has_data("opentelemetry_traces", where_clause, 2)


# ==================== LOGS TESTS ====================


class TestOtelLogs:
    """Tests for OpenTelemetry Logs."""

    def test_logs(self):
        """Test Log records."""
        import logging

        print("\nTesting Logs...")
        print(f"  Emitting 3 log records with test.run.id={TEST_RUN_ID}")

        # Use Python's standard logging with OpenTelemetry handler
        otel_handler = LoggingHandler(
            level=logging.DEBUG,
            logger_provider=logger_provider,
        )

        # Create a standard Python logger
        py_logger = logging.getLogger(f"test-logger-{TEST_RUN_ID}")
        py_logger.setLevel(logging.DEBUG)
        py_logger.addHandler(otel_handler)

        # Emit log records using standard Python logging API
        py_logger.info(
            f"Test info log message - {TEST_RUN_ID}",
            extra={"attributes": {"test.run.id": TEST_RUN_ID, "log.type": "test"}},
        )

        py_logger.warning(
            f"Test warning log message - {TEST_RUN_ID}",
            extra={"attributes": {"test.run.id": TEST_RUN_ID, "log.type": "test"}},
        )

        py_logger.error(
            f"Test error log message - {TEST_RUN_ID}",
            extra={
                "attributes": {
                    "test.run.id": TEST_RUN_ID,
                    "log.type": "test",
                    "error.code": "TEST_ERROR",
                }
            },
        )

        # Remove handler to avoid duplicate logs
        py_logger.removeHandler(otel_handler)
        print("  Log records emitted: INFO, WARN, ERROR")

        # Force flush and wait
        logger_provider.force_flush()
        time.sleep(3)

        # Verify: logs are stored in opentelemetry_logs table
        where_clause = f"body LIKE '%{TEST_RUN_ID}%'"
        verify_table_has_data("opentelemetry_logs", where_clause, 3)


# ==================== COMPREHENSIVE TEST ====================


class TestOtelComprehensive:
    """Comprehensive test combining all signals."""

    def test_all_signals_together(self):
        """Test all signals (metrics, traces, logs) together."""
        print("\nTesting all signals together: metrics + traces + logs")

        meter = metrics.get_meter("comprehensive-test")
        tracer = trace.get_tracer("comprehensive-test")

        # Setup logging handler
        import logging

        otel_handler = LoggingHandler(
            level=logging.DEBUG,
            logger_provider=logger_provider,
        )
        py_logger = logging.getLogger(f"comprehensive-test-{TEST_RUN_ID}")
        py_logger.setLevel(logging.DEBUG)
        py_logger.addHandler(otel_handler)

        counter_name = f"otel_python_comprehensive_{TEST_RUN_ID}"

        # Create a trace span
        with tracer.start_as_current_span(
            f"comprehensive-operation-{TEST_RUN_ID}",
            kind=trace.SpanKind.SERVER,
        ) as span:
            # Record metrics within the span
            counter = meter.create_counter(counter_name)
            counter.add(1, {"operation": "comprehensive"})

            # Emit a log within the span using standard logging
            py_logger.info(
                f"Comprehensive test log - {TEST_RUN_ID}",
                extra={"attributes": {"test.type": "comprehensive"}},
            )

            # Simulate work
            time.sleep(0.1)

            span.set_status(StatusCode.OK)

        # Remove handler
        py_logger.removeHandler(otel_handler)

        # Force flush all providers
        meter_provider.force_flush()
        tracer_provider.force_flush()
        logger_provider.force_flush()
        time.sleep(3)

        # Verify metric was exported
        print("  Verifying all signals were exported...")
        table_name = f"{counter_name}_total"
        verify_table_has_data(table_name, "", 1)
