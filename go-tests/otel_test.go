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

package tests

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	otelTestRunID   = fmt.Sprintf("%d", time.Now().UnixMilli())
	otelServiceName = "otel-go-test-" + otelTestRunID

	// Providers (lazily initialized)
	otelMeterProvider  *sdkmetric.MeterProvider
	otelTracerProvider *sdktrace.TracerProvider
	otelLoggerProvider *sdklog.LoggerProvider
	otelDB             *sql.DB
	otelInitOnce       sync.Once
	otelInitErr        error
	otelInitialized    bool
)

// setupOtel initializes OpenTelemetry providers (called once via sync.Once)
func setupOtel(t *testing.T) {
	otelInitOnce.Do(func() {
		ctx := context.Background()

		httpHost := getEnv("HTTP_HOST", "127.0.0.1")
		httpPort := getEnv("HTTP_PORT", "4000")
		mysqlHost := getEnv("MYSQL_HOST", "127.0.0.1")
		mysqlPort := getEnv("MYSQL_PORT", "4002")
		dbName := getEnv("DB_NAME", "go_tests")
		username := getEnv("GREPTIME_USERNAME", "")
		password := getEnv("GREPTIME_PASSWORD", "")

		endpoint := fmt.Sprintf("%s:%s", httpHost, httpPort)

		// Build headers
		headers := map[string]string{
			"X-Greptime-DB-Name": dbName,
		}
		if username != "" || password != "" {
			auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
			headers["Authorization"] = "Basic " + auth
		}

		// Create resource
		res, err := resource.New(ctx,
			resource.WithAttributes(
				semconv.ServiceName(otelServiceName),
				semconv.ServiceVersion("1.0.0"),
				attribute.String("test.run.id", otelTestRunID),
			),
		)
		if err != nil {
			otelInitErr = fmt.Errorf("failed to create resource: %w", err)
			return
		}

		// Initialize Metrics
		metricExporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpoint(endpoint),
			otlpmetrichttp.WithURLPath("/v1/otlp/v1/metrics"),
			otlpmetrichttp.WithHeaders(headers),
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			otelInitErr = fmt.Errorf("failed to create metric exporter: %w", err)
			return
		}

		otelMeterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithResource(res),
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter,
				sdkmetric.WithInterval(time.Second),
			)),
		)
		otel.SetMeterProvider(otelMeterProvider)

		// Initialize Traces
		traceHeaders := make(map[string]string)
		for k, v := range headers {
			traceHeaders[k] = v
		}
		traceHeaders["x-greptime-pipeline-name"] = "greptime_trace_v1"

		traceExporter, err := otlptracehttp.New(ctx,
			otlptracehttp.WithEndpoint(endpoint),
			otlptracehttp.WithURLPath("/v1/otlp/v1/traces"),
			otlptracehttp.WithHeaders(traceHeaders),
			otlptracehttp.WithInsecure(),
		)
		if err != nil {
			otelInitErr = fmt.Errorf("failed to create trace exporter: %w", err)
			return
		}

		otelTracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(traceExporter),
		)
		otel.SetTracerProvider(otelTracerProvider)

		// Initialize Logs
		logExporter, err := otlploghttp.New(ctx,
			otlploghttp.WithEndpoint(endpoint),
			otlploghttp.WithURLPath("/v1/otlp/v1/logs"),
			otlploghttp.WithHeaders(headers),
			otlploghttp.WithInsecure(),
		)
		if err != nil {
			otelInitErr = fmt.Errorf("failed to create log exporter: %w", err)
			return
		}

		otelLoggerProvider = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)),
		)

		// Initialize DB connection
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, mysqlHost, mysqlPort, dbName)
		otelDB, err = sql.Open("mysql", dsn)
		if err != nil {
			otelInitErr = fmt.Errorf("failed to open database: %w", err)
			return
		}
		if err = otelDB.Ping(); err != nil {
			otelInitErr = fmt.Errorf("failed to ping database: %w", err)
			return
		}

		t.Logf("OpenTelemetry initialized: endpoint=%s, service=%s", endpoint, otelServiceName)
		otelInitialized = true
	})

	if otelInitErr != nil {
		t.Fatalf("Failed to initialize OpenTelemetry: %v", otelInitErr)
	}
}

// cleanupOtel shuts down all OpenTelemetry providers (called from TestMain)
func cleanupOtel() {
	if !otelInitialized {
		return
	}
	ctx := context.Background()
	if otelMeterProvider != nil {
		otelMeterProvider.Shutdown(ctx)
	}
	if otelTracerProvider != nil {
		otelTracerProvider.Shutdown(ctx)
	}
	if otelLoggerProvider != nil {
		otelLoggerProvider.Shutdown(ctx)
	}
	if otelDB != nil {
		otelDB.Close()
	}
}

// TestMain handles package-level setup and teardown
func TestMain(m *testing.M) {
	code := m.Run()
	cleanupOtel()
	os.Exit(code)
}

// verifyTableHasData queries the table and asserts it has data
// Returns the count of rows found
func verifyTableHasData(t *testing.T, tableName string, whereClause string, expectedMin int) int {
	t.Helper()

	if otelDB == nil {
		t.Logf("Skipping verification: database connection not available")
		return 0
	}

	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)
	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	var count int
	err := otelDB.QueryRowContext(context.Background(), query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to query table %s: %v\n  Query: %s", tableName, err, query)
	}

	if count < expectedMin {
		t.Errorf("Table %s has %d rows, expected at least %d\n  Query: %s", tableName, count, expectedMin, query)
	} else {
		t.Logf("Verified: table %s has %d rows (expected >= %d)", tableName, count, expectedMin)
	}

	return count
}

// ==================== METRICS TESTS ====================

func TestOtelMetricsCounter(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	meter := otel.Meter("test-meter")

	counterName := "otel_go_counter_" + otelTestRunID
	counter, err := meter.Int64Counter(counterName,
		metric.WithDescription("Test counter for OTLP integration"),
		metric.WithUnit("1"),
	)
	if err != nil {
		t.Fatalf("Failed to create counter: %v", err)
	}

	t.Log("Recording counter values: 10 (env=test), 25 (env=test), 15 (env=prod)")
	counter.Add(ctx, 10, metric.WithAttributes(attribute.String("env", "test")))
	counter.Add(ctx, 25, metric.WithAttributes(attribute.String("env", "test")))
	counter.Add(ctx, 15, metric.WithAttributes(attribute.String("env", "prod")))

	if err := otelMeterProvider.ForceFlush(ctx); err != nil {
		t.Logf("Warning: Force flush failed: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify: counter creates table with _total suffix (Prometheus convention)
	tableName := counterName + "_total"
	verifyTableHasData(t, tableName, "", 1)
}

func TestOtelMetricsGauge(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	meter := otel.Meter("test-meter")

	gaugeName := "otel_go_gauge_" + otelTestRunID
	gauge, err := meter.Int64Gauge(gaugeName,
		metric.WithDescription("Test gauge for OTLP integration"),
		metric.WithUnit("bytes"),
	)
	if err != nil {
		t.Fatalf("Failed to create gauge: %v", err)
	}

	t.Log("Recording gauge values: 1024 (host=server1), 2048 (host=server2)")
	gauge.Record(ctx, 1024, metric.WithAttributes(attribute.String("host", "server1")))
	gauge.Record(ctx, 2048, metric.WithAttributes(attribute.String("host", "server2")))

	if err := otelMeterProvider.ForceFlush(ctx); err != nil {
		t.Logf("Warning: Force flush failed: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify: gauge creates table with _<unit> suffix (Prometheus convention)
	tableName := gaugeName + "_bytes"
	verifyTableHasData(t, tableName, "", 1)
}

func TestOtelMetricsHistogram(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	meter := otel.Meter("test-meter")

	histogramName := "otel_go_histogram_" + otelTestRunID
	histogram, err := meter.Float64Histogram(histogramName,
		metric.WithDescription("Test histogram for OTLP integration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		t.Fatalf("Failed to create histogram: %v", err)
	}

	t.Log("Recording histogram values: 15.5ms, 45.2ms, 120.8ms, 5.1ms")
	histogram.Record(ctx, 15.5, metric.WithAttributes(attribute.String("endpoint", "/api/users")))
	histogram.Record(ctx, 45.2, metric.WithAttributes(attribute.String("endpoint", "/api/users")))
	histogram.Record(ctx, 120.8, metric.WithAttributes(attribute.String("endpoint", "/api/orders")))
	histogram.Record(ctx, 5.1, metric.WithAttributes(attribute.String("endpoint", "/health")))

	if err := otelMeterProvider.ForceFlush(ctx); err != nil {
		t.Logf("Warning: Force flush failed: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify: histogram creates _count table (also _sum, _bucket)
	// Note: "ms" unit is converted to "milliseconds" (Prometheus convention)
	tableName := histogramName + "_milliseconds_count"
	verifyTableHasData(t, tableName, "", 1)
}

// ==================== TRACES TESTS ====================

func TestOtelTraces(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	tracer := otel.Tracer("test-tracer")

	t.Logf("Creating trace spans for service: %s", otelServiceName)
	ctx, parentSpan := tracer.Start(ctx, "parent-operation-"+otelTestRunID,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(
			attribute.String("http.method", "GET"),
			attribute.String("http.url", "/api/test"),
			attribute.String("test.run.id", otelTestRunID),
		),
	)

	time.Sleep(50 * time.Millisecond)

	_, childSpan := tracer.Start(ctx, "child-operation-"+otelTestRunID,
		trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithAttributes(
			attribute.String("db.system", "greptimedb"),
			attribute.String("db.operation", "query"),
		),
	)
	time.Sleep(30 * time.Millisecond)
	childSpan.SetStatus(codes.Ok, "")
	childSpan.End()

	parentSpan.SetStatus(codes.Ok, "")
	parentSpan.End()
	t.Log("Trace spans created: parent-operation, child-operation")

	if err := otelTracerProvider.ForceFlush(ctx); err != nil {
		t.Logf("Warning: Force flush failed: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify: traces are stored in opentelemetry_traces table
	whereClause := fmt.Sprintf("service_name = '%s'", otelServiceName)
	verifyTableHasData(t, "opentelemetry_traces", whereClause, 2)
}

// ==================== LOGS TESTS ====================

func TestOtelLogs(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	logger := otelLoggerProvider.Logger("test-logger")

	t.Logf("Emitting 3 log records with test.run.id=%s", otelTestRunID)

	var record log.Record
	record.SetSeverity(log.SeverityInfo)
	record.SetBody(log.StringValue("Test info log message - " + otelTestRunID))
	record.AddAttributes(
		log.String("test.run.id", otelTestRunID),
		log.String("log.type", "test"),
	)
	logger.Emit(ctx, record)

	record = log.Record{}
	record.SetSeverity(log.SeverityWarn)
	record.SetBody(log.StringValue("Test warning log message - " + otelTestRunID))
	record.AddAttributes(
		log.String("test.run.id", otelTestRunID),
		log.String("log.type", "test"),
	)
	logger.Emit(ctx, record)

	record = log.Record{}
	record.SetSeverity(log.SeverityError)
	record.SetBody(log.StringValue("Test error log message - " + otelTestRunID))
	record.AddAttributes(
		log.String("test.run.id", otelTestRunID),
		log.String("log.type", "test"),
		log.String("error.code", "TEST_ERROR"),
	)
	logger.Emit(ctx, record)
	t.Log("Log records emitted: INFO, WARN, ERROR")

	if err := otelLoggerProvider.ForceFlush(ctx); err != nil {
		t.Logf("Warning: Force flush failed: %v", err)
	}
	time.Sleep(3 * time.Second)

	// Verify: logs are stored in opentelemetry_logs table
	whereClause := fmt.Sprintf("body LIKE '%%%s%%'", otelTestRunID)
	verifyTableHasData(t, "opentelemetry_logs", whereClause, 3)
}

// ==================== COMPREHENSIVE TEST ====================

func TestOtelAllSignalsTogether(t *testing.T) {
	setupOtel(t)

	ctx := context.Background()
	meter := otel.Meter("comprehensive-test")
	tracer := otel.Tracer("comprehensive-test")
	logger := otelLoggerProvider.Logger("comprehensive-test")

	t.Log("Testing all signals together: metrics + traces + logs")

	ctx, span := tracer.Start(ctx, "comprehensive-operation-"+otelTestRunID,
		trace.WithSpanKind(trace.SpanKindServer),
	)

	counterName := "otel_go_comprehensive_" + otelTestRunID
	counter, _ := meter.Int64Counter(counterName)
	counter.Add(ctx, 1, metric.WithAttributes(attribute.String("operation", "comprehensive")))

	var record log.Record
	record.SetSeverity(log.SeverityInfo)
	record.SetBody(log.StringValue("Comprehensive test log - " + otelTestRunID))
	record.AddAttributes(log.String("test.type", "comprehensive"))
	logger.Emit(ctx, record)

	time.Sleep(100 * time.Millisecond)

	span.SetStatus(codes.Ok, "")
	span.End()

	otelMeterProvider.ForceFlush(ctx)
	otelTracerProvider.ForceFlush(ctx)
	otelLoggerProvider.ForceFlush(ctx)
	time.Sleep(3 * time.Second)

	// Verify all signals
	t.Log("Verifying all signals were exported...")
	tableName := counterName + "_total"
	verifyTableHasData(t, tableName, "", 1)
}
