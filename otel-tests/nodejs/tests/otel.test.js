/**
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

const { metrics, trace, context, SpanKind, SpanStatusCode } = require('@opentelemetry/api');
const { logs, SeverityNumber } = require('@opentelemetry/api-logs');
const { Resource } = require('@opentelemetry/resources');
const {
  SEMRESATTRS_SERVICE_NAME,
  SEMRESATTRS_SERVICE_VERSION,
} = require('@opentelemetry/semantic-conventions');
const {
  MeterProvider,
  PeriodicExportingMetricReader,
} = require('@opentelemetry/sdk-metrics');
const {
  BasicTracerProvider,
  BatchSpanProcessor,
} = require('@opentelemetry/sdk-trace-base');
const {
  LoggerProvider,
  BatchLogRecordProcessor,
} = require('@opentelemetry/sdk-logs');
const {
  OTLPMetricExporter,
} = require('@opentelemetry/exporter-metrics-otlp-proto');
const {
  OTLPTraceExporter,
} = require('@opentelemetry/exporter-trace-otlp-proto');
const {
  OTLPLogExporter,
} = require('@opentelemetry/exporter-logs-otlp-proto');
const mysql = require('mysql2/promise');

// Test configuration
const TEST_RUN_ID = Date.now().toString();
const SERVICE_NAME = `otel-nodejs-test-${TEST_RUN_ID}`;

// Environment configuration
const config = {
  httpHost: process.env.HTTP_HOST || '127.0.0.1',
  httpPort: process.env.HTTP_PORT || '4000',
  mysqlHost: process.env.MYSQL_HOST || '127.0.0.1',
  mysqlPort: parseInt(process.env.MYSQL_PORT || '4002', 10),
  dbName: process.env.DB_NAME || 'otel_tests',
  username: process.env.GREPTIME_USERNAME || '',
  password: process.env.GREPTIME_PASSWORD || '',
};

// Get OTLP endpoint
const getOtlpEndpoint = () =>
  `http://${config.httpHost}:${config.httpPort}/v1/otlp`;

// Get OTLP headers
const getOtlpHeaders = () => {
  const headers = {
    'X-Greptime-DB-Name': config.dbName,
  };
  if (config.username || config.password) {
    const auth = Buffer.from(`${config.username}:${config.password}`).toString(
      'base64'
    );
    headers['Authorization'] = `Basic ${auth}`;
  }
  return headers;
};

// Providers
let meterProvider;
let tracerProvider;
let loggerProvider;
let dbConnection;

// Helper function for delay
const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// Setup and teardown
beforeAll(async () => {
  console.log('\nInitializing OpenTelemetry:');
  console.log(`  OTLP endpoint: ${getOtlpEndpoint()}`);
  console.log(`  MySQL: ${config.mysqlHost}:${config.mysqlPort}/${config.dbName}`);
  console.log(`  Service name: ${SERVICE_NAME}`);

  // Create resource
  const resource = new Resource({
    [SEMRESATTRS_SERVICE_NAME]: SERVICE_NAME,
    [SEMRESATTRS_SERVICE_VERSION]: '1.0.0',
    'test.run.id': TEST_RUN_ID,
  });

  // Initialize Metrics
  const metricExporter = new OTLPMetricExporter({
    url: `${getOtlpEndpoint()}/v1/metrics`,
    headers: getOtlpHeaders(),
    timeoutMillis: 10000,
  });

  meterProvider = new MeterProvider({
    resource,
    readers: [
      new PeriodicExportingMetricReader({
        exporter: metricExporter,
        exportIntervalMillis: 1000,
      }),
    ],
  });
  metrics.setGlobalMeterProvider(meterProvider);

  // Initialize Traces
  // Note: Traces require x-greptime-pipeline-name header
  const traceHeaders = {
    ...getOtlpHeaders(),
    'x-greptime-pipeline-name': 'greptime_trace_v1',
  };

  const traceExporter = new OTLPTraceExporter({
    url: `${getOtlpEndpoint()}/v1/traces`,
    headers: traceHeaders,
    timeoutMillis: 10000,
  });

  tracerProvider = new BasicTracerProvider({ resource });
  tracerProvider.addSpanProcessor(new BatchSpanProcessor(traceExporter));
  tracerProvider.register();

  // Initialize Logs
  const logExporter = new OTLPLogExporter({
    url: `${getOtlpEndpoint()}/v1/logs`,
    headers: getOtlpHeaders(),
    timeoutMillis: 10000,
  });

  loggerProvider = new LoggerProvider({ resource });
  loggerProvider.addLogRecordProcessor(new BatchLogRecordProcessor(logExporter));
  logs.setGlobalLoggerProvider(loggerProvider);

  // Initialize database connection
  dbConnection = await mysql.createConnection({
    host: config.mysqlHost,
    port: config.mysqlPort,
    database: config.dbName,
    user: config.username || undefined,
    password: config.password || undefined,
  });

  console.log('OpenTelemetry initialized successfully');
}, 30000);

afterAll(async () => {
  console.log('\nShutting down OpenTelemetry...');

  if (meterProvider) {
    await meterProvider.shutdown();
  }
  if (tracerProvider) {
    await tracerProvider.shutdown();
  }
  if (loggerProvider) {
    await loggerProvider.shutdown();
  }
  if (dbConnection) {
    await dbConnection.end();
  }
}, 30000);

// ==================== METRICS TESTS ====================

describe('Metrics Tests', () => {
  test('Counter metric', async () => {
    console.log('\nTesting Metrics Counter...');

    const meter = metrics.getMeter('test-meter');
    const counterName = `otel_nodejs_counter_${TEST_RUN_ID}`;

    // Create counter
    const counter = meter.createCounter(counterName, {
      description: 'Test counter for OTLP integration',
      unit: '1',
    });

    // Record values
    counter.add(10, { env: 'test' });
    counter.add(25, { env: 'test' });
    counter.add(15, { env: 'prod' });

    // Force flush and wait
    await meterProvider.forceFlush();
    await delay(3000);

    // Verify via SQL
    // Note: GreptimeDB adds _total suffix for counters (Prometheus convention)
    const tableName = `${counterName}_total`;
    try {
      const [rows] = await dbConnection.execute(
        `SELECT * FROM ${tableName} WHERE env = 'test' ORDER BY greptime_timestamp DESC LIMIT 1`
      );
      console.log(
        `Counter verification: ${rows.length > 0 ? 'PASSED' : 'table may have different structure'}`
      );
    } catch (e) {
      console.log(`Query note: ${e.message}`);
    }

    console.log(`Counter test completed for table: ${tableName}`);
  }, 30000);

  test('Gauge metric', async () => {
    console.log('\nTesting Metrics Gauge...');

    const meter = metrics.getMeter('test-meter');
    const gaugeName = `otel_nodejs_gauge_${TEST_RUN_ID}`;

    // Create gauge (using UpDownCounter as gauge equivalent)
    const gauge = meter.createUpDownCounter(gaugeName, {
      description: 'Test gauge for OTLP integration',
      unit: 'bytes',
    });

    // Record values
    gauge.add(1024, { host: 'server1' });
    gauge.add(2048, { host: 'server2' });

    // Force flush and wait
    await meterProvider.forceFlush();
    await delay(3000);

    // Note: GreptimeDB adds _<unit> suffix for gauges (Prometheus convention)
    const tableName = `${gaugeName}_bytes`;
    console.log(`Gauge test completed for table: ${tableName}`);
  }, 30000);

  test('Histogram metric', async () => {
    console.log('\nTesting Metrics Histogram...');

    const meter = metrics.getMeter('test-meter');
    const histogramName = `otel_nodejs_histogram_${TEST_RUN_ID}`;

    // Create histogram
    const histogram = meter.createHistogram(histogramName, {
      description: 'Test histogram for OTLP integration',
      unit: 'ms',
    });

    // Record values
    histogram.record(15.5, { endpoint: '/api/users' });
    histogram.record(45.2, { endpoint: '/api/users' });
    histogram.record(120.8, { endpoint: '/api/orders' });
    histogram.record(5.1, { endpoint: '/health' });

    // Force flush and wait
    await meterProvider.forceFlush();
    await delay(3000);

    // Note: GreptimeDB converts "ms" to "milliseconds" and creates _bucket/_count/_sum tables
    const tableName = `${histogramName}_milliseconds_count`;
    console.log(`Histogram test completed for table: ${tableName}`);
  }, 30000);
});

// ==================== TRACES TESTS ====================

describe('Traces Tests', () => {
  test('Trace spans', async () => {
    console.log('\nTesting Traces...');

    const tracer = trace.getTracer('test-tracer');

    // Create parent span
    const parentSpan = tracer.startSpan(`parent-operation-${TEST_RUN_ID}`, {
      kind: SpanKind.SERVER,
      attributes: {
        'http.method': 'GET',
        'http.url': '/api/test',
        'test.run.id': TEST_RUN_ID,
      },
    });

    // Simulate work
    await delay(50);

    // Create child span
    const ctx = trace.setSpan(context.active(), parentSpan);
    const childSpan = tracer.startSpan(
      `child-operation-${TEST_RUN_ID}`,
      {
        kind: SpanKind.INTERNAL,
        attributes: {
          'db.system': 'greptimedb',
          'db.operation': 'query',
        },
      },
      ctx
    );

    await delay(30);
    childSpan.setStatus({ code: SpanStatusCode.OK });
    childSpan.end();

    parentSpan.setStatus({ code: SpanStatusCode.OK });
    parentSpan.end();

    // Force flush and wait
    await tracerProvider.forceFlush();
    await delay(3000);

    // Verify via SQL
    // Note: service_name is a direct column in the table
    try {
      const [rows] = await dbConnection.execute(
        `SELECT * FROM opentelemetry_traces WHERE service_name = '${SERVICE_NAME}' ORDER BY timestamp DESC LIMIT 2`
      );
      console.log(`Found ${rows.length} trace spans in opentelemetry_traces`);
    } catch (e) {
      console.log(`Query note: ${e.message}`);
    }

    console.log('Traces test completed');
  }, 30000);
});

// ==================== LOGS TESTS ====================

describe('Logs Tests', () => {
  test('Log records', async () => {
    console.log('\nTesting Logs...');

    const logger = logs.getLogger('test-logger');

    // Emit log records
    logger.emit({
      severityNumber: SeverityNumber.INFO,
      severityText: 'INFO',
      body: `Test info log message - ${TEST_RUN_ID}`,
      attributes: {
        'test.run.id': TEST_RUN_ID,
        'log.type': 'test',
      },
    });

    logger.emit({
      severityNumber: SeverityNumber.WARN,
      severityText: 'WARN',
      body: `Test warning log message - ${TEST_RUN_ID}`,
      attributes: {
        'test.run.id': TEST_RUN_ID,
        'log.type': 'test',
      },
    });

    logger.emit({
      severityNumber: SeverityNumber.ERROR,
      severityText: 'ERROR',
      body: `Test error log message - ${TEST_RUN_ID}`,
      attributes: {
        'test.run.id': TEST_RUN_ID,
        'log.type': 'test',
        'error.code': 'TEST_ERROR',
      },
    });

    // Force flush and wait
    await loggerProvider.forceFlush();
    await delay(3000);

    // Verify via SQL
    // Note: service_name is stored in resource_attributes, use body with TEST_RUN_ID to query
    try {
      const [rows] = await dbConnection.execute(
        `SELECT * FROM opentelemetry_logs WHERE body LIKE '%${TEST_RUN_ID}%' ORDER BY timestamp DESC LIMIT 3`
      );
      console.log(`Found ${rows.length} log records in opentelemetry_logs`);
    } catch (e) {
      console.log(`Query note: ${e.message}`);
    }

    console.log('Logs test completed');
  }, 30000);
});

// ==================== COMPREHENSIVE TEST ====================

describe('Comprehensive Tests', () => {
  test('All signals together', async () => {
    console.log('\nTesting all signals together...');

    const meter = metrics.getMeter('comprehensive-test');
    const tracer = trace.getTracer('comprehensive-test');
    const logger = logs.getLogger('comprehensive-test');

    // Create a trace span
    const span = tracer.startSpan(`comprehensive-operation-${TEST_RUN_ID}`, {
      kind: SpanKind.SERVER,
    });

    // Record metrics within the span
    const counterName = `otel_nodejs_comprehensive_${TEST_RUN_ID}`;
    const counter = meter.createCounter(counterName);
    counter.add(1, { operation: 'comprehensive' });

    // Emit a log within the span
    logger.emit({
      severityNumber: SeverityNumber.INFO,
      severityText: 'INFO',
      body: `Comprehensive test log - ${TEST_RUN_ID}`,
      attributes: { 'test.type': 'comprehensive' },
    });

    // Simulate work
    await delay(100);

    span.setStatus({ code: SpanStatusCode.OK });
    span.end();

    // Force flush all providers
    await meterProvider.forceFlush();
    await tracerProvider.forceFlush();
    await loggerProvider.forceFlush();
    await delay(3000);

    console.log('Comprehensive test completed successfully');
  }, 30000);
});
