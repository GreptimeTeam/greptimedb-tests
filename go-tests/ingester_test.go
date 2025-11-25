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
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	greptime "github.com/GreptimeTeam/greptimedb-ingester-go"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table"
	"github.com/GreptimeTeam/greptimedb-ingester-go/table/types"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to get environment variable with default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// Setup GreptimeDB client
func setupGreptimeClient(t *testing.T) *greptime.Client {
	host := getEnv("GRPC_HOST", "127.0.0.1")
	portStr := getEnv("GRPC_PORT", "4001")
	database := getEnv("DB_NAME", "public")
	username := getEnv("GREPTIME_USERNAME", "")
	password := getEnv("GREPTIME_PASSWORD", "")

	port, err := strconv.Atoi(portStr)
	require.NoError(t, err, "Invalid GRPC_PORT")

	cfg := greptime.NewConfig(host).
		WithPort(port).
		WithDatabase(database)

	if username != "" && password != "" {
		cfg = cfg.WithAuth(username, password)
	}

	client, err := greptime.NewClient(cfg)
	require.NoError(t, err, "Failed to create GreptimeDB client")

	t.Logf("GreptimeDB client initialized: database=%s, endpoint=%s:%d", database, host, port)
	return client
}

// Setup MySQL connection for verification
func setupMySQLConn(t *testing.T) *sql.DB {
	host := getEnv("MYSQL_HOST", "127.0.0.1")
	port := getEnv("MYSQL_PORT", "4002")
	database := getEnv("DB_NAME", "public")
	username := getEnv("GREPTIME_USERNAME", "")
	password := getEnv("GREPTIME_PASSWORD", "")

	var dsn string
	if username != "" && password != "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", username, password, host, port, database)
	} else {
		dsn = fmt.Sprintf("tcp(%s:%s)/%s?parseTime=true", host, port, database)
	}

	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err, "Failed to connect to MySQL")

	err = db.Ping()
	require.NoError(t, err, "Failed to ping MySQL")

	t.Logf("MySQL connection established: %s", dsn)
	return db
}

// Drop table helper
func dropTable(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	require.NoError(t, err, "Failed to drop table")
}

// TestIngesterInsertAndUpdate tests insert and update operations using GreptimeDB Ingester SDK.
// Verifies the data using MySQL driver queries.
func TestIngesterInsertAndUpdate(t *testing.T) {
	t.Log("Starting ingester insert/update test")

	client := setupGreptimeClient(t)
	defer client.Close()

	db := setupMySQLConn(t)
	defer db.Close()

	tableName := "test_ingester_go"
	ctx := context.Background()

	// Clean up
	dropTable(t, db, tableName)

	t.Log("Creating table and inserting first row using Ingester SDK")

	// Create table with various data types
	tbl, err := table.New(tableName)
	require.NoError(t, err)

	// Define schema
	err = tbl.AddTagColumn("row_id", types.STRING)
	require.NoError(t, err)
	err = tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("int_col", types.INT32)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("double_col", types.FLOAT64)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("float_col", types.FLOAT32)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("string_col", types.STRING)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("bool_col", types.BOOLEAN)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("binary_col", types.BINARY)
	require.NoError(t, err)

	// Insert first row
	binaryData1 := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xFF, 0xFE}
	ts1 := time.UnixMilli(1732435200000) // 2024-11-24 10:00:00
	err = tbl.AddRow(
		"row1",                  // row_id (tag)
		ts1,                     // ts
		int32(42),               // int_col
		float64(3.14159),        // double_col
		float32(2.718),          // float_col
		"Hello GreptimeDB! 你好🚀", // string_col
		true,                    // bool_col
		binaryData1,             // binary_col
	)
	require.NoError(t, err)

	// Insert second row
	binaryData2 := []byte{0x0A, 0x0B, 0x0C, 0xAB, 0xCD}
	ts2 := time.UnixMilli(1732438800000) // 2024-11-24 11:00:00
	err = tbl.AddRow(
		"row2",             // row_id (tag)
		ts2,                // ts
		int32(999),         // int_col
		float64(123.456),   // double_col
		float32(78.9),      // float_col
		"Ingester test 测试", // string_col
		false,              // bool_col
		binaryData2,        // binary_col
	)
	require.NoError(t, err)

	// Write to GreptimeDB
	resp, err := client.Write(ctx, tbl)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("Inserted 2 rows successfully, affected rows: %d", resp.GetAffectedRows().GetValue())

	// Verify inserted data using MySQL
	t.Log("Verifying inserted rows using MySQL")
	rows, err := db.Query(fmt.Sprintf("SELECT row_id, ts, int_col, double_col, float_col, string_col, bool_col, binary_col FROM %s ORDER BY row_id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	// Verify first row
	require.True(t, rows.Next(), "Expected first row")
	var rowID string
	var ts time.Time
	var intCol int32
	var doubleCol float64
	var floatCol float32
	var stringCol string
	var boolCol bool
	var binaryCol []byte

	err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &floatCol, &stringCol, &boolCol, &binaryCol)
	require.NoError(t, err)
	assert.Equal(t, "row1", rowID)
	assert.Equal(t, int32(42), intCol)
	assert.InDelta(t, 3.14159, doubleCol, 0.00001)
	assert.InDelta(t, 2.718, floatCol, 0.001)
	assert.Equal(t, "Hello GreptimeDB! 你好🚀", stringCol)
	assert.True(t, boolCol)
	assert.Equal(t, binaryData1, binaryCol)

	// Verify second row
	require.True(t, rows.Next(), "Expected second row")
	err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &floatCol, &stringCol, &boolCol, &binaryCol)
	require.NoError(t, err)
	assert.Equal(t, "row2", rowID)
	assert.Equal(t, int32(999), intCol)
	assert.InDelta(t, 123.456, doubleCol, 0.001)
	assert.InDelta(t, 78.9, floatCol, 0.01)
	assert.Equal(t, "Ingester test 测试", stringCol)
	assert.False(t, boolCol)
	assert.Equal(t, binaryData2, binaryCol)

	assert.False(t, rows.Next(), "Should have exactly 2 rows")
	rows.Close()
	t.Log("✓ Verified 2 rows inserted correctly")

	// UPDATE by inserting with SAME tag (row_id) and SAME timestamp
	t.Log("Updating row by inserting with same tag (row_id) and timestamp using Ingester SDK")
	tbl2, err := table.New(tableName)
	require.NoError(t, err)

	// Define schema (same as before)
	err = tbl2.AddTagColumn("row_id", types.STRING)
	require.NoError(t, err)
	err = tbl2.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("int_col", types.INT32)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("double_col", types.FLOAT64)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("float_col", types.FLOAT32)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("string_col", types.STRING)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("bool_col", types.BOOLEAN)
	require.NoError(t, err)
	err = tbl2.AddFieldColumn("binary_col", types.BINARY)
	require.NoError(t, err)

	// Insert row with same tag and timestamp to update
	binaryData3 := []byte{0x11, 0x22}
	err = tbl2.AddRow(
		"row1",          // Same row_id (tag)
		ts1,             // Same timestamp
		int32(100),      // Updated int_col
		float64(9.999),  // Updated double_col
		float32(1.234),  // Updated float_col
		"Updated value", // Updated string_col
		false,           // Updated bool_col
		binaryData3,     // Updated binary_col
	)
	require.NoError(t, err)

	resp, err = client.Write(ctx, tbl2)
	require.NoError(t, err)
	t.Logf("Row updated (overwritten) successfully, affected rows: %d", resp.GetAffectedRows().GetValue())

	// Verify the row was overwritten (not added)
	t.Log("Verifying row was overwritten (not added) using MySQL")
	rows, err = db.Query(fmt.Sprintf("SELECT row_id, ts, int_col, double_col, float_col, string_col, bool_col, binary_col FROM %s ORDER BY row_id, ts", tableName))
	require.NoError(t, err)
	defer rows.Close()

	// Should only have 2 rows total (row1 was overwritten, not added)
	require.True(t, rows.Next(), "Expected first row")
	err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &floatCol, &stringCol, &boolCol, &binaryCol)
	require.NoError(t, err)
	assert.Equal(t, "row1", rowID)
	assert.Equal(t, int32(100), intCol, "row1 should be updated")
	assert.InDelta(t, 9.999, doubleCol, 0.001)
	assert.Equal(t, "Updated value", stringCol)
	assert.False(t, boolCol)
	assert.Equal(t, binaryData3, binaryCol)

	require.True(t, rows.Next(), "Expected second row")
	err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &floatCol, &stringCol, &boolCol, &binaryCol)
	require.NoError(t, err)
	assert.Equal(t, "row2", rowID)
	assert.Equal(t, int32(999), intCol)

	assert.False(t, rows.Next(), "Should have exactly 2 rows (row1 overwritten, not added)")
	rows.Close()
	t.Log("✓ Verified row was overwritten correctly")

	// Query with WHERE clause
	t.Log("Testing MySQL SELECT with WHERE clause")
	rows, err = db.Query(fmt.Sprintf("SELECT row_id, ts, int_col, double_col, float_col, string_col, bool_col, binary_col FROM %s WHERE int_col > ? ORDER BY row_id", tableName), 50)
	require.NoError(t, err)
	defer rows.Close()

	count := 0
	for rows.Next() {
		count++
		err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &floatCol, &stringCol, &boolCol, &binaryCol)
		require.NoError(t, err)
		t.Logf("Found row: %s, int_col=%d", rowID, intCol)
	}
	assert.Equal(t, 2, count, "Should find 2 rows with int_col > 50")
	rows.Close()
	t.Log("✓ WHERE clause query verified")

	// Cleanup
	t.Log("Dropping table")
	dropTable(t, db, tableName)

	// Verify table was dropped
	var tableExists int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", tableName)).Scan(&tableExists)
	require.NoError(t, err)
	assert.Equal(t, 0, tableExists, "Table should be dropped")

	t.Log("Ingester insert/update test completed successfully")
}

// TestIngesterBatchWrite tests batch write operations using GreptimeDB Ingester SDK.
// Inserts multiple rows in a single table and verifies using MySQL queries.
func TestIngesterBatchWrite(t *testing.T) {
	t.Log("Starting ingester batch write test")

	client := setupGreptimeClient(t)
	defer client.Close()

	db := setupMySQLConn(t)
	defer db.Close()

	tableName := "test_batch_ingester_go"
	ctx := context.Background()

	// Clean up
	dropTable(t, db, tableName)

	t.Log("Creating table schema for batch write")

	// Create table with schema
	tbl, err := table.New(tableName)
	require.NoError(t, err)

	// Define schema
	err = tbl.AddTagColumn("row_id", types.STRING)
	require.NoError(t, err)
	err = tbl.AddTimestampColumn("ts", types.TIMESTAMP_MILLISECOND)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("int_col", types.INT32)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("double_col", types.FLOAT64)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("string_col", types.STRING)
	require.NoError(t, err)
	err = tbl.AddFieldColumn("bool_col", types.BOOLEAN)
	require.NoError(t, err)

	// Insert multiple rows in batch
	batchSize := 5
	t.Logf("Inserting %d rows in batch using Ingester SDK", batchSize)

	baseTime := int64(1732435200000) // 2024-11-24 10:00:00
	for i := 1; i <= batchSize; i++ {
		timestamp := time.UnixMilli(baseTime + int64(i*3600000)) // Base time + i hours
		err = tbl.AddRow(
			fmt.Sprintf("batch_row_%d", i), // row_id (tag)
			timestamp,                      // ts
			int32(i*100),                   // int_col
			float64(i)*1.5,                 // double_col
			fmt.Sprintf("Batch test row %d 测试", i), // string_col
			i%2 == 0, // bool_col
		)
		require.NoError(t, err)
		t.Logf("Added row %d to batch: batch_row_%d, ts=%v", i, i, timestamp)
	}

	// Write batch to GreptimeDB
	t.Log("Writing batch to GreptimeDB")
	resp, err := client.Write(ctx, tbl)
	require.NoError(t, err)
	require.NotNil(t, resp)
	t.Logf("Batch write completed successfully, affected rows: %d", resp.GetAffectedRows().GetValue())

	// Verify all rows were inserted correctly using MySQL
	t.Log("Verifying inserted rows using MySQL")
	rows, err := db.Query(fmt.Sprintf("SELECT row_id, ts, int_col, double_col, string_col, bool_col FROM %s ORDER BY row_id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	rowCount := 0
	for rows.Next() {
		rowCount++
		var rowID string
		var ts time.Time
		var intCol int32
		var doubleCol float64
		var stringCol string
		var boolCol bool

		err = rows.Scan(&rowID, &ts, &intCol, &doubleCol, &stringCol, &boolCol)
		require.NoError(t, err)

		t.Logf("Row %d: %s | int=%d, double=%f, string=%s, bool=%v",
			rowCount, rowID, intCol, doubleCol, stringCol, boolCol)

		// Verify data matches what we inserted
		assert.Equal(t, fmt.Sprintf("batch_row_%d", rowCount), rowID)
		assert.Equal(t, int32(rowCount*100), intCol)
		assert.InDelta(t, float64(rowCount)*1.5, doubleCol, 0.01)
		assert.Equal(t, fmt.Sprintf("Batch test row %d 测试", rowCount), stringCol)
		assert.Equal(t, rowCount%2 == 0, boolCol)
	}
	rows.Close()

	assert.Equal(t, batchSize, rowCount, fmt.Sprintf("Should have inserted exactly %d rows", batchSize))
	t.Logf("✓ Verified all %d rows were inserted correctly", batchSize)

	// Cleanup
	t.Log("Dropping table")
	dropTable(t, db, tableName)

	// Verify table was dropped
	var tableExists int
	err = db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '%s'", tableName)).Scan(&tableExists)
	require.NoError(t, err)
	assert.Equal(t, 0, tableExists, "Table should be dropped")

	t.Log("Ingester batch write test completed successfully")
}
