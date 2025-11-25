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
	"database/sql"
	"fmt"
	"net/url"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCrudOperations tests CRUD operations for both MySQL and PostgreSQL drivers.
// Validates comprehensive CRUD operations on a single table with all supported GreptimeDB data types.
func TestCrudOperations(t *testing.T) {
	drivers := []string{"mysql", "postgresql"}

	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) {
			t.Logf("Starting CRUD test for driver: %s", driver)

			db := connectDriver(t, driver, "")
			defer db.Close()

			tableName := fmt.Sprintf("test_all_types_%s", driver)

			// Drop table if exists
			t.Logf("[%s] Dropping table if exists: %s", driver, tableName)
			dropTableDriver(t, db, tableName)

			// Create table
			t.Logf("[%s] Creating table: %s", driver, tableName)
			createTableSQL := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
				ts TIMESTAMP TIME INDEX,
				row_id STRING PRIMARY KEY,
				int_col INTEGER,
				double_col DOUBLE,
				float_col FLOAT,
				string_col STRING,
				date_col DATE,
				timestamp_col TIMESTAMP,
				bool_col BOOLEAN,
				binary_col BINARY
			)`, tableName)
			_, err := db.Exec(createTableSQL)
			require.NoError(t, err)

			// INSERT with literal
			t.Logf("[%s] Inserting row with SQL literal", driver)
			literalInsert := fmt.Sprintf(`INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col)
				VALUES ('2024-11-24 10:00:00', 'row1', 42, 3.14159, 2.718, 'Hello GreptimeDB! 你好🚀', '2024-01-15', '2024-11-24 10:00:00', true, X'0102030405FFFE')`, tableName)
			_, err = db.Exec(literalInsert)
			require.NoError(t, err)

			// INSERT with prepared statement
			t.Logf("[%s] Inserting row with PreparedStatement", driver)
			preparedInsert := fmt.Sprintf(`INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, tableName)

			ts := time.Date(2024, 11, 24, 11, 0, 0, 0, time.UTC)
			dateVal := time.Date(2024, 6, 20, 0, 0, 0, 0, time.UTC)
			binary := []byte{0x0A, 0x0B, 0x0C, 0xAB, 0xCD}

			if driver == "postgresql" {
				// PostgreSQL uses $1, $2, ... placeholders
				preparedInsert = fmt.Sprintf(`INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, tableName)
			}

			_, err = db.Exec(preparedInsert, ts, "row2", 999, 123.456, float32(78.9), "PreparedStatement test 测试", dateVal, ts, false, binary)
			require.NoError(t, err)

			// SELECT and verify
			t.Logf("[%s] Selecting and verifying inserted rows", driver)
			rows, err := db.Query(fmt.Sprintf("SELECT row_id, ts, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col FROM %s ORDER BY row_id", tableName))
			require.NoError(t, err)
			defer rows.Close()

			// Verify first row
			require.True(t, rows.Next())
			var rowID string
			var tsVal time.Time
			var intCol int
			var doubleCol float64
			var floatCol float32
			var stringCol string
			var dateCol time.Time
			var timestampCol time.Time
			var boolCol bool
			var binaryCol []byte

			err = rows.Scan(&rowID, &tsVal, &intCol, &doubleCol, &floatCol, &stringCol, &dateCol, &timestampCol, &boolCol, &binaryCol)
			require.NoError(t, err)
			assert.Equal(t, "row1", rowID)
			assert.Equal(t, 42, intCol)
			assert.InDelta(t, 3.14159, doubleCol, 0.00001)
			assert.InDelta(t, 2.718, floatCol, 0.001)
			assert.Equal(t, "Hello GreptimeDB! 你好🚀", stringCol)
			assert.Equal(t, "2024-01-15", dateCol.Format("2006-01-02"))
			assert.True(t, boolCol)
			assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0xFF, 0xFE}, binaryCol)

			// Verify second row
			require.True(t, rows.Next())
			err = rows.Scan(&rowID, &tsVal, &intCol, &doubleCol, &floatCol, &stringCol, &dateCol, &timestampCol, &boolCol, &binaryCol)
			require.NoError(t, err)
			assert.Equal(t, "row2", rowID)
			assert.Equal(t, 999, intCol)
			assert.InDelta(t, 123.456, doubleCol, 0.001)
			assert.InDelta(t, 78.9, floatCol, 0.01)
			assert.Equal(t, "PreparedStatement test 测试", stringCol)
			assert.Equal(t, "2024-06-20", dateCol.Format("2006-01-02"))
			assert.False(t, boolCol)
			assert.Equal(t, binary, binaryCol)

			assert.False(t, rows.Next())
			rows.Close()

			// UPDATE by inserting with SAME primary key + SAME time index
			t.Logf("[%s] Updating row by inserting with same primary key + time index", driver)
			sameTs := time.Date(2024, 11, 24, 10, 0, 0, 0, time.UTC)
			updateSQL := fmt.Sprintf(`INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, tableName)
			if driver == "postgresql" {
				updateSQL = fmt.Sprintf(`INSERT INTO %s (ts, row_id, int_col, double_col, float_col, string_col, date_col, timestamp_col, bool_col, binary_col)
					VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`, tableName)
			}
			_, err = db.Exec(updateSQL, sameTs, "row1", 100, 9.999, float32(1.234), "Updated value", time.Date(2024, 12, 25, 0, 0, 0, 0, time.UTC), sameTs, false, []byte{0x11, 0x22})
			require.NoError(t, err)

			// Verify the row was overwritten (not added)
			t.Logf("[%s] Verifying row was overwritten (not added)", driver)
			rows, err = db.Query(fmt.Sprintf("SELECT row_id, int_col, double_col, string_col FROM %s ORDER BY row_id, ts", tableName))
			require.NoError(t, err)
			defer rows.Close()

			// Should only have 2 rows total (row1 was overwritten, not added)
			require.True(t, rows.Next())
			err = rows.Scan(&rowID, &intCol, &doubleCol, &stringCol)
			require.NoError(t, err)
			assert.Equal(t, "row1", rowID)
			assert.Equal(t, 100, intCol, "row1 should be updated")
			assert.InDelta(t, 9.999, doubleCol, 0.001)
			assert.Equal(t, "Updated value", stringCol)

			require.True(t, rows.Next())
			err = rows.Scan(&rowID, &intCol, &doubleCol, &stringCol)
			require.NoError(t, err)
			assert.Equal(t, "row2", rowID)
			assert.Equal(t, 999, intCol)

			assert.False(t, rows.Next(), "Should have exactly 2 rows (row1 overwritten, not added)")
			rows.Close()

			// SELECT with WHERE
			t.Logf("[%s] Testing SELECT with WHERE clause", driver)
			whereSQL := fmt.Sprintf("SELECT row_id, int_col FROM %s WHERE int_col > ? ORDER BY row_id", tableName)
			if driver == "postgresql" {
				whereSQL = fmt.Sprintf("SELECT row_id, int_col FROM %s WHERE int_col > $1 ORDER BY row_id", tableName)
			}
			rows, err = db.Query(whereSQL, 50)
			require.NoError(t, err)
			defer rows.Close()

			count := 0
			for rows.Next() {
				count++
			}
			assert.Greater(t, count, 0)
			rows.Close()

			// DELETE (DROP TABLE)
			t.Logf("[%s] Dropping table", driver)
			dropTableDriver(t, db, tableName)

			// Verify table was dropped using SHOW TABLES
			rows, err = db.Query(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableName))
			require.NoError(t, err)
			assert.False(t, rows.Next(), "Table should be dropped")
			rows.Close()

			t.Logf("[%s] CRUD test completed successfully", driver)
		})
	}
}

// connectDriver connects to GreptimeDB using the specified driver and timezone
func connectDriver(t *testing.T, driver, timezone string) *sql.DB {
	username := getEnv("GREPTIME_USERNAME", "")
	password := getEnv("GREPTIME_PASSWORD", "")

	var dsn string
	if driver == "mysql" {
		host := getEnv("MYSQL_HOST", "127.0.0.1")
		port := getEnv("MYSQL_PORT", "4002")
		database := getEnv("DB_NAME", "public")

		dsn = fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", username, password, host, port, database)
		if timezone != "" {
			dsn += fmt.Sprintf("&loc=%s", url.QueryEscape(timezone))
		}
		db, err := sql.Open("mysql", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		// Set server-side timezone (loc only affects client parsing)
		if timezone != "" {
			_, err = db.Exec(fmt.Sprintf("SET time_zone = '%s'", timezone))
			require.NoError(t, err)
		}

		t.Logf("MySQL connection established: %s (timezone=%s)", dsn, timezone)
		return db
	} else if driver == "postgresql" {
		host := getEnv("POSTGRES_HOST", "127.0.0.1")
		port := getEnv("POSTGRES_PORT", "4003")
		database := getEnv("DB_NAME", "public")

		dsn = fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", host, port, database, username, password)
		db, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		require.NoError(t, db.Ping())

		if timezone != "" {
			_, err = db.Exec(fmt.Sprintf("SET TIME ZONE = '%s'", timezone))
			require.NoError(t, err)
		}

		t.Logf("PostgreSQL connection established: %s", dsn)
		return db
	}

	t.Fatalf("Unknown driver: %s", driver)
	return nil
}

// dropTableDriver drops a table
func dropTableDriver(t *testing.T, db *sql.DB, tableName string) {
	_, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
	require.NoError(t, err)
}

// TestTimezoneInsertAndSelect tests timezone behavior for INSERT and SELECT operations.
// Validates that timestamp strings are interpreted using client timezone (MySQL only).
func TestTimezoneInsertAndSelect(t *testing.T) {
	t.Log("Starting timezone test for MySQL driver")
	tableName := "test_timezone_mysql"

	// Part 1: INSERT from different timezones
	t.Log("Part 1: Inserting from UTC")
	db := connectDriver(t, "mysql", "UTC")
	dropTableDriver(t, db, tableName)

	createSQL := fmt.Sprintf(`CREATE TABLE %s (
		ts TIMESTAMP TIME INDEX,
		row_id STRING PRIMARY KEY,
		timezone_used STRING
	)`, tableName)
	_, err := db.Exec(createSQL)
	require.NoError(t, err)

	// Insert from UTC
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'utc_row', 'UTC')", tableName))
	require.NoError(t, err)
	db.Close()

	// Insert from Asia/Shanghai (UTC+8)
	t.Log("Part 2: Inserting from Asia/Shanghai")
	db = connectDriver(t, "mysql", "Asia/Shanghai")
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'shanghai_row', 'Asia/Shanghai')", tableName))
	require.NoError(t, err)
	db.Close()

	// Insert from America/New_York (UTC-5)
	t.Log("Part 3: Inserting from America/New_York")
	db = connectDriver(t, "mysql", "America/New_York")
	_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (ts, row_id, timezone_used) VALUES ('2024-01-01 12:00:00', 'newyork_row', 'America/New_York')", tableName))
	require.NoError(t, err)
	db.Close()

	// Part 2: Verify timestamps stored in UTC (connection timezone affects INSERT interpretation)
	t.Log("Part 4: Verifying timestamps from UTC connection")
	db = connectDriver(t, "mysql", "UTC")
	defer db.Close()

	rows, err := db.Query(fmt.Sprintf("SELECT row_id, ts, timezone_used FROM %s ORDER BY row_id", tableName))
	require.NoError(t, err)
	defer rows.Close()

	// New York: 2024-01-01 12:00:00 (local) -> 2024-01-01 17:00:00 (UTC)
	require.True(t, rows.Next())
	var rowID, timezoneUsed string
	var ts time.Time
	err = rows.Scan(&rowID, &ts, &timezoneUsed)
	require.NoError(t, err)
	assert.Equal(t, "newyork_row", rowID)
	assert.Equal(t, "America/New_York", timezoneUsed)
	assert.Equal(t, 17, ts.Hour(), "Expected UTC 17:00:00")

	// Shanghai: 2024-01-01 12:00:00 (local) -> 2024-01-01 04:00:00 (UTC)
	require.True(t, rows.Next())
	err = rows.Scan(&rowID, &ts, &timezoneUsed)
	require.NoError(t, err)
	assert.Equal(t, "shanghai_row", rowID)
	assert.Equal(t, "Asia/Shanghai", timezoneUsed)
	assert.Equal(t, 4, ts.Hour(), "Expected UTC 04:00:00")

	// UTC: 2024-01-01 12:00:00 (local) -> 2024-01-01 12:00:00 (UTC)
	require.True(t, rows.Next())
	err = rows.Scan(&rowID, &ts, &timezoneUsed)
	require.NoError(t, err)
	assert.Equal(t, "utc_row", rowID)
	assert.Equal(t, "UTC", timezoneUsed)
	assert.Equal(t, 12, ts.Hour(), "Expected UTC 12:00:00")

	assert.False(t, rows.Next())
	rows.Close()

	// Part 3: Verify timestamps from different connection timezones
	t.Log("Part 5: Verifying timestamps are consistent across different connection timezones")
	db.Close()

	db = connectDriver(t, "mysql", "Asia/Shanghai")
	row := db.QueryRow(fmt.Sprintf("SELECT ts FROM %s WHERE row_id = 'utc_row'", tableName))
	err = row.Scan(&ts)
	require.NoError(t, err)
	// When viewed from Shanghai timezone, UTC 12:00 should appear as 20:00 local
	assert.Equal(t, 20, ts.Hour(), "Expected Shanghai time 20:00:00")
	db.Close()

	db = connectDriver(t, "mysql", "America/New_York")
	row = db.QueryRow(fmt.Sprintf("SELECT ts FROM %s WHERE row_id = 'utc_row'", tableName))
	err = row.Scan(&ts)
	require.NoError(t, err)
	// When viewed from New York timezone, UTC 12:00 should appear as 07:00 local
	assert.Equal(t, 7, ts.Hour(), "Expected New York time 07:00:00")
	db.Close()

	// Part 4: Verify WHERE clause interprets timestamp literals using connection timezone
	t.Log("Part 6: Verifying WHERE clause uses connection timezone")
	db = connectDriver(t, "mysql", "Asia/Shanghai")
	row = db.QueryRow(fmt.Sprintf("SELECT row_id FROM %s WHERE ts = '2024-01-01 12:00:00'", tableName))
	err = row.Scan(&rowID)
	require.NoError(t, err)
	assert.Equal(t, "shanghai_row", rowID)
	db.Close()

	db = connectDriver(t, "mysql", "UTC")
	row = db.QueryRow(fmt.Sprintf("SELECT row_id FROM %s WHERE ts = '2024-01-01 12:00:00'", tableName))
	err = row.Scan(&rowID)
	require.NoError(t, err)
	assert.Equal(t, "utc_row", rowID)
	db.Close()

	// Cleanup
	db = connectDriver(t, "mysql", "UTC")
	dropTableDriver(t, db, tableName)
	db.Close()

	t.Log("Timezone test completed successfully")
}

// TestBatchInsert tests batch insert using prepared statements.
// Validates that batch operations work correctly for both MySQL and PostgreSQL drivers.
func TestBatchInsert(t *testing.T) {
	drivers := []string{"mysql", "postgresql"}

	for _, driver := range drivers {
		t.Run(driver, func(t *testing.T) {
			t.Logf("Starting batch insert test for driver: %s", driver)

			db := connectDriver(t, driver, "")
			defer db.Close()

			tableName := fmt.Sprintf("test_batch_insert_%s", driver)

			// Drop table if exists
			t.Logf("[%s] Dropping table if exists: %s", driver, tableName)
			dropTableDriver(t, db, tableName)

			// Create table
			t.Logf("[%s] Creating table: %s", driver, tableName)
			createSQL := fmt.Sprintf(`CREATE TABLE %s (
				ts TIMESTAMP TIME INDEX,
				row_id STRING PRIMARY KEY,
				int_col INTEGER,
				double_col DOUBLE,
				string_col STRING,
				bool_col BOOLEAN
			)`, tableName)
			_, err := db.Exec(createSQL)
			require.NoError(t, err)

			// Prepare batch insert with 5 rows
			batchSize := 5
			t.Logf("[%s] Preparing batch insert with %d rows", driver, batchSize)

			insertSQL := fmt.Sprintf("INSERT INTO %s (ts, row_id, int_col, double_col, string_col, bool_col) VALUES (?, ?, ?, ?, ?, ?)", tableName)
			if driver == "postgresql" {
				insertSQL = fmt.Sprintf("INSERT INTO %s (ts, row_id, int_col, double_col, string_col, bool_col) VALUES ($1, $2, $3, $4, $5, $6)", tableName)
			}

			stmt, err := db.Prepare(insertSQL)
			require.NoError(t, err)
			defer stmt.Close()

			// Insert 5 rows
			for i := 1; i <= batchSize; i++ {
				ts := time.Date(2024, 11, 24, 10+i, 0, 0, 0, time.UTC)
				_, err = stmt.Exec(ts, fmt.Sprintf("batch_row_%d", i), i*100, float64(i)*1.5, fmt.Sprintf("Batch test row %d 测试", i), i%2 == 0)
				require.NoError(t, err)
				t.Logf("[%s] Inserted row %d: batch_row_%d", driver, i, i)
			}

			// Verify all rows were inserted correctly
			t.Logf("[%s] Verifying inserted rows", driver)
			rows, err := db.Query(fmt.Sprintf("SELECT row_id, int_col, double_col, string_col, bool_col FROM %s ORDER BY row_id", tableName))
			require.NoError(t, err)
			defer rows.Close()

			rowCount := 0
			for rows.Next() {
				rowCount++
				var rowID, stringCol string
				var intCol int
				var doubleCol float64
				var boolCol bool

				err = rows.Scan(&rowID, &intCol, &doubleCol, &stringCol, &boolCol)
				require.NoError(t, err)

				t.Logf("[%s] Row %d: %s | int=%d, double=%f, string=%s, bool=%v",
					driver, rowCount, rowID, intCol, doubleCol, stringCol, boolCol)

				// Verify data matches what we inserted
				assert.Equal(t, fmt.Sprintf("batch_row_%d", rowCount), rowID)
				assert.Equal(t, rowCount*100, intCol)
				assert.InDelta(t, float64(rowCount)*1.5, doubleCol, 0.01)
				assert.Equal(t, fmt.Sprintf("Batch test row %d 测试", rowCount), stringCol)
				assert.Equal(t, rowCount%2 == 0, boolCol)
			}
			rows.Close()

			assert.Equal(t, batchSize, rowCount, fmt.Sprintf("Should have inserted exactly %d rows", batchSize))
			t.Logf("[%s] ✓ Verified all %d rows were inserted correctly", driver, batchSize)

			// Cleanup
			t.Logf("[%s] Dropping table", driver)
			dropTableDriver(t, db, tableName)

			t.Logf("[%s] Batch insert test completed successfully", driver)
		})
	}
}
